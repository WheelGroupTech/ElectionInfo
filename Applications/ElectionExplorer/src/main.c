/**
 * @file main.c
 * @brief ElectionExplorer Win32 GUI: menu, DPI-aware layout, voter grid.
 */

#include "main.h"
#include "resource.h"
#include "voter_table.h"
#include "filter.h"
#include "xlsx.h"
#include "ee_cvr.h"
#include "ee_results.h"
#include "ee_audit.h"
#include "voter_roster.h"
#include "settings.h"

#include <commctrl.h>
#include <commdlg.h>
#include <intsafe.h>
#include <shellapi.h>
#include <strsafe.h>
#include <stdlib.h>
#include <uxtheme.h>
#include <windowsx.h>
#include <winver.h>

#pragma comment(lib, "comctl32.lib")
#pragma comment(lib, "comdlg32.lib")
#pragma comment(lib, "uxtheme.lib")
#pragma comment(lib, "shell32.lib")
#pragma comment(lib, "version.lib")

/* -------------------------------------------------------------------------- */
/* Constants                                                                  */
/* -------------------------------------------------------------------------- */

static const wchar_t k_WindowClassName[] = L"ElectionExplorerMainWindow";
static const wchar_t k_WindowTitle[] = L"Election Explorer";
static const wchar_t k_ProgressClassName[] = L"ElectionExplorerLoadProgress";
static const wchar_t k_OptionsClassName[] = L"ElectionExplorerOptions";
static const wchar_t k_FilterClassName[] = L"ElectionExplorerFilter";
static const wchar_t k_ReportClassName[] = L"ElectionExplorerReport";
static const wchar_t k_CompareClassName[] = L"ElectionExplorerCompare";
static const wchar_t k_DiffClassName[] = L"ElectionExplorerDiff";
static const wchar_t k_CvrClassName[] = L"ElectionExplorerCvr";
static const wchar_t k_CvrReportClassName[] = L"ElectionExplorerCvrReport";
static const wchar_t k_CvrValueReportClassName[] = L"ElectionExplorerCvrValueReport";
static const wchar_t k_CvrRcvClassName[] = L"ElectionExplorerCvrRcv";
static const wchar_t k_RosterTotalsClassName[] = L"ElectionExplorerRosterTotals";
static const wchar_t k_CvrFilterClassName[] = L"ElectionExplorerCvrFilter";
static const wchar_t k_ResultsClassName[] = L"ElectionExplorerResults";
static const wchar_t k_AuditClassName[] = L"ElectionExplorerAudit";
static const wchar_t k_ResTurnoutClassName[] = L"ElectionExplorerResultsTurnout";
static const wchar_t k_ResCompareClassName[] = L"ElectionExplorerResultsCompare";

static const int k_DefaultWidth = 1100;
static const int k_DefaultHeight = 720;
static const int k_DefaultFrozenWidth = 640;
static const uint32_t k_NameUpdateProgressMinRows = 25000;
/* Below this row count a duplicate scan runs synchronously (sub-100 ms); at or
 * above it we run on a worker thread behind the cancelable progress modal. */
static const uint32_t k_ScanModalMinRows = 250000;

/* Which scan / compare produced the current row-mark layer. */
enum
{
    EE_SCAN_NONE = 0,
    EE_SCAN_DUP_VOTER_IDS = 1,
    EE_SCAN_DUP_NAME_DOB = 2,
    EE_SCAN_CMP_ONLY = 3,     /* rows only in this file (compare) */
    EE_SCAN_CMP_CHANGED = 4,  /* matched rows whose fields differ */
    EE_SCAN_CMP_IDENTICAL = 5, /* matched rows identical in both */
    EE_SCAN_DUP_VOTING = 6     /* name + DOB duplicates who voted (a roster) */
};

/* Report kinds (Precinct / Address summary windows). */
enum
{
    EE_REPORT_PRECINCT = 1,
    EE_REPORT_ADDRESS = 2
};

typedef struct ReportWindow ReportWindow;
typedef struct RosterTotalsWindow RosterTotalsWindow;

static const int k_ZoomMin = 50;
static const int k_ZoomMax = 250;
static const int k_ZoomDefault = 100;

typedef enum EeMapEngine
{
    EeMap_Google = 0,
    EeMap_Bing,
    EeMap_Apple,
    EeMap_OpenStreetMap
} EeMapEngine;

enum
{
    IDC_LIST_FROZEN = 1001,
    IDC_LIST_SCROLL = 1002,
    IDC_STATUS_BAR = 1003,
    IDC_PANE_TITLE_FROZEN = 1004,
    IDC_PANE_TITLE_SCROLL = 1005
};

/* -------------------------------------------------------------------------- */
/* App state                                                                  */
/* -------------------------------------------------------------------------- */

typedef struct AppState
{
    HINSTANCE instance;
    HWND hwnd_main;
    HWND hwnd_frozen;
    HWND hwnd_scroll;
    HWND hwnd_frozen_title;
    HWND hwnd_scroll_title;
    HWND hwnd_frozen_hsb_pad; /* Fills H-scroll gap so row bottoms align */
    HWND hwnd_scroll_hsb_pad;
    HWND hwnd_status;
    HWND hwnd_progress;
    HWND hwnd_progress_bar;
    HWND hwnd_progress_status;
    HWND hwnd_options;
    HWND hwnd_filter;
    BOOL copy_prepend_normalized;
    BOOL name_surname_first;
    int zoom_percent;  /* 50..250; 100 is actual size */
    EeMapEngine map_engine;
    HFONT font_ui;     /* Dialogs / status (DPI only) */
    HFONT font_grid;   /* List cells (DPI × zoom) */
    HFONT font_header; /* Bold captions (DPI × zoom) */
    HBRUSH brush_header;
    HPEN pen_header_line;
    UINT dpi;
    int pad;
    int frozen_width;   /* Left pane width (client pixels), user-resizable */
    int splitter_width; /* Gap / hit strip between panes */
    BOOL splitting;     /* Dragging the left-pane splitter */
    COLORREF header_bg;
    COLORREF header_line;
    EeVoterTable table;
    BOOL is_cvr_ui; /* TRUE for the resource-only AppState a CVR window owns (not a
                     * voter viewer): keeps it out of the voter-window behaviors and
                     * makes "Load Voter List" from a CVR window open a fresh viewer. */
    BOOL is_roster; /* a Voter Roster window: one pane showing roster_cols (columns of a
                    * table built by voter_roster.c); otherwise the same viewer machinery
                    * (filters, duplicates, reports, copy, export, options) */
    uint32_t roster_cols[64];      /* table columns shown in the single pane, in order */
    uint32_t nroster_cols;
    EeRosterLoadInfo roster_info;  /* load summary (owned) */
    BOOL loading;
    BOOL close_pending;
    volatile LONG load_cancel;
    HANDLE load_thread;
    wchar_t load_path[MAX_PATH];
    int load_sheet_index; /* worksheet to load for .xlsx (0 for CSV/TSV) */
    wchar_t load_error[512];
    EeLoadStatus load_status;
    CRITICAL_SECTION progress_lock;
    EeLoadProgress last_progress;
    BOOL progress_dirty;
    LONG sel_syncing;
    LONG sel_sync_posted;
    HWND sel_sync_source;
    EeFilterSet filters;
    uint32_t *filter_map;
    uint32_t filter_count;

    /* Duplicate "mark" layer: per physical row, ANDed with the filter view.
     * Independent per window; indexed by physical row so it survives sorts. */
    uint8_t *mark_rows;
    uint32_t mark_count;
    BOOL mark_active;
    int mark_kind;           /* EE_SCAN_* that produced mark_rows */
    wchar_t mark_label[160]; /* status-bar description of the active mark view */

    /* Two-file compare initiated from this viewer (as file A). Reuses the scan
     * thread / progress modal (scan_thread, scanning, scan_cancel). */
    struct AppState *cmp_other; /* the other viewer (file B) */
    uint16_t *cmp_class_a;      /* a->row_count entries (EE_CMP_*), transient */
    uint16_t *cmp_class_b;      /* b->row_count entries (EE_CMP_*), transient */
    EeCompareResult cmp_result;
    BOOL cmp_ok;

    /* Background duplicate scan (mirrors the load-thread machinery). */
    HANDLE scan_thread;
    volatile LONG scan_cancel;
    int scan_kind;
    uint8_t *scan_marks;
    uint32_t scan_count;
    BOOL scan_ok;
    BOOL scanning;

    /* Modeless Precinct/Address report windows spawned from this viewer. */
    ReportWindow *report_precinct;
    ReportWindow *report_address;
    RosterTotalsWindow *report_totals; /* Voter Roster: Voting Totals report */

    struct AppState *next;
} AppState;

static AppState *g_viewers = NULL;
static int g_viewer_count = 0;

/* Persisted user options, loaded once at startup and re-saved when the Options
 * dialog commits a change. Seeds the first viewer window's defaults. */
static EeSettings g_settings;

/* Modeless two-file compare summary window (one at a time). Owns the transient
 * per-row classification arrays and points at both viewers it summarizes. */
typedef struct CompareWindow
{
    HWND hwnd;
    HWND list;
    HWND status;
    HWND diff_btn;    /* "Show Differences…" */
    HWND exp_sum_btn; /* "Export Summary…" */
    HWND exp_all_btn; /* "Export All Records…" */
    AppState *a;      /* initiating viewer (file A) */
    AppState *b;      /* other viewer (file B) */
    EeCompareResult result;
    uint16_t *class_a; /* a->row_count entries, owned */
    uint16_t *class_b; /* b->row_count entries, owned */
    uint32_t rows_a;   /* a->row_count captured at compute time */
    uint32_t rows_b;
    int buckets[16];   /* summary row -> CMP_BUCKET_* (only those that apply) */
    int nbuckets;
} CompareWindow;

static CompareWindow *g_compare = NULL;

/* One differing field of one matched voter, for the side-by-side detail list. */
typedef struct DiffRow
{
    uint32_t row_a; /* physical row in A */
    uint32_t row_b; /* physical row in B */
    uint32_t col;   /* column in A: EE_COL_NAME / _ADDRESS / _PRECINCT, or a roster's
                     * Voting Method / Date Voted column */
    uint32_t col_b; /* the same field's column in B */
    int field;      /* DIFF_FIELD_* */
} DiffRow;

/* Modeless side-by-side "which field differs" window (one at a time). */
typedef struct DiffWindow
{
    HWND hwnd;
    HWND list;
    HWND status;
    AppState *a;
    AppState *b;
    DiffRow *rows; /* one entry per differing (voter, field) */
    uint32_t row_count;
    int sort_col;  /* -1 = unsorted; else 0..3 */
    BOOL sort_asc;
} DiffWindow;

static DiffWindow *g_diff = NULL;
static WNDPROC g_old_frozen_proc = NULL;
static WNDPROC g_old_scroll_proc = NULL;
static WNDPROC g_old_title_proc = NULL;

static int App_ClampZoom(int zoom_percent);
static EeMapEngine App_ClampMapEngine(int engine);
static int App_PickSheet(AppState *app, const wchar_t (*names)[EE_XLSX_SHEET_NAME_CCH], int count);

/* Case-insensitive test for an Excel workbook path: ".xlsx", or ".xls" (Excel 97-2003,
 * which the .xlsx reader also reads). */
static BOOL is_xlsx_path(const wchar_t *path)
{
    const wchar_t *dot = (path != NULL) ? wcsrchr(path, L'.') : NULL;
    return dot != NULL && (_wcsicmp(dot, L".xlsx") == 0 || _wcsicmp(dot, L".xls") == 0);
}
static LRESULT CALLBACK FrozenSubclass(HWND hwnd, UINT msg, WPARAM wParam, LPARAM lParam);
static LRESULT CALLBACK ScrollSubclass(HWND hwnd, UINT msg, WPARAM wParam, LPARAM lParam);
static LRESULT CALLBACK PaneTitleSubclass(HWND hwnd, UINT msg, WPARAM wParam, LPARAM lParam);

static AppState *App_FromMain(HWND hwnd);
static AppState *App_FromChild(HWND hwnd);
static AppState *App_CreateViewer(HINSTANCE instance,
                                  int nCmdShow,
                                  HWND offset_from,
                                  const AppState *prefs);
static AppState *App_CreateViewerEx(HINSTANCE instance,
                                    int nCmdShow,
                                    HWND offset_from,
                                    const AppState *prefs,
                                    BOOL roster);
static HMENU App_CreateRosterMenu(void);
static void App_BeginOpenRoster(AppState *app);
static void App_ShowRosterTotals(AppState *app);
static void App_ShowDuplicateVotersVoting(AppState *app);
static const wchar_t *App_PathBaseName(const wchar_t *path);
static void Diff_CellW(const EeVoterTable *t, uint32_t row, uint32_t col, wchar_t *out, int cch);
static void App_CloseRosterTotals(AppState *app);
static void App_StartLoad(AppState *app, const wchar_t *path, int sheet_index);
static void App_RequestClose(AppState *app);
static void App_ExitAll(void);
static void App_ApplyFilter(AppState *app);
static BOOL App_ShowFilter(AppState *app);
static void App_ResetFilter(AppState *app);
static void App_ClearMarks(AppState *app);
static void App_ShowReport(AppState *app, int kind);
static void App_CloseReports(AppState *app);
static void App_ExportVoters(AppState *app, BOOL selection_only);
static void App_ExportVoterReport(ReportWindow *rw, BOOL selection_only);
static LRESULT CALLBACK ReportWndProc(HWND hwnd, UINT msg, WPARAM wParam, LPARAM lParam);
static void App_BeginOpenCvr(AppState *app);
static void App_BeginOpenResults(AppState *app);
static void App_BeginOpenAudit(AppState *app);
static void Res_FillCompareMenu(HMENU popup, HWND self, BOOL with_cvr, HWND *targets, int max_targets);
static void App_RunResultsCompare(HWND from, HWND other);
static void App_RunBallotRecon(HWND audit, HWND other);
static BOOL Aud_IsAuditWindow(HWND h);
static void App_ShowResultsCompareHelp(AppState *app, HWND owner);
static void App_FreeCvrUi(AppState *ui);
static LRESULT CALLBACK CvrWndProc(HWND hwnd, UINT msg, WPARAM wParam, LPARAM lParam);
static void App_StartDuplicateScan(AppState *app, int kind);
static void App_OnScanFinished(AppState *app);
static void App_ApplyMarks(AppState *app,
                           uint8_t *marks,
                           uint32_t count,
                           int kind,
                           uint32_t sort_col,
                           const wchar_t *label);
static void App_ApplyDuplicateMarks(AppState *app, uint8_t *marks, uint32_t count, int kind);
static void App_StartCompare(AppState *a, AppState *b);
static void App_OnCompareFinished(AppState *app);
static void App_CloseCompare(AppState *app);
static void App_CloseDiff(AppState *app);
static void App_ShowDifferences(CompareWindow *cw);
static AppState *App_OtherViewerByIndex(const AppState *app, int index);
static LRESULT CALLBACK CompareWndProc(HWND hwnd, UINT msg, WPARAM wParam, LPARAM lParam);
static LRESULT CALLBACK DiffWndProc(HWND hwnd, UINT msg, WPARAM wParam, LPARAM lParam);

/* Position of the "Compare" popup in the menu bar (File, Edit, Filter, Reports,
 * Compare); rebuilt on demand in WM_INITMENUPOPUP. */
#define k_CompareMenuPos 4
static void App_AddQuickFilter(AppState *app,
                               uint32_t column,
                               const wchar_t *value,
                               EeFilterAction action);
static void App_DestroyFilter(AppState *app);

static AppState *App_FromMain(HWND hwnd)
{
    if (hwnd == NULL)
    {
        return NULL;
    }
    return (AppState *)GetWindowLongPtrW(hwnd, GWLP_USERDATA);
}

static AppState *App_FromChild(HWND hwnd)
{
    if (hwnd == NULL)
    {
        return NULL;
    }
    return App_FromMain(GetParent(hwnd));
}

static void App_LinkViewer(AppState *app)
{
    if (app == NULL)
    {
        return;
    }
    app->next = g_viewers;
    g_viewers = app;
    g_viewer_count++;
}

static void App_UnlinkViewer(AppState *app)
{
    AppState **pp;

    if (app == NULL)
    {
        return;
    }
    for (pp = &g_viewers; *pp != NULL; pp = &(*pp)->next)
    {
        if (*pp == app)
        {
            *pp = app->next;
            app->next = NULL;
            if (g_viewer_count > 0)
            {
                g_viewer_count--;
            }
            return;
        }
    }
}

static BOOL App_PathsEqual(const wchar_t *a, const wchar_t *b)
{
    if (a == NULL || b == NULL || a[0] == L'\0' || b[0] == L'\0')
    {
        return FALSE;
    }
    return CompareStringOrdinal(a, -1, b, -1, TRUE) == CSTR_EQUAL;
}

static BOOL App_CanonicalPath(const wchar_t *in, wchar_t *out, DWORD cch)
{
    DWORD n;

    if (in == NULL || out == NULL || cch == 0)
    {
        return FALSE;
    }
    n = GetFullPathNameW(in, cch, out, NULL);
    return n > 0 && n < cch;
}

static AppState *App_FindViewerByPath(const wchar_t *path)
{
    AppState *p;

    for (p = g_viewers; p != NULL; p = p->next)
    {
        if (!p->is_roster && App_PathsEqual(p->load_path, path))
        {
            return p;
        }
    }
    return NULL;
}

static void App_ActivateViewer(AppState *app)
{
    if (app == NULL || app->hwnd_main == NULL)
    {
        return;
    }
    if (IsIconic(app->hwnd_main))
    {
        ShowWindow(app->hwnd_main, SW_RESTORE);
    }
    SetForegroundWindow(app->hwnd_main);
}

/* Open CVR filter windows (modeless; need dialog-key routing like the voter one).
 * CVR windows live outside g_viewers, so track their filter windows here. */
#define EE_MAX_CVR_FILTERS 32
static HWND g_cvr_filter_windows[EE_MAX_CVR_FILTERS];
static int g_cvr_filter_count;

static void CvrFilter_Register(HWND hwnd)
{
    if (hwnd != NULL && g_cvr_filter_count < EE_MAX_CVR_FILTERS)
    {
        g_cvr_filter_windows[g_cvr_filter_count++] = hwnd;
    }
}

static void CvrFilter_Unregister(HWND hwnd)
{
    int i;
    for (i = 0; i < g_cvr_filter_count; i++)
    {
        if (g_cvr_filter_windows[i] == hwnd)
        {
            g_cvr_filter_windows[i] = g_cvr_filter_windows[--g_cvr_filter_count];
            return;
        }
    }
}

/* Open CVR viewer windows. They are first-class top-level windows (each owns its own
 * resource-only AppState) that live outside g_viewers, so track them here to keep the
 * app alive while any is open and to close them on Exit. */
#define EE_MAX_CVR_WINDOWS 64
static HWND g_cvr_windows[EE_MAX_CVR_WINDOWS];
static int g_cvr_window_count;

static void CvrWindow_Register(HWND hwnd)
{
    if (hwnd != NULL && g_cvr_window_count < EE_MAX_CVR_WINDOWS)
    {
        g_cvr_windows[g_cvr_window_count++] = hwnd;
    }
}

static void CvrWindow_Unregister(HWND hwnd)
{
    int i;
    for (i = 0; i < g_cvr_window_count; i++)
    {
        if (g_cvr_windows[i] == hwnd)
        {
            g_cvr_windows[i] = g_cvr_windows[--g_cvr_window_count];
            return;
        }
    }
}

/* PostQuitMessage only once no voter window and no CVR window remain. */
static void App_MaybeQuit(void)
{
    if (g_viewer_count <= 0 && g_cvr_window_count <= 0)
    {
        PostQuitMessage(0);
    }
}

static BOOL App_RouteDialogMessage(MSG *msg)
{
    AppState *p;
    int i;

    for (p = g_viewers; p != NULL; p = p->next)
    {
        if (p->hwnd_options != NULL && IsDialogMessageW(p->hwnd_options, msg))
        {
            return TRUE;
        }
        if (p->hwnd_progress != NULL && IsDialogMessageW(p->hwnd_progress, msg))
        {
            return TRUE;
        }
        if (p->hwnd_filter != NULL && IsDialogMessageW(p->hwnd_filter, msg))
        {
            return TRUE;
        }
    }
    for (i = 0; i < g_cvr_filter_count; i++)
    {
        if (IsDialogMessageW(g_cvr_filter_windows[i], msg))
        {
            return TRUE;
        }
    }
    return FALSE;
}

static void App_InitViewerState(AppState *app, HINSTANCE instance, const AppState *prefs)
{
    ZeroMemory(app, sizeof(*app));
    app->instance = instance;
    app->dpi = 96;
    if (prefs != NULL)
    {
        app->copy_prepend_normalized = prefs->copy_prepend_normalized;
        app->name_surname_first = prefs->name_surname_first;
        app->zoom_percent = App_ClampZoom(prefs->zoom_percent);
        app->map_engine = App_ClampMapEngine((int)prefs->map_engine);
    }
    else
    {
        /* First window of the run: seed from the options saved last run. */
        app->copy_prepend_normalized = g_settings.copy_prepend_normalized;
        app->name_surname_first = g_settings.name_surname_first;
        app->zoom_percent = App_ClampZoom(g_settings.zoom_percent);
        app->map_engine = App_ClampMapEngine(g_settings.map_engine);
    }
    InitializeCriticalSection(&app->progress_lock);
    EeVoterTable_Init(&app->table);
    EeFilter_Init(&app->filters);
}

static void App_SubclassViewer(AppState *app)
{
    WNDPROC old;

    if (app == NULL)
    {
        return;
    }
    if (app->hwnd_frozen != NULL)
    {
        old = (WNDPROC)SetWindowLongPtrW(app->hwnd_frozen, GWLP_WNDPROC, (LONG_PTR)FrozenSubclass);
        if (g_old_frozen_proc == NULL)
        {
            g_old_frozen_proc = old;
        }
    }
    if (app->hwnd_scroll != NULL)
    {
        old = (WNDPROC)SetWindowLongPtrW(app->hwnd_scroll, GWLP_WNDPROC, (LONG_PTR)ScrollSubclass);
        if (g_old_scroll_proc == NULL)
        {
            g_old_scroll_proc = old;
        }
    }
    if (app->hwnd_frozen_title != NULL)
    {
        old = (WNDPROC)SetWindowLongPtrW(app->hwnd_frozen_title,
                                         GWLP_WNDPROC,
                                         (LONG_PTR)PaneTitleSubclass);
        if (g_old_title_proc == NULL)
        {
            g_old_title_proc = old;
        }
    }
    if (app->hwnd_scroll_title != NULL)
    {
        SetWindowLongPtrW(app->hwnd_scroll_title, GWLP_WNDPROC, (LONG_PTR)PaneTitleSubclass);
    }
}

/* -------------------------------------------------------------------------- */
/* DPI helpers                                                                */
/* -------------------------------------------------------------------------- */

static int Scale(AppState *app, int value_96)
{
    return MulDiv(value_96, (int)app->dpi, 96);
}

static int App_ClampZoom(int zoom_percent)
{
    if (zoom_percent < k_ZoomMin)
    {
        return k_ZoomMin;
    }
    if (zoom_percent > k_ZoomMax)
    {
        return k_ZoomMax;
    }
    return zoom_percent;
}

static EeMapEngine App_ClampMapEngine(int engine)
{
    if (engine < (int)EeMap_Google || engine > (int)EeMap_OpenStreetMap)
    {
        return EeMap_Google;
    }
    return (EeMapEngine)engine;
}

static const wchar_t *App_MapEngineName(EeMapEngine engine)
{
    switch (App_ClampMapEngine((int)engine))
    {
        case EeMap_Bing:
            return L"Bing Maps";
        case EeMap_Apple:
            return L"Apple Maps";
        case EeMap_OpenStreetMap:
            return L"Open Street Maps";
        case EeMap_Google:
        default:
            return L"Google Maps";
    }
}

static BOOL App_AppendUrlEncoded(wchar_t *dst, size_t cch, const wchar_t *text)
{
    char utf8[EE_FILTER_VALUE_CCH * 3];
    int n;
    int i;

    if (dst == NULL || cch == 0 || text == NULL)
    {
        return FALSE;
    }
    n = WideCharToMultiByte(CP_UTF8, 0, text, -1, utf8, (int)sizeof(utf8), NULL, NULL);
    if (n <= 0)
    {
        return FALSE;
    }
    for (i = 0; i < n - 1; i++)
    {
        unsigned char b = (unsigned char)utf8[i];
        size_t used = wcslen(dst);
        if ((b >= 'A' && b <= 'Z') || (b >= 'a' && b <= 'z') || (b >= '0' && b <= '9') ||
            b == '-' || b == '_' || b == '.' || b == '~')
        {
            if (used + 1 >= cch)
            {
                return FALSE;
            }
            dst[used] = (wchar_t)b;
            dst[used + 1] = L'\0';
        }
        else
        {
            if (used + 3 >= cch)
            {
                return FALSE;
            }
            if (FAILED(StringCchPrintfW(dst + used, cch - used, L"%%%02X", b)))
            {
                return FALSE;
            }
        }
    }
    return TRUE;
}

static BOOL App_BuildMapUrl(EeMapEngine engine, const wchar_t *address, wchar_t *url, size_t cch)
{
    const wchar_t *prefix;

    if (address == NULL || address[0] == L'\0' || url == NULL || cch == 0)
    {
        return FALSE;
    }
    switch (App_ClampMapEngine((int)engine))
    {
        case EeMap_Bing:
            prefix = L"https://www.bing.com/maps?q=";
            break;
        case EeMap_Apple:
            prefix = L"https://maps.apple.com/?q=";
            break;
        case EeMap_OpenStreetMap:
            prefix = L"https://www.openstreetmap.org/search?query=";
            break;
        case EeMap_Google:
        default:
            prefix = L"https://www.google.com/maps/search/?api=1&query=";
            break;
    }
    if (FAILED(StringCchCopyW(url, cch, prefix)))
    {
        return FALSE;
    }
    return App_AppendUrlEncoded(url, cch, address);
}

static BOOL App_ShowAddressInMaps(AppState *app, const wchar_t *address)
{
    wchar_t url[2048];
    HINSTANCE rc;

    if (app == NULL || address == NULL || address[0] == L'\0')
    {
        return FALSE;
    }
    if (!App_BuildMapUrl(app->map_engine, address, url, ARRAYSIZE(url)))
    {
        MessageBoxW(app->hwnd_main,
                    L"Could not build a map link for this address.",
                    k_WindowTitle,
                    MB_ICONERROR | MB_OK);
        return FALSE;
    }
    rc = ShellExecuteW(app->hwnd_main, L"open", url, NULL, NULL, SW_SHOWNORMAL);
    if ((INT_PTR)rc <= 32)
    {
        MessageBoxW(app->hwnd_main,
                    L"Could not open the map in a browser.",
                    k_WindowTitle,
                    MB_ICONERROR | MB_OK);
        return FALSE;
    }
    return TRUE;
}

static wchar_t fold_ascii_w(wchar_t ch)
{
    if (ch >= L'A' && ch <= L'Z')
    {
        return (wchar_t)(ch - L'A' + L'a');
    }
    return ch;
}

static BOOL App_ExtractHttpsUrl(const wchar_t *text, wchar_t *url, size_t url_cch)
{
    static const wchar_t k_prefix[] = L"https://";
    const wchar_t *start;
    const wchar_t *p;
    size_t n;

    if (text == NULL || url == NULL || url_cch < 9)
    {
        return FALSE;
    }
    url[0] = L'\0';
    start = NULL;
    for (p = text; *p != L'\0'; p++)
    {
        size_t i;
        BOOL match = TRUE;
        for (i = 0; k_prefix[i] != L'\0'; i++)
        {
            if (fold_ascii_w(p[i]) != k_prefix[i])
            {
                match = FALSE;
                break;
            }
        }
        if (match)
        {
            start = p;
            break;
        }
    }
    if (start == NULL)
    {
        return FALSE;
    }
    p = start;
    while (*p != L'\0' && *p != L' ' && *p != L'\t' && *p != L'\r' && *p != L'\n' && *p != L'<' &&
           *p != L'>' && *p != L'"' && *p != L'\'' && *p != L'(' && *p != L')')
    {
        p++;
    }
    n = (size_t)(p - start);
    while (n > 0)
    {
        wchar_t last = start[n - 1];
        if (last == L'.' || last == L',' || last == L';' || last == L':' || last == L']')
        {
            n--;
            continue;
        }
        break;
    }
    if (n <= 8)
    {
        return FALSE;
    }
    if (n + 1 > url_cch)
    {
        n = url_cch - 1;
    }
    memcpy(url, start, n * sizeof(wchar_t));
    url[n] = L'\0';
    return TRUE;
}

static BOOL App_OpenHttpsLink(AppState *app, const wchar_t *url)
{
    HINSTANCE rc;

    if (app == NULL || url == NULL || url[0] == L'\0')
    {
        return FALSE;
    }
    rc = ShellExecuteW(app->hwnd_main, L"open", url, NULL, NULL, SW_SHOWNORMAL);
    if ((INT_PTR)rc <= 32)
    {
        MessageBoxW(app->hwnd_main,
                    L"Could not open the link in a browser.",
                    k_WindowTitle,
                    MB_ICONERROR | MB_OK);
        return FALSE;
    }
    return TRUE;
}

static int ScaleDisplay(AppState *app, int value_96)
{
    int z = app->zoom_percent > 0 ? app->zoom_percent : k_ZoomDefault;
    return MulDiv(Scale(app, value_96), z, 100);
}

static int App_PaneTitleHeight(AppState *app)
{
    int h = ScaleDisplay(app, 22);
    if (h < 18)
    {
        h = 18;
    }
    return h;
}

static void App_UpdateDpiMetrics(AppState *app, UINT dpi)
{
    NONCLIENTMETRICSW ncm;
    HFONT font;
    HFONT header_font;
    LOGFONTW lf;

    {
        UINT old_dpi = app->dpi ? app->dpi : 96u;
        app->dpi = dpi == 0 ? 96u : dpi;
        app->pad = Scale(app, 4);
        app->splitter_width = Scale(app, 6);
        if (app->splitter_width < 4)
        {
            app->splitter_width = 4;
        }
        /* Preserve user-chosen pane width across DPI changes. */
        if (app->frozen_width <= 0)
        {
            app->frozen_width = ScaleDisplay(app, k_DefaultFrozenWidth);
        }
        else if (old_dpi != app->dpi)
        {
            app->frozen_width = MulDiv(app->frozen_width, (int)app->dpi, (int)old_dpi);
        }
    }

    /* Light grey header band; slightly darker double rule under captions. */
    app->header_bg = RGB(230, 230, 230);
    app->header_line = RGB(96, 96, 96);

    if (app->brush_header != NULL)
    {
        DeleteObject(app->brush_header);
        app->brush_header = NULL;
    }
    app->brush_header = CreateSolidBrush(app->header_bg);

    if (app->pen_header_line != NULL)
    {
        DeleteObject(app->pen_header_line);
        app->pen_header_line = NULL;
    }
    {
        int pen_w = Scale(app, 1);
        if (pen_w < 1)
        {
            pen_w = 1;
        }
        app->pen_header_line = CreatePen(PS_SOLID, pen_w, app->header_line);
    }

    ZeroMemory(&ncm, sizeof(ncm));
    ncm.cbSize = sizeof(ncm);
    if (SystemParametersInfoForDpi(SPI_GETNONCLIENTMETRICS, sizeof(ncm), &ncm, 0, app->dpi))
    {
        int zoom = App_ClampZoom(app->zoom_percent > 0 ? app->zoom_percent : k_ZoomDefault);
        HFONT grid_font;

        font = CreateFontIndirectW(&ncm.lfMessageFont);
        if (font != NULL)
        {
            if (app->font_ui != NULL)
            {
                DeleteObject(app->font_ui);
            }
            app->font_ui = font;
        }

        lf = ncm.lfMessageFont;
        if (zoom != 100 && lf.lfHeight != 0)
        {
            lf.lfHeight = MulDiv(lf.lfHeight, zoom, 100);
        }
        grid_font = CreateFontIndirectW(&lf);
        if (grid_font != NULL)
        {
            if (app->font_grid != NULL)
            {
                DeleteObject(app->font_grid);
            }
            app->font_grid = grid_font;
        }

        lf.lfWeight = FW_BOLD;
        header_font = CreateFontIndirectW(&lf);
        if (header_font != NULL)
        {
            if (app->font_header != NULL)
            {
                DeleteObject(app->font_header);
            }
            app->font_header = header_font;
        }
    }
}

static void App_ApplyHeaderStyle(AppState *app, HWND hwnd_list)
{
    HWND header;

    if (hwnd_list == NULL)
    {
        return;
    }
    header = ListView_GetHeader(hwnd_list);
    if (header == NULL)
    {
        return;
    }
    if (app->font_header != NULL)
    {
        SendMessageW(header, WM_SETFONT, (WPARAM)app->font_header, TRUE);
    }
    /* Flat header so our custom fill/lines are not covered by theme buttons. */
    SetWindowTheme(header, L"", L"");
    InvalidateRect(header, NULL, TRUE);
}

static void App_SetListRowHeight(HWND hwnd_list, int height)
{
    HIMAGELIST himl;
    HIMAGELIST old;

    if (hwnd_list == NULL || height <= 0)
    {
        return;
    }
    himl = ImageList_Create(1, height, ILC_COLOR, 1, 1);
    if (himl == NULL)
    {
        return;
    }
    old = ListView_SetImageList(hwnd_list, himl, LVSIL_SMALL);
    if (old != NULL)
    {
        ImageList_Destroy(old);
    }
}

static int App_MeasureGridRowHeight(AppState *app)
{
    HDC hdc;
    HFONT font;
    int h = ScaleDisplay(app, 20);

    font = app->font_grid != NULL ? app->font_grid : app->font_ui;
    if (font == NULL || app->hwnd_main == NULL)
    {
        return h < 8 ? 8 : h;
    }
    hdc = GetDC(app->hwnd_main);
    if (hdc != NULL)
    {
        TEXTMETRICW tm;
        HFONT old = (HFONT)SelectObject(hdc, font);
        if (GetTextMetricsW(hdc, &tm))
        {
            h = tm.tmHeight + tm.tmExternalLeading + ScaleDisplay(app, 6);
        }
        SelectObject(hdc, old);
        ReleaseDC(app->hwnd_main, hdc);
    }
    if (h < 8)
    {
        h = 8;
    }
    return h;
}

static void App_ApplyFont(AppState *app)
{
    HFONT cell_font;
    int row_h;

    if (app->font_ui == NULL)
    {
        return;
    }
    cell_font = app->font_grid != NULL ? app->font_grid : app->font_ui;
    row_h = App_MeasureGridRowHeight(app);
    if (app->hwnd_frozen)
    {
        SendMessageW(app->hwnd_frozen, WM_SETFONT, (WPARAM)cell_font, TRUE);
        App_SetListRowHeight(app->hwnd_frozen, row_h);
        App_ApplyHeaderStyle(app, app->hwnd_frozen);
    }
    if (app->hwnd_scroll)
    {
        SendMessageW(app->hwnd_scroll, WM_SETFONT, (WPARAM)cell_font, TRUE);
        App_SetListRowHeight(app->hwnd_scroll, row_h);
        App_ApplyHeaderStyle(app, app->hwnd_scroll);
    }
    if (app->hwnd_status)
    {
        SendMessageW(app->hwnd_status, WM_SETFONT, (WPARAM)app->font_ui, TRUE);
    }
    if (app->font_header != NULL)
    {
        if (app->hwnd_frozen_title)
        {
            SendMessageW(app->hwnd_frozen_title, WM_SETFONT, (WPARAM)app->font_header, TRUE);
        }
        if (app->hwnd_scroll_title)
        {
            SendMessageW(app->hwnd_scroll_title, WM_SETFONT, (WPARAM)app->font_header, TRUE);
        }
    }
}

/**
 * @brief Custom-draw list header: bold text, light grey fill, double underline.
 */
static LRESULT App_HeaderCustomDraw(AppState *app, NMCUSTOMDRAW *cd, BOOL center_all)
{
    NMCUSTOMDRAW *nmcd = cd;

    if (app == NULL || nmcd == NULL)
    {
        return CDRF_DODEFAULT;
    }

    switch (nmcd->dwDrawStage)
    {
        case CDDS_PREPAINT:
            if (app->brush_header != NULL)
            {
                FillRect(nmcd->hdc, &nmcd->rc, app->brush_header);
            }
            return CDRF_NOTIFYITEMDRAW | CDRF_NOTIFYPOSTPAINT;

        case CDDS_ITEMPREPAINT:
        {
            HWND header = nmcd->hdr.hwndFrom;
            int index = (int)nmcd->dwItemSpec;
            HDITEMW item;
            wchar_t text[256];
            RECT rc = nmcd->rc;
            RECT rc_text;
            HFONT old_font;
            COLORREF old_bk;
            COLORREF old_text;
            int old_bk_mode;
            HPEN old_pen;
            int y1;
            int y2;
            int arrow_room = 0;
            UINT fmt = 0;

            ZeroMemory(&item, sizeof(item));
            item.mask = HDI_TEXT | HDI_FORMAT;
            item.pszText = text;
            item.cchTextMax = ARRAYSIZE(text);
            text[0] = L'\0';
            Header_GetItem(header, index, &item);
            fmt = item.fmt;

            if (app->brush_header != NULL)
            {
                FillRect(nmcd->hdc, &rc, app->brush_header);
            }

            /* Soft vertical divider between columns. */
            {
                HPEN grid_pen = CreatePen(PS_SOLID, 1, RGB(200, 200, 200));
                HPEN prev = (HPEN)SelectObject(nmcd->hdc, grid_pen);
                MoveToEx(nmcd->hdc, rc.right - 1, rc.top, NULL);
                LineTo(nmcd->hdc, rc.right - 1, rc.bottom);
                SelectObject(nmcd->hdc, prev);
                DeleteObject(grid_pen);
            }

            if ((fmt & HDF_SORTUP) || (fmt & HDF_SORTDOWN))
            {
                arrow_room = ScaleDisplay(app, 14);
            }

            /*
         * ListView often clears LVCFMT_CENTER on column 0 (first column). Treat
         * the frozen pane's Voter ID header (index 0) as center-aligned always.
         */
            {
                BOOL force_center =
                    center_all || (app->hwnd_frozen != NULL &&
                                   header == ListView_GetHeader(app->hwnd_frozen) && index == 0);
                BOOL is_center = force_center || ((fmt & HDF_CENTER) != 0);
                BOOL is_right = !is_center && ((fmt & HDF_RIGHT) != 0);
                UINT dt = DT_VCENTER | DT_SINGLELINE | DT_END_ELLIPSIS | DT_NOPREFIX;
                int pad = ScaleDisplay(app, 4);

                rc_text = rc;
                rc_text.top += ScaleDisplay(app, 2);
                rc_text.bottom -= ScaleDisplay(app, 5);

                if (is_center)
                {
                    /* Symmetric padding so the caption matches centered cell text. */
                    rc_text.left += pad;
                    rc_text.right -= pad;
                    dt |= DT_CENTER;
                }
                else if (is_right)
                {
                    rc_text.left += pad;
                    rc_text.right -= pad + arrow_room;
                    dt |= DT_RIGHT;
                }
                else
                {
                    rc_text.left += ScaleDisplay(app, 6);
                    rc_text.right -= pad + arrow_room;
                    dt |= DT_LEFT;
                }

                old_font = (HFONT)SelectObject(nmcd->hdc,
                                               app->font_header ? app->font_header : app->font_ui);
                old_bk_mode = SetBkMode(nmcd->hdc, TRANSPARENT);
                old_bk = SetBkColor(nmcd->hdc, app->header_bg);
                old_text = SetTextColor(nmcd->hdc, RGB(0, 0, 0));
                DrawTextW(nmcd->hdc, text, -1, &rc_text, dt);
            }

            if (arrow_room > 0)
            {
                int cx = rc.right - ScaleDisplay(app, 8);
                int cy = (rc.top + rc.bottom) / 2;
                int half = ScaleDisplay(app, 4);
                POINT pts[3];
                HBRUSH br = CreateSolidBrush(RGB(60, 60, 60));
                HBRUSH old_br = (HBRUSH)SelectObject(nmcd->hdc, br);
                HPEN null_pen = (HPEN)GetStockObject(NULL_PEN);
                HPEN old_p = (HPEN)SelectObject(nmcd->hdc, null_pen);

                if (fmt & HDF_SORTUP)
                {
                    pts[0].x = cx;
                    pts[0].y = cy - half / 2;
                    pts[1].x = cx - half;
                    pts[1].y = cy + half / 2;
                    pts[2].x = cx + half;
                    pts[2].y = cy + half / 2;
                }
                else
                {
                    pts[0].x = cx;
                    pts[0].y = cy + half / 2;
                    pts[1].x = cx - half;
                    pts[1].y = cy - half / 2;
                    pts[2].x = cx + half;
                    pts[2].y = cy - half / 2;
                }
                Polygon(nmcd->hdc, pts, 3);
                SelectObject(nmcd->hdc, old_br);
                SelectObject(nmcd->hdc, old_p);
                DeleteObject(br);
            }

            /* Double rule under the header caption. */
            y2 = rc.bottom - ScaleDisplay(app, 2);
            y1 = y2 - ScaleDisplay(app, 2);
            if (y1 < rc.top)
            {
                y1 = rc.top;
            }
            old_pen = (HPEN)SelectObject(nmcd->hdc,
                                         app->pen_header_line ? app->pen_header_line
                                                              : (HPEN)GetStockObject(BLACK_PEN));
            MoveToEx(nmcd->hdc, rc.left, y1, NULL);
            LineTo(nmcd->hdc, rc.right, y1);
            MoveToEx(nmcd->hdc, rc.left, y2, NULL);
            LineTo(nmcd->hdc, rc.right, y2);
            SelectObject(nmcd->hdc, old_pen);

            SetTextColor(nmcd->hdc, old_text);
            SetBkColor(nmcd->hdc, old_bk);
            SetBkMode(nmcd->hdc, old_bk_mode);
            if (old_font != NULL)
            {
                SelectObject(nmcd->hdc, old_font);
            }
            return CDRF_SKIPDEFAULT;
        }

        case CDDS_POSTPAINT:
            /* Ensure a continuous double line across the full header width. */
            if (app->pen_header_line != NULL)
            {
                RECT rc = nmcd->rc;
                int y2 = rc.bottom - ScaleDisplay(app, 2);
                int y1 = y2 - ScaleDisplay(app, 2);
                HPEN old_pen = (HPEN)SelectObject(nmcd->hdc, app->pen_header_line);
                MoveToEx(nmcd->hdc, rc.left, y1, NULL);
                LineTo(nmcd->hdc, rc.right, y1);
                MoveToEx(nmcd->hdc, rc.left, y2, NULL);
                LineTo(nmcd->hdc, rc.right, y2);
                SelectObject(nmcd->hdc, old_pen);
            }
            return CDRF_DODEFAULT;

        default:
            return CDRF_DODEFAULT;
    }
}

/* -------------------------------------------------------------------------- */
/* Status bar                                                                 */
/* -------------------------------------------------------------------------- */

static void App_SetStatus(AppState *app, const wchar_t *text)
{
    if (app->hwnd_status != NULL)
    {
        SendMessageW(app->hwnd_status, SB_SETTEXTW, 0, (LPARAM)text);
    }
}

static void App_UpdateRowStatus(AppState *app)
{
    wchar_t buf[192];
    const wchar_t *noun = app->is_roster ? L"voter records" : L"voters";
    if (app->table.row_count == 0)
    {
        App_SetStatus(app, app->is_roster ? L"No voter roster loaded." : L"No voter list loaded.");
        return;
    }
    if (app->mark_active)
    {
        const wchar_t *what = (app->mark_label[0] != L'\0') ? app->mark_label : L"marked rows";
        if (EeFilter_HasEnabled(&app->filters))
        {
            StringCchPrintfW(buf,
                             ARRAYSIZE(buf),
                             L"%s within filter: %u of %u %s",
                             what,
                             app->filter_count,
                             app->table.row_count,
                             noun);
        }
        else
        {
            StringCchPrintfW(buf,
                             ARRAYSIZE(buf),
                             L"Showing %u %s of %u %s",
                             app->filter_count,
                             what,
                             app->table.row_count,
                             noun);
        }
    }
    else if (EeFilter_HasEnabled(&app->filters))
    {
        StringCchPrintfW(buf,
                         ARRAYSIZE(buf),
                         L"Filtered: %u of %u %s",
                         app->filter_count,
                         app->table.row_count,
                         noun);
    }
    else
    {
        StringCchPrintfW(buf, ARRAYSIZE(buf), L"%u %s loaded", app->table.row_count, noun);
    }
    App_SetStatus(app, buf);
}

static uint32_t App_VisibleCount(const AppState *app)
{
    if (app == NULL)
    {
        return 0;
    }
    if (app->mark_active || EeFilter_HasEnabled(&app->filters))
    {
        return app->filter_count;
    }
    return app->table.row_count;
}

static uint32_t App_ViewRowFromDisplay(const AppState *app, uint32_t display)
{
    if (app == NULL)
    {
        return display;
    }
    if (app->filter_map != NULL && display < app->filter_count)
    {
        return app->filter_map[display];
    }
    return display;
}

static void App_ApplyFilter(AppState *app)
{
    uint32_t *map = NULL;
    uint32_t count = 0;

    if (app == NULL)
    {
        return;
    }
    free(app->filter_map);
    app->filter_map = NULL;
    app->filter_count = 0;
    if (!EeFilter_BuildMap(&app->filters, &app->table, &map, &count))
    {
        /* Build failed: fall back to the unfiltered view. */
        map = NULL;
        count = app->table.row_count;
    }

    /* Intersect the filter (or identity) view with the marked physical rows so a
     * duplicates view is ANDed with any active filter. Marks are per physical
     * row; map entries are view rows, so translate through view_index. */
    if (app->mark_active && app->mark_rows != NULL && app->table.row_count > 0)
    {
        uint32_t *isect = (uint32_t *)malloc((size_t)app->table.row_count * sizeof(uint32_t));
        if (isect != NULL)
        {
            uint32_t n = 0;
            uint32_t k;
            uint32_t base = (map != NULL) ? count : app->table.row_count;
            for (k = 0; k < base; k++)
            {
                uint32_t view_row = (map != NULL) ? map[k] : k;
                uint32_t phys =
                    (app->table.view_index != NULL) ? app->table.view_index[view_row] : view_row;
                if (phys < app->table.row_count && app->mark_rows[phys] && n < app->table.row_count)
                {
                    isect[n++] = view_row;
                }
            }
            free(map);
            map = isect;
            count = n;
        }
    }

    app->filter_map = map;
    app->filter_count = count;

    if (app->hwnd_frozen)
    {
        ListView_SetItemCountEx(app->hwnd_frozen,
                                (int)App_VisibleCount(app),
                                LVSICF_NOINVALIDATEALL);
        InvalidateRect(app->hwnd_frozen, NULL, TRUE);
    }
    if (app->hwnd_scroll)
    {
        ListView_SetItemCountEx(app->hwnd_scroll,
                                (int)App_VisibleCount(app),
                                LVSICF_NOINVALIDATEALL);
        InvalidateRect(app->hwnd_scroll, NULL, TRUE);
    }
    App_UpdateRowStatus(app);
}

/* -------------------------------------------------------------------------- */
/* List views                                                                 */
/* -------------------------------------------------------------------------- */

static void App_ClearListColumns(HWND hwnd)
{
    HWND header = ListView_GetHeader(hwnd);
    int count = Header_GetItemCount(header);
    int i;
    for (i = count - 1; i >= 0; i--)
    {
        ListView_DeleteColumn(hwnd, i);
    }
}

static void App_SetHeaderSortArrow(AppState *app,
                                   HWND hwnd_list,
                                   int column,
                                   int sort_column,
                                   BOOL ascending)
{
    HWND header = ListView_GetHeader(hwnd_list);
    int count = Header_GetItemCount(header);
    int i;
    HDITEMW item;
    BOOL frozen = (app != NULL && hwnd_list == app->hwnd_frozen);

    for (i = 0; i < count; i++)
    {
        ZeroMemory(&item, sizeof(item));
        item.mask = HDI_FORMAT;
        Header_GetItem(header, i, &item);
        item.fmt &= ~(HDF_SORTUP | HDF_SORTDOWN);
        if (i == column && column == sort_column)
        {
            item.fmt |= ascending ? HDF_SORTUP : HDF_SORTDOWN;
        }
        /* Keep Voter ID header center-aligned after sort-arrow updates. */
        if (frozen && i == 0)
        {
            item.fmt &= ~(HDF_LEFT | HDF_RIGHT | HDF_CENTER);
            item.fmt |= HDF_CENTER | HDF_STRING;
        }
        Header_SetItem(header, i, &item);
    }
}

static void App_FitFrozenColumns(AppState *app)
{
    HWND header;
    RECT rc;
    int inner_w;
    int id_w;
    int pct_w;
    int name_w;
    int addr_w;
    int gap;

    if (app->hwnd_frozen == NULL)
    {
        return;
    }

    header = ListView_GetHeader(app->hwnd_frozen);
    if (header == NULL || Header_GetItemCount(header) < EE_FROZEN_COLUMN_COUNT)
    {
        return;
    }

    GetClientRect(app->hwnd_frozen, &rc);
    inner_w = rc.right - rc.left;
    if (inner_w <= 0)
    {
        return;
    }

    gap = ScaleDisplay(app, 2);
    id_w = ScaleDisplay(app, 110);
    pct_w = ScaleDisplay(app, 72);
    name_w = ScaleDisplay(app, 180);
    addr_w = inner_w - id_w - pct_w - name_w - gap;
    if (addr_w < ScaleDisplay(app, 180))
    {
        addr_w = ScaleDisplay(app, 180);
    }

    ListView_SetColumnWidth(app->hwnd_frozen, EE_COL_VOTER_ID, id_w);
    ListView_SetColumnWidth(app->hwnd_frozen, EE_COL_PRECINCT, pct_w);
    ListView_SetColumnWidth(app->hwnd_frozen, EE_COL_NAME, name_w);
    ListView_SetColumnWidth(app->hwnd_frozen, EE_COL_ADDRESS, addr_w);
}

/* The scroll pane's columns: every source column for a voter list, or the roster's
 * display columns (Voter ID, Precinct, Name, Voting Method, Date Voted, extras) for a
 * Voter Roster window, whose left pane is hidden. */
static int App_ScrollColumnCount(const AppState *app)
{
    if (app->is_roster)
    {
        return (int)app->nroster_cols;
    }
    return (app->table.column_count > EE_FROZEN_COLUMN_COUNT)
               ? (int)(app->table.column_count - EE_FROZEN_COLUMN_COUNT)
               : 0;
}

/* Table column shown at scroll-pane column @p local, or UINT32_MAX. */
static uint32_t App_ScrollToTable(const AppState *app, int local)
{
    if (local < 0)
    {
        return UINT32_MAX;
    }
    if (app->is_roster)
    {
        return ((uint32_t)local < app->nroster_cols) ? app->roster_cols[local] : UINT32_MAX;
    }
    return (uint32_t)local + EE_FROZEN_COLUMN_COUNT;
}

/* Scroll-pane column showing table column @p tcol, or -1 (not in the scroll pane). */
static int App_TableToScroll(const AppState *app, uint32_t tcol)
{
    uint32_t i;
    if (app->is_roster)
    {
        for (i = 0; i < app->nroster_cols; i++)
        {
            if (app->roster_cols[i] == tcol)
            {
                return (int)i;
            }
        }
        return -1;
    }
    return (tcol >= EE_FROZEN_COLUMN_COUNT) ? (int)(tcol - EE_FROZEN_COLUMN_COUNT) : -1;
}

/* Put the sort arrow on whichever pane shows @p table_column. */
static void App_ShowSortArrows(AppState *app, uint32_t table_column)
{
    if (!app->is_roster && table_column < EE_FROZEN_COLUMN_COUNT)
    {
        App_SetHeaderSortArrow(app,
                               app->hwnd_frozen,
                               (int)table_column,
                               (int)table_column,
                               app->table.sort_ascending);
        App_SetHeaderSortArrow(app, app->hwnd_scroll, -1, -1, TRUE);
    }
    else
    {
        int local = App_TableToScroll(app, table_column);
        App_SetHeaderSortArrow(app, app->hwnd_frozen, -1, -1, TRUE);
        App_SetHeaderSortArrow(app, app->hwnd_scroll, local, local, app->table.sort_ascending);
    }
}

static void App_RebuildColumns(AppState *app)
{
    LVCOLUMNW col;
    uint32_t i;
    int col_width;

    App_ClearListColumns(app->hwnd_frozen);
    App_ClearListColumns(app->hwnd_scroll);

    if (app->table.column_count == 0)
    {
        App_ApplyFilter(app);
        return;
    }

    col_width = ScaleDisplay(app, 120);

    /* A report-mode ListView header desyncs and stops hit-testing once the
     * cumulative column width passes the Win32 16-bit limit (32,767 px): the body
     * keeps scrolling but the header freezes, so far-right columns (e.g. the ~385
     * voter-history columns in a Travis "…With_History" file) can't be sorted and
     * their titles no longer line up. Clamp the scroll-pane column width so all
     * scroll columns fit under that limit; with many columns they simply get
     * narrower (the user can still widen individual ones). Computed in device
     * pixels so it holds regardless of DPI/zoom. */
    {
        int scroll_cols = App_ScrollColumnCount(app);
        if (scroll_cols > 0)
        {
            int cap = 32000 / scroll_cols; /* margin under the 32,767 px header limit */
            int floor_w = ScaleDisplay(app, 20);
            if (col_width < floor_w)
            {
                col_width = floor_w; /* keep columns minimally usable when possible */
            }
            if (col_width > cap)
            {
                col_width = cap; /* but the limit always wins */
            }
        }
    }

    /* Frozen: Voter ID (center) + Precinct + Name + Address (left) */
    for (i = 0; i < EE_FROZEN_COLUMN_COUNT && i < app->table.column_count; i++)
    {
        ZeroMemory(&col, sizeof(col));
        col.mask = LVCF_TEXT | LVCF_WIDTH | LVCF_SUBITEM | LVCF_FMT;
        /* Column 0 is forced left by ListView for item text; we center-draw it. */
        col.fmt = (i == EE_COL_VOTER_ID) ? LVCFMT_CENTER : LVCFMT_LEFT;
        if (i == EE_COL_VOTER_ID)
        {
            col.cx = ScaleDisplay(app, 110);
        }
        else if (i == EE_COL_PRECINCT)
        {
            col.cx = ScaleDisplay(app, 72);
        }
        else if (i == EE_COL_NAME)
        {
            col.cx = ScaleDisplay(app, 180);
        }
        else
        {
            col.cx = ScaleDisplay(app, 240);
        }
        col.pszText = app->table.column_titles[i];
        col.iSubItem = (int)i;
        ListView_InsertColumn(app->hwnd_frozen, (int)i, &col);
    }
    /* ListView may strip center format from column 0; force header alignment. */
    if (app->table.column_count > 0)
    {
        HWND header = ListView_GetHeader(app->hwnd_frozen);
        HDITEMW hdi;
        ZeroMemory(&hdi, sizeof(hdi));
        hdi.mask = HDI_FORMAT;
        if (header != NULL && Header_GetItem(header, 0, &hdi))
        {
            hdi.fmt &= ~(HDF_LEFT | HDF_RIGHT | HDF_CENTER);
            hdi.fmt |= HDF_CENTER | HDF_STRING;
            Header_SetItem(header, 0, &hdi);
        }
    }

    /* Scrollable: the remaining source columns (a roster window: its display columns) */
    {
        int idx;
        int n = App_ScrollColumnCount(app);
        for (idx = 0; idx < n; idx++)
        {
            uint32_t tc = App_ScrollToTable(app, idx);
            int cx = col_width;
            if (tc >= app->table.column_count)
            {
                continue;
            }
            if (app->is_roster)
            {
                const wchar_t *t = app->table.column_titles[tc] ? app->table.column_titles[tc] : L"";
                if (tc == EE_COL_VOTER_ID)
                    cx = ScaleDisplay(app, 110);
                else if (tc == EE_COL_PRECINCT)
                    cx = ScaleDisplay(app, 80);
                else if (tc == EE_COL_NAME)
                    cx = ScaleDisplay(app, 230);
                else if (wcscmp(t, L"Voting Method") == 0)
                    cx = ScaleDisplay(app, 170);
                else if (wcscmp(t, L"Date Voted") == 0)
                    cx = ScaleDisplay(app, 100);
                else if (wcscmp(t, L"Source File") == 0)
                    cx = ScaleDisplay(app, 260);
            }
            ZeroMemory(&col, sizeof(col));
            col.mask = LVCF_TEXT | LVCF_WIDTH | LVCF_SUBITEM | LVCF_FMT;
            col.fmt = LVCFMT_LEFT;
            col.cx = cx;
            col.pszText = app->table.column_titles[tc];
            col.iSubItem = idx;
            ListView_InsertColumn(app->hwnd_scroll, idx, &col);
        }
    }

    App_ApplyFilter(app);

    if (app->table.sort_column >= 0)
    {
        App_ShowSortArrows(app, (uint32_t)app->table.sort_column);
    }

    App_ApplyHeaderStyle(app, app->hwnd_frozen);
    App_ApplyHeaderStyle(app, app->hwnd_scroll);
    App_FitFrozenColumns(app);
    InvalidateRect(app->hwnd_frozen, NULL, TRUE);
    InvalidateRect(app->hwnd_scroll, NULL, TRUE);

    /* Column widths affect H-scroll; re-equalize pane bottoms. */
    if (app->hwnd_main != NULL)
    {
        PostMessageW(app->hwnd_main, EEM_SYNC_PANE_SCROLLUI, 0, 0);
    }
}

static void App_SyncVerticalScroll(AppState *app, HWND source)
{
    HWND target;
    int top_src;
    int top_dst;
    int row_height;
    RECT rc;

    /* A roster window shows one pane: the hidden left pane cannot scroll, and syncing to
     * it would snap the visible pane back to the top. */
    if (app->hwnd_frozen == NULL || app->hwnd_scroll == NULL || app->is_roster)
    {
        return;
    }

    target = (source == app->hwnd_frozen) ? app->hwnd_scroll : app->hwnd_frozen;
    top_src = ListView_GetTopIndex(source);
    top_dst = ListView_GetTopIndex(target);
    if (top_src == top_dst)
    {
        return;
    }

    if (!ListView_GetItemRect(source, top_src, &rc, LVIR_BOUNDS))
    {
        row_height = ScaleDisplay(app, 20);
    }
    else
    {
        row_height = rc.bottom - rc.top;
        if (row_height <= 0)
        {
            row_height = ScaleDisplay(app, 20);
        }
    }

    ListView_Scroll(target, 0, (top_src - top_dst) * row_height);
}

/* Refresh the filtered view and header sort arrows after a sort. */
static void App_RefreshSortUi(AppState *app, uint32_t table_column)
{
    App_ApplyFilter(app);

    App_ShowSortArrows(app, table_column);
    {
        HWND hf = ListView_GetHeader(app->hwnd_frozen);
        HWND hs = ListView_GetHeader(app->hwnd_scroll);
        if (hf)
        {
            InvalidateRect(hf, NULL, TRUE);
        }
        if (hs)
        {
            InvalidateRect(hs, NULL, TRUE);
        }
    }
    InvalidateRect(app->hwnd_frozen, NULL, TRUE);
    InvalidateRect(app->hwnd_scroll, NULL, TRUE);
}

/* Force an ascending sort on a display column, then refresh view and headers.
 * Used to group a duplicates view by its key so the shared values are adjacent. */
static void App_SortByTableColumnAscending(AppState *app, uint32_t table_column)
{
    if (app == NULL || table_column >= app->table.column_count || app->table.row_count == 0)
    {
        return;
    }
    /* Reset so SortByColumn takes the ascending branch even if already sorted here. */
    app->table.sort_column = -1;
    if (EeVoterTable_SortByColumn(&app->table, table_column))
    {
        App_RefreshSortUi(app, table_column);
    }
}

static void App_SortFromHeader(AppState *app, HWND hwnd_list, int local_column)
{
    uint32_t table_column;
    HCURSOR prev;

    if (app->loading || app->table.row_count == 0)
    {
        return;
    }

    if (hwnd_list == app->hwnd_frozen)
    {
        table_column = (uint32_t)local_column;
    }
    else
    {
        table_column = App_ScrollToTable(app, local_column);
    }

    if (table_column >= app->table.column_count)
    {
        return;
    }

    prev = SetCursor(LoadCursorW(NULL, IDC_WAIT));
    App_SetStatus(app, L"Sorting…");
    if (EeVoterTable_SortByColumn(&app->table, table_column))
    {
        App_RefreshSortUi(app, table_column);
    }
    SetCursor(prev);
}

/* -------------------------------------------------------------------------- */
/* Progress window                                                            */
/* -------------------------------------------------------------------------- */

static LRESULT CALLBACK ProgressWndProc(HWND hwnd, UINT msg, WPARAM wParam, LPARAM lParam)
{
    AppState *app = (AppState *)GetWindowLongPtrW(hwnd, GWLP_USERDATA);

    switch (msg)
    {
        case WM_CREATE:
        {
            CREATESTRUCTW *cs = (CREATESTRUCTW *)lParam;
            app = (AppState *)cs->lpCreateParams;
            SetWindowLongPtrW(hwnd, GWLP_USERDATA, (LONG_PTR)app);
            return 0;
        }

        case WM_COMMAND:
            if (LOWORD(wParam) == IDC_PROGRESS_CANCEL && app != NULL)
            {
                InterlockedExchange(&app->load_cancel, 1);
                InterlockedExchange(&app->scan_cancel, 1);
                if (app->hwnd_progress_status)
                {
                    SetWindowTextW(app->hwnd_progress_status, L"Cancelling…");
                }
                EnableWindow(GetDlgItem(hwnd, IDC_PROGRESS_CANCEL), FALSE);
            }
            return 0;

        case WM_CLOSE:
            /* Force cancel rather than destroy mid-operation. */
            if (app != NULL && (app->loading || app->scanning))
            {
                InterlockedExchange(&app->load_cancel, 1);
                InterlockedExchange(&app->scan_cancel, 1);
                return 0;
            }
            break;

        default:
            break;
    }
    return DefWindowProcW(hwnd, msg, wParam, lParam);
}

static void App_DestroyProgress(AppState *app)
{
    if (app->hwnd_progress != NULL)
    {
        DestroyWindow(app->hwnd_progress);
        app->hwnd_progress = NULL;
        app->hwnd_progress_bar = NULL;
        app->hwnd_progress_status = NULL;
    }
}

static BOOL App_ShowProgress(AppState *app)
{
    RECT rc_main;
    RECT rc_wnd;
    RECT rc_client;
    const DWORD style = WS_POPUP | WS_CAPTION | WS_SYSMENU;
    const DWORD ex_style = WS_EX_DLGMODALFRAME | WS_EX_TOPMOST;
    int client_w = Scale(app, 420);
    int client_h = Scale(app, 140);
    int margin = Scale(app, 16);
    int status_h = Scale(app, 20);
    int bar_y = Scale(app, 48);
    int bar_h = Scale(app, 22);
    int btn_w = Scale(app, 90);
    int btn_h = Scale(app, 28);
    int gap = Scale(app, 12);
    int outer_w;
    int outer_h;
    int x;
    int y;
    int btn_x;
    int btn_y;
    HWND btn;

    App_DestroyProgress(app);

    /* CreateWindow size includes caption and frame. Convert from the desired
     * client size so the Cancel button is not placed on top of the bar. */
    rc_wnd.left = 0;
    rc_wnd.top = 0;
    rc_wnd.right = client_w;
    rc_wnd.bottom = client_h;
    if (!AdjustWindowRectExForDpi(&rc_wnd, style, FALSE, ex_style, app->dpi))
    {
        rc_wnd.left = 0;
        rc_wnd.top = 0;
        rc_wnd.right = client_w + Scale(app, 16);
        rc_wnd.bottom = client_h + Scale(app, 40);
    }
    outer_w = rc_wnd.right - rc_wnd.left;
    outer_h = rc_wnd.bottom - rc_wnd.top;

    GetWindowRect(app->hwnd_main, &rc_main);
    x = rc_main.left + ((rc_main.right - rc_main.left) - outer_w) / 2;
    y = rc_main.top + ((rc_main.bottom - rc_main.top) - outer_h) / 2;

    app->hwnd_progress = CreateWindowExW(ex_style,
                                         k_ProgressClassName,
                                         L"Loading voter list",
                                         style,
                                         x,
                                         y,
                                         outer_w,
                                         outer_h,
                                         app->hwnd_main,
                                         NULL,
                                         app->instance,
                                         app);
    if (app->hwnd_progress == NULL)
    {
        return FALSE;
    }

    GetClientRect(app->hwnd_progress, &rc_client);
    client_w = rc_client.right - rc_client.left;
    client_h = rc_client.bottom - rc_client.top;

    app->hwnd_progress_status = CreateWindowExW(0,
                                                L"STATIC",
                                                L"Reading file…",
                                                WS_CHILD | WS_VISIBLE | SS_LEFT,
                                                margin,
                                                margin,
                                                client_w - margin * 2,
                                                status_h,
                                                app->hwnd_progress,
                                                (HMENU)(INT_PTR)IDC_PROGRESS_STATUS,
                                                app->instance,
                                                NULL);

    app->hwnd_progress_bar = CreateWindowExW(0,
                                             PROGRESS_CLASSW,
                                             NULL,
                                             WS_CHILD | WS_VISIBLE,
                                             margin,
                                             bar_y,
                                             client_w - margin * 2,
                                             bar_h,
                                             app->hwnd_progress,
                                             (HMENU)(INT_PTR)IDC_PROGRESS_BAR,
                                             app->instance,
                                             NULL);
    SendMessageW(app->hwnd_progress_bar, PBM_SETRANGE32, 0, 100);
    SendMessageW(app->hwnd_progress_bar, PBM_SETPOS, 0, 0);

    btn_x = client_w - margin - btn_w;
    btn_y = client_h - margin - btn_h;
    if (btn_y < bar_y + bar_h + gap)
    {
        btn_y = bar_y + bar_h + gap;
    }

    btn = CreateWindowExW(0,
                          L"BUTTON",
                          L"Cancel",
                          WS_CHILD | WS_VISIBLE | WS_TABSTOP | BS_DEFPUSHBUTTON,
                          btn_x,
                          btn_y,
                          btn_w,
                          btn_h,
                          app->hwnd_progress,
                          (HMENU)(INT_PTR)IDC_PROGRESS_CANCEL,
                          app->instance,
                          NULL);

    if (app->font_ui)
    {
        SendMessageW(app->hwnd_progress_status, WM_SETFONT, (WPARAM)app->font_ui, TRUE);
        SendMessageW(btn, WM_SETFONT, (WPARAM)app->font_ui, TRUE);
    }

    EnableWindow(app->hwnd_main, FALSE);
    ShowWindow(app->hwnd_progress, SW_SHOW);
    UpdateWindow(app->hwnd_progress);
    return TRUE;
}

static void App_UpdateProgressUi(AppState *app, const EeLoadProgress *p)
{
    wchar_t text[160];

    if (app->hwnd_progress_bar)
    {
        SendMessageW(app->hwnd_progress_bar, PBM_SETPOS, p->percent, 0);
    }
    if (app->hwnd_progress_status)
    {
        StringCchPrintfW(text,
                         ARRAYSIZE(text),
                         L"Loaded %u rows… %u%%",
                         p->rows_loaded,
                         p->percent);
        SetWindowTextW(app->hwnd_progress_status, text);
    }
}

/* -------------------------------------------------------------------------- */
/* Background load                                                            */
/* -------------------------------------------------------------------------- */

static BOOL CALLBACK LoadProgressThunk(const EeLoadProgress *progress, void *user)
{
    AppState *app = (AppState *)user;

    EnterCriticalSection(&app->progress_lock);
    app->last_progress = *progress;
    app->progress_dirty = TRUE;
    LeaveCriticalSection(&app->progress_lock);

    PostMessageW(app->hwnd_main, EEM_LOAD_PROGRESS, 0, 0);

    if (InterlockedCompareExchange(&app->load_cancel, 0, 0) != 0)
    {
        return FALSE;
    }
    return TRUE;
}

static DWORD WINAPI LoadThreadProc(void *param)
{
    AppState *app = (AppState *)param;

    app->load_error[0] = L'\0';
    if (is_xlsx_path(app->load_path))
    {
        app->load_status = EeVoterTable_LoadXlsxSheet(app->load_path,
                                                      app->load_sheet_index,
                                                      &app->table,
                                                      &app->load_cancel,
                                                      LoadProgressThunk,
                                                      app,
                                                      app->load_error,
                                                      ARRAYSIZE(app->load_error));
    }
    else
    {
        app->load_status = EeVoterTable_LoadFromFile(app->load_path,
                                                     &app->table,
                                                     &app->load_cancel,
                                                     LoadProgressThunk,
                                                     app,
                                                     app->load_error,
                                                     ARRAYSIZE(app->load_error));
    }

    PostMessageW(app->hwnd_main, EEM_LOAD_FINISHED, 0, 0);
    return 0;
}

static void App_StartLoad(AppState *app, const wchar_t *path, int sheet_index)
{
    if (app == NULL || path == NULL || path[0] == L'\0' || app->loading)
    {
        return;
    }

    StringCchCopyW(app->load_path, ARRAYSIZE(app->load_path), path);
    app->load_sheet_index = (sheet_index > 0) ? sheet_index : 0;
    InterlockedExchange(&app->load_cancel, 0);
    App_ClearMarks(app);   /* prior duplicates view indexed the old table */
    App_CloseReports(app); /* reports summarize the outgoing table */
    App_CloseCompare(app); /* a compare of the outgoing table is now stale */
    App_CloseDiff(app);    /* as is a side-by-side differences view */
    app->loading = TRUE;
    App_SetStatus(app, L"Loading voter list…");

    if (!App_ShowProgress(app))
    {
        app->loading = FALSE;
        app->load_path[0] = L'\0';
        MessageBoxW(app->hwnd_main,
                    L"Could not create the progress window.",
                    k_WindowTitle,
                    MB_ICONERROR | MB_OK);
        return;
    }

    app->table.name_surname_first = app->name_surname_first;
    app->load_thread = CreateThread(NULL, 0, LoadThreadProc, app, 0, NULL);
    if (app->load_thread == NULL)
    {
        app->loading = FALSE;
        app->load_path[0] = L'\0';
        EnableWindow(app->hwnd_main, TRUE);
        App_DestroyProgress(app);
        MessageBoxW(app->hwnd_main,
                    L"Could not start the load thread.",
                    k_WindowTitle,
                    MB_ICONERROR | MB_OK);
    }
}

static void App_BeginOpenVoterList(AppState *app)
{
    OPENFILENAMEW ofn;
    wchar_t path[MAX_PATH];
    wchar_t canon[MAX_PATH];
    AppState *existing;

    if (app == NULL || app->loading)
    {
        return;
    }

    path[0] = L'\0';
    ZeroMemory(&ofn, sizeof(ofn));
    ofn.lStructSize = sizeof(ofn);
    ofn.hwndOwner = app->hwnd_main;
    ofn.lpstrFilter = L"Voter lists (*.csv;*.txt;*.xlsx;*.xls)\0*.csv;*.txt;*.xlsx;*.xls\0"
                      L"Excel workbooks (*.xlsx;*.xls)\0*.xlsx;*.xls\0"
                      L"CSV (*.csv)\0*.csv\0"
                      L"Tab-delimited (*.txt)\0*.txt\0"
                      L"All files (*.*)\0*.*\0";
    ofn.lpstrFile = path;
    ofn.nMaxFile = ARRAYSIZE(path);
    ofn.Flags = OFN_FILEMUSTEXIST | OFN_PATHMUSTEXIST | OFN_EXPLORER;
    ofn.lpstrTitle = L"Load Voter List";

    if (!GetOpenFileNameW(&ofn))
    {
        return;
    }
    if (!App_CanonicalPath(path, canon, ARRAYSIZE(canon)))
    {
        StringCchCopyW(canon, ARRAYSIZE(canon), path);
    }

    existing = App_FindViewerByPath(canon);
    if (existing != NULL)
    {
        App_ActivateViewer(existing);
        return;
    }

    /* For an Excel workbook, let the user choose which worksheet to load. */
    {
        int sheet_index = 0;
        if (is_xlsx_path(canon))
        {
            wchar_t names[EE_XLSX_MAX_SHEETS][EE_XLSX_SHEET_NAME_CCH];
            int count = 0;
            wchar_t serr[256];
            EeLoadStatus ls =
                EeXlsx_ListSheets(canon, names, EE_XLSX_MAX_SHEETS, &count, serr, ARRAYSIZE(serr));
            if (ls != EeLoadStatus_Ok || count <= 0)
            {
                MessageBoxW(app->hwnd_main,
                            (serr[0] != L'\0') ? serr : L"Could not read the Excel workbook.",
                            k_WindowTitle,
                            MB_ICONERROR | MB_OK);
                return;
            }
            if (count > 1)
            {
                sheet_index = App_PickSheet(app, names, count);
                if (sheet_index < 0)
                {
                    return; /* user cancelled */
                }
            }
        }

        /* From a CVR window (a resource-only AppState) always open a fresh viewer,
         * as its "window" is the CVR grid, not an empty voter list. */
        if (app->is_cvr_ui || app->is_roster || app->load_path[0] != L'\0' ||
            app->table.row_count > 0)
        {
            AppState *created = App_CreateViewer(app->instance, SW_SHOWNORMAL, app->hwnd_main, app);
            if (created == NULL)
            {
                MessageBoxW(app->hwnd_main,
                            L"Could not open another window.",
                            k_WindowTitle,
                            MB_ICONERROR | MB_OK);
                return;
            }
            App_StartLoad(created, canon, sheet_index);
            return;
        }

        App_StartLoad(app, canon, sheet_index);
    }
}

static void App_OnLoadFinished(AppState *app)
{
    EeLoadProgress done;

    if (app->load_thread != NULL)
    {
        WaitForSingleObject(app->load_thread, INFINITE);
        CloseHandle(app->load_thread);
        app->load_thread = NULL;
    }

    app->loading = FALSE;

    if (app->close_pending)
    {
        EnableWindow(app->hwnd_main, TRUE);
        App_DestroyProgress(app);
        DestroyWindow(app->hwnd_main);
        return;
    }

    /* 100% only when data is ready for interaction. */
    done.percent = 100;
    done.rows_loaded = app->table.row_count;
    done.bytes_read = 0;
    done.bytes_total = 0;
    App_UpdateProgressUi(app, &done);
    UpdateWindow(app->hwnd_progress);

    EnableWindow(app->hwnd_main, TRUE);
    App_DestroyProgress(app);
    SetForegroundWindow(app->hwnd_main);

    if (app->load_status == EeLoadStatus_Ok)
    {
        wchar_t title[MAX_PATH + 64];
        App_RebuildColumns(app);
        App_UpdateRowStatus(app);
        StringCchPrintfW(title, ARRAYSIZE(title), L"%s — %s", k_WindowTitle, app->load_path);
        SetWindowTextW(app->hwnd_main, title);
    }
    else if (app->load_status == EeLoadStatus_Cancelled)
    {
        EeVoterTable_Clear(&app->table);
        app->load_path[0] = L'\0';
        App_RebuildColumns(app);
        App_SetStatus(app, L"Load cancelled.");
        SetWindowTextW(app->hwnd_main, k_WindowTitle);
    }
    else
    {
        EeVoterTable_Clear(&app->table);
        app->load_path[0] = L'\0';
        App_RebuildColumns(app);
        App_SetStatus(app, L"Load failed.");
        SetWindowTextW(app->hwnd_main, k_WindowTitle);
        MessageBoxW(app->hwnd_main,
                    app->load_error[0] ? app->load_error : L"Failed to load the voter list.",
                    k_WindowTitle,
                    MB_ICONERROR | MB_OK);
    }
}

/* -------------------------------------------------------------------------- */
/* Layout                                                                     */
/* -------------------------------------------------------------------------- */

static int App_ClampFrozenWidth(AppState *app, int width, int client_w)
{
    int min_w = ScaleDisplay(app, 140);
    int max_w = client_w - app->pad * 2 - app->splitter_width - Scale(app, 120);

    if (min_w < 80)
    {
        min_w = 80;
    }
    if (max_w < min_w)
    {
        max_w = min_w;
    }
    if (width < min_w)
    {
        width = min_w;
    }
    if (width > max_w)
    {
        width = max_w;
    }
    return width;
}

/**
 * @brief Client-space rectangle of the splitter strip between left and right panes.
 */
static void App_GetSplitterRect(AppState *app, RECT *out_rc)
{
    RECT client;
    RECT sr;
    int status_h = 0;
    int grid_top;
    int grid_h;
    int frozen_w;

    ZeroMemory(out_rc, sizeof(*out_rc));
    if (app->hwnd_main == NULL || app->is_roster)
    {
        return; /* no splitter in a single-pane roster window */
    }

    GetClientRect(app->hwnd_main, &client);
    if (app->hwnd_status)
    {
        GetWindowRect(app->hwnd_status, &sr);
        status_h = sr.bottom - sr.top;
    }
    grid_top = app->pad;
    grid_h = (client.bottom - client.top) - status_h - app->pad * 2;
    if (grid_h < 0)
    {
        grid_h = 0;
    }
    frozen_w = App_ClampFrozenWidth(app, app->frozen_width, client.right - client.left);
    out_rc->left = app->pad + frozen_w;
    out_rc->top = grid_top;
    out_rc->right = out_rc->left + app->splitter_width;
    out_rc->bottom = grid_top + grid_h;
}

static BOOL App_HitTestSplitter(AppState *app, int x, int y)
{
    RECT rc;
    if (app->is_roster)
    {
        return FALSE;
    }
    App_GetSplitterRect(app, &rc);
    /* Widen hit target slightly for easier grabbing. */
    rc.left -= Scale(app, 2);
    rc.right += Scale(app, 2);
    return (x >= rc.left && x < rc.right && y >= rc.top && y < rc.bottom);
}

/**
 * @brief Thickness of a visible horizontal scrollbar, or 0 if hidden.
 */
static int App_GetVisibleHScrollThickness(HWND hwnd)
{
    SCROLLBARINFO sbi;

    if (hwnd == NULL)
    {
        return 0;
    }
    ZeroMemory(&sbi, sizeof(sbi));
    sbi.cbSize = sizeof(sbi);
    if (!GetScrollBarInfo(hwnd, OBJID_HSCROLL, &sbi))
    {
        return 0;
    }
    if ((sbi.rgstate[0] & STATE_SYSTEM_INVISIBLE) != 0)
    {
        return 0;
    }
    if (sbi.rcScrollBar.bottom <= sbi.rcScrollBar.top)
    {
        return 0;
    }
    return sbi.rcScrollBar.bottom - sbi.rcScrollBar.top;
}

/**
 * @brief Keep left/right item areas the same height when only one pane has H-scroll.
 *
 * When the left pane is wide enough that its horizontal scrollbar disappears but
 * the right pane still has one, the right pane's last row is covered by that
 * bar while the left still shows it. Match chrome by shortening the pane that
 * lacks an H-scroll and filling the gap with a scrollbar-colored pad.
 */
static void App_SyncPaneScrollChrome(AppState *app)
{
    int hsb_left;
    int hsb_right;
    int pad_left;
    int pad_right;
    int sys_hsb;
    RECT fr;
    RECT sr;
    int frozen_h;
    int scroll_h;
    int left_extent;
    int right_extent;
    int grid_bottom;
    int grid_top;
    int grid_h;

    if (app->hwnd_frozen == NULL || app->hwnd_scroll == NULL || app->is_roster)
    {
        return;
    }

    hsb_left = App_GetVisibleHScrollThickness(app->hwnd_frozen);
    hsb_right = App_GetVisibleHScrollThickness(app->hwnd_scroll);
    sys_hsb = GetSystemMetricsForDpi(SM_CYHSCROLL, app->dpi);
    if (sys_hsb < 1)
    {
        sys_hsb = GetSystemMetrics(SM_CYHSCROLL);
    }

    /*
     * If the right pane reports no H-scroll yet but has more column width than
     * its client, treat it as needing the system bar height (layout race).
     */
    if (hsb_right == 0 && app->table.column_count > EE_FROZEN_COLUMN_COUNT)
    {
        HWND header = ListView_GetHeader(app->hwnd_scroll);
        int cols = header ? Header_GetItemCount(header) : 0;
        int total = 0;
        int i;
        RECT crc;
        for (i = 0; i < cols; i++)
        {
            total += ListView_GetColumnWidth(app->hwnd_scroll, i);
        }
        GetClientRect(app->hwnd_scroll, &crc);
        if (total > (crc.right - crc.left) + 1)
        {
            hsb_right = sys_hsb;
        }
    }

    pad_left = (hsb_right > hsb_left) ? (hsb_right - hsb_left) : 0;
    pad_right = (hsb_left > hsb_right) ? (hsb_left - hsb_right) : 0;

    GetWindowRect(app->hwnd_frozen, &fr);
    GetWindowRect(app->hwnd_scroll, &sr);
    MapWindowPoints(NULL, app->hwnd_main, (POINT *)&fr, 2);
    MapWindowPoints(NULL, app->hwnd_main, (POINT *)&sr, 2);

    /* Recover full grid height from list + any existing pad (stable re-sync). */
    left_extent = fr.bottom;
    right_extent = sr.bottom;
    grid_top = fr.top;

    if (app->hwnd_frozen_hsb_pad && IsWindowVisible(app->hwnd_frozen_hsb_pad))
    {
        RECT pr;
        GetWindowRect(app->hwnd_frozen_hsb_pad, &pr);
        MapWindowPoints(NULL, app->hwnd_main, (POINT *)&pr, 2);
        if (pr.bottom > left_extent)
        {
            left_extent = pr.bottom;
        }
    }
    if (app->hwnd_scroll_hsb_pad && IsWindowVisible(app->hwnd_scroll_hsb_pad))
    {
        RECT pr;
        GetWindowRect(app->hwnd_scroll_hsb_pad, &pr);
        MapWindowPoints(NULL, app->hwnd_main, (POINT *)&pr, 2);
        if (pr.bottom > right_extent)
        {
            right_extent = pr.bottom;
        }
    }

    grid_bottom = (left_extent > right_extent) ? left_extent : right_extent;
    grid_h = grid_bottom - grid_top;
    if (grid_h < 0)
    {
        grid_h = 0;
    }

    frozen_h = grid_h - pad_left;
    scroll_h = grid_h - pad_right;
    if (frozen_h < 0)
    {
        frozen_h = 0;
    }
    if (scroll_h < 0)
    {
        scroll_h = 0;
    }

    MoveWindow(app->hwnd_frozen, fr.left, grid_top, fr.right - fr.left, frozen_h, TRUE);
    MoveWindow(app->hwnd_scroll, sr.left, grid_top, sr.right - sr.left, scroll_h, TRUE);

    if (app->hwnd_frozen_hsb_pad)
    {
        if (pad_left > 0)
        {
            MoveWindow(app->hwnd_frozen_hsb_pad,
                       fr.left,
                       grid_top + frozen_h,
                       fr.right - fr.left,
                       pad_left,
                       TRUE);
            ShowWindow(app->hwnd_frozen_hsb_pad, SW_SHOW);
        }
        else
        {
            ShowWindow(app->hwnd_frozen_hsb_pad, SW_HIDE);
        }
    }
    if (app->hwnd_scroll_hsb_pad)
    {
        if (pad_right > 0)
        {
            MoveWindow(app->hwnd_scroll_hsb_pad,
                       sr.left,
                       grid_top + scroll_h,
                       sr.right - sr.left,
                       pad_right,
                       TRUE);
            ShowWindow(app->hwnd_scroll_hsb_pad, SW_SHOW);
        }
        else
        {
            ShowWindow(app->hwnd_scroll_hsb_pad, SW_HIDE);
        }
    }
}

static void App_Layout(AppState *app)
{
    RECT rc;
    int status_h = 0;
    int client_h;
    int client_w;
    int grid_top;
    int grid_h;
    int frozen_w;
    int split_w;
    int scroll_x;
    int scroll_w;
    int title_h;
    int list_top;
    int list_h;

    if (app->hwnd_main == NULL)
    {
        return;
    }

    GetClientRect(app->hwnd_main, &rc);
    client_w = rc.right - rc.left;
    client_h = rc.bottom - rc.top;

    if (app->hwnd_status)
    {
        RECT sr;
        SendMessageW(app->hwnd_status, WM_SIZE, 0, 0);
        GetWindowRect(app->hwnd_status, &sr);
        status_h = sr.bottom - sr.top;
    }

    grid_top = app->pad;
    grid_h = client_h - status_h - app->pad * 2;
    if (grid_h < 0)
    {
        grid_h = 0;
    }

    if (app->is_roster)
    {
        /* Voter Roster window: a single pane over the whole client area (the left pane
         * stays hidden; it still mirrors the selection for the shared code paths). */
        if (app->hwnd_frozen_title)
            ShowWindow(app->hwnd_frozen_title, SW_HIDE);
        if (app->hwnd_scroll_title)
            ShowWindow(app->hwnd_scroll_title, SW_HIDE);
        if (app->hwnd_frozen)
            ShowWindow(app->hwnd_frozen, SW_HIDE);
        if (app->hwnd_frozen_hsb_pad)
            ShowWindow(app->hwnd_frozen_hsb_pad, SW_HIDE);
        if (app->hwnd_scroll_hsb_pad)
            ShowWindow(app->hwnd_scroll_hsb_pad, SW_HIDE);
        if (app->hwnd_scroll)
        {
            int w = client_w - app->pad * 2;
            MoveWindow(app->hwnd_scroll, app->pad, grid_top, (w > 0) ? w : 0, grid_h, TRUE);
        }
        return;
    }

    split_w = app->splitter_width > 0 ? app->splitter_width : Scale(app, 6);
    if (app->frozen_width <= 0)
    {
        app->frozen_width = ScaleDisplay(app, k_DefaultFrozenWidth);
    }
    /* Clamp for display only. Writing back would lock WM_CREATE's empty
     * client (or a later shrink) over the default pane width. */
    frozen_w = App_ClampFrozenWidth(app, app->frozen_width, client_w);
    scroll_x = app->pad + frozen_w + split_w;
    scroll_w = client_w - scroll_x - app->pad;
    if (scroll_w < 0)
    {
        scroll_w = 0;
    }

    title_h = App_PaneTitleHeight(app);
    list_top = grid_top + title_h;
    list_h = grid_h - title_h;
    if (list_h < 0)
    {
        list_h = 0;
    }

    if (app->hwnd_frozen_title)
    {
        MoveWindow(app->hwnd_frozen_title, app->pad, grid_top, frozen_w, title_h, TRUE);
    }
    if (app->hwnd_scroll_title)
    {
        MoveWindow(app->hwnd_scroll_title, scroll_x, grid_top, scroll_w, title_h, TRUE);
    }

    /* Place both panes at full list height so H-scroll visibility is real. */
    if (app->hwnd_frozen)
    {
        MoveWindow(app->hwnd_frozen, app->pad, list_top, frozen_w, list_h, TRUE);
    }
    if (app->hwnd_scroll)
    {
        MoveWindow(app->hwnd_scroll, scroll_x, list_top, scroll_w, list_h, TRUE);
    }
    if (app->hwnd_frozen_hsb_pad)
    {
        ShowWindow(app->hwnd_frozen_hsb_pad, SW_HIDE);
    }
    if (app->hwnd_scroll_hsb_pad)
    {
        ShowWindow(app->hwnd_scroll_hsb_pad, SW_HIDE);
    }

    App_FitFrozenColumns(app);

    /* ListView updates non-client scrollbars during size; equalize after. */
    App_SyncPaneScrollChrome(app);

    /* Defer a second pass — column layout can change H-scroll after paint. */
    if (app->hwnd_main)
    {
        PostMessageW(app->hwnd_main, EEM_SYNC_PANE_SCROLLUI, 0, 0);
    }

    /* Splitter strip is painted by the main window. */
    {
        RECT split_rc;
        App_GetSplitterRect(app, &split_rc);
        InvalidateRect(app->hwnd_main, &split_rc, TRUE);
    }
}

/* -------------------------------------------------------------------------- */
/* Create controls                                                            */
/* -------------------------------------------------------------------------- */

static BOOL App_CreateControls(AppState *app)
{
    DWORD lv_style =
        WS_CHILD | WS_VISIBLE | WS_TABSTOP | LVS_REPORT | LVS_OWNERDATA | LVS_SHOWSELALWAYS;
    DWORD lv_ex = LVS_EX_FULLROWSELECT | LVS_EX_DOUBLEBUFFER | LVS_EX_LABELTIP | LVS_EX_GRIDLINES |
                  LVS_EX_HEADERDRAGDROP;

    app->hwnd_frozen_title =
        CreateWindowExW(0,
                        L"STATIC",
                        L"Normalized Data",
                        WS_CHILD | WS_VISIBLE | SS_CENTER | SS_CENTERIMAGE | SS_NOPREFIX,
                        0,
                        0,
                        0,
                        0,
                        app->hwnd_main,
                        (HMENU)(INT_PTR)IDC_PANE_TITLE_FROZEN,
                        app->instance,
                        NULL);
    app->hwnd_scroll_title =
        CreateWindowExW(0,
                        L"STATIC",
                        L"File Data",
                        WS_CHILD | WS_VISIBLE | SS_CENTER | SS_CENTERIMAGE | SS_NOPREFIX,
                        0,
                        0,
                        0,
                        0,
                        app->hwnd_main,
                        (HMENU)(INT_PTR)IDC_PANE_TITLE_SCROLL,
                        app->instance,
                        NULL);
    app->hwnd_frozen = CreateWindowExW(WS_EX_CLIENTEDGE,
                                       WC_LISTVIEWW,
                                       L"",
                                       lv_style | WS_VSCROLL | WS_HSCROLL,
                                       0,
                                       0,
                                       0,
                                       0,
                                       app->hwnd_main,
                                       (HMENU)(INT_PTR)IDC_LIST_FROZEN,
                                       app->instance,
                                       NULL);
    app->hwnd_scroll = CreateWindowExW(WS_EX_CLIENTEDGE,
                                       WC_LISTVIEWW,
                                       L"",
                                       lv_style | WS_VSCROLL | WS_HSCROLL,
                                       0,
                                       0,
                                       0,
                                       0,
                                       app->hwnd_main,
                                       (HMENU)(INT_PTR)IDC_LIST_SCROLL,
                                       app->instance,
                                       NULL);
    /* Pads match the other pane's H-scroll thickness when this pane has none. */
    app->hwnd_frozen_hsb_pad = CreateWindowExW(0,
                                               L"STATIC",
                                               L"",
                                               WS_CHILD,
                                               0,
                                               0,
                                               0,
                                               0,
                                               app->hwnd_main,
                                               NULL,
                                               app->instance,
                                               NULL);
    app->hwnd_scroll_hsb_pad = CreateWindowExW(0,
                                               L"STATIC",
                                               L"",
                                               WS_CHILD,
                                               0,
                                               0,
                                               0,
                                               0,
                                               app->hwnd_main,
                                               NULL,
                                               app->instance,
                                               NULL);
    app->hwnd_status = CreateWindowExW(0,
                                       STATUSCLASSNAMEW,
                                       L"",
                                       WS_CHILD | WS_VISIBLE | SBARS_SIZEGRIP,
                                       0,
                                       0,
                                       0,
                                       0,
                                       app->hwnd_main,
                                       (HMENU)(INT_PTR)IDC_STATUS_BAR,
                                       app->instance,
                                       NULL);

    if (!app->hwnd_frozen || !app->hwnd_scroll || !app->hwnd_status || !app->hwnd_frozen_title ||
        !app->hwnd_scroll_title)
    {
        return FALSE;
    }

    ListView_SetExtendedListViewStyle(app->hwnd_frozen, lv_ex);
    ListView_SetExtendedListViewStyle(app->hwnd_scroll, lv_ex);
    /* Classic item theme so selection is the system highlight on the full row,
     * not a blue focus cell with a grey remainder. */
    SetWindowTheme(app->hwnd_frozen, L"", L"");
    SetWindowTheme(app->hwnd_scroll, L"", L"");

    /* Prefer no H-scroll until content needs it; pad logic equalizes heights. */
    ShowScrollBar(app->hwnd_frozen, SB_HORZ, FALSE);

    App_ApplyFont(app);
    App_SetStatus(app, L"No voter list loaded.");
    return TRUE;
}

static HMENU App_CreateMenu(void)
{
    HMENU menu = CreateMenu();
    HMENU file_menu = CreatePopupMenu();
    HMENU edit_menu = CreatePopupMenu();
    AppendMenuW(file_menu, MF_STRING, IDM_FILE_OPEN_VOTER_LIST, L"&Load Voter List…\tCtrl+O");
    AppendMenuW(file_menu, MF_STRING, IDM_FILE_OPEN_CVR, L"Load Cast &Vote Records…");
    AppendMenuW(file_menu, MF_STRING, IDM_FILE_OPEN_ROSTER, L"Load Voter &Roster…");
    AppendMenuW(file_menu, MF_STRING, IDM_FILE_OPEN_RESULTS, L"Load Election Re&sults…");
    AppendMenuW(file_menu, MF_STRING, IDM_FILE_OPEN_AUDIT, L"Load &Audit Logs…");
    AppendMenuW(file_menu, MF_STRING, IDM_FILE_CLOSE_VOTER_LIST, L"&Close Voter List");
    AppendMenuW(file_menu, MF_STRING, IDM_FILE_EXPORT_VOTERS, L"&Export Voter List…");
    AppendMenuW(file_menu, MF_SEPARATOR, 0, NULL);
    AppendMenuW(file_menu, MF_STRING, IDM_FILE_EXIT, L"E&xit");
    AppendMenuW(edit_menu, MF_STRING, IDM_EDIT_COPY, L"&Copy\tCtrl+C");
    AppendMenuW(edit_menu, MF_SEPARATOR, 0, NULL);
    AppendMenuW(edit_menu, MF_STRING, IDM_EDIT_OPTIONS, L"&Options…");
    HMENU filter_menu = CreatePopupMenu();
    AppendMenuW(filter_menu, MF_STRING, IDM_FILTER_EDIT, L"&Filter…\tCtrl+L");
    AppendMenuW(filter_menu, MF_STRING, IDM_FILTER_RESET, L"&Reset Filter");
    AppendMenuW(filter_menu, MF_STRING, IDM_FILTER_DUP_VOTER_IDS, L"Show &Duplicate Voter IDs…");
    AppendMenuW(filter_menu, MF_STRING, IDM_FILTER_DUP_VOTERS, L"Show Duplicate &Voters…");
    AppendMenuW(filter_menu, MF_STRING, IDM_FILTER_DUP_VOTERS_VOTING, L"Show Duplicate Voters V&oting…");
    AppendMenuW(filter_menu, MF_SEPARATOR, 0, NULL);
    AppendMenuW(filter_menu, MF_STRING, IDM_FILTER_RESET_VIEW, L"Reset Vie&w…");
    HMENU reports_menu = CreatePopupMenu();
    AppendMenuW(reports_menu, MF_STRING, IDM_REPORT_PRECINCT, L"Display &Precinct Report…");
    AppendMenuW(reports_menu, MF_STRING, IDM_REPORT_ADDRESS, L"Display &Address Report…");
    /* Compare is populated on demand (WM_INITMENUPOPUP) with the other open files.
     * It MUST stay at index k_CompareMenuPos in the bar. */
    HMENU compare_menu = CreatePopupMenu();
    HMENU help_menu = CreatePopupMenu();
    AppendMenuW(help_menu, MF_STRING, IDM_HELP_LOADING, L"&Loading Voter Lists");
    AppendMenuW(help_menu, MF_STRING, IDM_HELP_OPTIONS, L"&Options");
    AppendMenuW(help_menu, MF_STRING, IDM_HELP_FILTERS, L"&Filters");
    AppendMenuW(help_menu, MF_STRING, IDM_HELP_REPORTS, L"&Reports");
    AppendMenuW(help_menu, MF_STRING, IDM_HELP_EXPORT, L"&Export");
    AppendMenuW(help_menu, MF_STRING, IDM_HELP_COMPARE, L"&Compare");
    AppendMenuW(help_menu, MF_SEPARATOR, 0, NULL);
    AppendMenuW(help_menu, MF_STRING, IDM_HELP_ABOUT, L"&About Election Explorer…");
    AppendMenuW(menu, MF_POPUP, (UINT_PTR)file_menu, L"&File");
    AppendMenuW(menu, MF_POPUP, (UINT_PTR)edit_menu, L"&Edit");
    AppendMenuW(menu, MF_POPUP, (UINT_PTR)filter_menu, L"F&ilter");
    AppendMenuW(menu, MF_POPUP, (UINT_PTR)reports_menu, L"&Reports");
    AppendMenuW(menu, MF_POPUP, (UINT_PTR)compare_menu, L"&Compare");
    AppendMenuW(menu, MF_POPUP, (UINT_PTR)help_menu, L"&Help");
    return menu;
}

/* -------------------------------------------------------------------------- */
/* Selection, copy, options                                                   */
/* -------------------------------------------------------------------------- */

static BOOL App_HasSelection(const AppState *app)
{
    if (app == NULL || app->hwnd_frozen == NULL || app->table.row_count == 0)
    {
        return FALSE;
    }
    return ListView_GetNextItem(app->hwnd_frozen, -1, LVNI_SELECTED) >= 0;
}

static void App_RequestSelectionSync(AppState *app, HWND source)
{
    if (app == NULL || source == NULL || InterlockedCompareExchange(&app->sel_syncing, 0, 0) != 0)
    {
        return;
    }
    app->sel_sync_source = source;
    if (InterlockedCompareExchange(&app->sel_sync_posted, 1, 0) == 0)
    {
        PostMessageW(app->hwnd_main, EEM_SYNC_SELECTION, 0, 0);
    }
}

/**
 * Copy the source pane's selected set onto the other pane. Owner-data
 * list views often omit per-item deselect notifications, so a one-item
 * mirror leaves stale highlights behind.
 */
static void App_CopySelectionToOtherPane(AppState *app, HWND source)
{
    HWND dest;
    int i;

    if (app == NULL || source == NULL)
    {
        return;
    }
    dest = (source == app->hwnd_frozen) ? app->hwnd_scroll : app->hwnd_frozen;
    if (dest == NULL)
    {
        return;
    }

    InterlockedExchange(&app->sel_syncing, 1);
    ListView_SetItemState(dest, -1, 0, LVIS_SELECTED | LVIS_FOCUSED);
    i = -1;
    while ((i = ListView_GetNextItem(source, i, LVNI_SELECTED)) >= 0)
    {
        ListView_SetItemState(dest, i, LVIS_SELECTED, LVIS_SELECTED);
    }
    InterlockedExchange(&app->sel_syncing, 0);
    InvalidateRect(dest, NULL, TRUE);
}

static void App_ClearSelection(AppState *app)
{
    if (app == NULL)
    {
        return;
    }
    InterlockedExchange(&app->sel_syncing, 1);
    if (app->hwnd_frozen != NULL)
    {
        ListView_SetItemState(app->hwnd_frozen, -1, 0, LVIS_SELECTED | LVIS_FOCUSED);
    }
    if (app->hwnd_scroll != NULL)
    {
        ListView_SetItemState(app->hwnd_scroll, -1, 0, LVIS_SELECTED | LVIS_FOCUSED);
    }
    InterlockedExchange(&app->sel_syncing, 0);
}

static BOOL App_SetClipboardUtf8(HWND hwnd, const char *utf8)
{
    int wlen;
    SIZE_T bytes;
    HGLOBAL mem = NULL;
    wchar_t *locked = NULL;
    BOOL ok = FALSE;

    if (hwnd == NULL || utf8 == NULL)
    {
        return FALSE;
    }

    wlen = MultiByteToWideChar(CP_UTF8, 0, utf8, -1, NULL, 0);
    if (wlen <= 0)
    {
        wlen = MultiByteToWideChar(CP_ACP, 0, utf8, -1, NULL, 0);
    }
    if (wlen <= 0)
    {
        return FALSE;
    }
    if (FAILED(SizeTMult((size_t)wlen, sizeof(wchar_t), &bytes)))
    {
        return FALSE;
    }
    if (!OpenClipboard(hwnd))
    {
        return FALSE;
    }
    EmptyClipboard();

    mem = GlobalAlloc(GMEM_MOVEABLE, bytes);
    if (mem == NULL)
    {
        goto cleanup;
    }
    locked = (wchar_t *)GlobalLock(mem);
    if (locked == NULL)
    {
        goto cleanup;
    }
    if (MultiByteToWideChar(CP_UTF8, 0, utf8, -1, locked, wlen) == 0 &&
        MultiByteToWideChar(CP_ACP, 0, utf8, -1, locked, wlen) == 0)
    {
        goto cleanup;
    }
    GlobalUnlock(mem);
    locked = NULL;
    if (SetClipboardData(CF_UNICODETEXT, mem) == NULL)
    {
        goto cleanup;
    }
    mem = NULL;
    ok = TRUE;

cleanup:
    if (locked != NULL && mem != NULL)
    {
        GlobalUnlock(mem);
    }
    if (mem != NULL)
    {
        GlobalFree(mem);
    }
    CloseClipboard();
    return ok;
}

static void App_CopySelection(AppState *app)
{
    uint32_t *rows = NULL;
    uint32_t n = 0;
    uint32_t cap = 0;
    int i;
    char *text = NULL;
    wchar_t status[80];

    if (app == NULL || app->loading || app->table.row_count == 0)
    {
        return;
    }

    i = ListView_GetNextItem(app->hwnd_frozen, -1, LVNI_SELECTED);
    while (i >= 0)
    {
        if (n == cap)
        {
            uint32_t new_cap = cap ? cap * 2u : 64u;
            uint32_t *grown;
            size_t bytes;
            if (new_cap < cap)
            {
                goto fail;
            }
            if (FAILED(SizeTMult((size_t)new_cap, sizeof(uint32_t), &bytes)))
            {
                goto fail;
            }
            grown = (uint32_t *)realloc(rows, bytes);
            if (grown == NULL)
            {
                goto fail;
            }
            rows = grown;
            cap = new_cap;
        }
        rows[n++] = App_ViewRowFromDisplay(app, (uint32_t)i);
        i = ListView_GetNextItem(app->hwnd_frozen, i, LVNI_SELECTED);
    }

    if (n == 0)
    {
        App_SetStatus(app, L"Nothing selected to copy.");
        free(rows);
        return;
    }

    if (!EeVoterTable_FormatCopyUtf8(&app->table,
                                     rows,
                                     n,
                                     app->copy_prepend_normalized && !app->is_roster,
                                     &text,
                                     NULL))
    {
        goto fail;
    }
    if (!App_SetClipboardUtf8(app->hwnd_main, text))
    {
        goto fail;
    }

    StringCchPrintfW(status,
                     ARRAYSIZE(status),
                     L"Copied %u row%s to the clipboard.",
                     n,
                     n == 1u ? L"" : L"s");
    App_SetStatus(app, status);
    free(text);
    free(rows);
    return;

fail:
    free(text);
    free(rows);
    MessageBoxW(app->hwnd_main,
                L"Could not copy the selection to the clipboard.",
                k_WindowTitle,
                MB_ICONERROR | MB_OK);
}

static void App_ShowCopyContextMenu(AppState *app, HWND hwnd_list, int screen_x, int screen_y)
{
    POINT pt;
    LVHITTESTINFO ht;
    HMENU menu;
    UINT cmd;

    if (app == NULL || hwnd_list == NULL || app->loading)
    {
        return;
    }

    if (screen_x == -1 && screen_y == -1)
    {
        int item = ListView_GetNextItem(hwnd_list, -1, LVNI_FOCUSED);
        RECT rc;
        if (item < 0)
        {
            item = ListView_GetNextItem(hwnd_list, -1, LVNI_SELECTED);
        }
        if (item >= 0 && ListView_GetItemRect(hwnd_list, item, &rc, LVIR_LABEL))
        {
            pt.x = rc.left + Scale(app, 8);
            pt.y = rc.bottom;
            ClientToScreen(hwnd_list, &pt);
        }
        else if (!GetCursorPos(&pt))
        {
            return;
        }
    }
    else
    {
        pt.x = screen_x;
        pt.y = screen_y;
        ht.pt = pt;
        ScreenToClient(hwnd_list, &ht.pt);
        ListView_HitTest(hwnd_list, &ht);
        if (ht.iItem < 0)
        {
            return;
        }
        if ((ListView_GetItemState(hwnd_list, ht.iItem, LVIS_SELECTED) & LVIS_SELECTED) == 0)
        {
            int sel = ListView_GetNextItem(hwnd_list, -1, LVNI_SELECTED);
            while (sel >= 0)
            {
                int next = ListView_GetNextItem(hwnd_list, sel, LVNI_SELECTED);
                ListView_SetItemState(hwnd_list, sel, 0, LVIS_SELECTED);
                sel = next;
            }
            ListView_SetItemState(hwnd_list,
                                  ht.iItem,
                                  LVIS_SELECTED | LVIS_FOCUSED,
                                  LVIS_SELECTED | LVIS_FOCUSED);
        }
    }

    if (!App_HasSelection(app))
    {
        return;
    }

    menu = CreatePopupMenu();
    if (menu == NULL)
    {
        return;
    }
    AppendMenuW(menu, MF_STRING, IDM_EDIT_COPY, L"&Copy");

    {
        LVHITTESTINFO sub;
        POINT client = pt;
        uint32_t hit_col = 0;
        wchar_t hit_value[2048];
        wchar_t link_url[2048];
        BOOL have_cell = FALSE;
        BOOL have_address = FALSE;
        BOOL have_link = FALSE;

        hit_value[0] = L'\0';
        link_url[0] = L'\0';
        ScreenToClient(hwnd_list, &client);
        ZeroMemory(&sub, sizeof(sub));
        sub.pt = client;
        ListView_SubItemHitTest(hwnd_list, &sub);
        if (sub.iItem >= 0)
        {
            if (hwnd_list == app->hwnd_frozen)
            {
                hit_col = (uint32_t)sub.iSubItem;
            }
            else
            {
                hit_col = App_ScrollToTable(app, sub.iSubItem);
            }
            if (hit_col < app->table.column_count)
            {
                uint32_t view_row = App_ViewRowFromDisplay(app, (uint32_t)sub.iItem);
                EeVoterTable_GetViewCellW(&app->table,
                                          view_row,
                                          hit_col,
                                          hit_value,
                                          ARRAYSIZE(hit_value));
                if (hit_value[0] != L'\0')
                {
                    have_cell = TRUE;
                    if (hwnd_list == app->hwnd_frozen && hit_col == EE_COL_ADDRESS)
                    {
                        have_address = TRUE;
                    }
                    if (hwnd_list == app->hwnd_scroll &&
                        App_ExtractHttpsUrl(hit_value, link_url, ARRAYSIZE(link_url)))
                    {
                        have_link = TRUE;
                    }
                }
            }
        }

        if (have_cell)
        {
            wchar_t shown[48];
            wchar_t inc[96];
            wchar_t exc[96];
            StringCchCopyW(shown, ARRAYSIZE(shown), hit_value);
            if (wcslen(hit_value) >= 40)
            {
                shown[36] = L'.';
                shown[37] = L'.';
                shown[38] = L'.';
                shown[39] = L'\0';
            }
            StringCchPrintfW(inc, ARRAYSIZE(inc), L"&Include \"%s\"", shown);
            StringCchPrintfW(exc, ARRAYSIZE(exc), L"&Exclude \"%s\"", shown);
            AppendMenuW(menu, MF_SEPARATOR, 0, NULL);
            AppendMenuW(menu, MF_STRING, IDM_FILTER_INCLUDE, inc);
            AppendMenuW(menu, MF_STRING, IDM_FILTER_EXCLUDE, exc);
        }
        if (have_address)
        {
            AppendMenuW(menu, MF_SEPARATOR, 0, NULL);
            AppendMenuW(menu, MF_STRING, IDM_SHOW_IN_MAPS, L"Show in &Maps…");
        }
        if (have_link)
        {
            AppendMenuW(menu, MF_SEPARATOR, 0, NULL);
            AppendMenuW(menu, MF_STRING, IDM_OPEN_LINK, L"Open &link…");
        }
        AppendMenuW(menu, MF_SEPARATOR, 0, NULL);
        AppendMenuW(menu, MF_STRING, IDM_FILTER_EDIT, L"&Filter…");
        AppendMenuW(menu, MF_SEPARATOR, 0, NULL);
        AppendMenuW(menu, MF_STRING, IDM_EXPORT_SELECTED, L"Export &Selected…");

        cmd = (UINT)TrackPopupMenu(menu,
                                   TPM_RIGHTBUTTON | TPM_RETURNCMD,
                                   pt.x,
                                   pt.y,
                                   0,
                                   app->hwnd_main,
                                   NULL);
        DestroyMenu(menu);
        if (cmd == IDM_EDIT_COPY)
        {
            App_CopySelection(app);
        }
        else if (cmd == IDM_EXPORT_SELECTED)
        {
            App_ExportVoters(app, TRUE);
        }
        else if (cmd == IDM_FILTER_INCLUDE && have_cell)
        {
            App_AddQuickFilter(app, hit_col, hit_value, EeFilt_Include);
        }
        else if (cmd == IDM_FILTER_EXCLUDE && have_cell)
        {
            App_AddQuickFilter(app, hit_col, hit_value, EeFilt_Exclude);
        }
        else if (cmd == IDM_SHOW_IN_MAPS && have_address)
        {
            App_ShowAddressInMaps(app, hit_value);
        }
        else if (cmd == IDM_OPEN_LINK && have_link)
        {
            App_OpenHttpsLink(app, link_url);
        }
        else if (cmd == IDM_FILTER_EDIT)
        {
            if (!App_ShowFilter(app))
            {
                MessageBoxW(app->hwnd_main,
                            L"Could not open the filter window.",
                            k_WindowTitle,
                            MB_ICONERROR | MB_OK);
            }
        }
    }
}

static void App_PumpUi(void)
{
    MSG msg;
    while (PeekMessageW(&msg, NULL, 0, 0, PM_REMOVE))
    {
        if (msg.message == WM_QUIT)
        {
            PostQuitMessage((int)msg.wParam);
            break;
        }
        if (App_RouteDialogMessage(&msg))
        {
            continue;
        }
        TranslateMessage(&msg);
        DispatchMessageW(&msg);
    }
}

static BOOL CALLBACK NameProgressThunk(const EeLoadProgress *progress, void *user)
{
    AppState *app = (AppState *)user;
    wchar_t text[160];

    if (app->hwnd_progress_bar)
    {
        SendMessageW(app->hwnd_progress_bar, PBM_SETPOS, progress->percent, 0);
    }
    if (app->hwnd_progress_status)
    {
        StringCchPrintfW(text, ARRAYSIZE(text), L"Updating names… %u%%", progress->percent);
        SetWindowTextW(app->hwnd_progress_status, text);
    }
    UpdateWindow(app->hwnd_progress);
    App_PumpUi();
    return TRUE;
}

static void App_ApplyNameFormat(AppState *app, BOOL surname_first)
{
    BOOL show_progress = FALSE;
    HCURSOR prev;

    app->name_surname_first = surname_first;
    if (app->table.row_count == 0)
    {
        app->table.name_surname_first = surname_first;
        return;
    }
    if (app->table.name_surname_first == surname_first)
    {
        return;
    }

    show_progress = app->table.row_count >= k_NameUpdateProgressMinRows;
    prev = SetCursor(LoadCursorW(NULL, IDC_WAIT));
    if (show_progress)
    {
        if (App_ShowProgress(app))
        {
            SetWindowTextW(app->hwnd_progress, L"Updating names");
            if (app->hwnd_progress_status)
            {
                SetWindowTextW(app->hwnd_progress_status, L"Rebuilding Name column…");
            }
            EnableWindow(GetDlgItem(app->hwnd_progress, IDC_PROGRESS_CANCEL), FALSE);
        }
        else
        {
            show_progress = FALSE;
        }
    }

    if (!EeVoterTable_SetNameSurnameFirst(&app->table,
                                          surname_first,
                                          show_progress ? NameProgressThunk : NULL,
                                          app))
    {
        if (show_progress)
        {
            EnableWindow(app->hwnd_main, TRUE);
            App_DestroyProgress(app);
        }
        SetCursor(prev);
        App_ClearSelection(app);
        MessageBoxW(app->hwnd_main,
                    L"Could not update the Name column.",
                    k_WindowTitle,
                    MB_ICONERROR | MB_OK);
        return;
    }

    if (show_progress)
    {
        EnableWindow(app->hwnd_main, TRUE);
        App_DestroyProgress(app);
    }
    SetCursor(prev);
    /* View indices no longer match the previous physical rows after a rebuild. */
    App_ClearSelection(app);
    InvalidateRect(app->hwnd_frozen, NULL, TRUE);
    InvalidateRect(app->hwnd_scroll, NULL, TRUE);
    App_FitFrozenColumns(app);
    App_ApplyFilter(app);
    App_SetStatus(app,
                  surname_first ? L"Names shown surname-first." : L"Names shown given-name first.");
}

static void App_RescaleScrollColumns(AppState *app, int old_zoom, int new_zoom)
{
    HWND header;
    int count;
    int i;

    if (app == NULL || app->hwnd_scroll == NULL || old_zoom <= 0 || new_zoom <= 0 ||
        old_zoom == new_zoom)
    {
        return;
    }
    header = ListView_GetHeader(app->hwnd_scroll);
    if (header == NULL)
    {
        return;
    }
    count = Header_GetItemCount(header);
    for (i = 0; i < count; i++)
    {
        int cx = ListView_GetColumnWidth(app->hwnd_scroll, i);
        int ncx = MulDiv(cx, new_zoom, old_zoom);
        if (ncx < 16)
        {
            ncx = 16;
        }
        ListView_SetColumnWidth(app->hwnd_scroll, i, ncx);
    }
}

static void App_ApplyZoom(AppState *app, int zoom_percent)
{
    int old_zoom;
    int new_zoom;
    wchar_t status[64];

    if (app == NULL)
    {
        return;
    }
    new_zoom = App_ClampZoom(zoom_percent);
    old_zoom = app->zoom_percent > 0 ? app->zoom_percent : k_ZoomDefault;
    if (old_zoom == new_zoom)
    {
        return;
    }

    app->zoom_percent = new_zoom;
    if (app->frozen_width > 0 && old_zoom > 0)
    {
        app->frozen_width = MulDiv(app->frozen_width, new_zoom, old_zoom);
    }
    App_UpdateDpiMetrics(app, app->dpi);
    App_ApplyFont(app);
    App_RescaleScrollColumns(app, old_zoom, new_zoom);
    App_Layout(app);
    if (app->hwnd_frozen)
    {
        InvalidateRect(app->hwnd_frozen, NULL, TRUE);
    }
    if (app->hwnd_scroll)
    {
        InvalidateRect(app->hwnd_scroll, NULL, TRUE);
    }
    if (app->hwnd_frozen_title)
    {
        InvalidateRect(app->hwnd_frozen_title, NULL, TRUE);
    }
    if (app->hwnd_scroll_title)
    {
        InvalidateRect(app->hwnd_scroll_title, NULL, TRUE);
    }
    StringCchPrintfW(status, ARRAYSIZE(status), L"Zoom: %d%%", new_zoom);
    App_SetStatus(app, status);
}

static void App_DestroyOptions(AppState *app)
{
    if (app != NULL && app->hwnd_options != NULL)
    {
        DestroyWindow(app->hwnd_options);
        app->hwnd_options = NULL;
    }
}

static LRESULT CALLBACK OptionsWndProc(HWND hwnd, UINT msg, WPARAM wParam, LPARAM lParam)
{
    AppState *app = (AppState *)GetWindowLongPtrW(hwnd, GWLP_USERDATA);

    switch (msg)
    {
        case WM_CREATE:
        {
            CREATESTRUCTW *cs = (CREATESTRUCTW *)lParam;
            app = (AppState *)cs->lpCreateParams;
            SetWindowLongPtrW(hwnd, GWLP_USERDATA, (LONG_PTR)app);
            return 0;
        }

        case WM_COMMAND:
            if (app == NULL)
            {
                return 0;
            }
            switch (LOWORD(wParam))
            {
                case IDOK:
                {
                    HWND chk_pre = GetDlgItem(hwnd, IDC_OPT_PREPEND);
                    HWND chk_sur = GetDlgItem(hwnd, IDC_OPT_SURNAME_FIRST);
                    BOOL surname_first;
                    app->copy_prepend_normalized =
                        (chk_pre != NULL &&
                         SendMessageW(chk_pre, BM_GETCHECK, 0, 0) == BST_CHECKED);
                    surname_first = (chk_sur != NULL &&
                                     SendMessageW(chk_sur, BM_GETCHECK, 0, 0) == BST_CHECKED);
                    {
                        BOOL parsed = FALSE;
                        UINT zoom = GetDlgItemInt(hwnd, IDC_OPT_ZOOM_EDIT, &parsed, FALSE);
                        HWND cmb_map = GetDlgItem(hwnd, IDC_OPT_MAP_ENGINE);
                        int map_sel =
                            (cmb_map != NULL) ? (int)SendMessageW(cmb_map, CB_GETCURSEL, 0, 0) : 0;
                        if (!parsed)
                        {
                            zoom =
                                (UINT)(app->zoom_percent > 0 ? app->zoom_percent : k_ZoomDefault);
                        }
                        if (cmb_map != NULL && map_sel >= 0)
                        {
                            app->map_engine = App_ClampMapEngine(
                                (int)SendMessageW(cmb_map, CB_GETITEMDATA, (WPARAM)map_sel, 0));
                        }
                        if (app->hwnd_progress == NULL)
                        {
                            EnableWindow(app->hwnd_main, TRUE);
                        }
                        DestroyWindow(hwnd);
                        App_ApplyNameFormat(app, surname_first);
                        App_ApplyZoom(app, (int)zoom);

                        /* Persist the committed options for the next run. */
                        g_settings.copy_prepend_normalized = app->copy_prepend_normalized;
                        g_settings.name_surname_first = app->name_surname_first;
                        g_settings.zoom_percent = app->zoom_percent;
                        g_settings.map_engine = (int)app->map_engine;
                        EeSettings_Save(&g_settings);
                    }
                    return 0;
                }
                case IDCANCEL:
                    if (app->hwnd_progress == NULL)
                    {
                        EnableWindow(app->hwnd_main, TRUE);
                    }
                    DestroyWindow(hwnd);
                    return 0;
                default:
                    break;
            }
            return 0;

        case WM_CLOSE:
            if (app != NULL && app->hwnd_progress == NULL)
            {
                EnableWindow(app->hwnd_main, TRUE);
            }
            DestroyWindow(hwnd);
            return 0;

        case WM_DESTROY:
            if (app != NULL && app->hwnd_options == hwnd)
            {
                app->hwnd_options = NULL;
            }
            return 0;

        default:
            break;
    }
    return DefWindowProcW(hwnd, msg, wParam, lParam);
}

static BOOL App_ShowOptions(AppState *app)
{
    RECT rc_main;
    RECT rc_wnd;
    RECT rc_client;
    const DWORD style = WS_POPUP | WS_CAPTION | WS_SYSMENU;
    const DWORD ex_style = WS_EX_DLGMODALFRAME | WS_EX_TOPMOST | WS_EX_CONTROLPARENT;
    int client_w;
    int client_h;
    int margin;
    int btn_w;
    int btn_h;
    int outer_w;
    int outer_h;
    int x;
    int y;
    HWND chk_prepend;
    HWND chk_surname;
    HWND lbl_zoom;
    HWND edit_zoom;
    HWND spin_zoom;
    HWND lbl_pct;
    HWND lbl_map;
    HWND cmb_map;
    HWND btn_ok;
    HWND btn_cancel;

    if (app == NULL)
    {
        return FALSE;
    }
    if (app->hwnd_options != NULL)
    {
        SetForegroundWindow(app->hwnd_options);
        return TRUE;
    }

    client_w = Scale(app, 420);
    client_h = Scale(app, app->is_roster ? 150 : 250);
    margin = Scale(app, 16);
    btn_w = Scale(app, 90);
    btn_h = Scale(app, 28);

    rc_wnd.left = 0;
    rc_wnd.top = 0;
    rc_wnd.right = client_w;
    rc_wnd.bottom = client_h;
    if (!AdjustWindowRectExForDpi(&rc_wnd, style, FALSE, ex_style, app->dpi))
    {
        rc_wnd.left = 0;
        rc_wnd.top = 0;
        rc_wnd.right = client_w + Scale(app, 16);
        rc_wnd.bottom = client_h + Scale(app, 40);
    }
    outer_w = rc_wnd.right - rc_wnd.left;
    outer_h = rc_wnd.bottom - rc_wnd.top;

    GetWindowRect(app->hwnd_main, &rc_main);
    x = rc_main.left + ((rc_main.right - rc_main.left) - outer_w) / 2;
    y = rc_main.top + ((rc_main.bottom - rc_main.top) - outer_h) / 2;

    app->hwnd_options = CreateWindowExW(ex_style,
                                        k_OptionsClassName,
                                        L"Options",
                                        style,
                                        x,
                                        y,
                                        outer_w,
                                        outer_h,
                                        app->hwnd_main,
                                        NULL,
                                        app->instance,
                                        app);
    if (app->hwnd_options == NULL)
    {
        return FALSE;
    }

    GetClientRect(app->hwnd_options, &rc_client);
    client_w = rc_client.right - rc_client.left;
    client_h = rc_client.bottom - rc_client.top;

    chk_prepend =
        CreateWindowExW(0,
                        L"BUTTON",
                        L"Pre-pend normalized data for copies (ID, Precinct, Name, Address)",
                        WS_CHILD | (app->is_roster ? 0 : WS_VISIBLE) | WS_TABSTOP | BS_AUTOCHECKBOX,
                        margin,
                        margin,
                        client_w - margin * 2,
                        Scale(app, 24),
                        app->hwnd_options,
                        (HMENU)(INT_PTR)IDC_OPT_PREPEND,
                        app->instance,
                        NULL);
    chk_surname = CreateWindowExW(0,
                                  L"BUTTON",
                                  L"Display name in surname-first format",
                                  WS_CHILD | WS_VISIBLE | WS_TABSTOP | BS_AUTOCHECKBOX,
                                  margin,
                                  app->is_roster ? margin : margin + Scale(app, 28),
                                  client_w - margin * 2,
                                  Scale(app, 24),
                                  app->hwnd_options,
                                  (HMENU)(INT_PTR)IDC_OPT_SURNAME_FIRST,
                                  app->instance,
                                  NULL);
    {
        int zy = margin + Scale(app, app->is_roster ? 36 : 64);
        int zh = Scale(app, 24);
        int edit_x = margin + Scale(app, 56);
        int edit_w = Scale(app, 56);
        wchar_t zoom_text[16];

        lbl_zoom = CreateWindowExW(0,
                                   L"STATIC",
                                   L"Zoom:",
                                   WS_CHILD | WS_VISIBLE | SS_LEFT | SS_CENTERIMAGE,
                                   margin,
                                   zy,
                                   Scale(app, 52),
                                   zh,
                                   app->hwnd_options,
                                   NULL,
                                   app->instance,
                                   NULL);
        StringCchPrintfW(zoom_text,
                         ARRAYSIZE(zoom_text),
                         L"%d",
                         App_ClampZoom(app->zoom_percent > 0 ? app->zoom_percent : k_ZoomDefault));
        edit_zoom = CreateWindowExW(WS_EX_CLIENTEDGE,
                                    L"EDIT",
                                    zoom_text,
                                    WS_CHILD | WS_VISIBLE | WS_TABSTOP | ES_NUMBER | ES_RIGHT,
                                    edit_x,
                                    zy,
                                    edit_w,
                                    zh,
                                    app->hwnd_options,
                                    (HMENU)(INT_PTR)IDC_OPT_ZOOM_EDIT,
                                    app->instance,
                                    NULL);
        spin_zoom = CreateWindowExW(0,
                                    UPDOWN_CLASSW,
                                    NULL,
                                    WS_CHILD | WS_VISIBLE | UDS_ALIGNRIGHT | UDS_ARROWKEYS |
                                        UDS_SETBUDDYINT | UDS_NOTHOUSANDS,
                                    0,
                                    0,
                                    0,
                                    0,
                                    app->hwnd_options,
                                    NULL,
                                    app->instance,
                                    NULL);
        lbl_pct = CreateWindowExW(0,
                                  L"STATIC",
                                  L"%  (50–250)",
                                  WS_CHILD | WS_VISIBLE | SS_LEFT | SS_CENTERIMAGE,
                                  edit_x + edit_w + Scale(app, 8),
                                  zy,
                                  Scale(app, 90),
                                  zh,
                                  app->hwnd_options,
                                  NULL,
                                  app->instance,
                                  NULL);
        if (spin_zoom != NULL)
        {
            SendMessageW(spin_zoom, UDM_SETRANGE32, (WPARAM)k_ZoomMin, (LPARAM)k_ZoomMax);
            SendMessageW(spin_zoom, UDM_SETBUDDY, (WPARAM)edit_zoom, 0);
            SendMessageW(
                spin_zoom,
                UDM_SETPOS32,
                0,
                (LPARAM)App_ClampZoom(app->zoom_percent > 0 ? app->zoom_percent : k_ZoomDefault));
        }
    }
    {
        int my = margin + Scale(app, 100);
        int mh = Scale(app, 24);
        int i;
        static const EeMapEngine engines[] = {EeMap_Google,
                                              EeMap_Bing,
                                              EeMap_Apple,
                                              EeMap_OpenStreetMap};

        lbl_map = CreateWindowExW(0,
                                  L"STATIC",
                                  L"Map engine:",
                                  WS_CHILD | (app->is_roster ? 0 : WS_VISIBLE) | SS_LEFT | SS_CENTERIMAGE,
                                  margin,
                                  my,
                                  Scale(app, 90),
                                  mh,
                                  app->hwnd_options,
                                  NULL,
                                  app->instance,
                                  NULL);
        cmb_map = CreateWindowExW(0,
                                  L"COMBOBOX",
                                  L"",
                                  WS_CHILD | (app->is_roster ? 0 : WS_VISIBLE) | WS_TABSTOP |
                                      WS_VSCROLL | CBS_DROPDOWNLIST | CBS_HASSTRINGS,
                                  margin + Scale(app, 94),
                                  my,
                                  Scale(app, 180),
                                  Scale(app, 200),
                                  app->hwnd_options,
                                  (HMENU)(INT_PTR)IDC_OPT_MAP_ENGINE,
                                  app->instance,
                                  NULL);
        if (cmb_map != NULL)
        {
            int sel = 0;
            for (i = 0; i < (int)ARRAYSIZE(engines); i++)
            {
                int idx = (int)
                    SendMessageW(cmb_map, CB_ADDSTRING, 0, (LPARAM)App_MapEngineName(engines[i]));
                if (idx >= 0)
                {
                    SendMessageW(cmb_map, CB_SETITEMDATA, (WPARAM)idx, (LPARAM)engines[i]);
                    if (engines[i] == App_ClampMapEngine((int)app->map_engine))
                    {
                        sel = idx;
                    }
                }
            }
            SendMessageW(cmb_map, CB_SETCURSEL, (WPARAM)sel, 0);
        }
    }
    btn_ok = CreateWindowExW(0,
                             L"BUTTON",
                             L"OK",
                             WS_CHILD | WS_VISIBLE | WS_TABSTOP | BS_DEFPUSHBUTTON,
                             client_w - margin - btn_w * 2 - Scale(app, 8),
                             client_h - margin - btn_h,
                             btn_w,
                             btn_h,
                             app->hwnd_options,
                             (HMENU)(INT_PTR)IDOK,
                             app->instance,
                             NULL);
    btn_cancel =
        CreateWindowExW(0,
                        L"BUTTON",
                        L"Cancel",
                        WS_CHILD | WS_VISIBLE | WS_TABSTOP | WS_CLIPSIBLINGS | BS_PUSHBUTTON,
                        client_w - margin - btn_w,
                        client_h - margin - btn_h,
                        btn_w,
                        btn_h,
                        app->hwnd_options,
                        (HMENU)(INT_PTR)IDCANCEL,
                        app->instance,
                        NULL);

    if (app->font_ui)
    {
        SendMessageW(chk_prepend, WM_SETFONT, (WPARAM)app->font_ui, TRUE);
        SendMessageW(chk_surname, WM_SETFONT, (WPARAM)app->font_ui, TRUE);
        SendMessageW(lbl_zoom, WM_SETFONT, (WPARAM)app->font_ui, TRUE);
        SendMessageW(edit_zoom, WM_SETFONT, (WPARAM)app->font_ui, TRUE);
        SendMessageW(lbl_pct, WM_SETFONT, (WPARAM)app->font_ui, TRUE);
        SendMessageW(lbl_map, WM_SETFONT, (WPARAM)app->font_ui, TRUE);
        if (cmb_map != NULL)
        {
            SendMessageW(cmb_map, WM_SETFONT, (WPARAM)app->font_ui, TRUE);
        }
        SendMessageW(btn_ok, WM_SETFONT, (WPARAM)app->font_ui, TRUE);
        SendMessageW(btn_cancel, WM_SETFONT, (WPARAM)app->font_ui, TRUE);
    }
    SendMessageW(chk_prepend,
                 BM_SETCHECK,
                 app->copy_prepend_normalized ? BST_CHECKED : BST_UNCHECKED,
                 0);
    SendMessageW(chk_surname,
                 BM_SETCHECK,
                 app->name_surname_first ? BST_CHECKED : BST_UNCHECKED,
                 0);

    EnableWindow(app->hwnd_main, FALSE);
    ShowWindow(app->hwnd_options, SW_SHOW);
    UpdateWindow(app->hwnd_options);
    return TRUE;
}

/* -------------------------------------------------------------------------- */
/* Filter dialog                                                              */
/* -------------------------------------------------------------------------- */

typedef struct FilterDlgState
{
    AppState *app;
    EeFilterSet draft;
    int edit_index;
    BOOL refreshing;
    BOOL values_ready;
    uint32_t values_column;
} FilterDlgState;

static RECT g_filter_dlg_rect;
static BOOL g_filter_dlg_have_rect;

static void FilterDlg_PopulateColumns(HWND combo, const EeVoterTable *table)
{
    uint32_t i;
    SendMessageW(combo, CB_RESETCONTENT, 0, 0);
    if (table == NULL)
    {
        return;
    }
    for (i = 0; i < table->column_count; i++)
    {
        int idx;
        const wchar_t *title = table->column_titles[i] ? table->column_titles[i] : L"";
        idx = (int)SendMessageW(combo, CB_ADDSTRING, 0, (LPARAM)title);
        if (idx >= 0)
        {
            SendMessageW(combo, CB_SETITEMDATA, (WPARAM)idx, (LPARAM)i);
        }
    }
    SendMessageW(combo, CB_SETCURSEL, 0, 0);
}

/* Filter-dialog column list for a viewer: a voter roster offers the columns it shows
 * (not the raw VUID / name-part columns behind them). */
static void App_FilterPopulateColumns(HWND combo, const AppState *app)
{
    uint32_t i;
    if (app == NULL || !app->is_roster)
    {
        FilterDlg_PopulateColumns(combo, (app != NULL) ? &app->table : NULL);
        return;
    }
    SendMessageW(combo, CB_RESETCONTENT, 0, 0);
    for (i = 0; i < app->nroster_cols; i++)
    {
        uint32_t c = app->roster_cols[i];
        const wchar_t *title =
            (c < app->table.column_count && app->table.column_titles[c]) ? app->table.column_titles[c] : L"";
        int idx = (int)SendMessageW(combo, CB_ADDSTRING, 0, (LPARAM)title);
        if (idx >= 0)
        {
            SendMessageW(combo, CB_SETITEMDATA, (WPARAM)idx, (LPARAM)c);
        }
    }
    SendMessageW(combo, CB_SETCURSEL, 0, 0);
}

static uint32_t FilterDlg_ComboData(HWND combo, uint32_t fallback)
{
    int sel = (int)SendMessageW(combo, CB_GETCURSEL, 0, 0);
    if (sel < 0)
    {
        return fallback;
    }
    return (uint32_t)SendMessageW(combo, CB_GETITEMDATA, (WPARAM)sel, 0);
}

static void FilterDlg_PopulateRelations(HWND combo, BOOL allow_order)
{
    static const EeFilterRelation rels[] =
        {EeRel_Contains, EeRel_Excludes, EeRel_Is, EeRel_IsNot, EeRel_BeginsWith, EeRel_EndsWith};
    EeFilterRelation keep;
    int keep_idx = 0;
    int i;

    keep = (EeFilterRelation)FilterDlg_ComboData(combo, (uint32_t)EeRel_Contains);
    SendMessageW(combo, CB_RESETCONTENT, 0, 0);
    for (i = 0; i < (int)ARRAYSIZE(rels); i++)
    {
        int idx = (int)SendMessageW(combo, CB_ADDSTRING, 0, (LPARAM)EeFilter_RelationText(rels[i]));
        if (idx >= 0)
        {
            SendMessageW(combo, CB_SETITEMDATA, (WPARAM)idx, (LPARAM)rels[i]);
            if (rels[i] == keep)
            {
                keep_idx = idx;
            }
        }
    }
    if (allow_order)
    {
        static const EeFilterRelation extra[] = {EeRel_LessThan, EeRel_MoreThan};
        for (i = 0; i < (int)ARRAYSIZE(extra); i++)
        {
            int idx =
                (int)SendMessageW(combo, CB_ADDSTRING, 0, (LPARAM)EeFilter_RelationText(extra[i]));
            if (idx >= 0)
            {
                SendMessageW(combo, CB_SETITEMDATA, (WPARAM)idx, (LPARAM)extra[i]);
                if (extra[i] == keep)
                {
                    keep_idx = idx;
                }
            }
        }
    }
    SendMessageW(combo, CB_SETCURSEL, (WPARAM)keep_idx, 0);
}

static BOOL FilterDlg_ColumnAllowsOrder(const AppState *app, uint32_t column)
{
    return app != NULL && EeVoterTable_ColumnIsNumericOrDate(&app->table, column);
}

static void FilterDlg_PopulateActions(HWND combo)
{
    SendMessageW(combo, CB_RESETCONTENT, 0, 0);
    SendMessageW(combo, CB_ADDSTRING, 0, (LPARAM)L"Include");
    SendMessageW(combo, CB_SETITEMDATA, 0, (LPARAM)EeFilt_Include);
    SendMessageW(combo, CB_ADDSTRING, 0, (LPARAM)L"Exclude");
    SendMessageW(combo, CB_SETITEMDATA, 1, (LPARAM)EeFilt_Exclude);
    SendMessageW(combo, CB_SETCURSEL, 0, 0);
}

static void FilterDlg_FillValues(HWND hwnd, FilterDlgState *st)
{
    HWND combo;
    wchar_t keep[EE_FILTER_VALUE_CCH];
    wchar_t **vals = NULL;
    uint32_t n = 0;
    uint32_t i;
    uint32_t column;
    HCURSOR prev;

    if (hwnd == NULL || st == NULL || st->app == NULL)
    {
        return;
    }
    combo = GetDlgItem(hwnd, IDC_FLT_VALUE);
    column = FilterDlg_ComboData(GetDlgItem(hwnd, IDC_FLT_COLUMN), 0);
    if (st->values_ready && st->values_column == column)
    {
        return;
    }

    keep[0] = L'\0';
    GetWindowTextW(combo, keep, ARRAYSIZE(keep));
    prev = SetCursor(LoadCursorW(NULL, IDC_WAIT));
    SendMessageW(combo, CB_RESETCONTENT, 0, 0);
    if (EeFilter_CollectDistinct(&st->app->table, column, EE_FILTER_MAX_DISTINCT, &vals, &n))
    {
        if (n > 0)
        {
            SendMessageW(combo, CB_INITSTORAGE, (WPARAM)n, (LPARAM)(n * 32u));
        }
        for (i = 0; i < n; i++)
        {
            SendMessageW(combo, CB_ADDSTRING, 0, (LPARAM)vals[i]);
            free(vals[i]);
        }
        free(vals);
    }
    SetWindowTextW(combo, keep);
    SetCursor(prev);
    st->values_ready = TRUE;
    st->values_column = column;
}

static void FilterDlg_InvalidateValues(HWND hwnd, FilterDlgState *st, BOOL clear_text)
{
    HWND combo = GetDlgItem(hwnd, IDC_FLT_VALUE);
    if (st != NULL)
    {
        st->values_ready = FALSE;
    }
    if (combo != NULL)
    {
        SendMessageW(combo, CB_RESETCONTENT, 0, 0);
        if (clear_text)
        {
            SetWindowTextW(combo, L"");
        }
    }
}

static void FilterDlg_RefreshList(HWND list, FilterDlgState *st)
{
    uint32_t i;
    if (list == NULL || st == NULL)
    {
        return;
    }
    st->refreshing = TRUE;
    ListView_DeleteAllItems(list);
    for (i = 0; i < st->draft.count; i++)
    {
        const EeFilterRule *r = &st->draft.rules[i];
        LVITEMW it;
        const wchar_t *colname = L"?";
        ZeroMemory(&it, sizeof(it));
        if (st->app != NULL && r->column < st->app->table.column_count &&
            st->app->table.column_titles[r->column] != NULL)
        {
            colname = st->app->table.column_titles[r->column];
        }
        it.mask = LVIF_TEXT | LVIF_PARAM;
        it.iItem = (int)i;
        it.pszText = (LPWSTR)colname;
        it.lParam = (LPARAM)i;
        ListView_InsertItem(list, &it);
        ListView_SetItemText(list, (int)i, 1, (LPWSTR)EeFilter_RelationText(r->relation));
        ListView_SetItemText(list, (int)i, 2, (LPWSTR)r->value);
        ListView_SetItemText(list, (int)i, 3, (LPWSTR)EeFilter_ActionText(r->action));
        ListView_SetCheckState(list, (int)i, r->enabled);
    }
    st->refreshing = FALSE;
}

static void FilterDlg_LoadRuleToControls(HWND hwnd, FilterDlgState *st, const EeFilterRule *r)
{
    HWND col = GetDlgItem(hwnd, IDC_FLT_COLUMN);
    HWND rel = GetDlgItem(hwnd, IDC_FLT_RELATION);
    HWND val = GetDlgItem(hwnd, IDC_FLT_VALUE);
    HWND act = GetDlgItem(hwnd, IDC_FLT_ACTION);
    int i;
    int n;

    n = (int)SendMessageW(col, CB_GETCOUNT, 0, 0);
    for (i = 0; i < n; i++)
    {
        if ((uint32_t)SendMessageW(col, CB_GETITEMDATA, (WPARAM)i, 0) == r->column)
        {
            SendMessageW(col, CB_SETCURSEL, (WPARAM)i, 0);
            break;
        }
    }
    FilterDlg_InvalidateValues(hwnd, st, FALSE);
    FilterDlg_PopulateRelations(
        rel,
        FilterDlg_ColumnAllowsOrder(st != NULL ? st->app : NULL, r->column));
    n = (int)SendMessageW(rel, CB_GETCOUNT, 0, 0);
    for (i = 0; i < n; i++)
    {
        if ((EeFilterRelation)SendMessageW(rel, CB_GETITEMDATA, (WPARAM)i, 0) == r->relation)
        {
            SendMessageW(rel, CB_SETCURSEL, (WPARAM)i, 0);
            break;
        }
    }
    SetWindowTextW(val, r->value);
    SendMessageW(act, CB_SETCURSEL, (WPARAM)(r->action == EeFilt_Exclude ? 1 : 0), 0);
}

static void FilterDlg_EditSelected(HWND hwnd, FilterDlgState *st)
{
    HWND list;
    int i;

    if (st == NULL)
    {
        return;
    }
    list = GetDlgItem(hwnd, IDC_FLT_LIST);
    i = ListView_GetNextItem(list, -1, LVNI_SELECTED);
    if (i >= 0 && (uint32_t)i < st->draft.count)
    {
        st->edit_index = i;
        FilterDlg_LoadRuleToControls(hwnd, st, &st->draft.rules[i]);
        SetWindowTextW(GetDlgItem(hwnd, IDC_FLT_ADD), L"Update");
    }
}

static void FilterDlg_SetSelectedEnabled(HWND hwnd, FilterDlgState *st, BOOL enabled)
{
    HWND list;
    int i;

    if (st == NULL)
    {
        return;
    }
    list = GetDlgItem(hwnd, IDC_FLT_LIST);
    for (i = 0; i < (int)st->draft.count; i++)
    {
        if (ListView_GetItemState(list, i, LVIS_SELECTED) & LVIS_SELECTED)
        {
            st->draft.rules[i].enabled = enabled;
        }
    }
    FilterDlg_RefreshList(list, st);
}

static BOOL FilterDlg_ReadControls(HWND hwnd, EeFilterRule *r)
{
    if (r == NULL)
    {
        return FALSE;
    }
    ZeroMemory(r, sizeof(*r));
    r->column = FilterDlg_ComboData(GetDlgItem(hwnd, IDC_FLT_COLUMN), 0);
    r->relation = (EeFilterRelation)FilterDlg_ComboData(GetDlgItem(hwnd, IDC_FLT_RELATION),
                                                        (uint32_t)EeRel_Contains);
    r->action = (EeFilterAction)FilterDlg_ComboData(GetDlgItem(hwnd, IDC_FLT_ACTION),
                                                    (uint32_t)EeFilt_Include);
    r->enabled = TRUE;
    GetWindowTextW(GetDlgItem(hwnd, IDC_FLT_VALUE), r->value, EE_FILTER_VALUE_CCH);
    return TRUE;
}

static HDWP FilterDlg_Defer(HDWP hdwp, HWND hwnd, int id, int x, int y, int w, int h)
{
    HWND child;
    if (hdwp == NULL)
    {
        return NULL;
    }
    child = GetDlgItem(hwnd, id);
    if (child == NULL)
    {
        return hdwp;
    }
    return DeferWindowPos(hdwp,
                          child,
                          NULL,
                          x,
                          y,
                          w,
                          h,
                          SWP_NOZORDER | SWP_NOACTIVATE | SWP_NOCOPYBITS);
}

static int FilterDlg_MinClientWidth(AppState *app)
{
    /* Left margin, four combos, Add, Remove, right margin (gaps included). */
    return Scale(app, 12 + 170 + 6 + 110 + 6 + 180 + 6 + 90 + 6 + 80 + 6 + 80 + 12);
}

static int FilterDlg_MinClientHeight(AppState *app)
{
    return Scale(app, 360);
}

static void FilterDlg_Layout(HWND hwnd, AppState *app)
{
    RECT rc;
    HDWP hdwp;
    int m = Scale(app, 12);
    int row_h = Scale(app, 24);
    int gap = Scale(app, 6);
    int btn_w = Scale(app, 80);
    int btn_h = Scale(app, 26);
    int cw;
    int ch;
    int prompt_y;
    int add_y;
    int list_y;
    int btn_y;
    int list_h;
    int x;

    GetClientRect(hwnd, &rc);
    cw = rc.right - rc.left;
    ch = rc.bottom - rc.top;

    prompt_y = m;
    add_y = prompt_y + row_h + gap;
    list_y = add_y + row_h + gap;
    btn_y = ch - m - btn_h;
    list_h = btn_y - gap - list_y;
    if (list_h < 0)
    {
        list_h = 0;
    }

    x = m;
    hdwp = BeginDeferWindowPos(14);
    hdwp = FilterDlg_Defer(hdwp, hwnd, IDC_FLT_PROMPT, m, prompt_y, cw - m * 2, row_h);
    hdwp = FilterDlg_Defer(hdwp, hwnd, IDC_FLT_COLUMN, x, add_y, Scale(app, 170), Scale(app, 200));
    x += Scale(app, 176);
    hdwp =
        FilterDlg_Defer(hdwp, hwnd, IDC_FLT_RELATION, x, add_y, Scale(app, 110), Scale(app, 200));
    x += Scale(app, 116);
    hdwp = FilterDlg_Defer(hdwp, hwnd, IDC_FLT_VALUE, x, add_y, Scale(app, 180), Scale(app, 200));
    x += Scale(app, 186);
    hdwp = FilterDlg_Defer(hdwp, hwnd, IDC_FLT_ACTION, x, add_y, Scale(app, 90), Scale(app, 200));
    x += Scale(app, 96);
    hdwp = FilterDlg_Defer(hdwp, hwnd, IDC_FLT_ADD, x, add_y, btn_w, btn_h);
    x += btn_w + gap;
    hdwp = FilterDlg_Defer(hdwp, hwnd, IDC_FLT_REMOVE, x, add_y, btn_w, btn_h);
    hdwp = FilterDlg_Defer(hdwp, hwnd, IDC_FLT_LIST, m, list_y, cw - m * 2, list_h);
    hdwp = FilterDlg_Defer(hdwp, hwnd, IDC_FLT_RESET, m, btn_y, btn_w, btn_h);
    /* Export / Load sit between Reset and the Apply / OK / Cancel group. */
    hdwp = FilterDlg_Defer(hdwp, hwnd, IDC_FLT_EXPORT, m + btn_w + gap, btn_y, Scale(app, 110), btn_h);
    hdwp = FilterDlg_Defer(hdwp,
                           hwnd,
                           IDC_FLT_LOAD,
                           m + btn_w + gap * 2 + Scale(app, 110),
                           btn_y,
                           Scale(app, 110),
                           btn_h);
    hdwp = FilterDlg_Defer(hdwp,
                           hwnd,
                           IDC_FLT_APPLY,
                           cw - m - btn_w * 3 - gap * 2,
                           btn_y,
                           btn_w,
                           btn_h);
    hdwp = FilterDlg_Defer(hdwp, hwnd, IDOK, cw - m - btn_w * 2 - gap, btn_y, btn_w, btn_h);
    hdwp = FilterDlg_Defer(hdwp, hwnd, IDCANCEL, cw - m - btn_w, btn_y, btn_w, btn_h);
    if (hdwp != NULL)
    {
        EndDeferWindowPos(hdwp);
    }

    /* Keep the button row above the list if a resize ever clips. */
    SetWindowPos(GetDlgItem(hwnd, IDC_FLT_RESET),
                 HWND_TOP,
                 0,
                 0,
                 0,
                 0,
                 SWP_NOMOVE | SWP_NOSIZE | SWP_NOACTIVATE);
    SetWindowPos(GetDlgItem(hwnd, IDC_FLT_EXPORT),
                 HWND_TOP,
                 0,
                 0,
                 0,
                 0,
                 SWP_NOMOVE | SWP_NOSIZE | SWP_NOACTIVATE);
    SetWindowPos(GetDlgItem(hwnd, IDC_FLT_LOAD),
                 HWND_TOP,
                 0,
                 0,
                 0,
                 0,
                 SWP_NOMOVE | SWP_NOSIZE | SWP_NOACTIVATE);
    SetWindowPos(GetDlgItem(hwnd, IDC_FLT_APPLY),
                 HWND_TOP,
                 0,
                 0,
                 0,
                 0,
                 SWP_NOMOVE | SWP_NOSIZE | SWP_NOACTIVATE);
    SetWindowPos(GetDlgItem(hwnd, IDOK),
                 HWND_TOP,
                 0,
                 0,
                 0,
                 0,
                 SWP_NOMOVE | SWP_NOSIZE | SWP_NOACTIVATE);
    SetWindowPos(GetDlgItem(hwnd, IDCANCEL),
                 HWND_TOP,
                 0,
                 0,
                 0,
                 0,
                 SWP_NOMOVE | SWP_NOSIZE | SWP_NOACTIVATE);
}

static void FilterDlg_Apply(FilterDlgState *st)
{
    if (st == NULL || st->app == NULL)
    {
        return;
    }
    if (!EeFilter_Copy(&st->app->filters, &st->draft))
    {
        MessageBoxW(st->app->hwnd_main,
                    L"Could not apply the filter.",
                    k_WindowTitle,
                    MB_ICONERROR | MB_OK);
        return;
    }
    App_ClearSelection(st->app);
    App_ApplyFilter(st->app);
}

static void FilterDlg_RestoreOwner(AppState *app)
{
    if (app == NULL || app->hwnd_main == NULL)
    {
        return;
    }
    /* Owner must be enabled before the dialog is destroyed; otherwise Windows
     * activates some other top-level window and the viewer drops behind it. */
    if (app->hwnd_progress == NULL)
    {
        EnableWindow(app->hwnd_main, TRUE);
        SetForegroundWindow(app->hwnd_main);
        SetActiveWindow(app->hwnd_main);
    }
}

static void FilterDlg_Close(HWND hwnd, FilterDlgState *st)
{
    if (st != NULL)
    {
        FilterDlg_RestoreOwner(st->app);
    }
    DestroyWindow(hwnd);
}

/* -------------------------------------------------------------------------- */
/* Export Filter / Load Filter (voter list, voter roster and CVR filter windows) */
/* -------------------------------------------------------------------------- */

static void App_BaseNameNoExt(const wchar_t *path, wchar_t *out, size_t cch);

static wchar_t g_filter_dir[MAX_PATH]; /* folder of the last exported or loaded filter */

static void FilterFile_RememberDir(const wchar_t *path)
{
    wchar_t *slash;
    StringCchCopyW(g_filter_dir, ARRAYSIZE(g_filter_dir), path);
    slash = wcsrchr(g_filter_dir, L'\\');
    if (slash != NULL)
    {
        *slash = L'\0';
    }
    else
    {
        g_filter_dir[0] = L'\0';
    }
}

/* TRUE when @p path ends with @p ext (case-insensitive). */
static BOOL FilterFile_HasExt(const wchar_t *path, const wchar_t *ext)
{
    size_t n = wcslen(path);
    size_t e = wcslen(ext);
    return n >= e && _wcsicmp(path + (n - e), ext) == 0;
}

/* The columns a voter-list or roster window's filter offers (the roster's shown columns). */
static EeFilterColumnRef *FilterFile_AppColumns(const AppState *app, uint32_t *out_n)
{
    uint32_t n = app->is_roster ? app->nroster_cols : app->table.column_count;
    EeFilterColumnRef *cols = (EeFilterColumnRef *)calloc(n ? n : 1, sizeof(EeFilterColumnRef));
    uint32_t i;
    *out_n = 0;
    if (cols == NULL)
    {
        return NULL;
    }
    for (i = 0; i < n; i++)
    {
        uint32_t c = app->is_roster ? app->roster_cols[i] : i;
        cols[i].column = c;
        cols[i].title = (c < app->table.column_count && app->table.column_titles[c]) ? app->table.column_titles[c] : L"";
    }
    *out_n = n;
    return cols;
}

/* Export Filter: save the window's rules (as listed, applied or not) with the standard
 * Save As dialog. Suggests "<loaded file> Filter.filter.csv"; *.filter.tsv writes tabs. */
static void FilterFile_Export(HWND owner,
                              const EeFilterSet *rules,
                              const EeFilterColumnRef *cols,
                              uint32_t ncols,
                              const wchar_t *base,
                              const wchar_t *fallback)
{
    OPENFILENAMEW ofn;
    wchar_t file[MAX_PATH];
    wchar_t err[256] = L"";
    wchar_t delim = L',';

    if (rules == NULL || rules->count == 0)
    {
        MessageBoxW(owner, L"There are no filter rules to export.", L"Export Filter", MB_ICONINFORMATION | MB_OK);
        return;
    }
    StringCchPrintfW(file, ARRAYSIZE(file), L"%s Filter.filter.csv",
                     (base != NULL && base[0] != L'\0') ? base : fallback);
    ZeroMemory(&ofn, sizeof(ofn));
    ofn.lStructSize = sizeof(ofn);
    ofn.hwndOwner = owner;
    ofn.lpstrFilter = L"Election Explorer filter, CSV (*.filter.csv)\0*.filter.csv\0"
                      L"Election Explorer filter, tab-delimited (*.filter.tsv)\0*.filter.tsv\0";
    ofn.nFilterIndex = 1;
    ofn.lpstrFile = file;
    ofn.nMaxFile = ARRAYSIZE(file);
    ofn.lpstrInitialDir = g_filter_dir[0] ? g_filter_dir : NULL;
    ofn.Flags = OFN_OVERWRITEPROMPT | OFN_PATHMUSTEXIST | OFN_NOREADONLYRETURN | OFN_EXPLORER;
    ofn.lpstrTitle = L"Export Filter";
    if (!GetSaveFileNameW(&ofn))
    {
        return;
    }
    /* An explicitly typed .csv / .tsv decides the format; a name without one gets the chosen
     * type's double extension (asking before replacing a file of that name). */
    if (FilterFile_HasExt(file, L".tsv"))
    {
        delim = L'\t';
    }
    else if (!FilterFile_HasExt(file, L".csv"))
    {
        delim = (ofn.nFilterIndex == 2) ? L'\t' : L',';
        StringCchCatW(file, ARRAYSIZE(file), delim == L'\t' ? L".filter.tsv" : L".filter.csv");
        if (GetFileAttributesW(file) != INVALID_FILE_ATTRIBUTES)
        {
            wchar_t msg[MAX_PATH + 64];
            StringCchPrintfW(msg, ARRAYSIZE(msg), L"%s already exists.\r\nDo you want to replace it?", file);
            if (MessageBoxW(owner, msg, L"Export Filter", MB_ICONWARNING | MB_YESNO | MB_DEFBUTTON2) != IDYES)
            {
                return;
            }
        }
    }
    if (!EeFilter_SaveFile(file, rules, cols, ncols, delim, err, ARRAYSIZE(err)))
    {
        MessageBoxW(owner, err, L"Export Filter", MB_ICONERROR | MB_OK);
        return;
    }
    FilterFile_RememberDir(file);
}

/* Load Filter: replace @p rules with a saved filter chosen in the standard Open dialog. The
 * window's columns are matched by title; rules it cannot use are listed. TRUE when @p rules
 * changed (the caller refreshes its list; the rules take effect on Apply / OK). */
static BOOL FilterFile_Load(HWND owner,
                            EeFilterSet *rules,
                            const EeFilterColumnRef *cols,
                            uint32_t ncols,
                            uint32_t relation_mask,
                            const EeVoterTable *validate)
{
    OPENFILENAMEW ofn;
    wchar_t file[MAX_PATH] = L"";
    wchar_t err[256] = L"";
    wchar_t skipped[2048];
    EeFilterSet loaded;
    uint32_t nskip = 0;
    uint32_t i;

    ZeroMemory(&ofn, sizeof(ofn));
    ofn.lStructSize = sizeof(ofn);
    ofn.hwndOwner = owner;
    ofn.lpstrFilter = L"Election Explorer filters (*.filter.csv;*.filter.tsv)\0*.filter.csv;*.filter.tsv\0"
                      L"CSV or tab-delimited files (*.csv;*.tsv;*.txt)\0*.csv;*.tsv;*.txt\0"
                      L"All files (*.*)\0*.*\0";
    ofn.nFilterIndex = 1;
    ofn.lpstrFile = file;
    ofn.nMaxFile = ARRAYSIZE(file);
    ofn.lpstrInitialDir = g_filter_dir[0] ? g_filter_dir : NULL;
    ofn.Flags = OFN_FILEMUSTEXIST | OFN_PATHMUSTEXIST | OFN_EXPLORER;
    ofn.lpstrTitle = L"Load Filter";
    if (!GetOpenFileNameW(&ofn))
    {
        return FALSE;
    }
    FilterFile_RememberDir(file);
    EeFilter_Init(&loaded);
    if (!EeFilter_LoadFile(file, cols, ncols, relation_mask, &loaded, &nskip, skipped, ARRAYSIZE(skipped), err,
                           ARRAYSIZE(err)))
    {
        MessageBoxW(owner, err, L"Load Filter", MB_ICONERROR | MB_OK);
        return FALSE;
    }
    /* A voter list's date column needs a date to compare with less than / more than. */
    for (i = 0; validate != NULL && i < loaded.count;)
    {
        if (!EeFilter_RuleIsValid(&loaded.rules[i], validate))
        {
            wchar_t line[EE_FILTER_VALUE_CCH + 96];
            StringCchPrintfW(line, ARRAYSIZE(line), L"\"%s\": not a date for a date column.\r\n", loaded.rules[i].value);
            StringCchCatW(skipped, ARRAYSIZE(skipped), line);
            EeFilter_Remove(&loaded, i);
            nskip++;
            continue;
        }
        i++;
    }
    if (loaded.count == 0 && nskip == 0)
    {
        MessageBoxW(owner, L"The file holds no filter rules.", L"Load Filter", MB_ICONINFORMATION | MB_OK);
        EeFilter_Clear(&loaded);
        return FALSE;
    }
    if (!EeFilter_Copy(rules, &loaded))
    {
        MessageBoxW(owner, L"Out of memory loading the filter.", L"Load Filter", MB_ICONERROR | MB_OK);
        EeFilter_Clear(&loaded);
        return FALSE;
    }
    if (nskip > 0)
    {
        wchar_t msg[2400];
        StringCchPrintfW(msg, ARRAYSIZE(msg), L"Loaded %u rule%s. %u rule%s could not be used here:\r\n\r\n%s",
                         loaded.count, loaded.count == 1 ? L"" : L"s", nskip, nskip == 1 ? L"" : L"s", skipped);
        MessageBoxW(owner, msg, L"Load Filter", MB_ICONWARNING | MB_OK);
    }
    EeFilter_Clear(&loaded);
    return TRUE;
}

static LRESULT CALLBACK FilterWndProc(HWND hwnd, UINT msg, WPARAM wParam, LPARAM lParam)
{
    FilterDlgState *st = (FilterDlgState *)GetWindowLongPtrW(hwnd, GWLP_USERDATA);

    switch (msg)
    {
        case WM_CREATE:
        {
            CREATESTRUCTW *cs = (CREATESTRUCTW *)lParam;
            st = (FilterDlgState *)cs->lpCreateParams;
            SetWindowLongPtrW(hwnd, GWLP_USERDATA, (LONG_PTR)st);
            return 0;
        }
        case WM_SIZE:
            if (st != NULL && st->app != NULL)
            {
                FilterDlg_Layout(hwnd, st->app);
            }
            return 0;
        case WM_EXITSIZEMOVE:
            RedrawWindow(hwnd,
                         NULL,
                         NULL,
                         RDW_ERASE | RDW_INVALIDATE | RDW_ALLCHILDREN | RDW_UPDATENOW);
            return 0;
        case WM_GETMINMAXINFO:
        {
            MINMAXINFO *mm = (MINMAXINFO *)lParam;
            RECT wr;
            DWORD style = (DWORD)GetWindowLongPtrW(hwnd, GWL_STYLE);
            DWORD ex_style = (DWORD)GetWindowLongPtrW(hwnd, GWL_EXSTYLE);
            UINT dpi = GetDpiForWindow(hwnd);
            int min_cw;
            int min_ch;

            if (dpi == 0)
            {
                dpi = 96;
            }
            if (st != NULL && st->app != NULL)
            {
                min_cw = FilterDlg_MinClientWidth(st->app);
                min_ch = FilterDlg_MinClientHeight(st->app);
                dpi = st->app->dpi;
            }
            else
            {
                min_cw = MulDiv(12 + 170 + 6 + 110 + 6 + 180 + 6 + 90 + 6 + 80 + 6 + 80 + 12,
                                (int)dpi,
                                96);
                min_ch = MulDiv(360, (int)dpi, 96);
            }
            wr.left = 0;
            wr.top = 0;
            wr.right = min_cw;
            wr.bottom = min_ch;
            if (!AdjustWindowRectExForDpi(&wr, style, FALSE, ex_style, dpi))
            {
                wr.right += 16;
                wr.bottom += 40;
            }
            mm->ptMinTrackSize.x = wr.right - wr.left;
            mm->ptMinTrackSize.y = wr.bottom - wr.top;
            return 0;
        }
        case WM_COMMAND:
            if (st == NULL)
            {
                return 0;
            }
            switch (LOWORD(wParam))
            {
                case IDC_FLT_COLUMN:
                    if (HIWORD(wParam) == CBN_SELCHANGE)
                    {
                        uint32_t col = FilterDlg_ComboData(GetDlgItem(hwnd, IDC_FLT_COLUMN), 0);
                        FilterDlg_PopulateRelations(GetDlgItem(hwnd, IDC_FLT_RELATION),
                                                    FilterDlg_ColumnAllowsOrder(st->app, col));
                        FilterDlg_InvalidateValues(hwnd, st, TRUE);
                    }
                    return 0;
                case IDC_FLT_VALUE:
                    if (HIWORD(wParam) == CBN_DROPDOWN)
                    {
                        FilterDlg_FillValues(hwnd, st);
                    }
                    return 0;
                case IDC_FLT_ADD:
                {
                    EeFilterRule r;
                    if (FilterDlg_ReadControls(hwnd, &r))
                    {
                        if (!EeFilter_RuleIsValid(&r, &st->app->table))
                        {
                            MessageBoxW(hwnd,
                                        L"Enter a date for this comparison "
                                        L"(for example 1/15/2020 or 20200115).",
                                        L"Election Explorer Filter",
                                        MB_ICONINFORMATION | MB_OK);
                            return 0;
                        }
                        if (st->edit_index >= 0 && (uint32_t)st->edit_index < st->draft.count)
                        {
                            EeFilter_Set(&st->draft, (uint32_t)st->edit_index, &r);
                        }
                        else
                        {
                            EeFilter_Add(&st->draft, &r);
                        }
                        st->edit_index = -1;
                        SetWindowTextW(GetDlgItem(hwnd, IDC_FLT_VALUE), L"");
                        SetWindowTextW(GetDlgItem(hwnd, IDC_FLT_ADD), L"Add");
                        FilterDlg_RefreshList(GetDlgItem(hwnd, IDC_FLT_LIST), st);
                    }
                    return 0;
                }
                case IDC_FLT_REMOVE:
                {
                    HWND list = GetDlgItem(hwnd, IDC_FLT_LIST);
                    int i;
                    for (i = ListView_GetItemCount(list) - 1; i >= 0; i--)
                    {
                        if (ListView_GetItemState(list, i, LVIS_SELECTED) & LVIS_SELECTED)
                        {
                            EeFilter_Remove(&st->draft, (uint32_t)i);
                        }
                    }
                    st->edit_index = -1;
                    SetWindowTextW(GetDlgItem(hwnd, IDC_FLT_ADD), L"Add");
                    FilterDlg_RefreshList(list, st);
                    return 0;
                }
                case IDC_FLT_EDIT:
                    FilterDlg_EditSelected(hwnd, st);
                    return 0;
                case IDC_FLT_ENABLE:
                    FilterDlg_SetSelectedEnabled(hwnd, st, TRUE);
                    return 0;
                case IDC_FLT_DISABLE:
                    FilterDlg_SetSelectedEnabled(hwnd, st, FALSE);
                    return 0;
                case IDC_FLT_RESET:
                    EeFilter_Clear(&st->draft);
                    st->edit_index = -1;
                    SetWindowTextW(GetDlgItem(hwnd, IDC_FLT_ADD), L"Add");
                    FilterDlg_RefreshList(GetDlgItem(hwnd, IDC_FLT_LIST), st);
                    return 0;
                case IDC_FLT_EXPORT:
                case IDC_FLT_LOAD:
                {
                    uint32_t ncols = 0;
                    EeFilterColumnRef *cols = FilterFile_AppColumns(st->app, &ncols);
                    if (cols == NULL)
                    {
                        return 0;
                    }
                    if (LOWORD(wParam) == IDC_FLT_EXPORT)
                    {
                        wchar_t base[192];
                        App_BaseNameNoExt(st->app->load_path, base, ARRAYSIZE(base));
                        FilterFile_Export(hwnd, &st->draft, cols, ncols, base,
                                          st->app->is_roster ? L"Voter Roster" : L"Voter List");
                    }
                    else if (FilterFile_Load(hwnd, &st->draft, cols, ncols, EE_FILTER_REL_ALL, &st->app->table))
                    {
                        st->edit_index = -1;
                        SetWindowTextW(GetDlgItem(hwnd, IDC_FLT_ADD), L"Add");
                        FilterDlg_RefreshList(GetDlgItem(hwnd, IDC_FLT_LIST), st);
                    }
                    free(cols);
                    return 0;
                }
                case IDC_FLT_APPLY:
                    FilterDlg_Apply(st);
                    return 0;
                case IDOK:
                    FilterDlg_Apply(st);
                    FilterDlg_Close(hwnd, st);
                    return 0;
                case IDCANCEL:
                    FilterDlg_Close(hwnd, st);
                    return 0;
                default:
                    break;
            }
            return 0;
        case WM_NOTIFY:
        {
            NMHDR *hdr = (NMHDR *)lParam;
            if (st != NULL && hdr != NULL && hdr->idFrom == IDC_FLT_LIST)
            {
                if (hdr->code == LVN_ITEMCHANGED && !st->refreshing)
                {
                    NMLISTVIEW *lv = (NMLISTVIEW *)hdr;
                    if (lv->uChanged & LVIF_STATE)
                    {
                        BOOL now = (lv->uNewState & LVIS_STATEIMAGEMASK) != 0;
                        BOOL was = (lv->uOldState & LVIS_STATEIMAGEMASK) != 0;
                        /* Checkboxes use state image index 1/2. */
                        UINT ni = (lv->uNewState & LVIS_STATEIMAGEMASK) >> 12;
                        UINT oi = (lv->uOldState & LVIS_STATEIMAGEMASK) >> 12;
                        if (ni != oi && lv->iItem >= 0 && (uint32_t)lv->iItem < st->draft.count)
                        {
                            st->draft.rules[lv->iItem].enabled = (ni == 2);
                        }
                        (void)now;
                        (void)was;
                    }
                }
                if (hdr->code == NM_DBLCLK)
                {
                    FilterDlg_EditSelected(hwnd, st);
                }
                if (hdr->code == LVN_KEYDOWN)
                {
                    NMLVKEYDOWN *kd = (NMLVKEYDOWN *)hdr;
                    if (kd->wVKey == VK_DELETE)
                    {
                        SendMessageW(hwnd, WM_COMMAND, IDC_FLT_REMOVE, 0);
                    }
                }
            }
            return 0;
        }
        case WM_CONTEXTMENU:
            if (st != NULL)
            {
                POINT pt;
                HMENU menu;
                UINT cmd;
                pt.x = GET_X_LPARAM(lParam);
                pt.y = GET_Y_LPARAM(lParam);
                if (pt.x == -1 && pt.y == -1)
                {
                    GetCursorPos(&pt);
                }
                menu = CreatePopupMenu();
                if (menu)
                {
                    AppendMenuW(menu, MF_STRING, IDC_FLT_EDIT, L"&Edit");
                    AppendMenuW(menu, MF_STRING, IDC_FLT_REMOVE, L"&Remove");
                    AppendMenuW(menu, MF_SEPARATOR, 0, NULL);
                    AppendMenuW(menu, MF_STRING, IDC_FLT_ENABLE, L"E&nable");
                    AppendMenuW(menu, MF_STRING, IDC_FLT_DISABLE, L"&Disable");
                    cmd = (UINT)TrackPopupMenu(menu,
                                               TPM_RIGHTBUTTON | TPM_RETURNCMD,
                                               pt.x,
                                               pt.y,
                                               0,
                                               hwnd,
                                               NULL);
                    DestroyMenu(menu);
                    if (cmd != 0)
                    {
                        SendMessageW(hwnd, WM_COMMAND, cmd, 0);
                    }
                }
            }
            return 0;
        case WM_KEYDOWN:
            if (wParam == VK_DELETE)
            {
                SendMessageW(hwnd, WM_COMMAND, IDC_FLT_REMOVE, 0);
                return 0;
            }
            break;
        case WM_CLOSE:
            FilterDlg_Close(hwnd, st);
            return 0;
        case WM_DESTROY:
        {
            RECT wr;
            GetWindowRect(hwnd, &wr);
            g_filter_dlg_rect = wr;
            g_filter_dlg_have_rect = TRUE;
            if (st != NULL)
            {
                if (st->app != NULL)
                {
                    if (st->app->hwnd_progress == NULL && st->app->hwnd_main != NULL)
                    {
                        EnableWindow(st->app->hwnd_main, TRUE);
                    }
                    st->app->hwnd_filter = NULL;
                }
                EeFilter_Clear(&st->draft);
                HeapFree(GetProcessHeap(), 0, st);
                SetWindowLongPtrW(hwnd, GWLP_USERDATA, 0);
            }
            return 0;
        }
        default:
            break;
    }
    return DefWindowProcW(hwnd, msg, wParam, lParam);
}

static BOOL App_ShowFilter(AppState *app)
{
    FilterDlgState *st;
    RECT rc_wnd;
    const DWORD style = WS_POPUP | WS_CAPTION | WS_SYSMENU | WS_THICKFRAME | WS_MINIMIZEBOX |
                        WS_CLIPCHILDREN | WS_CLIPSIBLINGS;
    const DWORD ex_style = WS_EX_DLGMODALFRAME | WS_EX_CONTROLPARENT;
    int client_w;
    int client_h;
    int x;
    int y;
    int outer_w;
    int outer_h;
    HWND hwnd;
    HWND list;
    LVCOLUMNW col;

    if (app == NULL)
    {
        return FALSE;
    }
    if (app->hwnd_filter != NULL)
    {
        SetForegroundWindow(app->hwnd_filter);
        return TRUE;
    }
    if (app->table.row_count == 0)
    {
        MessageBoxW(app->hwnd_main,
                    L"Load a voter list before filtering.",
                    k_WindowTitle,
                    MB_ICONINFORMATION | MB_OK);
        return TRUE;
    }

    st = (FilterDlgState *)HeapAlloc(GetProcessHeap(), HEAP_ZERO_MEMORY, sizeof(FilterDlgState));
    if (st == NULL)
    {
        return FALSE;
    }
    st->app = app;
    st->edit_index = -1;
    EeFilter_Init(&st->draft);
    if (!EeFilter_Copy(&st->draft, &app->filters))
    {
        HeapFree(GetProcessHeap(), 0, st);
        return FALSE;
    }

    client_w = FilterDlg_MinClientWidth(app);
    if (client_w < Scale(app, 800))
    {
        client_w = Scale(app, 800);
    }
    client_h = Scale(app, 480);
    rc_wnd.left = 0;
    rc_wnd.top = 0;
    rc_wnd.right = client_w;
    rc_wnd.bottom = client_h;
    if (!AdjustWindowRectExForDpi(&rc_wnd, style, FALSE, ex_style, app->dpi))
    {
        rc_wnd.right = client_w + Scale(app, 16);
        rc_wnd.bottom = client_h + Scale(app, 40);
    }
    outer_w = rc_wnd.right - rc_wnd.left;
    outer_h = rc_wnd.bottom - rc_wnd.top;
    if (g_filter_dlg_have_rect)
    {
        x = g_filter_dlg_rect.left;
        y = g_filter_dlg_rect.top;
        outer_w = g_filter_dlg_rect.right - g_filter_dlg_rect.left;
        outer_h = g_filter_dlg_rect.bottom - g_filter_dlg_rect.top;
        if (outer_w < rc_wnd.right - rc_wnd.left)
        {
            outer_w = rc_wnd.right - rc_wnd.left;
        }
    }
    else
    {
        RECT rc_main;
        GetWindowRect(app->hwnd_main, &rc_main);
        x = rc_main.left + ((rc_main.right - rc_main.left) - outer_w) / 2;
        y = rc_main.top + ((rc_main.bottom - rc_main.top) - outer_h) / 2;
    }

    hwnd = CreateWindowExW(ex_style,
                           k_FilterClassName,
                           L"Election Explorer Filter",
                           style,
                           x,
                           y,
                           outer_w,
                           outer_h,
                           app->hwnd_main,
                           NULL,
                           app->instance,
                           st);
    if (hwnd == NULL)
    {
        EeFilter_Clear(&st->draft);
        HeapFree(GetProcessHeap(), 0, st);
        return FALSE;
    }
    app->hwnd_filter = hwnd;

    CreateWindowExW(0,
                    L"STATIC",
                    L"Display entries matching these conditions:",
                    WS_CHILD | WS_VISIBLE | SS_LEFT | SS_CENTERIMAGE,
                    0,
                    0,
                    0,
                    0,
                    hwnd,
                    (HMENU)(INT_PTR)IDC_FLT_PROMPT,
                    app->instance,
                    NULL);
    CreateWindowExW(0,
                    L"COMBOBOX",
                    L"",
                    WS_CHILD | WS_VISIBLE | WS_TABSTOP | WS_VSCROLL | CBS_DROPDOWNLIST |
                        CBS_HASSTRINGS,
                    0,
                    0,
                    0,
                    0,
                    hwnd,
                    (HMENU)(INT_PTR)IDC_FLT_COLUMN,
                    app->instance,
                    NULL);
    CreateWindowExW(0,
                    L"COMBOBOX",
                    L"",
                    WS_CHILD | WS_VISIBLE | WS_TABSTOP | WS_VSCROLL | CBS_DROPDOWNLIST |
                        CBS_HASSTRINGS,
                    0,
                    0,
                    0,
                    0,
                    hwnd,
                    (HMENU)(INT_PTR)IDC_FLT_RELATION,
                    app->instance,
                    NULL);
    CreateWindowExW(0,
                    L"COMBOBOX",
                    L"",
                    WS_CHILD | WS_VISIBLE | WS_TABSTOP | WS_VSCROLL | CBS_DROPDOWN |
                        CBS_HASSTRINGS | CBS_AUTOHSCROLL,
                    0,
                    0,
                    0,
                    0,
                    hwnd,
                    (HMENU)(INT_PTR)IDC_FLT_VALUE,
                    app->instance,
                    NULL);
    CreateWindowExW(0,
                    L"COMBOBOX",
                    L"",
                    WS_CHILD | WS_VISIBLE | WS_TABSTOP | WS_VSCROLL | CBS_DROPDOWNLIST |
                        CBS_HASSTRINGS,
                    0,
                    0,
                    0,
                    0,
                    hwnd,
                    (HMENU)(INT_PTR)IDC_FLT_ACTION,
                    app->instance,
                    NULL);
    CreateWindowExW(0,
                    L"BUTTON",
                    L"Add",
                    WS_CHILD | WS_VISIBLE | WS_TABSTOP | WS_CLIPSIBLINGS | BS_PUSHBUTTON,
                    0,
                    0,
                    0,
                    0,
                    hwnd,
                    (HMENU)(INT_PTR)IDC_FLT_ADD,
                    app->instance,
                    NULL);
    CreateWindowExW(0,
                    L"BUTTON",
                    L"Remove",
                    WS_CHILD | WS_VISIBLE | WS_TABSTOP | WS_CLIPSIBLINGS | BS_PUSHBUTTON,
                    0,
                    0,
                    0,
                    0,
                    hwnd,
                    (HMENU)(INT_PTR)IDC_FLT_REMOVE,
                    app->instance,
                    NULL);
    list = CreateWindowExW(WS_EX_CLIENTEDGE,
                           WC_LISTVIEWW,
                           L"",
                           WS_CHILD | WS_VISIBLE | WS_TABSTOP | WS_CLIPSIBLINGS | LVS_REPORT |
                               LVS_SHOWSELALWAYS | LVS_SHAREIMAGELISTS,
                           0,
                           0,
                           0,
                           0,
                           hwnd,
                           (HMENU)(INT_PTR)IDC_FLT_LIST,
                           app->instance,
                           NULL);
    CreateWindowExW(0,
                    L"BUTTON",
                    L"Reset",
                    WS_CHILD | WS_VISIBLE | WS_TABSTOP | WS_CLIPSIBLINGS | BS_PUSHBUTTON,
                    0,
                    0,
                    0,
                    0,
                    hwnd,
                    (HMENU)(INT_PTR)IDC_FLT_RESET,
                    app->instance,
                    NULL);
    CreateWindowExW(0,
                    L"BUTTON",
                    L"Export Filter\x2026",
                    WS_CHILD | WS_VISIBLE | WS_TABSTOP | WS_CLIPSIBLINGS | BS_PUSHBUTTON,
                    0,
                    0,
                    0,
                    0,
                    hwnd,
                    (HMENU)(INT_PTR)IDC_FLT_EXPORT,
                    app->instance,
                    NULL);
    CreateWindowExW(0,
                    L"BUTTON",
                    L"Load Filter\x2026",
                    WS_CHILD | WS_VISIBLE | WS_TABSTOP | WS_CLIPSIBLINGS | BS_PUSHBUTTON,
                    0,
                    0,
                    0,
                    0,
                    hwnd,
                    (HMENU)(INT_PTR)IDC_FLT_LOAD,
                    app->instance,
                    NULL);
    CreateWindowExW(0,
                    L"BUTTON",
                    L"Apply",
                    WS_CHILD | WS_VISIBLE | WS_TABSTOP | WS_CLIPSIBLINGS | BS_PUSHBUTTON,
                    0,
                    0,
                    0,
                    0,
                    hwnd,
                    (HMENU)(INT_PTR)IDC_FLT_APPLY,
                    app->instance,
                    NULL);
    CreateWindowExW(0,
                    L"BUTTON",
                    L"OK",
                    WS_CHILD | WS_VISIBLE | WS_TABSTOP | WS_CLIPSIBLINGS | BS_DEFPUSHBUTTON,
                    0,
                    0,
                    0,
                    0,
                    hwnd,
                    (HMENU)(INT_PTR)IDOK,
                    app->instance,
                    NULL);
    CreateWindowExW(0,
                    L"BUTTON",
                    L"Cancel",
                    WS_CHILD | WS_VISIBLE | WS_TABSTOP | WS_CLIPSIBLINGS | BS_PUSHBUTTON,
                    0,
                    0,
                    0,
                    0,
                    hwnd,
                    (HMENU)(INT_PTR)IDCANCEL,
                    app->instance,
                    NULL);

    if (app->font_ui)
    {
        HWND child = GetWindow(hwnd, GW_CHILD);
        while (child)
        {
            SendMessageW(child, WM_SETFONT, (WPARAM)app->font_ui, TRUE);
            child = GetWindow(child, GW_HWNDNEXT);
        }
    }

    ListView_SetExtendedListViewStyle(list,
                                      LVS_EX_CHECKBOXES | LVS_EX_FULLROWSELECT |
                                          LVS_EX_DOUBLEBUFFER | LVS_EX_GRIDLINES);
    ZeroMemory(&col, sizeof(col));
    col.mask = LVCF_TEXT | LVCF_WIDTH;
    col.pszText = L"Column";
    col.cx = Scale(app, 180);
    ListView_InsertColumn(list, 0, &col);
    col.pszText = L"Relation";
    col.cx = Scale(app, 110);
    ListView_InsertColumn(list, 1, &col);
    col.pszText = L"Value";
    col.cx = Scale(app, 200);
    ListView_InsertColumn(list, 2, &col);
    col.pszText = L"Action";
    col.cx = Scale(app, 90);
    ListView_InsertColumn(list, 3, &col);

    App_FilterPopulateColumns(GetDlgItem(hwnd, IDC_FLT_COLUMN), app);
    FilterDlg_PopulateRelations(GetDlgItem(hwnd, IDC_FLT_RELATION),
                                FilterDlg_ColumnAllowsOrder(app, 0));
    FilterDlg_PopulateActions(GetDlgItem(hwnd, IDC_FLT_ACTION));
    FilterDlg_RefreshList(list, st);
    FilterDlg_Layout(hwnd, app);

    EnableWindow(app->hwnd_main, FALSE);
    ShowWindow(hwnd, SW_SHOW);
    UpdateWindow(hwnd);
    return TRUE;
}

static void App_ResetFilter(AppState *app)
{
    if (app == NULL)
    {
        return;
    }
    EeFilter_Clear(&app->filters);
    App_ClearMarks(app);
    App_ClearSelection(app);
    App_ApplyFilter(app);
}

/* -------------------------------------------------------------------------- */
/* Duplicate scan: mark layer + background thread                             */
/* -------------------------------------------------------------------------- */

static const wchar_t *App_DupNoneText(int kind)
{
    return (kind == EE_SCAN_DUP_NAME_DOB) ? L"No duplicate voters (same name and date of birth)."
                                          : L"No duplicate Voter IDs.";
}

static const wchar_t *App_DupErrorText(int kind)
{
    return (kind == EE_SCAN_DUP_NAME_DOB) ? L"Could not scan for duplicate voters."
                                          : L"Could not scan Voter IDs for duplicates.";
}

static void App_ClearMarks(AppState *app)
{
    if (app == NULL)
    {
        return;
    }
    free(app->mark_rows);
    app->mark_rows = NULL;
    app->mark_count = 0;
    app->mark_active = FALSE;
    app->mark_kind = EE_SCAN_NONE;
    app->mark_label[0] = L'\0';
}

/* Takes ownership of @p marks (row_count bytes) and shows it as the active mark
 * view, ANDed with any existing filter. Groups rows by @p sort_col so related
 * rows sit together; @p label describes the view in the status bar. */
static void App_ApplyMarks(AppState *app,
                           uint8_t *marks,
                           uint32_t count,
                           int kind,
                           uint32_t sort_col,
                           const wchar_t *label)
{
    if (app == NULL)
    {
        free(marks);
        return;
    }
    App_ClearMarks(app);
    app->mark_rows = marks;
    app->mark_count = count;
    app->mark_active = TRUE;
    app->mark_kind = kind;
    if (label != NULL)
    {
        StringCchCopyW(app->mark_label, ARRAYSIZE(app->mark_label), label);
    }
    App_ClearSelection(app);

    /* The sort refreshes the (mark-intersected) view; fall back to a plain apply
     * if it cannot run. */
    if (sort_col < app->table.column_count && app->table.row_count > 0)
    {
        HCURSOR prev = SetCursor(LoadCursorW(NULL, IDC_WAIT));
        App_SortByTableColumnAscending(app, sort_col);
        SetCursor(prev);
    }
    else
    {
        App_ApplyFilter(app);
    }
}

/* Takes ownership of @p marks and shows a duplicates view, keyed for grouping:
 * Voter ID for the ID scan, Name for the name+DOB scan. */
static void App_ApplyDuplicateMarks(AppState *app, uint8_t *marks, uint32_t count, int kind)
{
    uint32_t sort_col = (kind == EE_SCAN_DUP_NAME_DOB) ? EE_COL_NAME : EE_COL_VOTER_ID;
    const wchar_t *label =
        (kind == EE_SCAN_DUP_NAME_DOB) ? L"duplicate voters (name + DOB)" : L"duplicate Voter IDs";
    App_ApplyMarks(app, marks, count, kind, sort_col, label);
}

/* Synchronous path for smaller tables (the O(n) scan is sub-100 ms there). */
static void App_RunDuplicateScanSync(AppState *app, int kind)
{
    uint8_t *marks;
    uint32_t count = 0;
    BOOL ok;
    HCURSOR prev;

    marks = (uint8_t *)calloc((size_t)app->table.row_count, sizeof(uint8_t));
    if (marks == NULL)
    {
        MessageBoxW(app->hwnd_main, App_DupErrorText(kind), k_WindowTitle, MB_ICONERROR | MB_OK);
        return;
    }
    prev = SetCursor(LoadCursorW(NULL, IDC_WAIT));
    if (kind == EE_SCAN_DUP_NAME_DOB)
    {
        ok =
            EeVoterTable_MarkDuplicateVotersByNameDob(&app->table, marks, &count, NULL, NULL, NULL);
    }
    else
    {
        ok = EeVoterTable_MarkDuplicateVoterIds(&app->table, marks, &count, NULL, NULL, NULL);
    }
    SetCursor(prev);

    if (!ok)
    {
        free(marks);
        MessageBoxW(app->hwnd_main, App_DupErrorText(kind), k_WindowTitle, MB_ICONERROR | MB_OK);
        return;
    }
    if (count == 0)
    {
        free(marks);
        MessageBoxW(app->hwnd_main,
                    App_DupNoneText(kind),
                    k_WindowTitle,
                    MB_ICONINFORMATION | MB_OK);
        return;
    }
    App_ApplyDuplicateMarks(app, marks, count, kind);
}

static BOOL CALLBACK ScanProgressThunk(const EeLoadProgress *progress, void *user)
{
    AppState *app = (AppState *)user;

    EnterCriticalSection(&app->progress_lock);
    app->last_progress = *progress;
    app->progress_dirty = TRUE;
    LeaveCriticalSection(&app->progress_lock);

    PostMessageW(app->hwnd_main, EEM_SCAN_PROGRESS, 0, 0);

    if (InterlockedCompareExchange(&app->scan_cancel, 0, 0) != 0)
    {
        return FALSE;
    }
    return TRUE;
}

static DWORD WINAPI ScanThreadProc(void *param)
{
    AppState *app = (AppState *)param;
    uint32_t count = 0;
    BOOL ok;

    if (app->scan_kind == EE_SCAN_DUP_NAME_DOB)
    {
        ok = EeVoterTable_MarkDuplicateVotersByNameDob(&app->table,
                                                       app->scan_marks,
                                                       &count,
                                                       &app->scan_cancel,
                                                       ScanProgressThunk,
                                                       app);
    }
    else
    {
        ok = EeVoterTable_MarkDuplicateVoterIds(&app->table,
                                                app->scan_marks,
                                                &count,
                                                &app->scan_cancel,
                                                ScanProgressThunk,
                                                app);
    }
    app->scan_ok = ok;
    app->scan_count = count;

    PostMessageW(app->hwnd_main, EEM_SCAN_FINISHED, 0, 0);
    return 0;
}

static void App_StartDuplicateScan(AppState *app, int kind)
{
    if (app == NULL || app->scanning || app->loading || app->table.row_count == 0)
    {
        return;
    }

    app->scan_marks = (uint8_t *)calloc((size_t)app->table.row_count, sizeof(uint8_t));
    if (app->scan_marks == NULL)
    {
        MessageBoxW(app->hwnd_main, App_DupErrorText(kind), k_WindowTitle, MB_ICONERROR | MB_OK);
        return;
    }
    app->scan_kind = kind;
    app->scan_ok = FALSE;
    app->scan_count = 0;
    InterlockedExchange(&app->scan_cancel, 0);
    app->scanning = TRUE;

    if (!App_ShowProgress(app))
    {
        app->scanning = FALSE;
        free(app->scan_marks);
        app->scan_marks = NULL;
        MessageBoxW(app->hwnd_main,
                    L"Could not create the progress window.",
                    k_WindowTitle,
                    MB_ICONERROR | MB_OK);
        return;
    }
    SetWindowTextW(app->hwnd_progress, L"Scanning for duplicates");
    if (app->hwnd_progress_status)
    {
        SetWindowTextW(app->hwnd_progress_status,
                       (kind == EE_SCAN_DUP_NAME_DOB) ? L"Scanning name + date of birth…"
                                                      : L"Scanning Voter IDs…");
    }

    app->scan_thread = CreateThread(NULL, 0, ScanThreadProc, app, 0, NULL);
    if (app->scan_thread == NULL)
    {
        app->scanning = FALSE;
        EnableWindow(app->hwnd_main, TRUE);
        App_DestroyProgress(app);
        free(app->scan_marks);
        app->scan_marks = NULL;
        MessageBoxW(app->hwnd_main,
                    L"Could not start the scan thread.",
                    k_WindowTitle,
                    MB_ICONERROR | MB_OK);
    }
}

static void App_OnScanFinished(AppState *app)
{
    BOOL cancelled;

    if (app->scan_thread != NULL)
    {
        WaitForSingleObject(app->scan_thread, INFINITE);
        CloseHandle(app->scan_thread);
        app->scan_thread = NULL;
    }
    app->scanning = FALSE;
    cancelled = (InterlockedCompareExchange(&app->scan_cancel, 0, 0) != 0);

    EnableWindow(app->hwnd_main, TRUE);
    App_DestroyProgress(app);
    SetForegroundWindow(app->hwnd_main);

    if (app->close_pending)
    {
        free(app->scan_marks);
        app->scan_marks = NULL;
        DestroyWindow(app->hwnd_main);
        return;
    }
    if (!app->scan_ok)
    {
        free(app->scan_marks);
        app->scan_marks = NULL;
        MessageBoxW(app->hwnd_main,
                    App_DupErrorText(app->scan_kind),
                    k_WindowTitle,
                    MB_ICONERROR | MB_OK);
        return;
    }
    if (cancelled)
    {
        free(app->scan_marks);
        app->scan_marks = NULL;
        App_SetStatus(app, L"Duplicate scan cancelled.");
        return;
    }
    if (app->scan_count == 0)
    {
        free(app->scan_marks);
        app->scan_marks = NULL;
        MessageBoxW(app->hwnd_main,
                    App_DupNoneText(app->scan_kind),
                    k_WindowTitle,
                    MB_ICONINFORMATION | MB_OK);
        return;
    }

    {
        uint8_t *marks = app->scan_marks;
        uint32_t count = app->scan_count;
        int kind = app->scan_kind;
        app->scan_marks = NULL; /* ownership transfers to the mark layer */
        App_ApplyDuplicateMarks(app, marks, count, kind);
    }
}

static void App_ShowDuplicateVoterIds(AppState *app)
{
    if (app == NULL)
    {
        return;
    }
    if (app->loading || app->scanning || app->table.row_count == 0)
    {
        MessageBoxW(app->hwnd_main,
                    L"Load a voter list before checking for duplicate Voter IDs.",
                    k_WindowTitle,
                    MB_ICONINFORMATION | MB_OK);
        return;
    }

    if (app->table.row_count >= k_ScanModalMinRows)
    {
        App_StartDuplicateScan(app, EE_SCAN_DUP_VOTER_IDS);
    }
    else
    {
        App_RunDuplicateScanSync(app, EE_SCAN_DUP_VOTER_IDS);
    }
}

static void App_ShowDuplicateVoters(AppState *app)
{
    if (app == NULL)
    {
        return;
    }
    if (app->loading || app->scanning || app->table.row_count == 0)
    {
        MessageBoxW(app->hwnd_main,
                    L"Load a voter list before checking for duplicate voters.",
                    k_WindowTitle,
                    MB_ICONINFORMATION | MB_OK);
        return;
    }

    if (EeVoterTable_FindBirthdateColumn(&app->table) < 0)
    {
        MessageBoxW(app->hwnd_main,
                    L"No birth date data is available",
                    k_WindowTitle,
                    MB_ICONINFORMATION | MB_OK);
        return;
    }

    if (app->table.row_count >= k_ScanModalMinRows)
    {
        App_StartDuplicateScan(app, EE_SCAN_DUP_NAME_DOB);
    }
    else
    {
        App_RunDuplicateScanSync(app, EE_SCAN_DUP_NAME_DOB);
    }
}

static int App_PickItem(AppState *app,
                        const wchar_t *caption,
                        const wchar_t *prompt,
                        const wchar_t *ok_label,
                        const wchar_t *const *items,
                        int count);

/* Filter → Show Duplicate Voters Voting…: voters of this list with the same name and DOB
 * who voted (Voter ID in a loaded roster) under different Voter IDs. With several rosters
 * open, the user picks one. */
static void App_ShowDuplicateVotersVoting(AppState *app)
{
    AppState *rosters[32];
    wchar_t labels[32][MAX_PATH + 32];
    const wchar_t *items[32];
    int n = 0;
    int pick = 0;
    AppState *p;
    AppState *r;
    uint8_t *marks;
    uint32_t count = 0;
    uint32_t groups = 0;
    HCURSOR prev;
    BOOL ok;
    const wchar_t *base;
    wchar_t msg[MAX_PATH + 160];

    if (app == NULL || app->is_roster)
    {
        return;
    }
    if (app->loading || app->scanning || app->table.row_count == 0)
    {
        MessageBoxW(app->hwnd_main, L"Load a voter list before checking for duplicate voters.",
                    k_WindowTitle, MB_ICONINFORMATION | MB_OK);
        return;
    }
    if (EeVoterTable_FindBirthdateColumn(&app->table) < 0)
    {
        MessageBoxW(app->hwnd_main, L"No birth date data is available", k_WindowTitle,
                    MB_ICONINFORMATION | MB_OK);
        return;
    }
    for (p = g_viewers; p != NULL && n < (int)ARRAYSIZE(rosters); p = p->next)
    {
        if (p->is_roster && p->table.row_count > 0 && !p->loading)
        {
            rosters[n] = p;
            labels[n][0] = L'\0';
            GetWindowTextW(p->hwnd_main, labels[n], ARRAYSIZE(labels[n]));
            items[n] = labels[n];
            n++;
        }
    }
    if (n == 0)
    {
        MessageBoxW(app->hwnd_main,
                    L"Load a voter roster (File → Load Voter Roster…) to check which duplicate "
                    L"voters voted.",
                    k_WindowTitle, MB_ICONINFORMATION | MB_OK);
        return;
    }
    if (n > 1)
    {
        pick = App_PickItem(app, L"Select Voter Roster",
                            L"Several voter rosters are open. Choose the one to check:", L"OK",
                            items, n);
        if (pick < 0)
        {
            return;
        }
    }
    r = rosters[pick];

    marks = (uint8_t *)calloc((size_t)app->table.row_count, sizeof(uint8_t));
    if (marks == NULL)
    {
        MessageBoxW(app->hwnd_main, L"Out of memory.", k_WindowTitle, MB_ICONERROR | MB_OK);
        return;
    }
    prev = SetCursor(LoadCursorW(NULL, IDC_WAIT));
    ok = EeVoterTable_MarkDuplicateVotersVoting(&app->table, &r->table, marks, &count, &groups, NULL,
                                                NULL, NULL);
    SetCursor(prev);
    base = App_PathBaseName(r->load_path);
    if (!ok)
    {
        free(marks);
        MessageBoxW(app->hwnd_main, L"Out of memory.", k_WindowTitle, MB_ICONERROR | MB_OK);
        return;
    }
    if (count == 0)
    {
        free(marks);
        StringCchPrintfW(msg, ARRAYSIZE(msg),
                         L"No voters with the same name and date of birth voted under different "
                         L"Voter IDs in %s.",
                         base);
        MessageBoxW(app->hwnd_main, msg, k_WindowTitle, MB_ICONINFORMATION | MB_OK);
        return;
    }
    StringCchPrintfW(msg, ARRAYSIZE(msg), L"duplicate voters (name + DOB) voting in %s", base);
    App_ApplyMarks(app, marks, count, EE_SCAN_DUP_VOTING, EE_COL_NAME, msg);
    App_ActivateViewer(app);
}

static void App_AddQuickFilter(AppState *app,
                               uint32_t column,
                               const wchar_t *value,
                               EeFilterAction action)
{
    EeFilterRule r;
    if (app == NULL || value == NULL || app->table.row_count == 0)
    {
        return;
    }
    ZeroMemory(&r, sizeof(r));
    r.column = column;
    r.relation = EeRel_Is;
    r.action = action;
    r.enabled = TRUE;
    StringCchCopyW(r.value, ARRAYSIZE(r.value), value);
    if (EeFilter_Add(&app->filters, &r))
    {
        App_ClearSelection(app);
        App_ApplyFilter(app);
    }
}

static void App_DestroyFilter(AppState *app)
{
    if (app != NULL && app->hwnd_filter != NULL)
    {
        DestroyWindow(app->hwnd_filter);
        app->hwnd_filter = NULL;
    }
}

/* -------------------------------------------------------------------------- */
/* Reports (modeless Precinct / Address summary windows)                      */
/* -------------------------------------------------------------------------- */

struct ReportWindow
{
    AppState *app;       /* parent viewer (owns the table + filters)     */
    HWND hwnd;           /* this report's top-level window               */
    HWND list;           /* owner-data report list view                 */
    HWND status;         /* bottom status bar                           */
    int kind;            /* EE_REPORT_*                                  */
    uint32_t column;     /* source display column (Precinct / Address)  */
    EeValueCount *items; /* aggregated value + count, current sort order */
    uint32_t count;
    BOOL has_blank;      /* a "(blank)" row is present in items         */
    int sort_col;        /* 0 = value, 1 = count                        */
    BOOL sort_asc;
};

static WNDPROC g_old_report_list_proc = NULL;

/* Empty values are shown (and copied) as this label so incomplete records are
 * visible; the underlying value stays "" so filters match blank cells. */
static const wchar_t *report_display_value(const wchar_t *v)
{
    return (v == NULL || v[0] == L'\0') ? L"(blank)" : v;
}

/*
 * Cell drawing. The list view centers columns >= 1 from LVCFMT_CENTER, but always
 * left-aligns column 0, so the Precinct report centers its own column 0. The
 * "Number of Voters" column and the Address column are left to the default draw.
 */
static LRESULT Report_ListCustomDraw(ReportWindow *rw, NMLVCUSTOMDRAW *lvcd)
{
    HWND hwnd = lvcd->nmcd.hdr.hwndFrom;
    int item;
    BOOL selected;

    switch (lvcd->nmcd.dwDrawStage)
    {
        case CDDS_PREPAINT:
            return CDRF_NOTIFYITEMDRAW;

        case CDDS_ITEMPREPAINT:
            item = (int)lvcd->nmcd.dwItemSpec;
            selected = (ListView_GetItemState(hwnd, item, LVIS_SELECTED) & LVIS_SELECTED) != 0;
            if (selected)
            {
                lvcd->clrText = GetSysColor(COLOR_HIGHLIGHTTEXT);
                lvcd->clrTextBk = GetSysColor(COLOR_HIGHLIGHT);
                lvcd->nmcd.uItemState &= ~(CDIS_SELECTED | CDIS_FOCUS | CDIS_HOT);
                FillRect(lvcd->nmcd.hdc, &lvcd->nmcd.rc, GetSysColorBrush(COLOR_HIGHLIGHT));
            }
            return CDRF_NEWFONT | CDRF_NOTIFYSUBITEMDRAW;

        case CDDS_ITEMPREPAINT | CDDS_SUBITEM:
            item = (int)lvcd->nmcd.dwItemSpec;
            selected = (ListView_GetItemState(hwnd, item, LVIS_SELECTED) & LVIS_SELECTED) != 0;
            if (selected)
            {
                lvcd->clrText = GetSysColor(COLOR_HIGHLIGHTTEXT);
                lvcd->clrTextBk = GetSysColor(COLOR_HIGHLIGHT);
                lvcd->nmcd.uItemState &= ~(CDIS_SELECTED | CDIS_FOCUS | CDIS_HOT);
            }
            if (rw->kind == EE_REPORT_PRECINCT && lvcd->iSubItem == 0 && (uint32_t)item < rw->count)
            {
                RECT rc;
                HDC hdc = lvcd->nmcd.hdc;
                HFONT old_font;
                COLORREF old_text;
                COLORREF old_bk;
                int old_mode;
                HBRUSH brush;
                const wchar_t *text = report_display_value(rw->items[item].value);

                if (!ListView_GetSubItemRect(hwnd, item, 0, LVIR_BOUNDS, &rc))
                {
                    return selected ? CDRF_NEWFONT : CDRF_DODEFAULT;
                }
                /* LVIR_BOUNDS on subitem 0 spans the whole row; clamp to column 0. */
                {
                    RECT rc_label;
                    if (ListView_GetSubItemRect(hwnd, item, 0, LVIR_LABEL, &rc_label))
                    {
                        rc = rc_label;
                    }
                    else
                    {
                        rc.right = rc.left + ListView_GetColumnWidth(hwnd, 0);
                    }
                }

                if (selected)
                {
                    brush = GetSysColorBrush(COLOR_HIGHLIGHT);
                    old_text = SetTextColor(hdc, GetSysColor(COLOR_HIGHLIGHTTEXT));
                    old_bk = SetBkColor(hdc, GetSysColor(COLOR_HIGHLIGHT));
                }
                else
                {
                    brush = GetSysColorBrush(COLOR_WINDOW);
                    old_text = SetTextColor(hdc, GetSysColor(COLOR_WINDOWTEXT));
                    old_bk = SetBkColor(hdc, GetSysColor(COLOR_WINDOW));
                }
                FillRect(hdc, &rc, brush);

                old_font = (HFONT)SelectObject(
                    hdc,
                    rw->app->font_ui ? rw->app->font_ui : (HFONT)GetStockObject(DEFAULT_GUI_FONT));
                old_mode = SetBkMode(hdc, TRANSPARENT);
                {
                    RECT rc_text = rc;
                    rc_text.left += Scale(rw->app, 4);
                    rc_text.right -= Scale(rw->app, 4);
                    DrawTextW(hdc,
                              text,
                              -1,
                              &rc_text,
                              DT_CENTER | DT_VCENTER | DT_SINGLELINE | DT_END_ELLIPSIS |
                                  DT_NOPREFIX);
                }
                SetBkMode(hdc, old_mode);
                SelectObject(hdc, old_font);
                SetTextColor(hdc, old_text);
                SetBkColor(hdc, old_bk);
                return CDRF_SKIPDEFAULT;
            }
            return selected ? CDRF_NEWFONT : CDRF_DODEFAULT;

        default:
            return CDRF_DODEFAULT;
    }
}

/* Subclass the report list so its header draws bold/centered like the voter list
 * (header NM_CUSTOMDRAW is delivered to the list view, the header's parent). */
static LRESULT CALLBACK ReportListSubclass(HWND hwnd, UINT msg, WPARAM wParam, LPARAM lParam)
{
    if (msg == WM_NOTIFY)
    {
        NMHDR *nm = (NMHDR *)lParam;
        HWND header = ListView_GetHeader(hwnd);
        ReportWindow *rw = (ReportWindow *)GetWindowLongPtrW(GetParent(hwnd), GWLP_USERDATA);
        if (rw != NULL && nm != NULL && header != NULL && nm->hwndFrom == header &&
            nm->code == NM_CUSTOMDRAW)
        {
            return App_HeaderCustomDraw(rw->app, (NMCUSTOMDRAW *)lParam, TRUE);
        }
    }
    return CallWindowProcW(g_old_report_list_proc, hwnd, msg, wParam, lParam);
}

static void Report_UpdateStatus(ReportWindow *rw)
{
    wchar_t buf[128];
    const wchar_t *noun;
    if (rw->status == NULL)
    {
        return;
    }
    noun = (rw->kind == EE_REPORT_ADDRESS) ? L"addresses" : L"precincts";
    if (rw->has_blank)
    {
        StringCchPrintfW(buf, ARRAYSIZE(buf), L"%u unique %s (includes blank)", rw->count, noun);
    }
    else
    {
        StringCchPrintfW(buf, ARRAYSIZE(buf), L"%u unique %s", rw->count, noun);
    }
    SendMessageW(rw->status, SB_SETTEXTW, 0, (LPARAM)buf);
}

typedef struct ReportSortCtx
{
    int sort_col;
    BOOL asc;
    BOOL numeric_value; /* precinct sorts numerically */
} ReportSortCtx;

static int report_sort_cmp(void *ctxv, const void *a, const void *b)
{
    const ReportSortCtx *ctx = (const ReportSortCtx *)ctxv;
    const EeValueCount *pa = (const EeValueCount *)a;
    const EeValueCount *pb = (const EeValueCount *)b;
    const wchar_t *va = pa->value ? pa->value : L"";
    const wchar_t *vb = pb->value ? pb->value : L"";
    int c;

    if (ctx->sort_col == 1)
    {
        c = (pa->count < pb->count) ? -1 : (pa->count > pb->count ? 1 : 0);
        if (c == 0)
        {
            c = _wcsicmp(va, vb);
        }
    }
    else if (ctx->numeric_value)
    {
        unsigned long na = wcstoul(va, NULL, 10);
        unsigned long nb = wcstoul(vb, NULL, 10);
        c = (na < nb) ? -1 : (na > nb ? 1 : 0);
        if (c == 0)
        {
            c = _wcsicmp(va, vb);
        }
    }
    else
    {
        c = _wcsicmp(va, vb);
    }
    return ctx->asc ? c : -c;
}

static void Report_Sort(ReportWindow *rw)
{
    ReportSortCtx ctx;
    ctx.sort_col = rw->sort_col;
    ctx.asc = rw->sort_asc;
    ctx.numeric_value = (rw->column == EE_COL_PRECINCT);
    if (rw->items != NULL && rw->count > 1)
    {
        qsort_s(rw->items, rw->count, sizeof(EeValueCount), report_sort_cmp, &ctx);
    }
    if (rw->list != NULL)
    {
        ListView_RedrawItems(rw->list, 0, (int)rw->count);
        InvalidateRect(rw->list, NULL, FALSE);
    }
}

static void Report_CopySelected(ReportWindow *rw)
{
    int i;
    size_t total = 0;
    wchar_t *buf;
    wchar_t *p;
    char *utf8 = NULL;
    int u8len;

    if (rw->list == NULL)
    {
        return;
    }
    i = ListView_GetNextItem(rw->list, -1, LVNI_SELECTED);
    while (i >= 0)
    {
        if ((uint32_t)i < rw->count)
        {
            wchar_t num[16];
            const wchar_t *v = report_display_value(rw->items[i].value);
            StringCchPrintfW(num, ARRAYSIZE(num), L"%u", rw->items[i].count);
            total += wcslen(v) + wcslen(num) + 3; /* tab + CR + LF */
        }
        i = ListView_GetNextItem(rw->list, i, LVNI_SELECTED);
    }
    if (total == 0)
    {
        return;
    }
    buf = (wchar_t *)malloc((total + 1) * sizeof(wchar_t));
    if (buf == NULL)
    {
        return;
    }
    p = buf;
    i = ListView_GetNextItem(rw->list, -1, LVNI_SELECTED);
    while (i >= 0)
    {
        if ((uint32_t)i < rw->count)
        {
            const wchar_t *v = report_display_value(rw->items[i].value);
            const wchar_t *n;
            wchar_t num[16];
            StringCchPrintfW(num, ARRAYSIZE(num), L"%u", rw->items[i].count);
            while (*v)
            {
                *p++ = *v++;
            }
            *p++ = L'\t';
            for (n = num; *n; n++)
            {
                *p++ = *n;
            }
            *p++ = L'\r';
            *p++ = L'\n';
        }
        i = ListView_GetNextItem(rw->list, i, LVNI_SELECTED);
    }
    *p = L'\0';

    u8len = WideCharToMultiByte(CP_UTF8, 0, buf, -1, NULL, 0, NULL, NULL);
    if (u8len > 0)
    {
        utf8 = (char *)malloc((size_t)u8len);
        if (utf8 != NULL && WideCharToMultiByte(CP_UTF8, 0, buf, -1, utf8, u8len, NULL, NULL) > 0)
        {
            App_SetClipboardUtf8(rw->hwnd, utf8);
        }
    }
    free(utf8);
    free(buf);
}

static void Report_FilterSelected(ReportWindow *rw, EeFilterAction action)
{
    int i;
    BOOL any = FALSE;

    if (rw->app == NULL || rw->list == NULL)
    {
        return;
    }
    i = ListView_GetNextItem(rw->list, -1, LVNI_SELECTED);
    while (i >= 0)
    {
        /* Empty value is allowed: an "is (blank)" rule matches incomplete cells. */
        if ((uint32_t)i < rw->count && rw->items[i].value != NULL)
        {
            EeFilterRule r;
            ZeroMemory(&r, sizeof(r));
            r.column = rw->column;
            r.relation = EeRel_Is;
            r.action = action;
            r.enabled = TRUE;
            StringCchCopyW(r.value, ARRAYSIZE(r.value), rw->items[i].value);
            if (EeFilter_Add(&rw->app->filters, &r))
            {
                any = TRUE;
            }
        }
        i = ListView_GetNextItem(rw->list, i, LVNI_SELECTED);
    }
    if (any)
    {
        App_ClearSelection(rw->app);
        App_ApplyFilter(rw->app);
    }
}

static void Report_OnContextMenu(ReportWindow *rw, int iItem, int iSubItem, POINT screen)
{
    HMENU m;
    UINT cmd;

    if (iItem < 0 || (uint32_t)iItem >= rw->count)
    {
        return;
    }
    /* Match the main list: right-clicking an unselected row selects just it. */
    if (!(ListView_GetItemState(rw->list, iItem, LVIS_SELECTED) & LVIS_SELECTED))
    {
        ListView_SetItemState(rw->list, -1, 0, LVIS_SELECTED | LVIS_FOCUSED);
        ListView_SetItemState(rw->list,
                              iItem,
                              LVIS_SELECTED | LVIS_FOCUSED,
                              LVIS_SELECTED | LVIS_FOCUSED);
    }

    m = CreatePopupMenu();
    if (m == NULL)
    {
        return;
    }
    AppendMenuW(m, MF_STRING, IDM_EDIT_COPY, L"&Copy");
    if (iSubItem == 0)
    {
        wchar_t shown[48];
        wchar_t inc[96];
        wchar_t exc[96];
        BOOL is_blank = (rw->items[iItem].value == NULL || rw->items[iItem].value[0] == L'\0');
        const wchar_t *val = report_display_value(rw->items[iItem].value);
        StringCchCopyW(shown, ARRAYSIZE(shown), val);
        if (wcslen(val) >= 40)
        {
            shown[36] = L'.';
            shown[37] = L'.';
            shown[38] = L'.';
            shown[39] = L'\0';
        }
        StringCchPrintfW(inc, ARRAYSIZE(inc), L"&Include \"%s\"", shown);
        StringCchPrintfW(exc, ARRAYSIZE(exc), L"&Exclude \"%s\"", shown);
        AppendMenuW(m, MF_SEPARATOR, 0, NULL);
        AppendMenuW(m, MF_STRING, IDM_FILTER_INCLUDE, inc);
        AppendMenuW(m, MF_STRING, IDM_FILTER_EXCLUDE, exc);
        if (rw->kind == EE_REPORT_ADDRESS && !is_blank)
        {
            AppendMenuW(m, MF_SEPARATOR, 0, NULL);
            AppendMenuW(m, MF_STRING, IDM_SHOW_IN_MAPS, L"Show in &Maps…");
        }
    }
    AppendMenuW(m, MF_SEPARATOR, 0, NULL);
    AppendMenuW(m, MF_STRING, IDM_EXPORT_SELECTED, L"Export &Selected…");
    AppendMenuW(m, MF_STRING, IDM_EXPORT_ALL, L"Export &All…");

    cmd = (UINT)
        TrackPopupMenu(m, TPM_RIGHTBUTTON | TPM_RETURNCMD, screen.x, screen.y, 0, rw->hwnd, NULL);
    DestroyMenu(m);

    switch (cmd)
    {
        case IDM_EDIT_COPY:
            Report_CopySelected(rw);
            break;
        case IDM_EXPORT_SELECTED:
            App_ExportVoterReport(rw, TRUE);
            break;
        case IDM_EXPORT_ALL:
            App_ExportVoterReport(rw, FALSE);
            break;
        case IDM_FILTER_INCLUDE:
            Report_FilterSelected(rw, EeFilt_Include);
            break;
        case IDM_FILTER_EXCLUDE:
            Report_FilterSelected(rw, EeFilt_Exclude);
            break;
        case IDM_SHOW_IN_MAPS:
            if (rw->kind == EE_REPORT_ADDRESS)
            {
                App_ShowAddressInMaps(rw->app, rw->items[iItem].value);
            }
            break;
        default:
            break;
    }
}

static void Report_LayoutList(ReportWindow *rw, int width, int height)
{
    int num_w;
    int val_w;
    int sb_h = 0;

    if (rw->status != NULL)
    {
        RECT sb;
        SendMessageW(rw->status, WM_SIZE, 0, 0); /* dock to the bottom */
        if (GetWindowRect(rw->status, &sb))
        {
            sb_h = sb.bottom - sb.top;
        }
    }
    if (rw->list == NULL)
    {
        return;
    }
    if (height > sb_h)
    {
        height -= sb_h;
    }
    else
    {
        height = 0;
    }
    MoveWindow(rw->list, 0, 0, width, height, TRUE);
    num_w = Scale(rw->app, 130);
    val_w = width - num_w - Scale(rw->app, 24);
    if (val_w < Scale(rw->app, 120))
    {
        val_w = Scale(rw->app, 120);
    }
    ListView_SetColumnWidth(rw->list, 0, val_w);
    ListView_SetColumnWidth(rw->list, 1, num_w);
}

static LRESULT CALLBACK ReportWndProc(HWND hwnd, UINT msg, WPARAM wParam, LPARAM lParam)
{
    ReportWindow *rw = (ReportWindow *)GetWindowLongPtrW(hwnd, GWLP_USERDATA);

    switch (msg)
    {
        case WM_CREATE:
        {
            CREATESTRUCTW *cs = (CREATESTRUCTW *)lParam;
            LVCOLUMNW col;
            RECT rc;
            rw = (ReportWindow *)cs->lpCreateParams;
            SetWindowLongPtrW(hwnd, GWLP_USERDATA, (LONG_PTR)rw);
            rw->hwnd = hwnd;

            GetClientRect(hwnd, &rc);
            rw->list = CreateWindowExW(0,
                                       WC_LISTVIEWW,
                                       L"",
                                       WS_CHILD | WS_VISIBLE | LVS_REPORT | LVS_OWNERDATA |
                                           LVS_SHOWSELALWAYS,
                                       0,
                                       0,
                                       rc.right,
                                       rc.bottom,
                                       hwnd,
                                       NULL,
                                       rw->app->instance,
                                       NULL);
            if (rw->list == NULL)
            {
                return -1;
            }
            /* Subclass so the header draws bold/centered like the voter list. */
            {
                WNDPROC old = (WNDPROC)SetWindowLongPtrW(rw->list,
                                                         GWLP_WNDPROC,
                                                         (LONG_PTR)ReportListSubclass);
                if (g_old_report_list_proc == NULL)
                {
                    g_old_report_list_proc = old;
                }
            }
            ListView_SetExtendedListViewStyle(rw->list,
                                              LVS_EX_FULLROWSELECT | LVS_EX_DOUBLEBUFFER |
                                                  LVS_EX_GRIDLINES);
            if (rw->app->font_ui)
            {
                SendMessageW(rw->list, WM_SETFONT, (WPARAM)rw->app->font_ui, TRUE);
            }
            ZeroMemory(&col, sizeof(col));
            col.mask = LVCF_TEXT | LVCF_WIDTH | LVCF_FMT;
            /* Headers are centered for both reports; the Precinct data column is
             * centered by Report_ListCustomDraw, the count column by LVCFMT_CENTER. */
            col.fmt = LVCFMT_CENTER;
            col.pszText = (rw->kind == EE_REPORT_ADDRESS) ? L"Address" : L"Precinct";
            col.cx = Scale(rw->app, (rw->kind == EE_REPORT_ADDRESS) ? 320 : 160);
            ListView_InsertColumn(rw->list, 0, &col);
            col.fmt = LVCFMT_CENTER;
            col.pszText = L"Number of Voters";
            col.cx = Scale(rw->app, 130);
            ListView_InsertColumn(rw->list, 1, &col);

            rw->status = CreateWindowExW(0,
                                         STATUSCLASSNAMEW,
                                         NULL,
                                         WS_CHILD | WS_VISIBLE | SBARS_SIZEGRIP,
                                         0,
                                         0,
                                         0,
                                         0,
                                         hwnd,
                                         NULL,
                                         rw->app->instance,
                                         NULL);
            if (rw->status != NULL && rw->app->font_ui)
            {
                SendMessageW(rw->status, WM_SETFONT, (WPARAM)rw->app->font_ui, TRUE);
            }
            Report_UpdateStatus(rw);

            ListView_SetItemCountEx(rw->list, (int)rw->count, LVSICF_NOINVALIDATEALL);
            Report_Sort(rw); /* initial ascending by value */
            Report_LayoutList(rw, rc.right, rc.bottom);
            return 0;
        }

        case WM_SIZE:
            if (rw != NULL)
            {
                Report_LayoutList(rw, LOWORD(lParam), HIWORD(lParam));
            }
            return 0;

        case WM_SETFOCUS:
            if (rw != NULL && rw->list != NULL)
            {
                SetFocus(rw->list);
            }
            return 0;

        case WM_COMMAND:
            /* Ctrl+C is routed here by the shared accelerator table. */
            if (rw != NULL && LOWORD(wParam) == IDM_EDIT_COPY)
            {
                Report_CopySelected(rw);
                return 0;
            }
            break;

        case WM_NOTIFY:
        {
            NMHDR *hdr = (NMHDR *)lParam;
            if (rw == NULL || hdr->hwndFrom != rw->list)
            {
                break;
            }
            if (hdr->code == NM_CUSTOMDRAW)
            {
                return Report_ListCustomDraw(rw, (NMLVCUSTOMDRAW *)lParam);
            }
            if (hdr->code == LVN_GETDISPINFOW)
            {
                NMLVDISPINFOW *di = (NMLVDISPINFOW *)lParam;
                int idx = di->item.iItem;
                if ((di->item.mask & LVIF_TEXT) && idx >= 0 && (uint32_t)idx < rw->count)
                {
                    if (di->item.iSubItem == 0)
                    {
                        StringCchCopyW(di->item.pszText,
                                       di->item.cchTextMax,
                                       report_display_value(rw->items[idx].value));
                    }
                    else
                    {
                        StringCchPrintfW(di->item.pszText,
                                         di->item.cchTextMax,
                                         L"%u",
                                         rw->items[idx].count);
                    }
                }
                return 0;
            }
            if (hdr->code == LVN_COLUMNCLICK)
            {
                NMLISTVIEW *nlv = (NMLISTVIEW *)lParam;
                if (nlv->iSubItem == rw->sort_col)
                {
                    rw->sort_asc = !rw->sort_asc;
                }
                else
                {
                    rw->sort_col = nlv->iSubItem;
                    rw->sort_asc = TRUE;
                }
                Report_Sort(rw);
                return 0;
            }
            if (hdr->code == NM_RCLICK)
            {
                LPNMITEMACTIVATE ia = (LPNMITEMACTIVATE)lParam;
                POINT screen = ia->ptAction;
                ClientToScreen(rw->list, &screen);
                Report_OnContextMenu(rw, ia->iItem, ia->iSubItem, screen);
                return 0;
            }
            break;
        }

        case WM_DESTROY:
            if (rw != NULL)
            {
                if (rw->app != NULL)
                {
                    if (rw->app->report_precinct == rw)
                    {
                        rw->app->report_precinct = NULL;
                    }
                    if (rw->app->report_address == rw)
                    {
                        rw->app->report_address = NULL;
                    }
                }
                EeVoterTable_FreeValueCounts(rw->items, rw->count);
                free(rw);
                SetWindowLongPtrW(hwnd, GWLP_USERDATA, 0);
            }
            return 0;

        default:
            break;
    }
    return DefWindowProcW(hwnd, msg, wParam, lParam);
}

static const wchar_t *App_PathBaseName(const wchar_t *path)
{
    const wchar_t *base;
    const wchar_t *p;
    if (path == NULL)
    {
        return L"";
    }
    base = path;
    for (p = path; *p != L'\0'; p++)
    {
        if (*p == L'\\' || *p == L'/')
        {
            base = p + 1;
        }
    }
    return base;
}

/* -------------------------------------------------------------------------- */
/* Delimited-text export (CSV / TSV)                                          */
/* -------------------------------------------------------------------------- */

/* Small growable UTF-8 buffer for building export text from wide strings. */
typedef struct Utf8Export
{
    char *data;
    size_t len;
    size_t cap;
} Utf8Export;

static BOOL Utf8Export_Append(Utf8Export *b, const char *s, size_t n)
{
    if (n == 0)
    {
        return TRUE;
    }
    if (b->len + n + 1 > b->cap)
    {
        size_t nc = (b->cap == 0) ? 4096 : b->cap;
        char *nb;
        while (nc < b->len + n + 1)
        {
            if (nc > (size_t)-1 / 2)
            {
                nc = b->len + n + 1;
                break;
            }
            nc *= 2;
        }
        nb = (char *)realloc(b->data, nc);
        if (nb == NULL)
        {
            return FALSE;
        }
        b->data = nb;
        b->cap = nc;
    }
    memcpy(b->data + b->len, s, n);
    b->len += n;
    b->data[b->len] = '\0';
    return TRUE;
}

static BOOL Utf8Export_AppendChar(Utf8Export *b, char c)
{
    return Utf8Export_Append(b, &c, 1);
}

/* Append a wide string as one RFC-4180 field (quoted when it contains @p delim, a
 * quote, or a newline). */
static BOOL Utf8Export_AppendFieldW(Utf8Export *b, const wchar_t *w, char delim)
{
    char stackbuf[1024];
    char *heap = NULL;
    char *u8 = stackbuf;
    int need;
    const char *p;
    BOOL quote = FALSE;
    BOOL ok = TRUE;

    if (w == NULL)
    {
        w = L"";
    }
    need = WideCharToMultiByte(CP_UTF8, 0, w, -1, NULL, 0, NULL, NULL);
    if (need <= 0)
    {
        return TRUE; /* nothing to write */
    }
    if ((size_t)need > sizeof(stackbuf))
    {
        heap = (char *)malloc((size_t)need);
        if (heap == NULL)
        {
            return FALSE;
        }
        u8 = heap;
    }
    WideCharToMultiByte(CP_UTF8, 0, w, -1, u8, need, NULL, NULL);
    for (p = u8; *p != '\0'; p++)
    {
        if (*p == delim || *p == '"' || *p == '\n' || *p == '\r')
        {
            quote = TRUE;
            break;
        }
    }
    if (!quote)
    {
        ok = Utf8Export_Append(b, u8, strlen(u8));
    }
    else
    {
        ok = Utf8Export_Append(b, "\"", 1);
        for (p = u8; *p != '\0' && ok; p++)
        {
            if (*p == '"')
            {
                ok = Utf8Export_Append(b, "\"\"", 2);
            }
            else
            {
                ok = Utf8Export_Append(b, p, 1);
            }
        }
        ok = ok && Utf8Export_Append(b, "\"", 1);
    }
    free(heap);
    return ok;
}

/* Copy the file's base name without its extension into @p out (e.g.
 * "C:\x\L26 CVR.xlsx" -> "L26 CVR"). */
static void App_BaseNameNoExt(const wchar_t *path, wchar_t *out, size_t cch)
{
    const wchar_t *base = App_PathBaseName(path);
    const wchar_t *dot;
    size_t n;
    if (out == NULL || cch == 0)
    {
        return;
    }
    dot = NULL;
    {
        const wchar_t *p;
        for (p = base; *p != L'\0'; p++)
        {
            if (*p == L'.')
            {
                dot = p;
            }
        }
    }
    n = (dot != NULL) ? (size_t)(dot - base) : wcslen(base);
    if (n >= cch)
    {
        n = cch - 1;
    }
    memcpy(out, base, n * sizeof(wchar_t));
    out[n] = L'\0';
}

/* Prompt for an export path. @p base is the suggested file name (no extension);
 * the Save dialog defaults to UTF-8 CSV, with UTF-8 TSV as the alternative. On OK,
 * writes the full path to @p path and the field delimiter (',' or '\t') to
 * *out_delim, and returns TRUE. */
static BOOL App_PromptExportPath(HWND owner,
                                 const wchar_t *base,
                                 wchar_t *path,
                                 size_t cch,
                                 char *out_delim)
{
    OPENFILENAMEW ofn;
    wchar_t file[MAX_PATH];

    if (path == NULL || cch == 0 || out_delim == NULL)
    {
        return FALSE;
    }
    file[0] = L'\0';
    if (base != NULL && base[0] != L'\0')
    {
        StringCchCopyW(file, ARRAYSIZE(file), base);
        StringCchCatW(file, ARRAYSIZE(file), L".csv");
    }

    ZeroMemory(&ofn, sizeof(ofn));
    ofn.lStructSize = sizeof(ofn);
    ofn.hwndOwner = owner;
    ofn.lpstrFilter = L"UTF-8 CSV (*.csv)\0*.csv\0UTF-8 TSV (*.tsv)\0*.tsv\0";
    ofn.nFilterIndex = 1; /* CSV is the default */
    ofn.lpstrFile = file;
    ofn.nMaxFile = ARRAYSIZE(file);
    ofn.Flags = OFN_OVERWRITEPROMPT | OFN_PATHMUSTEXIST | OFN_NOREADONLYRETURN | OFN_EXPLORER;
    ofn.lpstrTitle = L"Export";

    if (!GetSaveFileNameW(&ofn))
    {
        return FALSE;
    }
    /* Honor an explicitly-typed .csv/.tsv extension; otherwise take the type from the
     * chosen filter and append the matching extension so the name matches the format. */
    {
        size_t len = wcslen(file);
        if (len >= 4 && _wcsicmp(file + (len - 4), L".tsv") == 0)
        {
            *out_delim = '\t';
        }
        else if (len >= 4 && _wcsicmp(file + (len - 4), L".csv") == 0)
        {
            *out_delim = ',';
        }
        else if (ofn.nFilterIndex == 2)
        {
            *out_delim = '\t';
            StringCchCatW(file, ARRAYSIZE(file), L".tsv");
        }
        else
        {
            *out_delim = ',';
            StringCchCatW(file, ARRAYSIZE(file), L".csv");
        }
    }
    StringCchCopyW(path, cch, file);
    return TRUE;
}

/* Write a UTF-8 byte buffer to @p path preceded by a UTF-8 BOM (so Excel opens it
 * as UTF-8). Shows an error box and returns FALSE on failure. */
static BOOL App_WriteExportUtf8(HWND owner, const wchar_t *path, const char *utf8, size_t len)
{
    HANDLE h;
    static const unsigned char kBom[3] = {0xEF, 0xBB, 0xBF};
    DWORD wrote = 0;
    BOOL ok = TRUE;

    h = CreateFileW(path, GENERIC_WRITE, 0, NULL, CREATE_ALWAYS, FILE_ATTRIBUTE_NORMAL, NULL);
    if (h == INVALID_HANDLE_VALUE)
    {
        MessageBoxW(owner, L"Could not create the export file.", L"Export", MB_ICONERROR | MB_OK);
        return FALSE;
    }
    if (!WriteFile(h, kBom, sizeof(kBom), &wrote, NULL) || wrote != sizeof(kBom))
    {
        ok = FALSE;
    }
    if (ok && len > 0)
    {
        size_t off = 0;
        while (off < len)
        {
            DWORD want = (DWORD)((len - off > 0x1000000u) ? 0x1000000u : (len - off));
            if (!WriteFile(h, utf8 + off, want, &wrote, NULL) || wrote == 0)
            {
                ok = FALSE;
                break;
            }
            off += wrote;
        }
    }
    CloseHandle(h);
    if (!ok)
    {
        MessageBoxW(owner, L"Could not write the export file.", L"Export", MB_ICONERROR | MB_OK);
        return FALSE;
    }
    return TRUE;
}

/* Collect the selected list-view item indices (those below @p count) into a malloc'd
 * array in display order; sets *out_n. Returns NULL with *out_n == 0 when nothing is
 * selected or on OOM. */
static uint32_t *App_CollectSelectedIndices(HWND list, uint32_t count, uint32_t *out_n)
{
    uint32_t *idx = NULL;
    uint32_t n = 0;
    uint32_t cap = 0;
    int i;

    *out_n = 0;
    if (list == NULL)
    {
        return NULL;
    }
    i = ListView_GetNextItem(list, -1, LVNI_SELECTED);
    while (i >= 0)
    {
        if ((uint32_t)i < count)
        {
            if (n == cap)
            {
                uint32_t nc = cap ? cap * 2u : 64u;
                uint32_t *g = (uint32_t *)realloc(idx, (size_t)nc * sizeof(uint32_t));
                if (g == NULL)
                {
                    free(idx);
                    return NULL;
                }
                idx = g;
                cap = nc;
            }
            idx[n++] = (uint32_t)i;
        }
        i = ListView_GetNextItem(list, i, LVNI_SELECTED);
    }
    *out_n = n;
    return idx;
}

/* Report-window exporter: build delimited UTF-8 from a small in-memory model via a
 * cell callback, then prompt + write. @p base already includes the window-specific
 * suffix (e.g. "L26-Selected_Precincts"). Used by the (small) report windows. */
typedef void (*EeExportCellFn)(void *user, uint32_t row, uint32_t col, wchar_t *buf, size_t cch);

static BOOL App_ExportReportModel(HWND owner,
                                  const wchar_t *base,
                                  uint32_t ncols,
                                  const wchar_t *const *headers,
                                  uint32_t nrows,
                                  EeExportCellFn get_cell,
                                  void *user)
{
    wchar_t path[MAX_PATH];
    char delim = ',';
    Utf8Export ex;
    uint32_t r;
    uint32_t c;
    BOOL ok = TRUE;

    if (get_cell == NULL || ncols == 0)
    {
        return FALSE;
    }
    if (!App_PromptExportPath(owner, base, path, ARRAYSIZE(path), &delim))
    {
        return FALSE; /* user cancelled */
    }
    ZeroMemory(&ex, sizeof(ex));
    if (headers != NULL)
    {
        for (c = 0; c < ncols && ok; c++)
        {
            if (c > 0)
            {
                ok = Utf8Export_AppendChar(&ex, delim);
            }
            ok = ok && Utf8Export_AppendFieldW(&ex, headers[c], delim);
        }
        ok = ok && Utf8Export_Append(&ex, "\r\n", 2);
    }
    for (r = 0; r < nrows && ok; r++)
    {
        for (c = 0; c < ncols && ok; c++)
        {
            wchar_t cell[1024];
            cell[0] = L'\0';
            get_cell(user, r, c, cell, ARRAYSIZE(cell));
            if (c > 0)
            {
                ok = Utf8Export_AppendChar(&ex, delim);
            }
            ok = ok && Utf8Export_AppendFieldW(&ex, cell, delim);
        }
        ok = ok && Utf8Export_Append(&ex, "\r\n", 2);
    }
    if (!ok)
    {
        free(ex.data);
        MessageBoxW(owner, L"Out of memory building the export.", L"Export", MB_ICONERROR | MB_OK);
        return FALSE;
    }
    ok = App_WriteExportUtf8(owner, path, ex.data ? ex.data : "", ex.len);
    free(ex.data);
    return ok;
}

/* Small modal: one checkbox "Include normalized data fields" (default off). */
typedef struct ExportOptData
{
    AppState *app;
    HWND owner;
    BOOL include_normalized;
} ExportOptData;

static INT_PTR CALLBACK ExportOptDlgProc(HWND dlg, UINT msg, WPARAM wParam, LPARAM lParam)
{
    ExportOptData *d = (ExportOptData *)GetWindowLongPtrW(dlg, GWLP_USERDATA);
    switch (msg)
    {
        case WM_INITDIALOG:
        {
            AppState *app;
            RECT rc;
            RECT owner;
            int margin;
            int btn_w;
            int btn_h;
            int gap;
            int cx;
            int cy;
            int ow;
            int oh;
            int x = CW_USEDEFAULT;
            int y = CW_USEDEFAULT;
            HWND chk;
            HWND ok;
            HWND cancel;

            d = (ExportOptData *)lParam;
            SetWindowLongPtrW(dlg, GWLP_USERDATA, (LONG_PTR)d);
            app = d->app;

            cx = Scale(app, 300);
            cy = Scale(app, 112);
            rc.left = 0;
            rc.top = 0;
            rc.right = cx;
            rc.bottom = cy;
            AdjustWindowRectEx(&rc,
                               (DWORD)GetWindowLongPtrW(dlg, GWL_STYLE),
                               FALSE,
                               (DWORD)GetWindowLongPtrW(dlg, GWL_EXSTYLE));
            ow = rc.right - rc.left;
            oh = rc.bottom - rc.top;
            if (d->owner != NULL && GetWindowRect(d->owner, &owner))
            {
                x = owner.left + ((owner.right - owner.left) - ow) / 2;
                y = owner.top + ((owner.bottom - owner.top) - oh) / 2;
            }
            SetWindowPos(dlg, NULL, x, y, ow, oh, SWP_NOZORDER | SWP_NOACTIVATE);

            GetClientRect(dlg, &rc);
            margin = Scale(app, 14);
            btn_w = Scale(app, 84);
            btn_h = Scale(app, 26);
            gap = Scale(app, 8);
            chk = CreateWindowExW(0,
                                  L"BUTTON",
                                  L"Include normalized data fields",
                                  WS_CHILD | WS_VISIBLE | WS_TABSTOP | BS_AUTOCHECKBOX,
                                  margin,
                                  margin,
                                  rc.right - 2 * margin,
                                  Scale(app, 24),
                                  dlg,
                                  (HMENU)(INT_PTR)IDC_EXPORT_NORMALIZED,
                                  app->instance,
                                  NULL);
            SendMessageW(chk, BM_SETCHECK, d->include_normalized ? BST_CHECKED : BST_UNCHECKED, 0);
            ok = CreateWindowExW(0,
                                 L"BUTTON",
                                 L"OK",
                                 WS_CHILD | WS_VISIBLE | WS_TABSTOP | BS_DEFPUSHBUTTON,
                                 rc.right - margin - 2 * btn_w - gap,
                                 rc.bottom - margin - btn_h,
                                 btn_w,
                                 btn_h,
                                 dlg,
                                 (HMENU)(INT_PTR)IDOK,
                                 app->instance,
                                 NULL);
            cancel = CreateWindowExW(0,
                                     L"BUTTON",
                                     L"Cancel",
                                     WS_CHILD | WS_VISIBLE | WS_TABSTOP,
                                     rc.right - margin - btn_w,
                                     rc.bottom - margin - btn_h,
                                     btn_w,
                                     btn_h,
                                     dlg,
                                     (HMENU)(INT_PTR)IDCANCEL,
                                     app->instance,
                                     NULL);
            if (app->font_ui != NULL)
            {
                SendMessageW(chk, WM_SETFONT, (WPARAM)app->font_ui, TRUE);
                SendMessageW(ok, WM_SETFONT, (WPARAM)app->font_ui, TRUE);
                SendMessageW(cancel, WM_SETFONT, (WPARAM)app->font_ui, TRUE);
            }
            SetFocus(ok);
            return (INT_PTR)FALSE;
        }

        case WM_COMMAND:
        {
            WORD id = LOWORD(wParam);
            if (id == IDOK)
            {
                if (d != NULL)
                {
                    d->include_normalized = (SendMessageW(GetDlgItem(dlg, IDC_EXPORT_NORMALIZED),
                                                          BM_GETCHECK,
                                                          0,
                                                          0) == BST_CHECKED);
                }
                EndDialog(dlg, 1);
                return (INT_PTR)TRUE;
            }
            if (id == IDCANCEL)
            {
                EndDialog(dlg, 0);
                return (INT_PTR)TRUE;
            }
            break;
        }

        case WM_CLOSE:
            EndDialog(dlg, 0);
            return (INT_PTR)TRUE;

        default:
            break;
    }
    return (INT_PTR)FALSE;
}

/* Ask whether to include normalized fields in a voter export. Returns TRUE if the
 * user confirmed (result in *out_include); FALSE if cancelled. */
static BOOL App_AskExportNormalized(AppState *app, HWND owner, BOOL *out_include)
{
    ExportOptData d;
    DWORD buf[64]; /* DWORD-aligned for DLGTEMPLATE */
    DLGTEMPLATE *dt = (DLGTEMPLATE *)buf;
    BYTE *p;
    static const wchar_t k_caption[] = L"Export Options";
    size_t clen = ARRAYSIZE(k_caption) - 1;

    if (app == NULL || out_include == NULL)
    {
        return FALSE;
    }
    d.app = app;
    d.owner = owner;
    d.include_normalized = FALSE; /* default off */

    ZeroMemory(buf, sizeof(buf));
    dt->style = WS_POPUP | WS_CAPTION | WS_SYSMENU | DS_MODALFRAME;
    dt->cx = 190;
    dt->cy = 74;
    p = (BYTE *)buf + sizeof(DLGTEMPLATE);
    *(WORD *)p = 0;
    p += sizeof(WORD);
    *(WORD *)p = 0;
    p += sizeof(WORD);
    memcpy(p, k_caption, clen * sizeof(WCHAR));
    ((WCHAR *)p)[clen] = L'\0';

    if (DialogBoxIndirectParamW(app->instance, dt, owner, ExportOptDlgProc, (LPARAM)&d) == 1)
    {
        *out_include = d.include_normalized;
        return TRUE;
    }
    return FALSE;
}

/* Export voter rows (all visible, or just the selected ones) as CSV/TSV, in the
 * current display order, with a header row. */
static void App_ExportVoters(AppState *app, BOOL selection_only)
{
    uint32_t *rows = NULL;
    uint32_t n = 0;
    BOOL filtered;
    BOOL include_normalized = FALSE;
    wchar_t base[192];
    wchar_t suggested[256];
    wchar_t path[MAX_PATH];
    char delim = ',';
    char *text = NULL;
    size_t len = 0;

    if (app == NULL || app->loading || app->table.row_count == 0)
    {
        return;
    }
    filtered = (app->mark_active || EeFilter_HasEnabled(&app->filters));

    if (selection_only)
    {
        int i = ListView_GetNextItem(app->hwnd_frozen, -1, LVNI_SELECTED);
        uint32_t cap = 0;
        while (i >= 0)
        {
            if (n == cap)
            {
                uint32_t nc = cap ? cap * 2u : 64u;
                uint32_t *g = (uint32_t *)realloc(rows, (size_t)nc * sizeof(uint32_t));
                if (g == NULL)
                {
                    free(rows);
                    return;
                }
                rows = g;
                cap = nc;
            }
            rows[n++] = App_ViewRowFromDisplay(app, (uint32_t)i);
            i = ListView_GetNextItem(app->hwnd_frozen, i, LVNI_SELECTED);
        }
        if (n == 0)
        {
            App_SetStatus(app, L"Nothing selected to export.");
            return;
        }
    }
    else
    {
        uint32_t vis = App_VisibleCount(app);
        uint32_t d;
        if (vis == 0)
        {
            App_SetStatus(app, L"Nothing to export.");
            return;
        }
        rows = (uint32_t *)malloc((size_t)vis * sizeof(uint32_t));
        if (rows == NULL)
        {
            return;
        }
        for (d = 0; d < vis; d++)
        {
            rows[d] = App_ViewRowFromDisplay(app, d);
        }
        n = vis;
    }

    /* A roster exports its own columns only (they reload into the same roster). */
    if (!app->is_roster && !App_AskExportNormalized(app, app->hwnd_main, &include_normalized))
    {
        free(rows);
        return;
    }

    App_BaseNameNoExt(app->load_path, base, ARRAYSIZE(base));
    StringCchPrintfW(suggested,
                     ARRAYSIZE(suggested),
                     L"%s%s",
                     base,
                     app->is_roster
                         ? (selection_only ? L"-Selected_Roster"
                                           : (filtered ? L"-Filtered_Roster" : L"-Roster"))
                         : (selection_only ? L"-Selected_Voters"
                                           : (filtered ? L"-Filtered_Voters" : L"-All_Voters")));

    if (!App_PromptExportPath(app->hwnd_main, suggested, path, ARRAYSIZE(path), &delim))
    {
        free(rows);
        return;
    }
    if (!EeVoterTable_FormatDelimitedUtf8(&app->table,
                                          rows,
                                          n,
                                          include_normalized,
                                          delim,
                                          TRUE,
                                          &text,
                                          &len))
    {
        free(rows);
        MessageBoxW(app->hwnd_main, L"Out of memory building the export.", L"Export",
                    MB_ICONERROR | MB_OK);
        return;
    }
    if (App_WriteExportUtf8(app->hwnd_main, path, text, len))
    {
        wchar_t st[96];
        StringCchPrintfW(st, ARRAYSIZE(st), L"Exported %u row%s.", n, n == 1u ? L"" : L"s");
        App_SetStatus(app, st);
    }
    free(text);
    free(rows);
}

/* Cell provider for a voter Precinct/Address report export. */
typedef struct ReportExportCtx
{
    ReportWindow *rw;
    const uint32_t *idx; /* export row -> item index, or NULL for identity */
} ReportExportCtx;

static void Report_ExportCell(void *user, uint32_t row, uint32_t col, wchar_t *buf, size_t cch)
{
    ReportExportCtx *c = (ReportExportCtx *)user;
    uint32_t item = (c->idx != NULL) ? c->idx[row] : row;
    if (item >= c->rw->count)
    {
        buf[0] = L'\0';
        return;
    }
    if (col == 0)
    {
        StringCchCopyW(buf, cch, report_display_value(c->rw->items[item].value));
    }
    else
    {
        StringCchPrintfW(buf, cch, L"%u", c->rw->items[item].count);
    }
}

static void App_ExportVoterReport(ReportWindow *rw, BOOL selection_only)
{
    const wchar_t *headers[2];
    const wchar_t *label;
    const wchar_t *plural;
    uint32_t *idx = NULL;
    uint32_t n = 0;
    ReportExportCtx ctx;
    wchar_t base[192];
    wchar_t suggested[256];

    if (rw == NULL || rw->app == NULL || rw->count == 0)
    {
        return;
    }
    label = (rw->kind == EE_REPORT_ADDRESS) ? L"Address" : L"Precinct";
    plural = (rw->kind == EE_REPORT_ADDRESS) ? L"Addresses" : L"Precincts";
    headers[0] = label;
    headers[1] = L"Number of Voters";

    if (selection_only)
    {
        idx = App_CollectSelectedIndices(rw->list, rw->count, &n);
        if (n == 0)
        {
            free(idx);
            return;
        }
    }
    else
    {
        n = rw->count;
    }

    ctx.rw = rw;
    ctx.idx = idx;

    App_BaseNameNoExt(rw->app->load_path, base, ARRAYSIZE(base));
    StringCchPrintfW(suggested,
                     ARRAYSIZE(suggested),
                     L"%s-%s_%s",
                     base,
                     selection_only ? L"Selected" : L"All",
                     plural);

    App_ExportReportModel(rw->hwnd, suggested, 2, headers, n, Report_ExportCell, &ctx);
    free(idx);
}

static void App_ShowReport(AppState *app, int kind)
{
    ReportWindow **slot;
    uint32_t column;
    const wchar_t *none_msg;
    const wchar_t *label;
    EeValueCount *items = NULL;
    uint32_t count = 0;
    uint32_t blank = 0;
    ReportWindow *rw;
    wchar_t title[MAX_PATH + 64];
    const wchar_t *base;
    RECT pr;
    int x = CW_USEDEFAULT;
    int y = CW_USEDEFAULT;
    int w;
    int h;

    if (app == NULL || app->loading || app->table.row_count == 0)
    {
        return;
    }
    if (kind == EE_REPORT_ADDRESS)
    {
        slot = &app->report_address;
        column = EE_COL_ADDRESS;
        none_msg = L"No address information available";
        label = L"Address";
    }
    else
    {
        slot = &app->report_precinct;
        column = EE_COL_PRECINCT;
        none_msg = L"No precinct information available";
        label = L"Precinct";
    }

    if (*slot != NULL)
    {
        SetForegroundWindow((*slot)->hwnd);
        return;
    }

    if (!EeVoterTable_CollectValueCounts(&app->table, column, &items, &count, &blank) || count == 0)
    {
        /* count == 0 means no actual precinct/address values (all blank or none). */
        EeVoterTable_FreeValueCounts(items, count);
        MessageBoxW(app->hwnd_main, none_msg, k_WindowTitle, MB_ICONINFORMATION | MB_OK);
        return;
    }

    /* Surface incomplete records: append a "(blank)" row for empty-valued cells,
     * shown only because real values also exist for this column. */
    if (blank > 0)
    {
        EeValueCount *grown =
            (EeValueCount *)realloc(items, ((size_t)count + 1) * sizeof(EeValueCount));
        wchar_t *empty = (grown != NULL) ? (wchar_t *)malloc(sizeof(wchar_t)) : NULL;
        if (grown != NULL && empty != NULL)
        {
            empty[0] = L'\0';
            grown[count].value = empty;
            grown[count].count = blank;
            items = grown;
            count++;
        }
        else
        {
            if (grown != NULL)
            {
                items = grown; /* keep the resized buffer; just no blank row */
            }
            blank = 0;         /* no blank row was added */
        }
    }

    rw = (ReportWindow *)calloc(1, sizeof(ReportWindow));
    if (rw == NULL)
    {
        EeVoterTable_FreeValueCounts(items, count);
        return;
    }
    rw->app = app;
    rw->kind = kind;
    rw->column = column;
    rw->items = items;
    rw->count = count;
    rw->has_blank = (blank > 0);
    rw->sort_col = 0;
    rw->sort_asc = TRUE;

    base = App_PathBaseName(app->load_path);
    if (base[0] == L'\0')
    {
        base = L"(voter list)";
    }
    StringCchPrintfW(title, ARRAYSIZE(title), L"%s Report - %s", label, base);

    if (GetWindowRect(app->hwnd_main, &pr))
    {
        x = pr.left + Scale(app, 48);
        y = pr.top + Scale(app, 48);
    }
    w = Scale(app, (kind == EE_REPORT_ADDRESS) ? 560 : 400);
    h = Scale(app, 600);

    /* Unowned top-level window so the main voter list can cover it. */
    rw->hwnd = CreateWindowExW(0,
                               k_ReportClassName,
                               title,
                               WS_OVERLAPPEDWINDOW | WS_CLIPCHILDREN,
                               x,
                               y,
                               w,
                               h,
                               NULL,
                               NULL,
                               app->instance,
                               rw);
    if (rw->hwnd == NULL)
    {
        EeVoterTable_FreeValueCounts(items, count);
        free(rw);
        return;
    }
    *slot = rw;
    ShowWindow(rw->hwnd, SW_SHOW);
    SetForegroundWindow(rw->hwnd);
}

static void App_CloseReports(AppState *app)
{
    if (app == NULL)
    {
        return;
    }
    /* DestroyWindow -> WM_DESTROY frees the ReportWindow and clears the slot. */
    if (app->report_precinct != NULL)
    {
        DestroyWindow(app->report_precinct->hwnd);
    }
    if (app->report_address != NULL)
    {
        DestroyWindow(app->report_address->hwnd);
    }
    App_CloseRosterTotals(app);
}

/* -------------------------------------------------------------------------- */
/* Two-file compare (by Voter ID)                                             */
/* -------------------------------------------------------------------------- */

/* The @p index-th other viewer (skipping @p app), or NULL. Matches the order
 * used to build the dynamic Compare menu. */
static AppState *App_OtherViewerByIndex(const AppState *app, int index)
{
    AppState *p;
    int i = 0;
    for (p = g_viewers; p != NULL; p = p->next)
    {
        if (p == app)
        {
            continue;
        }
        if (i == index)
        {
            return p;
        }
        i++;
    }
    return NULL;
}

/* Rebuild the Compare popup with one item per other open voter list. */
static void App_BuildCompareMenu(AppState *app, HMENU popup)
{
    AppState *p;
    int idx = 0;

    while (GetMenuItemCount(popup) > 0)
    {
        RemoveMenu(popup, 0, MF_BYPOSITION);
    }
    for (p = g_viewers; p != NULL; p = p->next)
    {
        wchar_t label[MAX_PATH + 32];
        const wchar_t *base;
        if (p == app)
        {
            continue;
        }
        if (IDM_COMPARE_WITH_FIRST + idx > IDM_COMPARE_WITH_LAST)
        {
            break;
        }
        base = App_PathBaseName(p->load_path);
        if (base[0] == L'\0')
        {
            base = L"(voter list)";
        }
        /* Name the kind when it differs from this window's, or is a roster. */
        StringCchPrintfW(label,
                         ARRAYSIZE(label),
                         L"Compare with %s%s",
                         base,
                         p->is_roster ? L" (voter roster)"
                                      : (app->is_roster ? L" (voter list)" : L""));
        AppendMenuW(popup,
                    (p->table.row_count > 0) ? MF_STRING : (MF_STRING | MF_GRAYED),
                    (UINT_PTR)(IDM_COMPARE_WITH_FIRST + idx),
                    label);
        idx++;
    }
    if (idx == 0)
    {
        AppendMenuW(popup,
                    MF_STRING | MF_GRAYED,
                    (UINT_PTR)IDM_COMPARE_WITH_FIRST,
                    app->is_roster ? L"(open another voter roster or a voter list to compare)"
                                   : L"(open another voter list or a voter roster to compare)");
    }
}

/* Rows of the Compare Summary, in display order. */
enum
{
    CMP_BUCKET_ONLY = 0,
    CMP_BUCKET_REPEATED,
    CMP_BUCKET_NAME_MINOR,
    CMP_BUCKET_NAME_MAJOR,
    CMP_BUCKET_ADDR_MINOR,
    CMP_BUCKET_ADDR_MAJOR,
    CMP_BUCKET_PCT,
    CMP_BUCKET_METHOD,
    CMP_BUCKET_DATE,
    CMP_BUCKET_IDENTICAL,
    CMP_BUCKET_COUNT
};

typedef struct CompareBucket
{
    const wchar_t *label;  /* summary-row text */
    const wchar_t *phrase; /* status-bar phrase for the highlighted view */
    uint16_t mask;         /* class bit to match; 0 = identical special case */
} CompareBucket;

static const CompareBucket k_CompareBuckets[CMP_BUCKET_COUNT] = {
    {L"Only here", L"voters only here vs", EE_CMP_ONLY_HERE},
    {L"Voter ID repeated", L"rows with a repeated Voter ID vs", EE_CMP_REPEATED},
    {L"Name (minor)", L"name (minor) changes vs", EE_CMP_NAME_MINOR},
    {L"Name (major)", L"name (major) changes vs", EE_CMP_NAME_MAJOR},
    {L"Address (minor)", L"address (minor) changes vs", EE_CMP_ADDR_MINOR},
    {L"Address (major)", L"address (major) changes vs", EE_CMP_ADDR_MAJOR},
    {L"Precinct changed", L"precinct changes vs", EE_CMP_PCT_CHANGED},
    {L"Voting Method changed", L"voting method changes vs", EE_CMP_METHOD_CHANGED},
    {L"Date Voted changed", L"date voted changes vs", EE_CMP_DATE_CHANGED},
    {L"Identical", L"identical voters vs", 0},
};

static BOOL compare_row_in_bucket(uint16_t cls, int bucket)
{
    if (bucket == CMP_BUCKET_IDENTICAL)
    {
        return (cls & EE_CMP_MATCHED) != 0 && (cls & EE_CMP_CHANGE_BITS) == 0;
    }
    return (cls & k_CompareBuckets[bucket].mask) != 0;
}

static void compare_bucket_counts(const EeCompareResult *r,
                                  int bucket,
                                  uint32_t *out_a,
                                  uint32_t *out_b)
{
    switch (bucket)
    {
        case CMP_BUCKET_ONLY:
            *out_a = r->only_a;
            *out_b = r->only_b;
            break;
        case CMP_BUCKET_REPEATED:
            *out_a = r->repeated_a;
            *out_b = r->repeated_b;
            break;
        case CMP_BUCKET_NAME_MINOR:
            *out_a = r->name_minor_a;
            *out_b = r->name_minor_b;
            break;
        case CMP_BUCKET_NAME_MAJOR:
            *out_a = r->name_major_a;
            *out_b = r->name_major_b;
            break;
        case CMP_BUCKET_ADDR_MINOR:
            *out_a = r->addr_minor_a;
            *out_b = r->addr_minor_b;
            break;
        case CMP_BUCKET_ADDR_MAJOR:
            *out_a = r->addr_major_a;
            *out_b = r->addr_major_b;
            break;
        case CMP_BUCKET_PCT:
            *out_a = r->pct_changed_a;
            *out_b = r->pct_changed_b;
            break;
        case CMP_BUCKET_METHOD:
            *out_a = r->method_changed_a;
            *out_b = r->method_changed_b;
            break;
        case CMP_BUCKET_DATE:
            *out_a = r->date_changed_a;
            *out_b = r->date_changed_b;
            break;
        case CMP_BUCKET_IDENTICAL:
            *out_a = r->identical_a;
            *out_b = r->identical_b;
            break;
        default:
            *out_a = 0;
            *out_b = 0;
            break;
    }
}

/* Build a fresh mark buffer of the rows in @p bucket and show it in @p app. */
static void App_ShowCompareCategory(AppState *app,
                                    const uint16_t *cls,
                                    uint32_t rows,
                                    int bucket,
                                    const wchar_t *other_base)
{
    uint8_t *marks;
    uint32_t i;
    uint32_t count = 0;
    wchar_t label[160];

    if (app == NULL || app->hwnd_main == NULL || cls == NULL || bucket < 0 ||
        bucket >= CMP_BUCKET_COUNT)
    {
        return;
    }
    if (rows != app->table.row_count || rows == 0)
    {
        MessageBoxW(app->hwnd_main,
                    L"The file changed since the comparison was run. Run Compare again.",
                    k_WindowTitle,
                    MB_ICONINFORMATION | MB_OK);
        return;
    }
    marks = (uint8_t *)calloc((size_t)rows, sizeof(uint8_t));
    if (marks == NULL)
    {
        MessageBoxW(app->hwnd_main, L"Out of memory.", k_WindowTitle, MB_ICONERROR | MB_OK);
        return;
    }
    for (i = 0; i < rows; i++)
    {
        if (compare_row_in_bucket(cls[i], bucket))
        {
            marks[i] = 1;
            count++;
        }
    }
    if (count == 0)
    {
        free(marks);
        MessageBoxW(app->hwnd_main,
                    L"No voters in that category.",
                    k_WindowTitle,
                    MB_ICONINFORMATION | MB_OK);
        return;
    }
    StringCchPrintfW(label,
                     ARRAYSIZE(label),
                     L"%s %s",
                     k_CompareBuckets[bucket].phrase,
                     other_base);
    App_ApplyMarks(app, marks, count, EE_SCAN_CMP_CHANGED, EE_COL_VOTER_ID, label);
    App_ActivateViewer(app);
}

/* Create (replacing any prior) the modeless Compare Summary window. Takes
 * ownership of @p class_a / @p class_b. */
static void App_ShowCompareWindow(AppState *a,
                                  AppState *b,
                                  const EeCompareResult *result,
                                  uint16_t *class_a,
                                  uint16_t *class_b,
                                  uint32_t rows_a,
                                  uint32_t rows_b)
{
    CompareWindow *cw;
    wchar_t title[MAX_PATH * 2];
    const wchar_t *base_a;
    const wchar_t *base_b;
    RECT pr;
    int x = CW_USEDEFAULT;
    int y = CW_USEDEFAULT;
    int w;
    int h;

    if (g_compare != NULL)
    {
        DestroyWindow(g_compare->hwnd); /* WM_DESTROY frees it and clears g_compare */
    }

    cw = (CompareWindow *)calloc(1, sizeof(CompareWindow));
    if (cw == NULL)
    {
        free(class_a);
        free(class_b);
        return;
    }
    cw->a = a;
    cw->b = b;
    cw->result = *result;
    cw->class_a = class_a;
    cw->class_b = class_b;
    cw->rows_a = rows_a;
    cw->rows_b = rows_b;
    {
        /* Address rows only for two voter lists; Voting Method / Date Voted only for two
         * voter rosters. */
        BOOL lists = !a->is_roster && !b->is_roster;
        BOOL rosters = a->is_roster && b->is_roster;
        int k;
        for (k = 0; k < CMP_BUCKET_COUNT && cw->nbuckets < (int)ARRAYSIZE(cw->buckets); k++)
        {
            if ((k == CMP_BUCKET_ADDR_MINOR || k == CMP_BUCKET_ADDR_MAJOR) && !lists)
                continue;
            if ((k == CMP_BUCKET_METHOD || k == CMP_BUCKET_DATE) && !rosters)
                continue;
            cw->buckets[cw->nbuckets++] = k;
        }
    }

    base_a = App_PathBaseName(a->load_path);
    base_b = App_PathBaseName(b->load_path);
    if (base_a[0] == L'\0')
    {
        base_a = L"(A)";
    }
    if (base_b[0] == L'\0')
    {
        base_b = L"(B)";
    }
    StringCchPrintfW(title, ARRAYSIZE(title), L"Compare - %s vs %s", base_a, base_b);

    if (GetWindowRect(a->hwnd_main, &pr))
    {
        x = pr.left + Scale(a, 60);
        y = pr.top + Scale(a, 60);
    }
    w = Scale(a, 540); /* room for the three buttons */
    h = Scale(a, 360);

    cw->hwnd = CreateWindowExW(0,
                               k_CompareClassName,
                               title,
                               WS_OVERLAPPEDWINDOW | WS_CLIPCHILDREN,
                               x,
                               y,
                               w,
                               h,
                               NULL,
                               NULL,
                               a->instance,
                               cw);
    if (cw->hwnd == NULL)
    {
        free(class_a);
        free(class_b);
        free(cw);
        return;
    }
    g_compare = cw;
    ShowWindow(cw->hwnd, SW_SHOW);
    SetForegroundWindow(cw->hwnd);
}

/* What to compare for the pair (a, b): two voter lists compare Name, Address and
 * Precinct; with a voter roster there is no address, names match on the parts (first +
 * last decide a roster vs a voter list, D3), and two rosters also compare Voting Method
 * and Date Voted. */
static void App_CompareOptions(const AppState *a, const AppState *b, EeCompareOptions *o)
{
    ZeroMemory(o, sizeof(*o));
    if (!a->is_roster && !b->is_roster)
    {
        o->compare_address = TRUE;
        o->names = EE_CMP_NAMES_FULL;
        return;
    }
    o->compare_address = FALSE;
    o->loose_precinct = TRUE;
    if (a->is_roster && b->is_roster)
    {
        o->names = EE_CMP_NAMES_PARTS;
        o->compare_vote = TRUE;
    }
    else
    {
        o->names = EE_CMP_NAMES_FIRST_LAST;
    }
}

/* Run the compare and present it. Takes both class buffers by ownership on the
 * synchronous path; the async path stores them on @p a first. */
static void App_RunCompareSync(AppState *a, AppState *b)
{
    uint16_t *class_a;
    uint16_t *class_b;
    EeCompareResult r;
    EeCompareOptions opts;
    HCURSOR prev;
    BOOL ok;

    App_CompareOptions(a, b, &opts);
    class_a = (uint16_t *)calloc((size_t)a->table.row_count + 1u, sizeof(uint16_t));
    class_b = (uint16_t *)calloc((size_t)b->table.row_count + 1u, sizeof(uint16_t));
    if (class_a == NULL || class_b == NULL)
    {
        free(class_a);
        free(class_b);
        MessageBoxW(a->hwnd_main, L"Out of memory.", k_WindowTitle, MB_ICONERROR | MB_OK);
        return;
    }
    prev = SetCursor(LoadCursorW(NULL, IDC_WAIT));
    ok =
        EeVoterTable_CompareByVoterIdEx(&a->table, &b->table, &opts, class_a, class_b, &r, NULL, NULL, NULL);
    SetCursor(prev);
    if (!ok)
    {
        free(class_a);
        free(class_b);
        MessageBoxW(a->hwnd_main,
                    L"Could not compare the two voter lists.",
                    k_WindowTitle,
                    MB_ICONERROR | MB_OK);
        return;
    }
    App_ShowCompareWindow(a, b, &r, class_a, class_b, a->table.row_count, b->table.row_count);
}

static DWORD WINAPI CompareThreadProc(void *param)
{
    AppState *app = (AppState *)param; /* file A */
    AppState *b = app->cmp_other;
    EeCompareResult r;
    EeCompareOptions opts;

    App_CompareOptions(app, b, &opts);
    app->cmp_ok = EeVoterTable_CompareByVoterIdEx(&app->table,
                                                &b->table,
                                                &opts,
                                                app->cmp_class_a,
                                                app->cmp_class_b,
                                                &r,
                                                &app->scan_cancel,
                                                ScanProgressThunk,
                                                app);
    app->cmp_result = r;
    PostMessageW(app->hwnd_main, EEM_CMP_FINISHED, 0, 0);
    return 0;
}

static void App_StartCompareThread(AppState *a, AppState *b)
{
    a->cmp_other = b;
    a->cmp_class_a = (uint16_t *)calloc((size_t)a->table.row_count + 1u, sizeof(uint16_t));
    a->cmp_class_b = (uint16_t *)calloc((size_t)b->table.row_count + 1u, sizeof(uint16_t));
    if (a->cmp_class_a == NULL || a->cmp_class_b == NULL)
    {
        free(a->cmp_class_a);
        free(a->cmp_class_b);
        a->cmp_class_a = NULL;
        a->cmp_class_b = NULL;
        MessageBoxW(a->hwnd_main, L"Out of memory.", k_WindowTitle, MB_ICONERROR | MB_OK);
        return;
    }
    a->cmp_ok = FALSE;
    a->scan_kind = EE_SCAN_NONE;
    InterlockedExchange(&a->scan_cancel, 0);
    a->scanning = TRUE;

    if (!App_ShowProgress(a))
    {
        a->scanning = FALSE;
        free(a->cmp_class_a);
        free(a->cmp_class_b);
        a->cmp_class_a = NULL;
        a->cmp_class_b = NULL;
        MessageBoxW(a->hwnd_main,
                    L"Could not create the progress window.",
                    k_WindowTitle,
                    MB_ICONERROR | MB_OK);
        return;
    }
    SetWindowTextW(a->hwnd_progress, L"Comparing voter lists");
    if (a->hwnd_progress_status)
    {
        SetWindowTextW(a->hwnd_progress_status, L"Matching by Voter ID…");
    }

    EnableWindow(a->hwnd_main, FALSE);
    if (b->hwnd_main != NULL)
    {
        EnableWindow(b->hwnd_main, FALSE);
    }

    a->scan_thread = CreateThread(NULL, 0, CompareThreadProc, a, 0, NULL);
    if (a->scan_thread == NULL)
    {
        a->scanning = FALSE;
        EnableWindow(a->hwnd_main, TRUE);
        if (b->hwnd_main != NULL)
        {
            EnableWindow(b->hwnd_main, TRUE);
        }
        App_DestroyProgress(a);
        free(a->cmp_class_a);
        free(a->cmp_class_b);
        a->cmp_class_a = NULL;
        a->cmp_class_b = NULL;
        MessageBoxW(a->hwnd_main,
                    L"Could not start the compare thread.",
                    k_WindowTitle,
                    MB_ICONERROR | MB_OK);
    }
}

static void App_OnCompareFinished(AppState *app)
{
    AppState *b = app->cmp_other;
    BOOL cancelled;

    if (app->scan_thread != NULL)
    {
        WaitForSingleObject(app->scan_thread, INFINITE);
        CloseHandle(app->scan_thread);
        app->scan_thread = NULL;
    }
    app->scanning = FALSE;
    cancelled = (InterlockedCompareExchange(&app->scan_cancel, 0, 0) != 0);

    EnableWindow(app->hwnd_main, TRUE);
    if (b != NULL && b->hwnd_main != NULL && IsWindow(b->hwnd_main))
    {
        EnableWindow(b->hwnd_main, TRUE);
    }
    App_DestroyProgress(app);
    SetForegroundWindow(app->hwnd_main);

    if (app->close_pending)
    {
        free(app->cmp_class_a);
        free(app->cmp_class_b);
        app->cmp_class_a = NULL;
        app->cmp_class_b = NULL;
        DestroyWindow(app->hwnd_main);
        return;
    }
    if (!app->cmp_ok || cancelled)
    {
        free(app->cmp_class_a);
        free(app->cmp_class_b);
        app->cmp_class_a = NULL;
        app->cmp_class_b = NULL;
        App_SetStatus(app, cancelled ? L"Compare cancelled." : L"Compare failed.");
        if (!cancelled)
        {
            MessageBoxW(app->hwnd_main,
                        L"Could not compare the two voter lists.",
                        k_WindowTitle,
                        MB_ICONERROR | MB_OK);
        }
        return;
    }

    {
        uint16_t *class_a = app->cmp_class_a;
        uint16_t *class_b = app->cmp_class_b;
        uint32_t rows_a = app->table.row_count;
        uint32_t rows_b = (b != NULL) ? b->table.row_count : 0;
        app->cmp_class_a = NULL; /* ownership passes to the compare window */
        app->cmp_class_b = NULL;
        App_ShowCompareWindow(app, b, &app->cmp_result, class_a, class_b, rows_a, rows_b);
    }
}

/* Entry point: validate, then run the compare (sync for small tables). */
static void App_StartCompare(AppState *a, AppState *b)
{
    if (a == NULL || b == NULL || a == b)
    {
        return;
    }
    if (a->loading || a->scanning || b->loading || b->scanning)
    {
        MessageBoxW(a->hwnd_main,
                    L"Finish loading or scanning both files before comparing.",
                    k_WindowTitle,
                    MB_ICONINFORMATION | MB_OK);
        return;
    }
    if (a->table.row_count == 0 || b->table.row_count == 0)
    {
        MessageBoxW(a->hwnd_main,
                    L"Both windows must have a voter list loaded to compare.",
                    k_WindowTitle,
                    MB_ICONINFORMATION | MB_OK);
        return;
    }

    if (a->table.row_count >= k_ScanModalMinRows || b->table.row_count >= k_ScanModalMinRows)
    {
        App_StartCompareThread(a, b);
    }
    else
    {
        App_RunCompareSync(a, b);
    }
}

/* Close the compare window if it summarizes @p app (as either side). */
static void App_CloseCompare(AppState *app)
{
    if (g_compare != NULL && (g_compare->a == app || g_compare->b == app))
    {
        DestroyWindow(g_compare->hwnd);
    }
}

/* Populate the 3x2 summary list and size the window to fit. */
static void Compare_Populate(CompareWindow *cw)
{
    int i;

    for (i = 0; i < cw->nbuckets; i++)
    {
        LVITEMW it;
        wchar_t num[32];
        uint32_t ca = 0;
        uint32_t cb = 0;
        ZeroMemory(&it, sizeof(it));
        it.mask = LVIF_TEXT;
        it.iItem = i;
        it.iSubItem = 0;
        it.pszText = (wchar_t *)k_CompareBuckets[cw->buckets[i]].label;
        ListView_InsertItem(cw->list, &it);
        compare_bucket_counts(&cw->result, cw->buckets[i], &ca, &cb);
        StringCchPrintfW(num, ARRAYSIZE(num), L"%u", ca);
        ListView_SetItemText(cw->list, i, 1, num);
        StringCchPrintfW(num, ARRAYSIZE(num), L"%u", cb);
        ListView_SetItemText(cw->list, i, 2, num);
    }
}

static void Compare_Layout(CompareWindow *cw, int cx, int cy)
{
    int sh = 0;
    int bh = Scale(cw->a, 26);
    int bw = Scale(cw->a, 150);
    int gap = Scale(cw->a, 8);
    int list_h;
    RECT rs;

    if (cw->status != NULL)
    {
        SendMessageW(cw->status, WM_SIZE, 0, 0);
        if (GetWindowRect(cw->status, &rs))
        {
            sh = rs.bottom - rs.top;
        }
    }
    list_h = cy - sh - bh - gap;
    if (list_h < 0)
    {
        list_h = 0;
    }
    if (cw->list != NULL)
    {
        MoveWindow(cw->list, 0, 0, cx, list_h, TRUE);
    }
    if (cw->diff_btn != NULL)
    {
        MoveWindow(cw->diff_btn, cx - gap - bw, list_h + gap / 2, bw, bh, TRUE);
    }
    if (cw->exp_all_btn != NULL)
    {
        MoveWindow(cw->exp_all_btn, cx - 2 * (gap + bw), list_h + gap / 2, bw, bh, TRUE);
    }
    if (cw->exp_sum_btn != NULL)
    {
        MoveWindow(cw->exp_sum_btn, cx - 3 * (gap + bw), list_h + gap / 2, bw, bh, TRUE);
    }
}

/* Apply the summary row (bucket) the user acted on to viewer @p to_b ? B : A. */
static void Compare_ShowRow(CompareWindow *cw, int row, BOOL to_b)
{
    const wchar_t *base_other;

    if (cw == NULL || row < 0 || row >= cw->nbuckets)
    {
        return;
    }
    row = cw->buckets[row]; /* summary row -> bucket */
    if (to_b)
    {
        base_other = App_PathBaseName(cw->a->load_path);
        if (base_other[0] == L'\0')
        {
            base_other = L"other file";
        }
        App_ShowCompareCategory(cw->b, cw->class_b, cw->rows_b, row, base_other);
    }
    else
    {
        base_other = App_PathBaseName(cw->b->load_path);
        if (base_other[0] == L'\0')
        {
            base_other = L"other file";
        }
        App_ShowCompareCategory(cw->a, cw->class_a, cw->rows_a, row, base_other);
    }
}

/* "<A>-vs-<B>" (file names without extensions) for suggested export names. */
static void Compare_BaseName(const CompareWindow *cw, wchar_t *out, size_t cch)
{
    wchar_t a[96];
    wchar_t b[96];
    App_BaseNameNoExt(cw->a->load_path, a, ARRAYSIZE(a));
    App_BaseNameNoExt(cw->b->load_path, b, ARRAYSIZE(b));
    StringCchPrintfW(out, cch, L"%s-vs-%s", a, b);
}

/* FALSE (after telling the user) when a file was reloaded since the compare ran. */
static BOOL Compare_StillCurrent(CompareWindow *cw)
{
    if (cw->a->table.row_count != cw->rows_a || cw->b->table.row_count != cw->rows_b)
    {
        MessageBoxW(cw->hwnd, L"A file changed since the comparison was run. Run Compare again.",
                    k_WindowTitle, MB_ICONINFORMATION | MB_OK);
        return FALSE;
    }
    return TRUE;
}

static void Compare_SummaryCell(void *user, uint32_t row, uint32_t col, wchar_t *buf, size_t cch)
{
    CompareWindow *cw = (CompareWindow *)user;
    uint32_t ca = 0;
    uint32_t cb = 0;
    int bucket = cw->buckets[row];
    if (col == 0)
    {
        StringCchCopyW(buf, cch, k_CompareBuckets[bucket].label);
        return;
    }
    compare_bucket_counts(&cw->result, bucket, &ca, &cb);
    StringCchPrintfW(buf, cch, L"%u", (col == 1) ? ca : cb);
}

/* Compare → Export Summary…: the category table (Category, count in A, count in B). */
static void Compare_ExportSummary(CompareWindow *cw)
{
    wchar_t base[256];
    wchar_t suggested[300];
    const wchar_t *headers[3];
    if (cw == NULL || cw->nbuckets == 0)
    {
        return;
    }
    headers[0] = L"Category";
    headers[1] = App_PathBaseName(cw->a->load_path);
    headers[2] = App_PathBaseName(cw->b->load_path);
    Compare_BaseName(cw, base, ARRAYSIZE(base));
    StringCchPrintfW(suggested, ARRAYSIZE(suggested), L"%s-Compare_Summary", base);
    App_ExportReportModel(cw->hwnd, suggested, 3, headers, (uint32_t)cw->nbuckets, Compare_SummaryCell,
                          cw);
}

/* One row per record of both files for Export All Records. */
typedef struct CompareRecordsCtx
{
    CompareWindow *cw;
    BOOL address;      /* two voter lists: an Address column */
    BOOL vote;         /* a roster is involved: Voting Method / Date Voted columns */
    int method_col[2]; /* per side, -1 = absent */
    int date_col[2];
    uint32_t ncols;
    const wchar_t *headers[10];
} CompareRecordsCtx;

static void Compare_RecordCell(void *user, uint32_t row, uint32_t col, wchar_t *buf, size_t cch)
{
    CompareRecordsCtx *x = (CompareRecordsCtx *)user;
    CompareWindow *cw = x->cw;
    int side = (row < cw->rows_a) ? 0 : 1;
    uint32_t r = side ? row - cw->rows_a : row;
    const AppState *app = side ? cw->b : cw->a;
    uint16_t cls = side ? cw->class_b[r] : cw->class_a[r];
    const wchar_t *h = x->headers[col];
    int src = -1;

    buf[0] = L'\0';
    if (wcscmp(h, L"File") == 0)
    {
        StringCchCopyW(buf, cch, App_PathBaseName(app->load_path));
        return;
    }
    if (wcscmp(h, L"Result") == 0)
    {
        StringCchCopyW(buf, cch,
                       (cls & EE_CMP_ONLY_HERE)                ? L"Only here"
                       : (cls & EE_CMP_REPEATED)               ? L"Voter ID repeated"
                       : ((cls & EE_CMP_CHANGE_BITS) == 0)     ? L"Identical"
                                                               : L"Changed");
        return;
    }
    if (wcscmp(h, L"Changed Fields") == 0)
    {
        static const struct
        {
            uint16_t bit;
            const wchar_t *label;
        } k_fields[] = {{EE_CMP_NAME_MINOR, L"Name (minor)"},     {EE_CMP_NAME_MAJOR, L"Name (major)"},
                        {EE_CMP_ADDR_MINOR, L"Address (minor)"},  {EE_CMP_ADDR_MAJOR, L"Address (major)"},
                        {EE_CMP_PCT_CHANGED, L"Precinct"},        {EE_CMP_METHOD_CHANGED, L"Voting Method"},
                        {EE_CMP_DATE_CHANGED, L"Date Voted"}};
        size_t k;
        for (k = 0; k < ARRAYSIZE(k_fields); k++)
        {
            if (cls & k_fields[k].bit)
            {
                if (buf[0] != L'\0')
                    StringCchCatW(buf, cch, L"; ");
                StringCchCatW(buf, cch, k_fields[k].label);
            }
        }
        return;
    }
    if (wcscmp(h, L"Voter ID") == 0)
        src = EE_COL_VOTER_ID;
    else if (wcscmp(h, L"Precinct") == 0)
        src = EE_COL_PRECINCT;
    else if (wcscmp(h, L"Name") == 0)
        src = EE_COL_NAME;
    else if (wcscmp(h, L"Address") == 0)
        src = EE_COL_ADDRESS;
    else if (wcscmp(h, L"Voting Method") == 0)
        src = x->method_col[side];
    else if (wcscmp(h, L"Date Voted") == 0)
        src = x->date_col[side];
    if (src >= 0)
    {
        Diff_CellW(&app->table, r, (uint32_t)src, buf, (int)cch);
    }
}

/* Compare → Export All Records…: every record of both files with its result. */
static void Compare_ExportRecords(CompareWindow *cw)
{
    CompareRecordsCtx x;
    wchar_t base[256];
    wchar_t suggested[300];
    if (cw == NULL || cw->class_a == NULL || cw->class_b == NULL || !Compare_StillCurrent(cw))
    {
        return;
    }
    ZeroMemory(&x, sizeof(x));
    x.cw = cw;
    x.address = !cw->a->is_roster && !cw->b->is_roster;
    x.vote = cw->a->is_roster || cw->b->is_roster;
    x.method_col[0] = EeVoterTable_FindColumnByTitle(&cw->a->table, L"Voting Method");
    x.method_col[1] = EeVoterTable_FindColumnByTitle(&cw->b->table, L"Voting Method");
    x.date_col[0] = EeVoterTable_FindColumnByTitle(&cw->a->table, L"Date Voted");
    x.date_col[1] = EeVoterTable_FindColumnByTitle(&cw->b->table, L"Date Voted");
    x.headers[x.ncols++] = L"File";
    x.headers[x.ncols++] = L"Voter ID";
    x.headers[x.ncols++] = L"Precinct";
    x.headers[x.ncols++] = L"Name";
    if (x.address)
        x.headers[x.ncols++] = L"Address";
    if (x.vote)
    {
        x.headers[x.ncols++] = L"Voting Method";
        x.headers[x.ncols++] = L"Date Voted";
    }
    x.headers[x.ncols++] = L"Result";
    x.headers[x.ncols++] = L"Changed Fields";
    Compare_BaseName(cw, base, ARRAYSIZE(base));
    StringCchPrintfW(suggested, ARRAYSIZE(suggested), L"%s-Compare_Records", base);
    App_ExportReportModel(cw->hwnd, suggested, x.ncols, x.headers, cw->rows_a + cw->rows_b,
                          Compare_RecordCell, &x);
}

static void Compare_OnContextMenu(CompareWindow *cw, int row, POINT screen)
{
    HMENU menu;
    const wchar_t *base_a;
    const wchar_t *base_b;
    wchar_t item_a[MAX_PATH + 32];
    wchar_t item_b[MAX_PATH + 32];
    int cmd;

    if (cw == NULL)
    {
        return;
    }
    if (row < 0 || row >= cw->nbuckets)
    {
        /* Outside the rows: only the exports apply. */
        menu = CreatePopupMenu();
        if (menu == NULL)
        {
            return;
        }
        AppendMenuW(menu, MF_STRING, IDC_CMP_EXPORT_SUMMARY, L"Export &Summary…");
        AppendMenuW(menu, MF_STRING, IDC_CMP_EXPORT_ALL, L"Export &All Records…");
        cmd = (int)TrackPopupMenu(menu, TPM_RETURNCMD | TPM_LEFTALIGN | TPM_TOPALIGN, screen.x,
                                  screen.y, 0, cw->hwnd, NULL);
        DestroyMenu(menu);
        if (cmd == IDC_CMP_EXPORT_SUMMARY)
            Compare_ExportSummary(cw);
        else if (cmd == IDC_CMP_EXPORT_ALL)
            Compare_ExportRecords(cw);
        return;
    }
    base_a = App_PathBaseName(cw->a->load_path);
    base_b = App_PathBaseName(cw->b->load_path);
    if (base_a[0] == L'\0')
    {
        base_a = L"file A";
    }
    if (base_b[0] == L'\0')
    {
        base_b = L"file B";
    }
    StringCchPrintfW(item_a, ARRAYSIZE(item_a), L"Show these rows in %s", base_a);
    StringCchPrintfW(item_b, ARRAYSIZE(item_b), L"Show these rows in %s", base_b);

    menu = CreatePopupMenu();
    if (menu == NULL)
    {
        return;
    }
    AppendMenuW(menu, MF_STRING, IDM_COMPARE_SHOW_A, item_a);
    AppendMenuW(menu, MF_STRING, IDM_COMPARE_SHOW_B, item_b);
    AppendMenuW(menu, MF_SEPARATOR, 0, NULL);
    AppendMenuW(menu, MF_STRING, IDC_CMP_EXPORT_SUMMARY, L"Export &Summary…");
    AppendMenuW(menu, MF_STRING, IDC_CMP_EXPORT_ALL, L"Export &All Records…");
    cmd = (int)TrackPopupMenu(menu,
                              TPM_RETURNCMD | TPM_LEFTALIGN | TPM_TOPALIGN,
                              screen.x,
                              screen.y,
                              0,
                              cw->hwnd,
                              NULL);
    DestroyMenu(menu);
    if (cmd == IDM_COMPARE_SHOW_A)
    {
        Compare_ShowRow(cw, row, FALSE);
    }
    else if (cmd == IDM_COMPARE_SHOW_B)
    {
        Compare_ShowRow(cw, row, TRUE);
    }
    else if (cmd == IDC_CMP_EXPORT_SUMMARY)
    {
        Compare_ExportSummary(cw);
    }
    else if (cmd == IDC_CMP_EXPORT_ALL)
    {
        Compare_ExportRecords(cw);
    }
}

static LRESULT CALLBACK CompareWndProc(HWND hwnd, UINT msg, WPARAM wParam, LPARAM lParam)
{
    CompareWindow *cw = (CompareWindow *)GetWindowLongPtrW(hwnd, GWLP_USERDATA);

    switch (msg)
    {
        case WM_CREATE:
        {
            CREATESTRUCTW *cs = (CREATESTRUCTW *)lParam;
            LVCOLUMNW col;
            RECT rc;
            const wchar_t *base_a;
            const wchar_t *base_b;
            wchar_t hdr_a[MAX_PATH];
            wchar_t hdr_b[MAX_PATH];

            cw = (CompareWindow *)cs->lpCreateParams;
            SetWindowLongPtrW(hwnd, GWLP_USERDATA, (LONG_PTR)cw);
            cw->hwnd = hwnd;

            GetClientRect(hwnd, &rc);
            cw->list = CreateWindowExW(0,
                                       WC_LISTVIEWW,
                                       L"",
                                       WS_CHILD | WS_VISIBLE | LVS_REPORT | LVS_SINGLESEL |
                                           LVS_SHOWSELALWAYS,
                                       0,
                                       0,
                                       rc.right,
                                       rc.bottom,
                                       hwnd,
                                       NULL,
                                       cw->a->instance,
                                       NULL);
            if (cw->list == NULL)
            {
                return -1;
            }
            ListView_SetExtendedListViewStyle(cw->list,
                                              LVS_EX_FULLROWSELECT | LVS_EX_DOUBLEBUFFER |
                                                  LVS_EX_GRIDLINES);
            if (cw->a->font_ui)
            {
                SendMessageW(cw->list, WM_SETFONT, (WPARAM)cw->a->font_ui, TRUE);
            }
            base_a = App_PathBaseName(cw->a->load_path);
            base_b = App_PathBaseName(cw->b->load_path);
            if (base_a[0] == L'\0')
            {
                base_a = L"file A";
            }
            if (base_b[0] == L'\0')
            {
                base_b = L"file B";
            }
            StringCchCopyW(hdr_a, ARRAYSIZE(hdr_a), base_a);
            StringCchCopyW(hdr_b, ARRAYSIZE(hdr_b), base_b);

            ZeroMemory(&col, sizeof(col));
            col.mask = LVCF_TEXT | LVCF_WIDTH | LVCF_FMT;
            col.fmt = LVCFMT_LEFT;
            col.pszText = L"Category";
            col.cx = Scale(cw->a, 170); /* fits "Voting Method changed" */
            ListView_InsertColumn(cw->list, 0, &col);
            col.fmt = LVCFMT_RIGHT;
            col.pszText = hdr_a;
            col.cx = Scale(cw->a, 150);
            ListView_InsertColumn(cw->list, 1, &col);
            col.pszText = hdr_b;
            col.cx = Scale(cw->a, 150);
            ListView_InsertColumn(cw->list, 2, &col);

            cw->status = CreateWindowExW(0,
                                         STATUSCLASSNAMEW,
                                         NULL,
                                         WS_CHILD | WS_VISIBLE | SBARS_SIZEGRIP,
                                         0,
                                         0,
                                         0,
                                         0,
                                         hwnd,
                                         NULL,
                                         cw->a->instance,
                                         NULL);
            if (cw->status != NULL)
            {
                if (cw->a->font_ui)
                {
                    SendMessageW(cw->status, WM_SETFONT, (WPARAM)cw->a->font_ui, TRUE);
                }
                SendMessageW(
                    cw->status,
                    SB_SETTEXTW,
                    0,
                    (LPARAM)L"Matched by Voter ID. Right-click a row to show it in a file.");
            }

            cw->diff_btn = CreateWindowExW(0,
                                           L"BUTTON",
                                           L"Show Differences…",
                                           WS_CHILD | WS_VISIBLE | WS_TABSTOP | BS_PUSHBUTTON,
                                           0,
                                           0,
                                           Scale(cw->a, 150),
                                           Scale(cw->a, 26),
                                           hwnd,
                                           (HMENU)(INT_PTR)IDC_CMP_DIFF,
                                           cw->a->instance,
                                           NULL);
            if (cw->diff_btn != NULL && cw->a->font_ui)
            {
                SendMessageW(cw->diff_btn, WM_SETFONT, (WPARAM)cw->a->font_ui, TRUE);
            }
            cw->exp_sum_btn = CreateWindowExW(0, L"BUTTON", L"Export Summary…",
                                              WS_CHILD | WS_VISIBLE | WS_TABSTOP | BS_PUSHBUTTON, 0, 0,
                                              Scale(cw->a, 150), Scale(cw->a, 26), hwnd,
                                              (HMENU)(INT_PTR)IDC_CMP_EXPORT_SUMMARY, cw->a->instance,
                                              NULL);
            cw->exp_all_btn = CreateWindowExW(0, L"BUTTON", L"Export All Records…",
                                              WS_CHILD | WS_VISIBLE | WS_TABSTOP | BS_PUSHBUTTON, 0, 0,
                                              Scale(cw->a, 150), Scale(cw->a, 26), hwnd,
                                              (HMENU)(INT_PTR)IDC_CMP_EXPORT_ALL, cw->a->instance, NULL);
            if (cw->a->font_ui)
            {
                if (cw->exp_sum_btn != NULL)
                    SendMessageW(cw->exp_sum_btn, WM_SETFONT, (WPARAM)cw->a->font_ui, TRUE);
                if (cw->exp_all_btn != NULL)
                    SendMessageW(cw->exp_all_btn, WM_SETFONT, (WPARAM)cw->a->font_ui, TRUE);
            }

            Compare_Populate(cw);
            Compare_Layout(cw, rc.right, rc.bottom);
            return 0;
        }

        case WM_COMMAND:
            if (cw != NULL && LOWORD(wParam) == IDC_CMP_DIFF)
            {
                App_ShowDifferences(cw);
                return 0;
            }
            if (cw != NULL && LOWORD(wParam) == IDC_CMP_EXPORT_SUMMARY)
            {
                Compare_ExportSummary(cw);
                return 0;
            }
            if (cw != NULL && LOWORD(wParam) == IDC_CMP_EXPORT_ALL)
            {
                Compare_ExportRecords(cw);
                return 0;
            }
            break;

        case WM_SIZE:
            if (cw != NULL)
            {
                Compare_Layout(cw, LOWORD(lParam), HIWORD(lParam));
            }
            return 0;

        case WM_SETFOCUS:
            if (cw != NULL && cw->list != NULL)
            {
                SetFocus(cw->list);
            }
            return 0;

        case WM_NOTIFY:
        {
            NMHDR *hdr = (NMHDR *)lParam;
            if (cw == NULL || hdr->hwndFrom != cw->list)
            {
                break;
            }
            if (hdr->code == NM_DBLCLK)
            {
                LPNMITEMACTIVATE ia = (LPNMITEMACTIVATE)lParam;
                /* Double-click the B-count column shows in B, else in A. */
                Compare_ShowRow(cw, ia->iItem, ia->iSubItem == 2);
                return 0;
            }
            if (hdr->code == NM_RCLICK)
            {
                LPNMITEMACTIVATE ia = (LPNMITEMACTIVATE)lParam;
                POINT screen = ia->ptAction;
                ClientToScreen(cw->list, &screen);
                Compare_OnContextMenu(cw, ia->iItem, screen);
                return 0;
            }
            break;
        }

        case WM_DESTROY:
            if (cw != NULL)
            {
                /* The differences window summarizes this comparison; close it too. */
                if (g_diff != NULL)
                {
                    DestroyWindow(g_diff->hwnd);
                }
                if (g_compare == cw)
                {
                    g_compare = NULL;
                }
                free(cw->class_a);
                free(cw->class_b);
                SetWindowLongPtrW(hwnd, GWLP_USERDATA, 0);
                free(cw);
            }
            return 0;

        default:
            break;
    }
    return DefWindowProcW(hwnd, msg, wParam, lParam);
}

/* -------------------------------------------------------------------------- */
/* Differences (side-by-side field detail)                                    */
/* -------------------------------------------------------------------------- */

/* Physical-row cell as wide text into @p out (empty on failure). */
static void Diff_CellW(const EeVoterTable *t, uint32_t row, uint32_t col, wchar_t *out, int cch)
{
    const char *utf8 = EeVoterTable_GetCellUtf8(t, row, col);
    if (out == NULL || cch <= 0)
    {
        return;
    }
    out[0] = L'\0';
    if (utf8 != NULL && utf8[0] != '\0')
    {
        MultiByteToWideChar(CP_UTF8, 0, utf8, -1, out, cch);
    }
}

enum
{
    DIFF_FIELD_NAME = 0,
    DIFF_FIELD_ADDRESS,
    DIFF_FIELD_PRECINCT,
    DIFF_FIELD_METHOD,
    DIFF_FIELD_DATE,
    DIFF_FIELD_COUNT
};

static const wchar_t *Diff_FieldName(int field)
{
    static const wchar_t *const k_names[DIFF_FIELD_COUNT] = {L"Name", L"Address", L"Precinct",
                                                             L"Voting Method", L"Date Voted"};
    return (field >= 0 && field < DIFF_FIELD_COUNT) ? k_names[field] : L"";
}

static void Diff_Layout(DiffWindow *dw, int cx, int cy)
{
    int sh = 0;
    RECT rs;
    if (dw->status != NULL)
    {
        SendMessageW(dw->status, WM_SIZE, 0, 0);
        if (GetWindowRect(dw->status, &rs))
        {
            sh = rs.bottom - rs.top;
        }
    }
    if (dw->list != NULL)
    {
        MoveWindow(dw->list, 0, 0, cx, (cy > sh) ? (cy - sh) : 0, TRUE);
    }
}

/* Build (replacing any prior) the side-by-side differences window for @p cw. */
static void App_ShowDifferences(CompareWindow *cw)
{
    EeCompareDiff *diffs = NULL;
    uint32_t diff_count = 0;
    EeCompareOptions opts;
    int vote_col_a[2] = {-1, -1}; /* Voting Method, Date Voted columns in A */
    int vote_col_b[2] = {-1, -1};
    DiffRow *rows = NULL;
    uint32_t row_count = 0;
    uint32_t i;
    DiffWindow *dw;
    HCURSOR prev;
    wchar_t title[MAX_PATH * 2];
    const wchar_t *base_a;
    const wchar_t *base_b;
    RECT pr;
    int x = CW_USEDEFAULT;
    int y = CW_USEDEFAULT;

    if (cw == NULL || cw->a == NULL || cw->b == NULL)
    {
        return;
    }
    if (cw->a->table.row_count != cw->rows_a || cw->b->table.row_count != cw->rows_b)
    {
        MessageBoxW(cw->hwnd,
                    L"A file changed since the comparison was run. Run Compare again.",
                    k_WindowTitle,
                    MB_ICONINFORMATION | MB_OK);
        return;
    }

    App_CompareOptions(cw->a, cw->b, &opts);
    vote_col_a[0] = EeVoterTable_FindColumnByTitle(&cw->a->table, L"Voting Method");
    vote_col_a[1] = EeVoterTable_FindColumnByTitle(&cw->a->table, L"Date Voted");
    vote_col_b[0] = EeVoterTable_FindColumnByTitle(&cw->b->table, L"Voting Method");
    vote_col_b[1] = EeVoterTable_FindColumnByTitle(&cw->b->table, L"Date Voted");
    prev = SetCursor(LoadCursorW(NULL, IDC_WAIT));
    if (!EeVoterTable_CollectDifferencesEx(&cw->a->table,
                                         &cw->b->table,
                                         &opts,
                                         &diffs,
                                         &diff_count,
                                         NULL,
                                         NULL,
                                         NULL))
    {
        SetCursor(prev);
        MessageBoxW(cw->hwnd,
                    L"Could not collect the differences.",
                    k_WindowTitle,
                    MB_ICONERROR | MB_OK);
        return;
    }

    /* Expand to one display row per differing field. */
    if (diff_count > 0)
    {
        rows = (DiffRow *)malloc((size_t)diff_count * DIFF_FIELD_COUNT * sizeof(DiffRow));
    }
    if (rows != NULL)
    {
        for (i = 0; i < diff_count; i++)
        {
            static const uint16_t masks[DIFF_FIELD_COUNT] = {
                EE_CMP_NAME_MINOR | EE_CMP_NAME_MAJOR, EE_CMP_ADDR_MINOR | EE_CMP_ADDR_MAJOR,
                EE_CMP_PCT_CHANGED, EE_CMP_METHOD_CHANGED, EE_CMP_DATE_CHANGED};
            const int cols_a[DIFF_FIELD_COUNT] = {EE_COL_NAME, EE_COL_ADDRESS, EE_COL_PRECINCT,
                                                  vote_col_a[0], vote_col_a[1]};
            const int cols_b[DIFF_FIELD_COUNT] = {EE_COL_NAME, EE_COL_ADDRESS, EE_COL_PRECINCT,
                                                  vote_col_b[0], vote_col_b[1]};
            int k;
            for (k = 0; k < DIFF_FIELD_COUNT; k++)
            {
                if ((diffs[i].bits & masks[k]) && cols_a[k] >= 0 && cols_b[k] >= 0)
                {
                    rows[row_count].row_a = diffs[i].row_a;
                    rows[row_count].row_b = diffs[i].row_b;
                    rows[row_count].col = (uint32_t)cols_a[k];
                    rows[row_count].col_b = (uint32_t)cols_b[k];
                    rows[row_count].field = k;
                    row_count++;
                }
            }
        }
    }
    free(diffs);
    SetCursor(prev);

    if (rows == NULL)
    {
        MessageBoxW(cw->hwnd,
                    L"No differing voters found.",
                    k_WindowTitle,
                    MB_ICONINFORMATION | MB_OK);
        return;
    }

    if (g_diff != NULL)
    {
        DestroyWindow(g_diff->hwnd);
    }
    dw = (DiffWindow *)calloc(1, sizeof(DiffWindow));
    if (dw == NULL)
    {
        free(rows);
        return;
    }
    dw->a = cw->a;
    dw->b = cw->b;
    dw->rows = rows;
    dw->row_count = row_count;
    dw->sort_col = -1;
    dw->sort_asc = TRUE;

    base_a = App_PathBaseName(cw->a->load_path);
    base_b = App_PathBaseName(cw->b->load_path);
    if (base_a[0] == L'\0')
    {
        base_a = L"(A)";
    }
    if (base_b[0] == L'\0')
    {
        base_b = L"(B)";
    }
    StringCchPrintfW(title, ARRAYSIZE(title), L"Differences - %s vs %s", base_a, base_b);

    if (GetWindowRect(cw->hwnd, &pr))
    {
        x = pr.left + Scale(cw->a, 40);
        y = pr.top + Scale(cw->a, 40);
    }
    dw->hwnd = CreateWindowExW(0,
                               k_DiffClassName,
                               title,
                               WS_OVERLAPPEDWINDOW | WS_CLIPCHILDREN,
                               x,
                               y,
                               Scale(cw->a, 720),
                               Scale(cw->a, 460),
                               NULL,
                               NULL,
                               cw->a->instance,
                               dw);
    if (dw->hwnd == NULL)
    {
        free(dw->rows);
        free(dw);
        return;
    }
    g_diff = dw;
    ShowWindow(dw->hwnd, SW_SHOW);
    SetForegroundWindow(dw->hwnd);
}

/* Close the differences window if it summarizes @p app (as either side). */
static void App_CloseDiff(AppState *app)
{
    if (g_diff != NULL && (g_diff->a == app || g_diff->b == app))
    {
        DestroyWindow(g_diff->hwnd);
    }
}

/* Subclass so the differences list draws its header bold on the grey header
 * background, like the voter list (header NM_CUSTOMDRAW is delivered to the list,
 * the header's parent). */
static WNDPROC g_old_diff_list_proc = NULL;

static LRESULT CALLBACK DiffListSubclass(HWND hwnd, UINT msg, WPARAM wParam, LPARAM lParam)
{
    if (msg == WM_NOTIFY)
    {
        NMHDR *nm = (NMHDR *)lParam;
        HWND header = ListView_GetHeader(hwnd);
        DiffWindow *dw = (DiffWindow *)GetWindowLongPtrW(GetParent(hwnd), GWLP_USERDATA);
        if (dw != NULL && nm != NULL && header != NULL && nm->hwndFrom == header &&
            nm->code == NM_CUSTOMDRAW)
        {
            return App_HeaderCustomDraw(dw->a, (NMCUSTOMDRAW *)lParam, FALSE);
        }
    }
    return CallWindowProcW(g_old_diff_list_proc, hwnd, msg, wParam, lParam);
}

static const char *Diff_FieldNameUtf8(int field)
{
    static const char *const k_names[DIFF_FIELD_COUNT] = {"Name", "Address", "Precinct",
                                                          "Voting Method", "Date Voted"};
    return (field >= 0 && field < DIFF_FIELD_COUNT) ? k_names[field] : "";
}

/* Voter IDs compare numerically when both are all digits, else case-insensitive. */
static int diff_cmp_ids(const char *a, const char *b)
{
    if (a[0] != '\0' && b[0] != '\0')
    {
        char *ea = NULL;
        char *eb = NULL;
        unsigned __int64 na = _strtoui64(a, &ea, 10);
        unsigned __int64 nb = _strtoui64(b, &eb, 10);
        if (ea != NULL && *ea == '\0' && eb != NULL && *eb == '\0')
        {
            return (na < nb) ? -1 : (na > nb ? 1 : 0);
        }
    }
    return _stricmp(a, b);
}

typedef struct DiffSortCtx
{
    const EeVoterTable *a;
    const EeVoterTable *b;
    int col;
    BOOL asc;
} DiffSortCtx;

static int diff_sort_cmp(void *ctxv, const void *pa, const void *pb)
{
    const DiffSortCtx *ctx = (const DiffSortCtx *)ctxv;
    const DiffRow *ra = (const DiffRow *)pa;
    const DiffRow *rb = (const DiffRow *)pb;
    int c;

    switch (ctx->col)
    {
        case 0:
            c = diff_cmp_ids(EeVoterTable_GetCellUtf8(ctx->a, ra->row_a, EE_COL_VOTER_ID),
                             EeVoterTable_GetCellUtf8(ctx->a, rb->row_a, EE_COL_VOTER_ID));
            break;
        case 1:
            c = _stricmp(Diff_FieldNameUtf8(ra->field), Diff_FieldNameUtf8(rb->field));
            break;
        case 2:
            c = _stricmp(EeVoterTable_GetCellUtf8(ctx->a, ra->row_a, ra->col),
                         EeVoterTable_GetCellUtf8(ctx->a, rb->row_a, rb->col));
            break;
        default: /* 3 */
            c = _stricmp(EeVoterTable_GetCellUtf8(ctx->b, ra->row_b, ra->col_b),
                         EeVoterTable_GetCellUtf8(ctx->b, rb->row_b, rb->col_b));
            break;
    }
    if (c == 0)
    {
        /* Stable-ish tiebreak so a voter's fields stay together. */
        c = diff_cmp_ids(EeVoterTable_GetCellUtf8(ctx->a, ra->row_a, EE_COL_VOTER_ID),
                         EeVoterTable_GetCellUtf8(ctx->a, rb->row_a, EE_COL_VOTER_ID));
    }
    return ctx->asc ? c : -c;
}

/* Show the sort arrow on the active column header. */
static void Diff_UpdateHeaderArrows(DiffWindow *dw)
{
    HWND hdr;
    int i;
    int n;

    if (dw->list == NULL)
    {
        return;
    }
    hdr = ListView_GetHeader(dw->list);
    if (hdr == NULL)
    {
        return;
    }
    n = Header_GetItemCount(hdr);
    for (i = 0; i < n; i++)
    {
        HDITEMW it;
        ZeroMemory(&it, sizeof(it));
        it.mask = HDI_FORMAT;
        Header_GetItem(hdr, i, &it);
        it.fmt &= ~(HDF_SORTUP | HDF_SORTDOWN);
        if (i == dw->sort_col)
        {
            it.fmt |= dw->sort_asc ? HDF_SORTUP : HDF_SORTDOWN;
        }
        Header_SetItem(hdr, i, &it);
    }
}

static void Diff_Sort(DiffWindow *dw, int col)
{
    DiffSortCtx ctx;

    if (dw == NULL || col < 0 || col > 3)
    {
        return;
    }
    if (dw->sort_col == col)
    {
        dw->sort_asc = !dw->sort_asc;
    }
    else
    {
        dw->sort_col = col;
        dw->sort_asc = TRUE;
    }
    ctx.a = &dw->a->table;
    ctx.b = &dw->b->table;
    ctx.col = col;
    ctx.asc = dw->sort_asc;
    if (dw->rows != NULL && dw->row_count > 1)
    {
        qsort_s(dw->rows, dw->row_count, sizeof(DiffRow), diff_sort_cmp, &ctx);
    }
    Diff_UpdateHeaderArrows(dw);
    if (dw->list != NULL)
    {
        ListView_RedrawItems(dw->list, 0, (int)dw->row_count);
        InvalidateRect(dw->list, NULL, FALSE);
    }
}

/* Copy the selected rows as tab-separated UTF-8 (Voter ID, Field, A value, B value). */
static void Diff_CopySelected(DiffWindow *dw)
{
    const EeVoterTable *a;
    const EeVoterTable *b;
    int i;
    size_t total = 0;
    char *buf;
    char *p;

    if (dw == NULL || dw->list == NULL)
    {
        return;
    }
    a = &dw->a->table;
    b = &dw->b->table;

    i = ListView_GetNextItem(dw->list, -1, LVNI_SELECTED);
    while (i >= 0)
    {
        if ((uint32_t)i < dw->row_count)
        {
            const DiffRow *dr = &dw->rows[i];
            total += strlen(EeVoterTable_GetCellUtf8(a, dr->row_a, EE_COL_VOTER_ID));
            total += strlen(Diff_FieldNameUtf8(dr->field));
            total += strlen(EeVoterTable_GetCellUtf8(a, dr->row_a, dr->col));
            total += strlen(EeVoterTable_GetCellUtf8(b, dr->row_b, dr->col_b));
            total += 5; /* 3 tabs + CR + LF */
        }
        i = ListView_GetNextItem(dw->list, i, LVNI_SELECTED);
    }
    if (total == 0)
    {
        return;
    }
    buf = (char *)malloc(total + 1);
    if (buf == NULL)
    {
        return;
    }
    p = buf;
    i = ListView_GetNextItem(dw->list, -1, LVNI_SELECTED);
    while (i >= 0)
    {
        if ((uint32_t)i < dw->row_count)
        {
            const DiffRow *dr = &dw->rows[i];
            const char *fields[4];
            int f;
            fields[0] = EeVoterTable_GetCellUtf8(a, dr->row_a, EE_COL_VOTER_ID);
            fields[1] = Diff_FieldNameUtf8(dr->field);
            fields[2] = EeVoterTable_GetCellUtf8(a, dr->row_a, dr->col);
            fields[3] = EeVoterTable_GetCellUtf8(b, dr->row_b, dr->col_b);
            for (f = 0; f < 4; f++)
            {
                size_t n = strlen(fields[f]);
                memcpy(p, fields[f], n);
                p += n;
                *p++ = (f < 3) ? '\t' : '\r';
            }
            *p++ = '\n';
        }
        i = ListView_GetNextItem(dw->list, i, LVNI_SELECTED);
    }
    *p = '\0';
    App_SetClipboardUtf8(dw->hwnd, buf);
    free(buf);
}

/* Right-click: select the row if needed, then a Copy menu. */
typedef struct DiffExportCtx
{
    DiffWindow *dw;
    const uint32_t *idx; /* export row -> display row, or NULL for all */
} DiffExportCtx;

static void Diff_ExportCell(void *user, uint32_t row, uint32_t col, wchar_t *buf, size_t cch)
{
    DiffExportCtx *c = (DiffExportCtx *)user;
    const DiffRow *dr = &c->dw->rows[(c->idx != NULL) ? c->idx[row] : row];
    buf[0] = L'\0';
    switch (col)
    {
        case 0:
            Diff_CellW(&c->dw->a->table, dr->row_a, EE_COL_VOTER_ID, buf, (int)cch);
            break;
        case 1:
            StringCchCopyW(buf, cch, Diff_FieldName(dr->field));
            break;
        case 2:
            Diff_CellW(&c->dw->a->table, dr->row_a, dr->col, buf, (int)cch);
            break;
        default:
            Diff_CellW(&c->dw->b->table, dr->row_b, dr->col_b, buf, (int)cch);
            break;
    }
}

/* Differences → Export Selected / Export All: Voter ID, Field, value in A, value in B, in
 * the order shown. */
static void Diff_Export(DiffWindow *dw, BOOL selection_only)
{
    const wchar_t *headers[4];
    wchar_t a[96];
    wchar_t b[96];
    wchar_t suggested[300];
    uint32_t *idx = NULL;
    uint32_t n;
    DiffExportCtx ctx;
    if (dw == NULL || dw->row_count == 0)
    {
        return;
    }
    if (selection_only)
    {
        idx = App_CollectSelectedIndices(dw->list, dw->row_count, &n);
        if (n == 0)
        {
            free(idx);
            return;
        }
    }
    else
    {
        n = dw->row_count;
    }
    headers[0] = L"Voter ID";
    headers[1] = L"Field";
    headers[2] = App_PathBaseName(dw->a->load_path);
    headers[3] = App_PathBaseName(dw->b->load_path);
    App_BaseNameNoExt(dw->a->load_path, a, ARRAYSIZE(a));
    App_BaseNameNoExt(dw->b->load_path, b, ARRAYSIZE(b));
    StringCchPrintfW(suggested, ARRAYSIZE(suggested), L"%s-vs-%s-%sDifferences", a, b,
                     selection_only ? L"Selected_" : L"");
    ctx.dw = dw;
    ctx.idx = idx;
    App_ExportReportModel(dw->hwnd, suggested, 4, headers, n, Diff_ExportCell, &ctx);
    free(idx);
}

static void Diff_OnContextMenu(DiffWindow *dw, int item, POINT screen)
{
    HMENU m;
    UINT cmd;

    if (dw == NULL || dw->list == NULL)
    {
        return;
    }
    if (item >= 0 && !(ListView_GetItemState(dw->list, item, LVIS_SELECTED) & LVIS_SELECTED))
    {
        ListView_SetItemState(dw->list, -1, 0, LVIS_SELECTED | LVIS_FOCUSED);
        ListView_SetItemState(dw->list,
                              item,
                              LVIS_SELECTED | LVIS_FOCUSED,
                              LVIS_SELECTED | LVIS_FOCUSED);
    }
    m = CreatePopupMenu();
    if (m == NULL)
    {
        return;
    }
    AppendMenuW(m, MF_STRING, IDM_EDIT_COPY, L"&Copy");
    AppendMenuW(m, MF_SEPARATOR, 0, NULL);
    AppendMenuW(m, MF_STRING, IDM_EXPORT_SELECTED, L"Export &Selected…");
    AppendMenuW(m, MF_STRING, IDM_EXPORT_ALL, L"Export &All…");
    cmd = (UINT)
        TrackPopupMenu(m, TPM_RIGHTBUTTON | TPM_RETURNCMD, screen.x, screen.y, 0, dw->hwnd, NULL);
    DestroyMenu(m);
    if (cmd == IDM_EDIT_COPY)
    {
        Diff_CopySelected(dw);
    }
    else if (cmd == IDM_EXPORT_SELECTED)
    {
        Diff_Export(dw, TRUE);
    }
    else if (cmd == IDM_EXPORT_ALL)
    {
        Diff_Export(dw, FALSE);
    }
}

static LRESULT CALLBACK DiffWndProc(HWND hwnd, UINT msg, WPARAM wParam, LPARAM lParam)
{
    DiffWindow *dw = (DiffWindow *)GetWindowLongPtrW(hwnd, GWLP_USERDATA);

    switch (msg)
    {
        case WM_CREATE:
        {
            CREATESTRUCTW *cs = (CREATESTRUCTW *)lParam;
            LVCOLUMNW col;
            RECT rc;
            const wchar_t *base_a;
            const wchar_t *base_b;
            wchar_t hdr_a[MAX_PATH];
            wchar_t hdr_b[MAX_PATH];
            wchar_t st[128];

            dw = (DiffWindow *)cs->lpCreateParams;
            SetWindowLongPtrW(hwnd, GWLP_USERDATA, (LONG_PTR)dw);
            dw->hwnd = hwnd;

            GetClientRect(hwnd, &rc);
            dw->list = CreateWindowExW(0,
                                       WC_LISTVIEWW,
                                       L"",
                                       WS_CHILD | WS_VISIBLE | LVS_REPORT | LVS_OWNERDATA |
                                           LVS_SHOWSELALWAYS,
                                       0,
                                       0,
                                       rc.right,
                                       rc.bottom,
                                       hwnd,
                                       NULL,
                                       dw->a->instance,
                                       NULL);
            if (dw->list == NULL)
            {
                return -1;
            }
            /* Subclass for the bold header draw (see DiffListSubclass). */
            {
                WNDPROC old =
                    (WNDPROC)SetWindowLongPtrW(dw->list, GWLP_WNDPROC, (LONG_PTR)DiffListSubclass);
                if (g_old_diff_list_proc == NULL)
                {
                    g_old_diff_list_proc = old;
                }
            }
            ListView_SetExtendedListViewStyle(dw->list,
                                              LVS_EX_FULLROWSELECT | LVS_EX_DOUBLEBUFFER |
                                                  LVS_EX_GRIDLINES);
            if (dw->a->font_ui)
            {
                SendMessageW(dw->list, WM_SETFONT, (WPARAM)dw->a->font_ui, TRUE);
            }
            base_a = App_PathBaseName(dw->a->load_path);
            base_b = App_PathBaseName(dw->b->load_path);
            if (base_a[0] == L'\0')
            {
                base_a = L"file A";
            }
            if (base_b[0] == L'\0')
            {
                base_b = L"file B";
            }
            StringCchCopyW(hdr_a, ARRAYSIZE(hdr_a), base_a);
            StringCchCopyW(hdr_b, ARRAYSIZE(hdr_b), base_b);

            ZeroMemory(&col, sizeof(col));
            col.mask = LVCF_TEXT | LVCF_WIDTH | LVCF_FMT;
            col.fmt = LVCFMT_LEFT;
            col.pszText = L"Voter ID";
            col.cx = Scale(dw->a, 120);
            ListView_InsertColumn(dw->list, 0, &col);
            col.pszText = L"Field";
            col.cx = Scale(dw->a, 80);
            ListView_InsertColumn(dw->list, 1, &col);
            col.pszText = hdr_a;
            col.cx = Scale(dw->a, 230);
            ListView_InsertColumn(dw->list, 2, &col);
            col.pszText = hdr_b;
            col.cx = Scale(dw->a, 230);
            ListView_InsertColumn(dw->list, 3, &col);

            dw->status = CreateWindowExW(0,
                                         STATUSCLASSNAMEW,
                                         NULL,
                                         WS_CHILD | WS_VISIBLE | SBARS_SIZEGRIP,
                                         0,
                                         0,
                                         0,
                                         0,
                                         hwnd,
                                         NULL,
                                         dw->a->instance,
                                         NULL);
            if (dw->status != NULL)
            {
                if (dw->a->font_ui)
                {
                    SendMessageW(dw->status, WM_SETFONT, (WPARAM)dw->a->font_ui, TRUE);
                }
                StringCchPrintfW(st,
                                 ARRAYSIZE(st),
                                 L"%u differing field(s) — click a header to sort, Ctrl+C to copy",
                                 dw->row_count);
                SendMessageW(dw->status, SB_SETTEXTW, 0, (LPARAM)st);
            }

            ListView_SetItemCountEx(dw->list, (int)dw->row_count, LVSICF_NOINVALIDATEALL);
            Diff_Layout(dw, rc.right, rc.bottom);
            return 0;
        }

        case WM_SIZE:
            if (dw != NULL)
            {
                Diff_Layout(dw, LOWORD(lParam), HIWORD(lParam));
            }
            return 0;

        case WM_SETFOCUS:
            if (dw != NULL && dw->list != NULL)
            {
                SetFocus(dw->list);
            }
            return 0;

        case WM_COMMAND:
            /* Ctrl+C is routed here by the shared accelerator table. */
            if (dw != NULL && LOWORD(wParam) == IDM_EDIT_COPY)
            {
                Diff_CopySelected(dw);
                return 0;
            }
            break;

        case WM_NOTIFY:
        {
            NMHDR *hdr = (NMHDR *)lParam;
            if (dw == NULL || hdr->hwndFrom != dw->list)
            {
                break;
            }
            if (hdr->code == LVN_GETDISPINFOW)
            {
                NMLVDISPINFOW *di = (NMLVDISPINFOW *)lParam;
                int idx = di->item.iItem;
                if ((di->item.mask & LVIF_TEXT) && idx >= 0 && (uint32_t)idx < dw->row_count)
                {
                    const DiffRow *dr = &dw->rows[idx];
                    switch (di->item.iSubItem)
                    {
                        case 0:
                            Diff_CellW(&dw->a->table,
                                       dr->row_a,
                                       EE_COL_VOTER_ID,
                                       di->item.pszText,
                                       di->item.cchTextMax);
                            break;
                        case 1:
                            StringCchCopyW(di->item.pszText,
                                           di->item.cchTextMax,
                                           Diff_FieldName(dr->field));
                            break;
                        case 2:
                            Diff_CellW(&dw->a->table,
                                       dr->row_a,
                                       dr->col,
                                       di->item.pszText,
                                       di->item.cchTextMax);
                            break;
                        case 3:
                            Diff_CellW(&dw->b->table,
                                       dr->row_b,
                                       dr->col_b,
                                       di->item.pszText,
                                       di->item.cchTextMax);
                            break;
                        default:
                            break;
                    }
                }
                return 0;
            }
            if (hdr->code == LVN_COLUMNCLICK)
            {
                NMLISTVIEW *nlv = (NMLISTVIEW *)lParam;
                Diff_Sort(dw, nlv->iSubItem);
                return 0;
            }
            if (hdr->code == NM_RCLICK)
            {
                LPNMITEMACTIVATE ia = (LPNMITEMACTIVATE)lParam;
                POINT screen = ia->ptAction;
                ClientToScreen(dw->list, &screen);
                Diff_OnContextMenu(dw, ia->iItem, screen);
                return 0;
            }
            break;
        }

        case WM_DESTROY:
            if (dw != NULL)
            {
                if (g_diff == dw)
                {
                    g_diff = NULL;
                }
                free(dw->rows);
                SetWindowLongPtrW(hwnd, GWLP_USERDATA, 0);
                free(dw);
            }
            return 0;

        default:
            break;
    }
    return DefWindowProcW(hwnd, msg, wParam, lParam);
}

/* -------------------------------------------------------------------------- */
/* Help / About                                                               */
/* -------------------------------------------------------------------------- */

static const wchar_t k_HelpLoading[] =
    L"Load Voter List (File menu, or Ctrl+O) opens a county voter-registration export: "
    L"comma- or tab-delimited text (.csv, .txt) or an Excel workbook (.xlsx or .xls — if it has "
    L"several sheets you pick one). Large lists load in the background with a progress "
    L"bar and can be canceled.\r\n\r\n"
    L"Counties lay out their files differently, so Election Explorer reads the header "
    L"row and recognizes the columns it needs by name — for example the Travis, Dallas "
    L"and El Paso County, Texas, layouts. From them it builds four normalized columns, "
    L"shown first; every original column follows, unchanged.\r\n\r\n"
    L"•  Voter ID — the statewide voter ID (VUID, State Voter ID, SOS Voter ID, …). A "
    L"generic \"VoterID\" or a county ID column is used only when there is no statewide "
    L"ID column.\r\n\r\n"
    L"•  Precinct — the leading precinct number (\"P 204\" and \"204.3\" both become "
    L"204).\r\n\r\n"
    L"•  Name — built from the first, middle, last and suffix columns, or a full-name "
    L"column (see Options for surname-first order).\r\n\r\n"
    L"•  Address — \"street, City, ST ZIP\". The street comes from the house number (and "
    L"a suffix such as 1/2), directions, street name and type, and the apartment, unit "
    L"or space number — or from a full-address column. When the file has no residence "
    L"state column, the state is taken from the residence ZIP codes (a list belongs to "
    L"one state). Columns that only look like city or state fields — district codes "
    L"such as a \"CITY\" column holding \"C10\", or \"STATE BOARD OF EDUCATION\" — are "
    L"ignored. A confidential (masked) address is kept exactly as the county masked "
    L"it.\r\n\r\n"
    L"To see who has voted in an election, use File → Load Voter Roster… instead; a roster "
    L"opens in its own window (see Help → Loading Voter Rosters there). An election's results "
    L"(Clarity Elections files, or ES&S, Hart and Dominion results reports) open with File → Load "
    L"Election Results…, also in a window of their own, and ES&S Electionware audit logs with "
    L"File → Load Audit Logs… (see Help → Loading Audit Logs there).";

static const wchar_t k_HelpRosterLoading[] =
    L"Load Voter Roster (File menu) opens the rosters a county publishes during an election "
    L"— who has voted, by day and voting method. Select one or more ZIP files as published, "
    L"Excel workbooks (.xlsx, or .xls from Excel 97-2003), CSV / TSV files (including a roster "
    L"exported from this "
    L"window), or PDF rosters. Rosters from these Texas counties are supported: Travis, "
    L"Tarrant, Harris, Dallas, Bexar, Collin, Denton, Fort Bend, Hidalgo, El Paso, Montgomery, "
    L"Williamson, Cameron, Brazoria, Bell, Galveston, Lubbock, Webb, McLennan, Smith, Brazos, "
    L"Grayson, Randall, Wichita, Gregg, Hunt, Tom Green, Bastrop, Potter, Bowie, Victoria, "
    L"Henderson, Angelina, Jefferson, and Guadalupe, plus the statewide and county rosters of the "
    L"Texas Secretary of State. If a selected file cannot be read, the load "
    L"summary lists it; when none can, a message names them. Large "
    L"rosters load in the background with a progress bar, an estimate of the time remaining, "
    L"and Cancel.\r\n\r\n"
    L"•  Travis: each spreadsheet is one day's list for one voting method. The date and "
    L"method come from the file name (for example \"10.22.2024 Early Vote.xlsx\"). A primary "
    L"roster has one sheet per party, and the party becomes a column.\r\n\r\n"
    L"•  Tarrant: one tab-delimited file per voting method for the whole election (select "
    L"the mail, early voting and Election Day ZIPs together), with addresses. These lists give "
    L"no voting date for early and Election Day voters, so Date Voted is blank and Voting "
    L"Totals shows them as \"(no date)\"; mail ballots carry their return date. In a primary "
    L"the party comes from the file name (\"…_Dem\", \"…_Rep\"). The full election roster "
    L"published after the canvass has a vote type that also identifies provisional and "
    L"limited ballots. Protected voters (\"--Redacted--\") are kept with their precinct.\r\n\r\n"
    L"•  Harris: CSV files in ZIPs, one per day for early voting (\"Cumulative_EV_…\"), one for "
    L"mail ballots (\"Cumulative_BBM_…\") and the Election Day roster, with addresses and "
    L"districts (select them together); each voter's voting or mail-return date is kept. The "
    L"voter history published after the canvass (\"…_VoterHistory\") has a vote type that also "
    L"identifies provisional and limited ballots (Harris's own codes: E = Election Day, P = "
    L"early in person, M or A = mail). Its official roster lists only \"In Person\" or \"Mail\" "
    L"with no voting date, so load the early voting and Election Day rosters to tell the two "
    L"apart. The county's disclaimer, file layout and polling-place lists are not loaded.\r\n\r\n"
    L"•  Dallas: one workbook each for mail ballots returned, in-person early voting and "
    L"Election Day, with each voter's voting or return date (select them together). The "
    L"state Voter ID is used, and the county's own number is kept as a column. The precinct "
    L"ZIPs (\"Precincts 1000 - 1731\") list early voting in person and by mail again, by "
    L"precinct, so load them instead of the early voting and mail reports, not with them.\r\n\r\n"
    L"•  Other counties publish daily or cumulative lists, one file per day or per voting "
    L"method (select an election's files together). The voting method comes from a vote-type "
    L"column, the file name (\"MAIL\", \"ABBM\", \"BBM\", \"EV\", \"ED\", \"IN PERSON\"), a "
    L"polling site named \"EV-…\" or \"ED-…\" (Hidalgo), or a mail list's received-date "
    L"column (Williamson's \"AV\" is a mail ballot). When nothing names it, the date in the file "
    L"name is placed on the Texas election calendar: the election day itself is Election Day, "
    L"the 17 days before it early voting (Bexar); other dates leave it unknown. Montgomery "
    L"lists a voter once for each jurisdiction on the ballot; those rows are merged into one "
    L"record with the jurisdictions joined. Title rows, a translation row under the header, "
    L"and Voter ID and name columns swapped under their titles (Brazoria 2026) are handled "
    L"and noted in the load summary. A \"CertificateNumber\" column (Collin, Webb) is the Voter "
    L"ID; a mail list with only a \"Certificate\" column (Webb from October 28, 2026: the "
    L"county's mail ballot certificate number) loads its voters with a blank Voter ID and the "
    L"number in a Certificate column. A mail list that also lists ballots sent but not yet received (Collin: \"-\" for the "
    L"received date) loads only the received ones and says how many it left out.\r\n\r\n"
    L"•  Texas Secretary of State: the Election Information & Turnout Data site "
    L"(goelect.txelections.civixapps.com) offers, for each statewide election, an early voting "
    L"roster for each day (\"Generate Statewide Report\", or one county's CSV) and, after the "
    L"election, an Election Day roster as a ZIP. Each lists every voter's county, name, Voter "
    L"ID, precinct, and \"MAIL-IN\" or \"IN-PERSON\" (early voting or Election Day, from the "
    L"file name), with no voting date. The statewide early voting roster is cumulative through "
    L"the day in its file name, so Date Voted is blank; load only the latest one (or load two in "
    L"separate windows and Compare them). One county's CSV (no county column) lists only that "
    L"day's voters, so the day in its file name is Date Voted and a county's daily files load "
    L"together. An Election Day roster's in-person voters get the day of the "
    L"election its file name names (a primary, primary runoff or general election; a special "
    L"election's is blank) as Date Voted, and its mail ballots no date. An Election Day ZIP's "
    L"file name ends with the day it was downloaded; the same ZIP downloaded twice is loaded "
    L"once, and its polling-place file is not loaded. In a primary, the party comes from the "
    L"file name; load both parties' "
    L"rosters together, and Show Duplicate Voter IDs finds anyone listed in both.\r\n\r\n"
    L"•  When a corrected file (\"…_Updated.xlsx\") is present, the original it replaces is "
    L"not loaded. A file whose voters all appear in a file of another voting method (a "
    L"misnamed copy) is skipped.\r\n\r\n"
    L"•  PDF rosters printed from a spreadsheet (Bexar's 2026 mail ballot lists, Webb, "
    L"Lubbock) are read as tables: the row with a Voter ID title is the header, and each "
    L"entry goes to the column it sits under, on every page. Scanned PDFs (Guadalupe's mail "
    L"lists) and PDFs without such a table are listed as not readable. Also handled: lines "
    L"printed as one string with the columns spaced apart (Randall, Gregg), pages printed "
    L"sideways (Randall's mail list), numbers set to the right of their titles (Brazos), "
    L"untitled row numbers (Potter), page titles repeated above each page's rows (Bowie), "
    L"and a check-in \"Time\" column that holds the date (Randall, Hunt).\r\n\r\n"
    L"•  File-name dates may have a 2-digit year (\"10.19.26\", \"101926\") or a weekday "
    L"instead of a year (Bastrop's \"Monday-October-19\": the most recent year that day fell "
    L"on a Monday). A \"VOTE METHOD\" column of \"MAIL\" and \"IN PERSON\" (Victoria) is read "
    L"per row, in person being early voting or Election Day by the voting date. A cumulative "
    L"roster with no voting dates (McLennan's list of early voters, Potter's early voting "
    L"roster) leaves Date Voted blank, and McLennan's list names no voting method. A report "
    L"saved as a workbook with its titles on rows of their own (Grayson's check-in report) is "
    L"read with each title over its column. A file-name date range (Jefferson's mail list "
    L"\"…01012026to10232026\") gives the day the list runs through, and a Voter ID "
    L"masked as \"**********\" (Jefferson) leaves that voter's ID blank.\r\n\r\n"
    L"•  Limited Ballot poll lists (the Secretary of State's form 5-38, which Travis posts "
    L"for each day of early voting, also inside its ZIPs) are recognized by their title, as a "
    L"PDF or a workbook. The form gives only each voter's name: a limited ballot voter is not "
    L"registered in the county, so Voter ID and Precinct are blank. The method is Limited, "
    L"and the date comes from the file name. Limited ballots are cast only during early "
    L"voting, so a file-name date outside it (a typo such as \"02.26.2027\" for the March 3, "
    L"2026 primary) is left blank and noted. An assisting person's name and address are kept "
    L"as columns.\r\n\r\n"
    L"•  Rows that are not voters (totals, notes, \"No ballots received.\", Voter IDs with no "
    L"name or precinct) are skipped. Unusual and blank Voter IDs are kept and counted. A "
    L"sheet missing its header row uses the header of a matching sheet.\r\n\r\n"
    L"The window shows Voter ID, Precinct, Name, Voting Method (Mail Ballot, Early Vote "
    L"In-Person, Election Day In-Person, Provisional, or Limited), Date Voted, and any extra "
    L"columns (Party, County, Notes, Assisting Person, Source File). Duplicate Voter IDs are kept; "
    L"Filter → Show Duplicate Voter IDs lists them. Reports → Display Load Summary shows what "
    L"the load skipped or flagged; the summary also opens after loading when there is "
    L"something to report.\r\n\r\n"
    L"See also Help → Reports (Voting Totals) and Help → Compare (with another roster or a "
    L"voter list).";

static const wchar_t k_HelpOptions[] =
    L"Options (Edit → Options) control display and copy behavior. Settings "
    L"apply to the current window and become the defaults for windows opened "
    L"afterward.\r\n\r\n"
    L"•  Pre-pend normalized data for copies — when you copy selected "
    L"rows, the normalized Voter ID, Precinct, Name, and Address are placed "
    L"before the original source columns.\r\n\r\n"
    L"•  Display name in surname-first format — shows the normalized "
    L"Name column as \"Last, First Middle\" instead of \"First Middle Last\".\r\n\r\n"
    L"•  Zoom — scales the grid text from 50% to 250% (100% is actual "
    L"size).\r\n\r\n"
    L"•  Map engine — the mapping service opened by \"Show in Maps\" "
    L"(Google, Bing, Apple, or OpenStreetMap).";

/* How Include / Exclude rules combine (shared by the voter-list and roster topics). */
#define EE_HELP_FILTER_RULES                                                                       \
    L"Filters (Filter menu, or Ctrl+L) narrow the visible rows. Each rule targets "                \
    L"one column and tests it with a relation — is, is not, begins with, ends "                    \
    L"with, contains, excludes, and (for date and numeric columns) less than / "                   \
    L"more than. Every rule is either an Include or an Exclude.\r\n\r\n"                          \
    L"How multiple rules combine:\r\n\r\n"                                                        \
    L"•  Exclude wins. If a row matches ANY enabled Exclude rule it is hidden, "                   \
    L"regardless of the Include rules.\r\n\r\n"                                                   \
    L"•  Includes on the SAME field are OR'd — a row passes that field if "                        \
    L"it matches at least one of them.\r\n\r\n"                                                   \
    L"•  Includes on DIFFERENT fields are AND'd — a row must satisfy each "                        \
    L"field that has Include rules.\r\n\r\n"                                                      \
    L"•  With no Include rules, every row is shown except those removed by "                       \
    L"Exclude rules.\r\n\r\n"                                                                 \
    L"Export Filter… (in the Filter dialog) saves the listed rules to a file — UTF-8 CSV (\"….filter.csv\") or tab-delimited (\"….filter.tsv\"), which open in Excel. "                                                              \
    L"Load Filter… replaces the listed rules with a saved filter; they take effect on Apply or "                                                              \
    L"OK. Columns are matched by name, so a filter saved for one file works on another with the "                                                              \
    L"same column names in any order; rules for columns this window does not have are listed and "                                                              \
    L"left out.\r\n\r\n"

static const wchar_t k_HelpFilters[] =
    L"Filters (Filter menu, or Ctrl+L) narrow the visible rows. Each rule targets "
    L"one column and tests it with a relation — is, is not, begins with, ends "
    L"with, contains, excludes, and (for date and numeric columns) less than / "
    L"more than. Every rule is either an Include or an Exclude.\r\n\r\n"
    L"How multiple rules combine:\r\n\r\n"
    L"•  Exclude wins. If a row matches ANY enabled Exclude rule it is hidden, "
    L"regardless of the Include rules.\r\n\r\n"
    L"•  Includes on the SAME field are OR'd — a row passes that field if "
    L"it matches at least one of them. Example: two Includes on Voter ID (is 100, "
    L"is 200) show rows whose Voter ID is 100 OR 200.\r\n\r\n"
    L"•  Includes on DIFFERENT fields are AND'd — a row must satisfy each "
    L"field that has Include rules. Example: Include Precinct is 101 together with "
    L"Include Name contains SMITH shows only voters in precinct 101 whose name "
    L"contains SMITH.\r\n\r\n"
    L"•  With no Include rules, every row is shown except those removed by "
    L"Exclude rules.\r\n\r\n"
    L"Add rules in the Filter dialog, or right-click a cell and choose "
    L"Include/Exclude to add a rule for that value. Reset Filter clears all rules; "
    L"a disabled rule is kept but ignored.\r\n\r\n"
    L"Export Filter… (in the Filter dialog) saves the listed rules to a file — UTF-8 CSV (\"….filter.csv\") or tab-delimited (\"….filter.tsv\"), which open in Excel. "
    L"Load Filter… replaces the listed rules with a saved filter; they take effect on Apply or "
    L"OK. Columns are matched by name, so a filter saved for one file works on another with the "
    L"same column names in any order; rules for columns this window does not have are listed and "
    L"left out.\r\n\r\n"
    L"Show Duplicate Voters lists voters who share a name and date of birth. Show "
    L"Duplicate Voters Voting does the same for voters who voted — whose Voter ID is in "
    L"an open voter roster — under two or more different Voter IDs (for example, two "
    L"people with the same name and birth date, told apart only by driver license "
    L"numbers that voter lists do not include). It needs a list with birth dates and a "
    L"loaded voter roster; with several rosters open you choose one. Reset View returns "
    L"to all rows.";

static const wchar_t k_HelpReports[] =
    L"Reports (Reports menu) summarize the loaded list by a single column.\r\n\r\n"
    L"•  Display Precinct Report — one row per distinct Precinct with the "
    L"number of voters in it.\r\n\r\n"
    L"•  Display Address Report — one row per distinct normalized Address "
    L"with its voter count.\r\n\r\n"
    L"Each report opens in its own window. Empty values are grouped into a "
    L"\"(blank)\" row so incomplete records stay visible. Reports summarize ALL "
    L"loaded rows — they ignore the active filter and the duplicates view. "
    L"Click a column header to sort. Right-click a row to Copy it, to Include or "
    L"Exclude that value in the main window's filter, or (Address) Show in Maps. A "
    L"report closes when its list is reloaded or its window is closed.";

static const wchar_t k_HelpCompare[] =
    L"Compare (Compare menu) compares the voter list in this window against another "
    L"open list, matching voters by normalized Voter ID.\r\n\r\n"
    L"Open two lists in separate windows, then choose Compare → \"Compare with "
    L"<file>\". The Compare Summary window reports, for each file:\r\n\r\n"
    L"•  Only here — voters whose Voter ID is not in the other file (rows "
    L"with a blank Voter ID are counted here).\r\n\r\n"
    L"•  Voter ID repeated — rows whose Voter ID appears on more rows in this file "
    L"than in the other. Rows pair one to one (identical rows first, then the closest), "
    L"so a county export that repeats a voter leaves the extra row here and the other "
    L"counts agree in both files.\r\n\r\n"
    L"•  Changed — voters found in both files whose Precinct, Name, or "
    L"Address differs.\r\n\r\n"
    L"•  Precinct changed — voters whose precinct differs but whose address "
    L"is unchanged (re-precincting). A precinct change that comes with an address "
    L"change is treated as a move and shown under Address, not here.\r\n\r\n"
    L"•  Identical — voters found in both files with matching Precinct, "
    L"Name, and Address.\r\n\r\n"
    L"Double-click a row to highlight those voters in the grid (the left count "
    L"column acts on this file, the right column on the other file), or right-click "
    L"to choose which file to show them in. Use Filter → Reset View to clear "
    L"the highlight. Large comparisons show a progress bar and can be canceled.\r\n\r\n"
    L"Show Differences… opens a side-by-side list of every changed voter's "
    L"differing fields — Voter ID, the field (Name / Address / Precinct), and its "
    L"value in each file. Right-click it to Copy, Export Selected or Export All.\r\n\r\n"
    L"Export Summary… saves the category counts; Export All Records… saves every record "
    L"of both files with its file, Voter ID, Precinct, Name, Address, result (Only here, "
    L"Voter ID repeated, Identical or Changed) and changed fields. Both write UTF-8 CSV "
    L"or TSV.";

static const wchar_t k_RosterHelpOptions[] =
    L"Options (Edit → Options) control how the roster is shown. Settings apply to this "
    L"window and become the defaults for windows opened afterward.\r\n\r\n"
    L"•  Display name in surname-first format — shows the Name column as \"Last, First "
    L"Middle\" instead of \"First Middle Last\". Compare is not affected: it compares the "
    L"name parts.\r\n\r\n"
    L"•  Zoom — scales the grid text from 50% to 250% (100% is actual size).";

static const wchar_t k_RosterHelpFilters[] =
    EE_HELP_FILTER_RULES
    L"In a voter roster the Filter dialog lists the columns the window shows — Voter ID, "
    L"Precinct, Name, Voting Method, Date Voted, and extra columns such as Party and Source "
    L"File. For example, Include Voting Method is Mail Ballot together with Include Date "
    L"Voted is 10/22/2024 shows the mail ballots of that day. Right-click a cell to Include "
    L"or Exclude its value; Reset Filter clears all rules.\r\n\r\n"
    L"Show Duplicate Voter IDs lists every row whose Voter ID appears more than once (a "
    L"voter listed for two days or methods, or a county error such as two voters sharing "
    L"one ID). Reset View returns to all rows.";

static const wchar_t k_RosterHelpReports[] =
    L"Reports (Reports menu) summarize all loaded records — they ignore the active filter "
    L"and the duplicates view.\r\n\r\n"
    L"•  Display Precinct Report — one row per Precinct with its Number of Voters (a "
    L"voter on several roster rows counts on each).\r\n\r\n"
    L"•  Display Voting Totals — voters per Date Voted and Voting Method, with a Total "
    L"column and a Total row. Each Voter ID is counted once, on its earliest record (by "
    L"date, then Mail Ballot, Early Vote, Election Day, Provisional, Limited); records with "
    L"a blank Voter ID are each counted, and the status bar says how many later records were "
    L"not counted. In a primary each day has a row per party plus an \"All parties\" row, "
    L"followed by a Total per party and an overall Total. A method with no roster files "
    L"shows \"not available\"; a method whose only files are PDFs that cannot be read (scanned) "
    L"shows \"not loaded\". Right-click to Copy (or Ctrl+C) or to Export Selected / Export All; copies "
    L"and exports use plain numbers.\r\n\r\n"
    L"•  Display Load Summary — what the load read, skipped (corrections, copies, PDFs, "
    L"rows that are not voters) and flagged (unusual, blank and duplicate Voter IDs).\r\n\r\n"
    L"Each report opens in its own window and closes with the roster window.";

static const wchar_t k_RosterHelpCompare[] =
    L"Compare (Compare menu) matches this roster, by Voter ID, against another open voter "
    L"roster or voter list. Choose Compare → \"Compare with <file>\".\r\n\r\n"
    L"•  Two rosters compare Precinct, Name (first, middle, last and suffix), Voting Method "
    L"and Date Voted — for example a roster against its own export, or two days of the "
    L"same election.\r\n\r\n"
    L"•  A roster and a voter list compare Precinct and Name (rosters have no address). "
    L"Names are equal when the first and last names match, ignoring middle names, case and "
    L"punctuation; other name differences are graded minor or major. Precincts written with "
    L"a prefix or leading zeros, such as \"P 267\" and \"267\", are equal.\r\n\r\n"
    L"The Compare Summary reports, for each file: Only here (Voter ID not in the other "
    L"file, or blank), Voter ID repeated (rows pair one to one, so a Voter ID on more rows "
    L"here than in the other file leaves extra rows), Name (minor / major), Precinct "
    L"changed, Voting Method changed and Date Voted changed (two rosters), and Identical. "
    L"Double-click a row, or right-click it, to show those records in either window; Filter "
    L"→ Reset View clears the highlight. Show Differences… lists every changed field side "
    L"by side (right-click it to Copy, Export Selected or Export All). Export Summary… saves "
    L"the category counts, and Export All Records… saves every record of both files with "
    L"its result and changed fields, as UTF-8 CSV or TSV.";

static const wchar_t k_RosterHelpExport[] =
    L"File → Export Voter Roster writes the visible rows (all rows, or the filtered or "
    L"duplicates view) to a UTF-8 CSV or TSV file in the order shown; right-click → Export "
    L"Selected writes only the selected rows. The suggested name ends \"-Roster\", "
    L"\"-Filtered_Roster\" or \"-Selected_Roster\".\r\n\r\n"
    L"The file holds the roster's own columns — VUID, Precinct, Last Name, First Name, "
    L"Middle Name and Name Suffix when present, Voting Method, Date Voted, Party, the extra "
    L"columns and Source File — so Load Voter Roster reads it back into an identical "
    L"roster (Compare the two to check).\r\n\r\n"
    L"In the Voting Totals and Precinct reports, right-click → Export Selected or Export "
    L"All saves the report; the Compare Summary and Differences windows export the "
    L"comparison. Files are written with a byte-order mark so Excel opens them as UTF-8.";

static const wchar_t k_HelpExport[] =
    L"Export writes rows to a UTF-8 file — CSV by default, or TSV — with a header "
    L"row of column titles. Rows are exported in the order and sort currently shown, "
    L"and the suggested file name is the loaded list's name plus a description of "
    L"what was exported.\r\n\r\n"
    L"•  File → Export Voter List — every currently visible row (all rows, or the "
    L"filtered/duplicates view if one is active). The name ends \"-Filtered_Voters\" "
    L"or \"-All_Voters\".\r\n\r\n"
    L"•  Right-click → Export Selected — only the selected rows "
    L"(\"-Selected_Voters\").\r\n\r\n"
    L"Both ask whether to Include normalized data fields (Voter ID, Precinct, Name, "
    L"Address); it is off by default, so only the original source columns are "
    L"written.\r\n\r\n"
    L"•  In a Precinct or Address report, right-click → Export Selected or Export "
    L"All to save that summary (\"-Selected_Precincts\"/\"-All_Precincts\", or the "
    L"Addresses equivalents).\r\n\r\n"
    L"•  In the Compare Summary, Export Summary… and Export All Records… save the "
    L"comparison (\"<A>-vs-<B>-Compare_Summary\", \"-Compare_Records\"); in the "
    L"Differences window, right-click → Export Selected or Export All "
    L"(\"-Differences\").\r\n\r\n"
    L"Choose CSV or TSV in the Save dialog's file-type list. Files are written with a "
    L"byte-order mark so Excel opens them as UTF-8; fields containing a comma, tab, "
    L"quote, or line break are quoted automatically.";

/* CVR-window help topics (parallel to the voter-list topics above). */
static const wchar_t k_CvrHelpLoading[] =
    L"Load Cast Vote Records (File menu) opens one or more CVR files from a single voting "
    L"system; files from different vendors can't be combined in one load. Large files "
    L"load in the background with a progress bar and can be canceled.\r\n\r\n"
    L"•  ES&S — Excel (.xlsx), CSV, TSV or tab-delimited text exports. Several files from "
    L"one export (for example early voting, Election Day and mail ballots) are combined "
    L"when they share the same columns; a file with different columns is rejected.\r\n\r\n"
    L"•  Hart InterCivic — the CVR .zip (one file per ballot sheet), the PDF \"CVR "
    L"Report\", or both. Loading a ZIP together with its PDF report keeps the votes from "
    L"the ZIP and adds each ballot's Voting Type, Polling Place, Device Type, Device "
    L"Serial and Device Data Id from the report, matched by Cvr Id. Reports a county has "
    L"redacted load too.\r\n\r\n"
    L"•  The older Hart Voting System (HVS 6.x, used by some Texas counties through 2024) "
    L"prints a \"Votes by Precinct\" report instead: one ballot per page with its precinct, "
    L"party and voting type (from the report title, e.g. \"Early Voting\"). It has no ballot "
    L"id, so each ballot is named by its file and page (Report Page) and the report loads on "
    L"its own, not with a ZIP. \"No Selection\" is an undervote; a write-in counts as "
    L"Write-in (the written name is not kept). Some printings cut long contest titles short "
    L"(\"JUSTICE, 3RD COURT OF APPEALS DIST\" for four Places); they are completed from the "
    L"full titles in ballot order, and the summary says how many.\r\n\r\n"
    L"•  Scanned Hart PDFs (page images with no text) are read with the OCR built into "
    L"Windows, which needs an OCR language such as English installed (Settings → Time & "
    L"language → Language & region). OCR takes roughly 0.15 seconds per page, and the "
    L"progress shows the page being read. An OCR Status column grades each record: OK; "
    L"Corrected (a misread value was repaired); Review (something could not be confirmed "
    L"— check it against the PDF); or No Votes Read (no readable contest rows, such as "
    L"votes the county redacted). A summary appears when the window opens.\r\n\r\n"
    L"•  Dominion (Liberty Vote) — the Democracy Suite CVR export .zip. A ranked-choice "
    L"contest gets one column per rank, \"<contest> (Rank 1)\", \"(Rank 2)\", …; see "
    L"Reports for the instant-runoff tabulation. Values the county redacted show as "
    L"\"<Redacted>\".\r\n\r\n"
    L"In ES&S and Hart primaries each party's contests are separate columns, prefixed "
    L"\"REP\" or \"DEM\". Each row is one ballot record; when ballots span several cards "
    L"or sheets, each card is its own record and the status bar gives the ballots and the "
    L"sheets.\r\n\r\n"
    L"To check a tabulation against the county's published results, load them with File → "
    L"Load Election Results… and use the Compare menu (see Help → Compare in the results "
    L"window). To check the ballots against the ballots the county's ES&S Electionware audit "
    L"log shows loaded into the EMS, load the logs with File → Load Audit Logs… and use "
    L"Compare → Check Ballot Counts with… (see Help → Compare in the audit log window).";

static const wchar_t k_CvrHelpOptions[] =
    L"Options (Edit → Options) control how the Cast Vote Records are tabulated. The "
    L"setting is saved and applies to future windows.\r\n\r\n"
    L"•  Merge image and text write-ins — a contest can record write-ins several "
    L"ways: a scanned write-in image (shown as \"[write-in]\"), the literal text "
    L"\"Write-in\", and the placeholder \"No image found\". When this is on (the "
    L"default), all of these are combined into a single \"write-in\" tally row, "
    L"matching how official results report one Write-in total; when off, each variant "
    L"is counted on its own row.\r\n\r\n"
    L"•  Party to display first for tabulation — for a primary, contests are split by "
    L"party (their names are prefixed \"REP\" or \"DEM\"). This chooses which party's "
    L"contests are listed first in a tabulation report; the other party follows, then "
    L"any non-partisan contests. Republican is the default. It affects only the order "
    L"of the tabulation report, not the CVR window's columns or any tally count.";

static const wchar_t k_CvrHelpFilters[] =
    L"Filters (Filter menu, or Ctrl+L) narrow the visible ballot records. Each rule "
    L"targets one CVR column and tests it with \"is\" or \"is not\", and is either an "
    L"Include or an Exclude.\r\n\r\n"
    L"The value is chosen from the list of selections that actually appear in that "
    L"column. For the numeric Cast Vote Record column you can type a record number "
    L"directly, because the suggestion list is capped for very large CVRs.\r\n\r\n"
    L"How multiple rules combine:\r\n\r\n"
    L"•  Exclude wins — a record matching any enabled Exclude rule is hidden.\r\n\r\n"
    L"•  Includes on the SAME column are OR'd; Includes on DIFFERENT columns are "
    L"AND'd.\r\n\r\n"
    L"•  With no Include rules, every record is shown except those removed by "
    L"Exclude rules.\r\n\r\n"
    L"Right-click a cell (or a report row) and choose Include/Exclude to add a rule "
    L"for that value. The status bar shows how many records are shown; Reset Filter "
    L"clears all rules. Filtering does not change Tabulate All CVR Votes or the "
    L"per-column reports; Tabulate Filtered CVR Votes and Tabulate Filtered "
    L"Ranked-Choice Contests count only the records the filter shows.\r\n\r\n"
    L"Export Filter… (in the Filter dialog) saves the listed rules to a file — UTF-8 CSV (\"….filter.csv\") or tab-delimited (\"….filter.tsv\"), which open in Excel. "
    L"Load Filter… replaces the listed rules with a saved filter; they take effect on Apply or "
    L"OK. Columns are matched by name, so a filter saved for one file works on another with the "
    L"same column names in any order; rules for columns this window does not have are listed and "
    L"left out. Only \"is\" and \"is not\" rules load into a CVR window.";

static const wchar_t k_CvrHelpReports[] =
    L"Reports (Reports menu) summarize the loaded Cast Vote Records.\r\n\r\n"
    L"•  Tabulate All CVR Votes — counts every contest over all ballot records: one "
    L"row per selection (candidate, Yes/No, and then write-in, overvote, and "
    L"undervote), with the contest name repeated on each of its rows. \"Vote for N\" "
    L"contests are summed across all of their columns. For a primary, contests are "
    L"split by party and listed party-by-party (see Options → Party to display "
    L"first).\r\n\r\n"
    L"•  Tabulate Filtered CVR Votes — the same tabulation, but counting only the "
    L"records currently shown by the active filter. It is unavailable (greyed) when no "
    L"filter is applied, since that would duplicate Tabulate All.\r\n\r\n"
    L"•  Tabulate Ranked-Choice Contests — for a ranked-choice contest (one column per "
    L"rank, \"<contest> (Rank 1)\", \"(Rank 2)\", …, as in Dominion exports), runs the "
    L"instant runoff round by round: each round a ballot counts for its highest-ranked "
    L"candidate still in the race; a skipped ranking is passed over; reaching an "
    L"overvoted ranking stops the ballot (Overvotes); unresolved write-ins are left out; "
    L"and the candidate with the fewest votes is eliminated until two remain. Each "
    L"contest lists its candidates (Winner first, with the round in which a majority "
    L"was reached, then by elimination round) and the Continuing, Blanks, Exhausted, "
    L"Overvotes and Non Transferable totals per round. These are the options used by "
    L"San Francisco's official Dominion RCV reports. A tie for last place is broken by "
    L"the earlier round and marked \"(tie)\" — officials resolve real ties by lot. "
    L"Tabulate Filtered Ranked-Choice Contests counts only the filtered records. In the "
    L"ordinary tabulation each rank is its own contest, so \"(Rank 1)\" shows the "
    L"first choices.\r\n\r\n"
    L"•  Display Batch / Precinct / Ballot Style Report — one row per distinct value "
    L"in that column with the number of ballot records carrying it. The Batch report "
    L"uses the \"Batch\" column (ES&S, Dominion) or the \"Batch Number\" column "
    L"(Hart); for Dominion the Precinct and Ballot Style reports use the \"Precinct "
    L"Portion\" and \"Ballot Type\" columns.\r\n\r\n"
    L"•  Display Polling Place / Device Serial / Voting Type Report — the same, for the "
    L"columns a Hart PDF CVR Report adds (load the PDF alone, or with its ZIP): where "
    L"ballots were cast, which scanner counted them, and Election Day / Early Voting / "
    L"Absentee. For Dominion, Polling Place is the tabulator's voting location and "
    L"Voting Type is the \"Counting Group\" (Election Day, Vote by Mail, …).\r\n\r\n"
    L"A report is unavailable (greyed) when the CVR has no such column or its data is "
    L"blank or redacted.\r\n\r\n"
    L"Each report opens in its own window; empty values group into a \"(blank)\" row. "
    L"These per-column reports count ALL ballot records and ignore any active "
    L"filter. Click a header to sort; right-click a row to Copy it, to "
    L"Include or Exclude that value in the CVR window's filter, or to Export the "
    L"selection.";

static const wchar_t k_CvrHelpExport[] =
    L"Export writes ballot records to a UTF-8 file — CSV by default, or TSV — with a "
    L"header row of column titles, in the order and sort currently shown. The "
    L"suggested file name is the first CVR file's name plus a description of what was "
    L"exported.\r\n\r\n"
    L"•  File → Export Cast Vote Records — every currently visible record (all "
    L"records, or the filtered view if a filter is active). The name ends "
    L"\"-Filtered_Records\" or \"-All_Records\".\r\n\r\n"
    L"•  Right-click → Export Selected — only the selected records "
    L"(\"-Selected_Records\").\r\n\r\n"
    L"•  In a report window, right-click → Export Selected or Export All to save that "
    L"summary (for example \"-All_Contests\", \"-Selected_Batches\", "
    L"\"-All_Ballot_Styles\").\r\n\r\n"
    L"Choose CSV or TSV in the Save dialog's file-type list. Files are written with a "
    L"byte-order mark so Excel opens them as UTF-8; fields containing a comma, tab, "
    L"quote, or line break are quoted automatically. An ES&S write-in that was a "
    L"scanned image is written as \"[write-in]\", which is still counted as a write-in "
    L"when the exported file is loaded again.";

static const wchar_t k_RepoUrl[] = L"https://github.com/WheelGroupTech/ElectionInfo";
static const wchar_t k_PrivacyUrl[] =
    L"https://wheelgrouptech.github.io/ElectionInfo/Applications/ElectionExplorer/PRIVACY";

typedef struct HelpDlgData
{
    AppState *app;
    const wchar_t *body;
} HelpDlgData;

typedef struct AboutDlgData
{
    AppState *app;
    HICON icon;
} AboutDlgData;

/* Read the running module's file version ("major.minor.patch.build"). */
static BOOL App_GetVersionString(wchar_t *out, size_t cch)
{
    wchar_t path[MAX_PATH];
    DWORD handle = 0;
    DWORD size;
    void *data;
    VS_FIXEDFILEINFO *ffi = NULL;
    UINT ffi_len = 0;
    BOOL ok = FALSE;

    if (GetModuleFileNameW(NULL, path, ARRAYSIZE(path)) == 0)
    {
        return FALSE;
    }
    size = GetFileVersionInfoSizeW(path, &handle);
    if (size == 0)
    {
        return FALSE;
    }
    data = malloc(size);
    if (data == NULL)
    {
        return FALSE;
    }
    if (GetFileVersionInfoW(path, 0, size, data) &&
        VerQueryValueW(data, L"\\", (void **)&ffi, &ffi_len) && ffi != NULL)
    {
        StringCchPrintfW(out,
                         cch,
                         L"%u.%u.%u.%u",
                         HIWORD(ffi->dwFileVersionMS),
                         LOWORD(ffi->dwFileVersionMS),
                         HIWORD(ffi->dwFileVersionLS),
                         LOWORD(ffi->dwFileVersionLS));
        ok = TRUE;
    }
    free(data);
    return ok;
}

/* Build a control-less modal dialog template (controls are created in
 * WM_INITDIALOG) with @p caption and run it modally, owned by the main window. */
/* Run an app modal dialog owned by (and centered over) @p owner. */
static INT_PTR App_RunModalDialogOwned(AppState *app,
                                       HWND owner,
                                       const wchar_t *caption,
                                       DLGPROC proc,
                                       LPARAM param)
{
    DWORD buf[128]; /* DWORD-aligned as DLGTEMPLATE requires */
    DLGTEMPLATE *dt = (DLGTEMPLATE *)buf;
    BYTE *p;
    size_t clen = (caption != NULL) ? wcslen(caption) : 0;

    ZeroMemory(buf, sizeof(buf));
    dt->style = WS_POPUP | WS_CAPTION | WS_SYSMENU | DS_MODALFRAME;
    dt->cx = 220; /* dialog units; the real pixel size is set in WM_INITDIALOG */
    dt->cy = 160;
    p = (BYTE *)buf + sizeof(DLGTEMPLATE);
    *(WORD *)p = 0; /* no menu */
    p += sizeof(WORD);
    *(WORD *)p = 0; /* default dialog class */
    p += sizeof(WORD);
    if (clen > 100)
    {
        clen = 100;
    }
    if (clen > 0)
    {
        memcpy(p, caption, clen * sizeof(WCHAR));
    }
    ((WCHAR *)p)[clen] = L'\0';
    return DialogBoxIndirectParamW(app->instance,
                                   dt,
                                   (owner != NULL) ? owner : app->hwnd_main,
                                   proc,
                                   param);
}

static INT_PTR App_RunModalDialog(AppState *app, const wchar_t *caption, DLGPROC proc, LPARAM param)
{
    return App_RunModalDialogOwned(app, app->hwnd_main, caption, proc, param);
}

/* Center a modal dialog of the given client size over the main window and return
 * the resulting client rectangle. */
static void App_CenterModalClient(HWND dlg, AppState *app, int client_w, int client_h)
{
    RECT rc;
    RECT owner;
    HWND owner_hwnd;
    int outer_w;
    int outer_h;
    int x = CW_USEDEFAULT;
    int y = CW_USEDEFAULT;

    rc.left = 0;
    rc.top = 0;
    rc.right = client_w;
    rc.bottom = client_h;
    AdjustWindowRectEx(&rc,
                       (DWORD)GetWindowLongPtrW(dlg, GWL_STYLE),
                       FALSE,
                       (DWORD)GetWindowLongPtrW(dlg, GWL_EXSTYLE));
    outer_w = rc.right - rc.left;
    outer_h = rc.bottom - rc.top;
    /* Center over the window that owns this dialog (the window the user acted in) —
     * that is the voter window for voter-launched dialogs and the CVR window for
     * CVR-launched ones. Fall back to the app's main window. */
    owner_hwnd = GetWindow(dlg, GW_OWNER);
    if (owner_hwnd == NULL)
    {
        owner_hwnd = app->hwnd_main;
    }
    if (owner_hwnd != NULL && GetWindowRect(owner_hwnd, &owner))
    {
        x = owner.left + ((owner.right - owner.left) - outer_w) / 2;
        y = owner.top + ((owner.bottom - owner.top) - outer_h) / 2;
    }
    SetWindowPos(dlg, NULL, x, y, outer_w, outer_h, SWP_NOZORDER | SWP_NOACTIVATE);
}

typedef struct SheetPickData
{
    AppState *app;
    const wchar_t (*names)[EE_XLSX_SHEET_NAME_CCH];
    const wchar_t *const *items; /* used instead of names when set (App_PickItem) */
    const wchar_t *prompt;       /* NULL = the worksheet prompt */
    const wchar_t *ok_label;     /* NULL = "Load" */
    int count;
    int selected;
} SheetPickData;

static INT_PTR CALLBACK SheetPickerDlgProc(HWND dlg, UINT msg, WPARAM wParam, LPARAM lParam)
{
    SheetPickData *d = (SheetPickData *)GetWindowLongPtrW(dlg, GWLP_USERDATA);

    switch (msg)
    {
        case WM_INITDIALOG:
        {
            AppState *app;
            RECT rc;
            int margin;
            int btn_w;
            int btn_h;
            int gap;
            int list_top;
            int i;
            HWND label;
            HWND list;
            HWND ok;
            HWND cancel;

            d = (SheetPickData *)lParam;
            SetWindowLongPtrW(dlg, GWLP_USERDATA, (LONG_PTR)d);
            app = d->app;

            App_CenterModalClient(dlg, app, Scale(app, 320), Scale(app, 250));
            GetClientRect(dlg, &rc);
            margin = Scale(app, 14);
            btn_w = Scale(app, 84);
            btn_h = Scale(app, 26);
            gap = Scale(app, 8);
            list_top = margin + Scale(app, 36);

            label = CreateWindowExW(0,
                                    L"STATIC",
                                    (d->prompt != NULL)
                                        ? d->prompt
                                        : L"This workbook has multiple sheets. Choose one to load:",
                                    WS_CHILD | WS_VISIBLE | SS_LEFT,
                                    margin,
                                    margin,
                                    rc.right - 2 * margin,
                                    Scale(app, 32),
                                    dlg,
                                    (HMENU)(INT_PTR)IDC_SHEET_LABEL,
                                    app->instance,
                                    NULL);
            list = CreateWindowExW(WS_EX_CLIENTEDGE,
                                   L"LISTBOX",
                                   L"",
                                   WS_CHILD | WS_VISIBLE | WS_TABSTOP | WS_VSCROLL | LBS_NOTIFY |
                                       LBS_HASSTRINGS,
                                   margin,
                                   list_top,
                                   rc.right - 2 * margin,
                                   rc.bottom - list_top - btn_h - 2 * margin,
                                   dlg,
                                   (HMENU)(INT_PTR)IDC_SHEET_LIST,
                                   app->instance,
                                   NULL);
            for (i = 0; i < d->count; i++)
            {
                SendMessageW(list, LB_ADDSTRING, 0,
                             (LPARAM)((d->items != NULL) ? d->items[i] : d->names[i]));
            }
            SendMessageW(list, LB_SETCURSEL, 0, 0);

            ok = CreateWindowExW(0,
                                 L"BUTTON",
                                 (d->ok_label != NULL) ? d->ok_label : L"Load",
                                 WS_CHILD | WS_VISIBLE | WS_TABSTOP | BS_DEFPUSHBUTTON,
                                 rc.right - margin - 2 * btn_w - gap,
                                 rc.bottom - margin - btn_h,
                                 btn_w,
                                 btn_h,
                                 dlg,
                                 (HMENU)(INT_PTR)IDOK,
                                 app->instance,
                                 NULL);
            cancel = CreateWindowExW(0,
                                     L"BUTTON",
                                     L"Cancel",
                                     WS_CHILD | WS_VISIBLE | WS_TABSTOP,
                                     rc.right - margin - btn_w,
                                     rc.bottom - margin - btn_h,
                                     btn_w,
                                     btn_h,
                                     dlg,
                                     (HMENU)(INT_PTR)IDCANCEL,
                                     app->instance,
                                     NULL);
            if (app->font_ui != NULL)
            {
                SendMessageW(label, WM_SETFONT, (WPARAM)app->font_ui, TRUE);
                SendMessageW(list, WM_SETFONT, (WPARAM)app->font_ui, TRUE);
                SendMessageW(ok, WM_SETFONT, (WPARAM)app->font_ui, TRUE);
                SendMessageW(cancel, WM_SETFONT, (WPARAM)app->font_ui, TRUE);
            }
            SetFocus(list);
            return (INT_PTR)FALSE;
        }

        case WM_COMMAND:
        {
            WORD id = LOWORD(wParam);
            if (id == IDOK || (id == IDC_SHEET_LIST && HIWORD(wParam) == LBN_DBLCLK))
            {
                HWND list = GetDlgItem(dlg, IDC_SHEET_LIST);
                int sel = (int)SendMessageW(list, LB_GETCURSEL, 0, 0);
                if (d != NULL)
                {
                    d->selected = (sel == LB_ERR) ? 0 : sel;
                }
                EndDialog(dlg, 1);
                return (INT_PTR)TRUE;
            }
            if (id == IDCANCEL)
            {
                EndDialog(dlg, 0);
                return (INT_PTR)TRUE;
            }
            break;
        }

        case WM_CLOSE:
            EndDialog(dlg, 0);
            return (INT_PTR)TRUE;

        default:
            break;
    }
    return (INT_PTR)FALSE;
}

/* Show the worksheet picker; returns the chosen 0-based index, or -1 if the user
 * cancelled. */
static int App_PickSheet(AppState *app, const wchar_t (*names)[EE_XLSX_SHEET_NAME_CCH], int count)
{
    SheetPickData d;
    ZeroMemory(&d, sizeof(d));
    d.app = app;
    d.names = names;
    d.count = count;
    d.selected = 0;
    if (App_RunModalDialog(app, L"Select Worksheet", SheetPickerDlgProc, (LPARAM)&d) == 1)
    {
        return d.selected;
    }
    return -1;
}

/* The same picker for any list of names; returns the chosen index, or -1. */
static int App_PickItem(AppState *app,
                        const wchar_t *caption,
                        const wchar_t *prompt,
                        const wchar_t *ok_label,
                        const wchar_t *const *items,
                        int count)
{
    SheetPickData d;
    ZeroMemory(&d, sizeof(d));
    d.app = app;
    d.items = items;
    d.prompt = prompt;
    d.ok_label = ok_label;
    d.count = count;
    if (App_RunModalDialog(app, caption, SheetPickerDlgProc, (LPARAM)&d) == 1)
    {
        return d.selected;
    }
    return -1;
}

static INT_PTR CALLBACK HelpTextDlgProc(HWND dlg, UINT msg, WPARAM wParam, LPARAM lParam)
{
    HelpDlgData *d = (HelpDlgData *)GetWindowLongPtrW(dlg, GWLP_USERDATA);

    switch (msg)
    {
        case WM_INITDIALOG:
        {
            AppState *app;
            RECT rc;
            int margin;
            int btn_w;
            int btn_h;
            int gap;
            HWND edit;
            HWND ok;

            d = (HelpDlgData *)lParam;
            SetWindowLongPtrW(dlg, GWLP_USERDATA, (LONG_PTR)d);
            app = d->app;

            App_CenterModalClient(dlg, app, Scale(app, 470), Scale(app, 360));
            GetClientRect(dlg, &rc);
            margin = Scale(app, 12);
            btn_w = Scale(app, 90);
            btn_h = Scale(app, 26);
            gap = Scale(app, 10);

            edit = CreateWindowExW(WS_EX_CLIENTEDGE,
                                   L"EDIT",
                                   L"",
                                   WS_CHILD | WS_VISIBLE | WS_TABSTOP | WS_VSCROLL | ES_MULTILINE |
                                       ES_READONLY | ES_AUTOVSCROLL,
                                   margin,
                                   margin,
                                   rc.right - 2 * margin,
                                   rc.bottom - 2 * margin - btn_h - gap,
                                   dlg,
                                   (HMENU)(INT_PTR)IDC_HELP_TEXT,
                                   app->instance,
                                   NULL);
            ok = CreateWindowExW(0,
                                 L"BUTTON",
                                 L"OK",
                                 WS_CHILD | WS_VISIBLE | WS_TABSTOP | BS_DEFPUSHBUTTON,
                                 rc.right - margin - btn_w,
                                 rc.bottom - margin - btn_h,
                                 btn_w,
                                 btn_h,
                                 dlg,
                                 (HMENU)(INT_PTR)IDOK,
                                 app->instance,
                                 NULL);
            if (app->font_ui != NULL)
            {
                if (edit != NULL)
                {
                    SendMessageW(edit, WM_SETFONT, (WPARAM)app->font_ui, TRUE);
                }
                if (ok != NULL)
                {
                    SendMessageW(ok, WM_SETFONT, (WPARAM)app->font_ui, TRUE);
                }
            }
            if (edit != NULL)
            {
                SetWindowTextW(edit, d->body);
                /* Keep the caret at the top so long topics start at the beginning. */
                SendMessageW(edit, EM_SETSEL, 0, 0);
            }
            if (ok != NULL)
            {
                SetFocus(ok);
            }
            return (INT_PTR)FALSE; /* focus set explicitly */
        }

        case WM_COMMAND:
            if (LOWORD(wParam) == IDOK || LOWORD(wParam) == IDCANCEL)
            {
                EndDialog(dlg, 0);
                return (INT_PTR)TRUE;
            }
            break;

        case WM_CLOSE:
            EndDialog(dlg, 0);
            return (INT_PTR)TRUE;

        default:
            break;
    }
    return (INT_PTR)FALSE;
}

static INT_PTR CALLBACK AboutDlgProc(HWND dlg, UINT msg, WPARAM wParam, LPARAM lParam)
{
    AboutDlgData *d = (AboutDlgData *)GetWindowLongPtrW(dlg, GWLP_USERDATA);

    switch (msg)
    {
        case WM_INITDIALOG:
        {
            AppState *app;
            RECT rc;
            int margin;
            int icon_sz;
            int text_x;
            int btn_w;
            int btn_h;
            int y;
            int row_bottom;
            wchar_t ver[64];
            wchar_t info[96];
            HWND ico;
            HWND text;
            HWND tagline;
            HWND oss_label;
            HWND link;
            HWND privacy_label;
            HWND privacy_link;
            HWND ok;
            wchar_t link_markup[256];
            wchar_t privacy_markup[320];

            d = (AboutDlgData *)lParam;
            SetWindowLongPtrW(dlg, GWLP_USERDATA, (LONG_PTR)d);
            app = d->app;

            App_CenterModalClient(dlg, app, Scale(app, 380), Scale(app, 320));
            GetClientRect(dlg, &rc);
            margin = Scale(app, 16);
            icon_sz = Scale(app, 64);
            text_x = margin + icon_sz + Scale(app, 16);
            btn_w = Scale(app, 90);
            btn_h = Scale(app, 26);

            d->icon = (HICON)LoadImageW(app->instance,
                                        MAKEINTRESOURCEW(IDI_APPICON),
                                        IMAGE_ICON,
                                        icon_sz,
                                        icon_sz,
                                        LR_DEFAULTCOLOR);
            ico = CreateWindowExW(0,
                                  L"STATIC",
                                  L"",
                                  WS_CHILD | WS_VISIBLE | SS_ICON | SS_REALSIZECONTROL,
                                  margin,
                                  margin,
                                  icon_sz,
                                  icon_sz,
                                  dlg,
                                  (HMENU)(INT_PTR)IDC_ABOUT_ICON,
                                  app->instance,
                                  NULL);
            if (ico != NULL && d->icon != NULL)
            {
                SendMessageW(ico, STM_SETICON, (WPARAM)d->icon, 0);
            }

            if (!App_GetVersionString(ver, ARRAYSIZE(ver)))
            {
                StringCchCopyW(ver, ARRAYSIZE(ver), L"(unknown)");
            }
            StringCchPrintfW(info, ARRAYSIZE(info), L"Election Explorer\r\nVersion %s", ver);
            text = CreateWindowExW(0,
                                   L"STATIC",
                                   info,
                                   WS_CHILD | WS_VISIBLE | SS_LEFT,
                                   text_x,
                                   margin,
                                   rc.right - text_x - margin,
                                   icon_sz,
                                   dlg,
                                   (HMENU)(INT_PTR)IDC_ABOUT_TEXT,
                                   app->instance,
                                   NULL);

            /* Full-width tagline below the icon row so it can wrap. */
            row_bottom = margin + icon_sz + Scale(app, 12);
            tagline =
                CreateWindowExW(0,
                                L"STATIC",
                                L"A powerful tool to view, compare, and analyze election data.",
                                WS_CHILD | WS_VISIBLE | SS_LEFT,
                                margin,
                                row_bottom,
                                rc.right - 2 * margin,
                                Scale(app, 40),
                                dlg,
                                (HMENU)(INT_PTR)-1,
                                app->instance,
                                NULL);

            y = row_bottom + Scale(app, 46);
            oss_label = CreateWindowExW(0,
                                        L"STATIC",
                                        L"Election Explorer is Open Source:",
                                        WS_CHILD | WS_VISIBLE | SS_LEFT,
                                        margin,
                                        y,
                                        rc.right - 2 * margin,
                                        Scale(app, 20),
                                        dlg,
                                        (HMENU)(INT_PTR)-1,
                                        app->instance,
                                        NULL);

            y += Scale(app, 22);
            StringCchPrintfW(link_markup,
                             ARRAYSIZE(link_markup),
                             L"<a href=\"%s\">%s</a>",
                             k_RepoUrl,
                             k_RepoUrl);
            link = CreateWindowExW(0,
                                   L"SysLink",
                                   link_markup,
                                   WS_CHILD | WS_VISIBLE | WS_TABSTOP,
                                   margin,
                                   y,
                                   rc.right - 2 * margin,
                                   Scale(app, 20),
                                   dlg,
                                   (HMENU)(INT_PTR)IDC_ABOUT_LINK,
                                   app->instance,
                                   NULL);

            y += Scale(app, 30);
            privacy_label = CreateWindowExW(0,
                                            L"STATIC",
                                            L"Privacy Policy:",
                                            WS_CHILD | WS_VISIBLE | SS_LEFT,
                                            margin,
                                            y,
                                            rc.right - 2 * margin,
                                            Scale(app, 20),
                                            dlg,
                                            (HMENU)(INT_PTR)-1,
                                            app->instance,
                                            NULL);

            y += Scale(app, 22);
            StringCchPrintfW(privacy_markup,
                             ARRAYSIZE(privacy_markup),
                             L"<a href=\"%s\">%s</a>",
                             k_PrivacyUrl,
                             k_PrivacyUrl);
            /* Taller than one line: this URL wraps to two lines at dialog width. */
            privacy_link = CreateWindowExW(0,
                                           L"SysLink",
                                           privacy_markup,
                                           WS_CHILD | WS_VISIBLE | WS_TABSTOP,
                                           margin,
                                           y,
                                           rc.right - 2 * margin,
                                           Scale(app, 36),
                                           dlg,
                                           (HMENU)(INT_PTR)IDC_ABOUT_PRIVACY,
                                           app->instance,
                                           NULL);

            ok = CreateWindowExW(0,
                                 L"BUTTON",
                                 L"OK",
                                 WS_CHILD | WS_VISIBLE | WS_TABSTOP | BS_DEFPUSHBUTTON,
                                 rc.right - margin - btn_w,
                                 rc.bottom - margin - btn_h,
                                 btn_w,
                                 btn_h,
                                 dlg,
                                 (HMENU)(INT_PTR)IDOK,
                                 app->instance,
                                 NULL);
            if (app->font_ui != NULL)
            {
                if (text != NULL)
                {
                    SendMessageW(text, WM_SETFONT, (WPARAM)app->font_ui, TRUE);
                }
                if (tagline != NULL)
                {
                    SendMessageW(tagline, WM_SETFONT, (WPARAM)app->font_ui, TRUE);
                }
                if (oss_label != NULL)
                {
                    SendMessageW(oss_label, WM_SETFONT, (WPARAM)app->font_ui, TRUE);
                }
                if (link != NULL)
                {
                    SendMessageW(link, WM_SETFONT, (WPARAM)app->font_ui, TRUE);
                }
                if (privacy_label != NULL)
                {
                    SendMessageW(privacy_label, WM_SETFONT, (WPARAM)app->font_ui, TRUE);
                }
                if (privacy_link != NULL)
                {
                    SendMessageW(privacy_link, WM_SETFONT, (WPARAM)app->font_ui, TRUE);
                }
                if (ok != NULL)
                {
                    SendMessageW(ok, WM_SETFONT, (WPARAM)app->font_ui, TRUE);
                }
            }
            if (ok != NULL)
            {
                SetFocus(ok);
            }
            return (INT_PTR)FALSE;
        }

        case WM_NOTIFY:
        {
            NMHDR *hdr = (NMHDR *)lParam;
            if (hdr != NULL &&
                (hdr->idFrom == IDC_ABOUT_LINK || hdr->idFrom == IDC_ABOUT_PRIVACY) &&
                (hdr->code == NM_CLICK || hdr->code == NM_RETURN))
            {
                NMLINK *nml = (NMLINK *)lParam;
                const wchar_t *fallback =
                    (hdr->idFrom == IDC_ABOUT_PRIVACY) ? k_PrivacyUrl : k_RepoUrl;
                const wchar_t *url = (nml->item.szUrl[0] != L'\0') ? nml->item.szUrl : fallback;
                ShellExecuteW(dlg, L"open", url, NULL, NULL, SW_SHOWNORMAL);
                return (INT_PTR)TRUE;
            }
            break;
        }

        case WM_COMMAND:
            if (LOWORD(wParam) == IDOK || LOWORD(wParam) == IDCANCEL)
            {
                EndDialog(dlg, 0);
                return (INT_PTR)TRUE;
            }
            break;

        case WM_CLOSE:
            EndDialog(dlg, 0);
            return (INT_PTR)TRUE;

        case WM_DESTROY:
            if (d != NULL && d->icon != NULL)
            {
                DestroyIcon(d->icon);
                d->icon = NULL;
            }
            return (INT_PTR)FALSE;

        default:
            break;
    }
    return (INT_PTR)FALSE;
}

/* Help topic modal owned by (and centered over) @p owner; NULL uses the main window. */
static void App_ShowHelpTopicOn(AppState *app, HWND owner, const wchar_t *caption,
                                const wchar_t *body)
{
    HelpDlgData d;
    if (app == NULL)
    {
        return;
    }
    d.app = app;
    d.body = body;
    App_RunModalDialogOwned(app, owner, caption, HelpTextDlgProc, (LPARAM)&d);
}

static void App_ShowHelpTopic(AppState *app, const wchar_t *caption, const wchar_t *body)
{
    App_ShowHelpTopicOn(app, (app != NULL) ? app->hwnd_main : NULL, caption, body);
}

static void App_ShowAboutOn(AppState *app, HWND owner)
{
    AboutDlgData d;
    if (app == NULL)
    {
        return;
    }
    d.app = app;
    d.icon = NULL;
    App_RunModalDialogOwned(app, owner, L"About Election Explorer", AboutDlgProc, (LPARAM)&d);
}

static void App_ShowAbout(AppState *app)
{
    App_ShowAboutOn(app, (app != NULL) ? app->hwnd_main : NULL);
}

/* -------------------------------------------------------------------------- */
/* Window procedure                                                           */
/* -------------------------------------------------------------------------- */

/**
 * @brief Full-row system highlight on both panes; center Voter ID text.
 *
 * Default (and Explorer-themed) drawing paints only the focused cell in
 * COLOR_HIGHLIGHT and the rest of the row in the inactive grey. Strip the
 * selected/focus item state and apply the highlight colors ourselves so every
 * selected cell matches the user's system selection color.
 */
static LRESULT App_ListCustomDraw(AppState *app, NMLVCUSTOMDRAW *lvcd)
{
    HWND hwnd = lvcd->nmcd.hdr.hwndFrom;
    BOOL frozen = (hwnd == app->hwnd_frozen);
    BOOL selected;
    int item;

    switch (lvcd->nmcd.dwDrawStage)
    {
        case CDDS_PREPAINT:
            return CDRF_NOTIFYITEMDRAW;

        case CDDS_ITEMPREPAINT:
            item = (int)lvcd->nmcd.dwItemSpec;
            selected = (ListView_GetItemState(hwnd, item, LVIS_SELECTED) & LVIS_SELECTED) != 0;
            if (selected)
            {
                lvcd->clrText = GetSysColor(COLOR_HIGHLIGHTTEXT);
                lvcd->clrTextBk = GetSysColor(COLOR_HIGHLIGHT);
                lvcd->nmcd.uItemState &= ~(CDIS_SELECTED | CDIS_FOCUS | CDIS_HOT);
                FillRect(lvcd->nmcd.hdc, &lvcd->nmcd.rc, GetSysColorBrush(COLOR_HIGHLIGHT));
            }
            return CDRF_NEWFONT | CDRF_NOTIFYSUBITEMDRAW;

        case CDDS_ITEMPREPAINT | CDDS_SUBITEM:
            item = (int)lvcd->nmcd.dwItemSpec;
            selected = (ListView_GetItemState(hwnd, item, LVIS_SELECTED) & LVIS_SELECTED) != 0;
            if (selected)
            {
                lvcd->clrText = GetSysColor(COLOR_HIGHLIGHTTEXT);
                lvcd->clrTextBk = GetSysColor(COLOR_HIGHLIGHT);
                lvcd->nmcd.uItemState &= ~(CDIS_SELECTED | CDIS_FOCUS | CDIS_HOT);
            }
            if (frozen && lvcd->iSubItem == 0)
            {
                RECT rc;
                wchar_t text[256];
                uint32_t view_row = App_ViewRowFromDisplay(app, (uint32_t)item);
                HDC hdc = lvcd->nmcd.hdc;
                HFONT old_font;
                COLORREF old_text;
                COLORREF old_bk;
                int old_mode;
                HBRUSH brush;

                if (!ListView_GetSubItemRect(hwnd, item, 0, LVIR_BOUNDS, &rc))
                {
                    return selected ? CDRF_NEWFONT : CDRF_DODEFAULT;
                }
                {
                    RECT rc_label;
                    if (ListView_GetSubItemRect(hwnd, item, 0, LVIR_LABEL, &rc_label))
                    {
                        rc = rc_label;
                    }
                    else
                    {
                        int col0_w = ListView_GetColumnWidth(hwnd, 0);
                        rc.right = rc.left + col0_w;
                    }
                }

                if (selected)
                {
                    brush = GetSysColorBrush(COLOR_HIGHLIGHT);
                    old_text = SetTextColor(hdc, GetSysColor(COLOR_HIGHLIGHTTEXT));
                    old_bk = SetBkColor(hdc, GetSysColor(COLOR_HIGHLIGHT));
                }
                else
                {
                    brush = GetSysColorBrush(COLOR_WINDOW);
                    old_text = SetTextColor(hdc, GetSysColor(COLOR_WINDOWTEXT));
                    old_bk = SetBkColor(hdc, GetSysColor(COLOR_WINDOW));
                }
                FillRect(hdc, &rc, brush);

                {
                    HPEN pen = CreatePen(PS_SOLID, 1, RGB(200, 200, 200));
                    HPEN old = (HPEN)SelectObject(hdc, pen);
                    MoveToEx(hdc, rc.right - 1, rc.top, NULL);
                    LineTo(hdc, rc.right - 1, rc.bottom);
                    SelectObject(hdc, old);
                    DeleteObject(pen);
                }

                text[0] = L'\0';
                EeVoterTable_GetViewCellW(&app->table, view_row, 0, text, ARRAYSIZE(text));
                old_font = (HFONT)SelectObject(hdc,
                                               app->font_grid ? app->font_grid
                                               : app->font_ui ? app->font_ui
                                                              : GetStockObject(DEFAULT_GUI_FONT));
                old_mode = SetBkMode(hdc, TRANSPARENT);
                {
                    RECT rc_text = rc;
                    rc_text.left += ScaleDisplay(app, 2);
                    rc_text.right -= ScaleDisplay(app, 2);
                    DrawTextW(hdc,
                              text,
                              -1,
                              &rc_text,
                              DT_CENTER | DT_VCENTER | DT_SINGLELINE | DT_END_ELLIPSIS |
                                  DT_NOPREFIX);
                }
                SetBkMode(hdc, old_mode);
                SelectObject(hdc, old_font);
                SetTextColor(hdc, old_text);
                SetBkColor(hdc, old_bk);
                return CDRF_SKIPDEFAULT;
            }
            return selected ? CDRF_NEWFONT : CDRF_DODEFAULT;

        default:
            return CDRF_DODEFAULT;
    }
}

static LRESULT App_OnNotify(AppState *app, NMHDR *hdr)
{
    if (hdr->code == NM_CUSTOMDRAW &&
        (hdr->hwndFrom == app->hwnd_frozen || hdr->hwndFrom == app->hwnd_scroll))
    {
        return App_ListCustomDraw(app, (NMLVCUSTOMDRAW *)hdr);
    }

    if (hdr->hwndFrom != app->hwnd_frozen && hdr->hwndFrom != app->hwnd_scroll)
    {
        return 0;
    }

    if (hdr->code == LVN_GETDISPINFOW)
    {
        NMLVDISPINFOW *di = (NMLVDISPINFOW *)hdr;
        uint32_t view_row = App_ViewRowFromDisplay(app, (uint32_t)di->item.iItem);
        uint32_t column;

        if (hdr->hwndFrom == app->hwnd_frozen)
        {
            column = (uint32_t)di->item.iSubItem;
        }
        else
        {
            column = App_ScrollToTable(app, di->item.iSubItem);
        }

        if ((di->item.mask & LVIF_TEXT) && column >= app->table.column_count)
        {
            if (di->item.cchTextMax > 0)
            {
                di->item.pszText[0] = L'\0';
            }
        }
        else if (di->item.mask & LVIF_TEXT)
        {
            EeVoterTable_GetViewCellW(&app->table,
                                      view_row,
                                      column,
                                      di->item.pszText,
                                      (size_t)di->item.cchTextMax);
        }
        return 0;
    }

    if (hdr->code == LVN_ODCACHEHINT)
    {
        return 0;
    }

    if (hdr->code == LVN_COLUMNCLICK)
    {
        NMLISTVIEW *nmlv = (NMLISTVIEW *)hdr;
        App_SortFromHeader(app, hdr->hwndFrom, nmlv->iSubItem);
        return 0;
    }

    if (hdr->code == LVN_ITEMCHANGED)
    {
        NMLISTVIEW *nmlv = (NMLISTVIEW *)hdr;
        if ((nmlv->uChanged & LVIF_STATE) &&
            ((nmlv->uNewState ^ nmlv->uOldState) & (LVIS_SELECTED | LVIS_FOCUSED)))
        {
            App_RequestSelectionSync(app, hdr->hwndFrom);
        }
        return 0;
    }

    if (hdr->code == LVN_ODSTATECHANGED)
    {
        App_RequestSelectionSync(app, hdr->hwndFrom);
        return 0;
    }

    if (hdr->code == LVN_ENDSCROLL)
    {
        /* Vista+ ; also handle WM_VSCROLL via subclass if needed */
        App_SyncVerticalScroll(app, hdr->hwndFrom);
        return 0;
    }

    return 0;
}

static LRESULT CALLBACK MainWndProc(HWND hwnd, UINT msg, WPARAM wParam, LPARAM lParam)
{
    AppState *app = App_FromMain(hwnd);

    if (app == NULL && msg != WM_CREATE)
    {
        return DefWindowProcW(hwnd, msg, wParam, lParam);
    }

    switch (msg)
    {
        case WM_CREATE:
        {
            HMENU menu;
            CREATESTRUCTW *cs = (CREATESTRUCTW *)lParam;
            app = (AppState *)cs->lpCreateParams;
            if (app == NULL)
            {
                return -1;
            }
            SetWindowLongPtrW(hwnd, GWLP_USERDATA, (LONG_PTR)app);
            app->hwnd_main = hwnd;
            App_UpdateDpiMetrics(app, GetDpiForWindow(hwnd));
            menu = app->is_roster ? App_CreateRosterMenu() : App_CreateMenu();
            SetMenu(hwnd, menu);
            if (!App_CreateControls(app))
            {
                return -1;
            }
            App_Layout(app);
            return 0;
        }

        case WM_SIZE:
            App_Layout(app);
            return 0;

        case WM_PAINT:
        {
            PAINTSTRUCT ps;
            HDC hdc = BeginPaint(hwnd, &ps);
            if (hdc != NULL)
            {
                RECT split_rc;
                FillRect(hdc, &ps.rcPaint, (HBRUSH)(COLOR_WINDOW + 1));
                App_GetSplitterRect(app, &split_rc);
                if (split_rc.right > split_rc.left && split_rc.bottom > split_rc.top)
                {
                    HBRUSH brush = GetSysColorBrush(COLOR_BTNFACE);
                    HPEN pen = CreatePen(PS_SOLID, 1, RGB(160, 160, 160));
                    HPEN old_pen;
                    int mid;
                    int y;
                    int grip = Scale(app, 12);

                    FillRect(hdc, &split_rc, brush);
                    old_pen = (HPEN)SelectObject(hdc, pen);
                    mid = (split_rc.left + split_rc.right) / 2;
                    MoveToEx(hdc, mid, split_rc.top + Scale(app, 4), NULL);
                    LineTo(hdc, mid, split_rc.bottom - Scale(app, 4));
                    /* Small grip dots */
                    for (y = (split_rc.top + split_rc.bottom) / 2 - grip;
                         y <= (split_rc.top + split_rc.bottom) / 2 + grip;
                         y += Scale(app, 4))
                    {
                        SetPixel(hdc, mid - 1, y, RGB(100, 100, 100));
                        SetPixel(hdc, mid + 1, y, RGB(100, 100, 100));
                    }
                    SelectObject(hdc, old_pen);
                    DeleteObject(pen);
                }
                EndPaint(hwnd, &ps);
            }
            return 0;
        }

        case WM_SETCURSOR:
            if ((HWND)wParam == hwnd && LOWORD(lParam) == HTCLIENT)
            {
                POINT pt;
                GetCursorPos(&pt);
                ScreenToClient(hwnd, &pt);
                if (App_HitTestSplitter(app, pt.x, pt.y) || app->splitting)
                {
                    SetCursor(LoadCursorW(NULL, IDC_SIZEWE));
                    return TRUE;
                }
            }
            break;

        case WM_LBUTTONDOWN:
        {
            int x = GET_X_LPARAM(lParam);
            int y = GET_Y_LPARAM(lParam);
            if (App_HitTestSplitter(app, x, y))
            {
                app->splitting = TRUE;
                SetCapture(hwnd);
                SetCursor(LoadCursorW(NULL, IDC_SIZEWE));
                return 0;
            }
            break;
        }

        case WM_MOUSEMOVE:
            if (app->splitting)
            {
                RECT client;
                int x = GET_X_LPARAM(lParam);
                int new_w;
                GetClientRect(hwnd, &client);
                new_w = x - app->pad;
                app->frozen_width = App_ClampFrozenWidth(app, new_w, client.right - client.left);
                App_Layout(app);
                return 0;
            }
            break;

        case WM_LBUTTONUP:
            if (app->splitting)
            {
                app->splitting = FALSE;
                ReleaseCapture();
                return 0;
            }
            break;

        case WM_CAPTURECHANGED:
            app->splitting = FALSE;
            break;

        case WM_DPICHANGED:
        {
            UINT dpi = HIWORD(wParam);
            RECT *const prc = (RECT *)lParam;
            App_UpdateDpiMetrics(app, dpi);
            App_ApplyFont(app);
            SetWindowPos(hwnd,
                         NULL,
                         prc->left,
                         prc->top,
                         prc->right - prc->left,
                         prc->bottom - prc->top,
                         SWP_NOZORDER | SWP_NOACTIVATE);
            App_RebuildColumns(app);
            App_Layout(app);
            return 0;
        }

        case WM_INITMENUPOPUP:
            EnableMenuItem((HMENU)wParam,
                           IDM_EDIT_COPY,
                           MF_BYCOMMAND |
                               ((App_HasSelection(app) && !app->loading) ? MF_ENABLED : MF_GRAYED));
            EnableMenuItem((HMENU)wParam,
                           IDM_FILTER_EDIT,
                           MF_BYCOMMAND | (app->loading ? MF_GRAYED : MF_ENABLED));
            EnableMenuItem((HMENU)wParam,
                           IDM_FILTER_RESET,
                           MF_BYCOMMAND | ((app->filters.count > 0 && !app->loading) ? MF_ENABLED
                                                                                     : MF_GRAYED));
            EnableMenuItem(
                (HMENU)wParam,
                IDM_FILTER_DUP_VOTER_IDS,
                MF_BYCOMMAND |
                    ((app->table.row_count > 0 && !app->loading) ? MF_ENABLED : MF_GRAYED));
            EnableMenuItem(
                (HMENU)wParam,
                IDM_FILTER_DUP_VOTERS,
                MF_BYCOMMAND |
                    ((app->table.row_count > 0 && !app->loading) ? MF_ENABLED : MF_GRAYED));
            {
                /* Needs birth dates in this list and a loaded voter roster to check. */
                BOOL roster_open = FALSE;
                AppState *p;
                for (p = g_viewers; p != NULL && !roster_open; p = p->next)
                {
                    roster_open = p->is_roster && p->table.row_count > 0 && !p->loading;
                }
                EnableMenuItem((HMENU)wParam,
                               IDM_FILTER_DUP_VOTERS_VOTING,
                               MF_BYCOMMAND |
                                   ((roster_open && !app->is_roster && app->table.row_count > 0 &&
                                     !app->loading && EeVoterTable_FindBirthdateColumn(&app->table) >= 0)
                                        ? MF_ENABLED
                                        : MF_GRAYED));
            }
            EnableMenuItem((HMENU)wParam,
                           IDM_FILTER_RESET_VIEW,
                           MF_BYCOMMAND |
                               ((app->mark_active && !app->loading) ? MF_ENABLED : MF_GRAYED));
            EnableMenuItem(
                (HMENU)wParam,
                IDM_REPORT_PRECINCT,
                MF_BYCOMMAND |
                    ((app->table.row_count > 0 && !app->loading) ? MF_ENABLED : MF_GRAYED));
            EnableMenuItem(
                (HMENU)wParam,
                IDM_REPORT_ADDRESS,
                MF_BYCOMMAND |
                    ((app->table.row_count > 0 && !app->loading) ? MF_ENABLED : MF_GRAYED));
            EnableMenuItem(
                (HMENU)wParam,
                IDM_FILE_EXPORT_VOTERS,
                MF_BYCOMMAND |
                    ((app->table.row_count > 0 && !app->loading) ? MF_ENABLED : MF_GRAYED));
            {
                HMENU bar = GetMenu(hwnd);
                if (bar != NULL && (HMENU)wParam == GetSubMenu(bar, k_CompareMenuPos))
                {
                    App_BuildCompareMenu(app, (HMENU)wParam);
                }
            }
            return 0;

        case WM_COMMAND:
            switch (LOWORD(wParam))
            {
                case IDM_FILE_OPEN_VOTER_LIST:
                    App_BeginOpenVoterList(app);
                    return 0;
                case IDM_FILE_OPEN_CVR:
                    App_BeginOpenCvr(app);
                    return 0;
                case IDM_FILE_OPEN_ROSTER:
                    App_BeginOpenRoster(app);
                    return 0;
                case IDM_FILE_OPEN_RESULTS:
                    App_BeginOpenResults(app);
                    return 0;
                case IDM_FILE_OPEN_AUDIT:
                    App_BeginOpenAudit(app);
                    return 0;
                case IDM_REPORT_LOAD_SUMMARY:
                    if (app->roster_info.note != NULL)
                    {
                        App_ShowHelpTopic(app, L"Voter Roster Load Summary", app->roster_info.note);
                    }
                    return 0;
                case IDM_FILE_CLOSE_VOTER_LIST:
                    App_RequestClose(app);
                    return 0;
                case IDM_FILE_EXPORT_VOTERS:
                    App_ExportVoters(app, FALSE);
                    return 0;
                case IDM_FILE_EXIT:
                    App_ExitAll();
                    return 0;
                case IDM_EDIT_COPY:
                    App_CopySelection(app);
                    return 0;
                case IDM_EDIT_OPTIONS:
                    if (!App_ShowOptions(app))
                    {
                        MessageBoxW(hwnd,
                                    L"Could not open the options window.",
                                    k_WindowTitle,
                                    MB_ICONERROR | MB_OK);
                    }
                    return 0;
                case IDM_FILTER_EDIT:
                    if (!App_ShowFilter(app))
                    {
                        MessageBoxW(hwnd,
                                    L"Could not open the filter window.",
                                    k_WindowTitle,
                                    MB_ICONERROR | MB_OK);
                    }
                    return 0;
                case IDM_FILTER_RESET:
                    App_ResetFilter(app);
                    return 0;
                case IDM_FILTER_DUP_VOTER_IDS:
                    App_ShowDuplicateVoterIds(app);
                    return 0;
                case IDM_FILTER_DUP_VOTERS:
                    App_ShowDuplicateVoters(app);
                    return 0;
                case IDM_FILTER_DUP_VOTERS_VOTING:
                    App_ShowDuplicateVotersVoting(app);
                    return 0;
                case IDM_FILTER_RESET_VIEW:
                    /* Leave the duplicates view and any filter: show all records. */
                    App_ResetFilter(app);
                    return 0;
                case IDM_REPORT_PRECINCT:
                    App_ShowReport(app, EE_REPORT_PRECINCT);
                    return 0;
                case IDM_REPORT_VOTING_TOTALS:
                    App_ShowRosterTotals(app);
                    return 0;
                case IDM_REPORT_ADDRESS:
                    App_ShowReport(app, EE_REPORT_ADDRESS);
                    return 0;
                case IDM_HELP_LOADING:
                    if (app->is_roster)
                    {
                        App_ShowHelpTopic(app, L"Help — Loading Voter Rosters", k_HelpRosterLoading);
                    }
                    else
                    {
                        App_ShowHelpTopic(app, L"Help — Loading Voter Lists", k_HelpLoading);
                    }
                    return 0;
                case IDM_HELP_OPTIONS:
                    App_ShowHelpTopic(app, L"Help — Options", app->is_roster ? k_RosterHelpOptions : k_HelpOptions);
                    return 0;
                case IDM_HELP_FILTERS:
                    App_ShowHelpTopic(app, L"Help — Filters", app->is_roster ? k_RosterHelpFilters : k_HelpFilters);
                    return 0;
                case IDM_HELP_REPORTS:
                    App_ShowHelpTopic(app, L"Help — Reports", app->is_roster ? k_RosterHelpReports : k_HelpReports);
                    return 0;
                case IDM_HELP_EXPORT:
                    App_ShowHelpTopic(app, L"Help — Export", app->is_roster ? k_RosterHelpExport : k_HelpExport);
                    return 0;
                case IDM_HELP_COMPARE:
                    App_ShowHelpTopic(app, L"Help — Compare", app->is_roster ? k_RosterHelpCompare : k_HelpCompare);
                    return 0;
                case IDM_HELP_ABOUT:
                    App_ShowAbout(app);
                    return 0;
                default:
                    if (LOWORD(wParam) >= IDM_COMPARE_WITH_FIRST &&
                        LOWORD(wParam) <= IDM_COMPARE_WITH_LAST)
                    {
                        AppState *other =
                            App_OtherViewerByIndex(app, LOWORD(wParam) - IDM_COMPARE_WITH_FIRST);
                        App_StartCompare(app, other);
                        return 0;
                    }
                    break;
            }
            break;

        case WM_NOTIFY:
            return App_OnNotify(app, (NMHDR *)lParam);

        case WM_VSCROLL:
            /* ListView may not send this to parent; left for completeness. */
            break;

        case EEM_LOAD_PROGRESS:
            EnterCriticalSection(&app->progress_lock);
            if (app->progress_dirty)
            {
                EeLoadProgress snap = app->last_progress;
                app->progress_dirty = FALSE;
                LeaveCriticalSection(&app->progress_lock);
                App_UpdateProgressUi(app, &snap);
            }
            else
            {
                LeaveCriticalSection(&app->progress_lock);
            }
            return 0;

        case EEM_LOAD_FINISHED:
            App_OnLoadFinished(app);
            return 0;

        case EEM_SCAN_PROGRESS:
        {
            EeLoadProgress snap = {0};
            BOOL have = FALSE;
            EnterCriticalSection(&app->progress_lock);
            if (app->progress_dirty)
            {
                snap = app->last_progress;
                app->progress_dirty = FALSE;
                have = TRUE;
            }
            LeaveCriticalSection(&app->progress_lock);
            if (have)
            {
                if (app->hwnd_progress_bar)
                {
                    SendMessageW(app->hwnd_progress_bar, PBM_SETPOS, snap.percent, 0);
                }
                if (app->hwnd_progress_status)
                {
                    wchar_t text[160];
                    StringCchPrintfW(text,
                                     ARRAYSIZE(text),
                                     L"Scanned %u rows… %u%%",
                                     snap.rows_loaded,
                                     snap.percent);
                    SetWindowTextW(app->hwnd_progress_status, text);
                }
            }
            return 0;
        }

        case EEM_SCAN_FINISHED:
            App_OnScanFinished(app);
            return 0;

        case EEM_CMP_FINISHED:
            App_OnCompareFinished(app);
            return 0;

        case EEM_SYNC_PANE_SCROLLUI:
            App_SyncPaneScrollChrome(app);
            return 0;

        case EEM_SYNC_SELECTION:
            InterlockedExchange(&app->sel_sync_posted, 0);
            App_CopySelectionToOtherPane(app, app->sel_sync_source);
            return 0;

        case WM_CTLCOLORSTATIC:
            if ((HWND)lParam == app->hwnd_frozen_title || (HWND)lParam == app->hwnd_scroll_title)
            {
                HDC hdc = (HDC)wParam;
                SetBkMode(hdc, OPAQUE);
                SetTextColor(hdc, RGB(0, 0, 0));
                SetBkColor(hdc, app->header_bg);
                if (app->brush_header != NULL)
                {
                    return (LRESULT)app->brush_header;
                }
                return (LRESULT)GetSysColorBrush(COLOR_BTNFACE);
            }
            if ((HWND)lParam == app->hwnd_frozen_hsb_pad ||
                (HWND)lParam == app->hwnd_scroll_hsb_pad)
            {
                HDC hdc = (HDC)wParam;
                SetBkColor(hdc, GetSysColor(COLOR_BTNFACE));
                return (LRESULT)GetSysColorBrush(COLOR_BTNFACE);
            }
            break;

        case WM_CLOSE:
            App_RequestClose(app);
            return 0;

        case WM_DESTROY:
            App_CloseReports(app);
            App_CloseCompare(app);
            App_CloseDiff(app);
            App_DestroyOptions(app);
            App_DestroyProgress(app);
            App_DestroyFilter(app);
            if (app->scan_thread != NULL)
            {
                InterlockedExchange(&app->scan_cancel, 1);
                WaitForSingleObject(app->scan_thread, INFINITE);
                CloseHandle(app->scan_thread);
                app->scan_thread = NULL;
            }
            /* A close during load is deferred until App_OnLoadFinished (which closes
             * the handle), so load_thread is normally NULL here; clean up defensively
             * anyway so the handle can never outlive the window. */
            if (app->load_thread != NULL)
            {
                InterlockedExchange(&app->load_cancel, 1);
                WaitForSingleObject(app->load_thread, INFINITE);
                CloseHandle(app->load_thread);
                app->load_thread = NULL;
            }
            free(app->scan_marks);
            app->scan_marks = NULL;
            free(app->cmp_class_a);
            free(app->cmp_class_b);
            app->cmp_class_a = NULL;
            app->cmp_class_b = NULL;
            App_ClearMarks(app);
            EeFilter_Clear(&app->filters);
            free(app->filter_map);
            app->filter_map = NULL;
            app->filter_count = 0;
            EeVoterTable_Clear(&app->table);
            EeRoster_FreeInfo(&app->roster_info);
            if (app->font_ui)
            {
                DeleteObject(app->font_ui);
                app->font_ui = NULL;
            }
            if (app->font_grid)
            {
                DeleteObject(app->font_grid);
                app->font_grid = NULL;
            }
            if (app->font_header)
            {
                DeleteObject(app->font_header);
                app->font_header = NULL;
            }
            if (app->brush_header)
            {
                DeleteObject(app->brush_header);
                app->brush_header = NULL;
            }
            if (app->pen_header_line)
            {
                DeleteObject(app->pen_header_line);
                app->pen_header_line = NULL;
            }
            DeleteCriticalSection(&app->progress_lock);
            App_UnlinkViewer(app);
            SetWindowLongPtrW(hwnd, GWLP_USERDATA, 0);
            HeapFree(GetProcessHeap(), 0, app);
            App_MaybeQuit();
            return 0;

        default:
            break;
    }
    return DefWindowProcW(hwnd, msg, wParam, lParam);
}

/* Subclass list views to sync vertical scrolling on wheel / scroll messages. */
static LRESULT CALLBACK PaneTitleSubclass(HWND hwnd, UINT msg, WPARAM wParam, LPARAM lParam)
{
    if (msg == WM_ERASEBKGND)
    {
        return 1;
    }
    if (msg == WM_PAINT)
    {
        PAINTSTRUCT ps;
        HDC hdc = BeginPaint(hwnd, &ps);
        if (hdc != NULL)
        {
            AppState *app = App_FromChild(hwnd);
            RECT rc;
            wchar_t text[64];
            HFONT font;
            HFONT old_font = NULL;
            HPEN pen;
            HPEN old_pen;
            int pen_w;
            HBRUSH brush = (app != NULL && app->brush_header != NULL)
                               ? app->brush_header
                               : GetSysColorBrush(COLOR_BTNFACE);

            GetClientRect(hwnd, &rc);
            FillRect(hdc, &rc, brush);

            text[0] = L'\0';
            GetWindowTextW(hwnd, text, ARRAYSIZE(text));
            font = (app != NULL && app->font_header != NULL) ? app->font_header
                   : (app != NULL)                           ? app->font_ui
                                                             : NULL;
            if (font != NULL)
            {
                old_font = (HFONT)SelectObject(hdc, font);
            }
            SetBkMode(hdc, TRANSPARENT);
            SetTextColor(hdc, RGB(0, 0, 0));
            DrawTextW(hdc, text, -1, &rc, DT_CENTER | DT_VCENTER | DT_SINGLELINE | DT_NOPREFIX);
            if (old_font != NULL)
            {
                SelectObject(hdc, old_font);
            }

            pen_w = (app != NULL) ? Scale(app, 1) : 1;
            if (pen_w < 1)
            {
                pen_w = 1;
            }
            pen = CreatePen(PS_SOLID, pen_w, RGB(0, 0, 0));
            old_pen = (HPEN)SelectObject(hdc, pen);
            /* Top */
            MoveToEx(hdc, rc.left, rc.top + pen_w / 2, NULL);
            LineTo(hdc, rc.right, rc.top + pen_w / 2);
            /* Bottom */
            MoveToEx(hdc, rc.left, rc.bottom - 1 - pen_w / 2, NULL);
            LineTo(hdc, rc.right, rc.bottom - 1 - pen_w / 2);
            /* Left */
            MoveToEx(hdc, rc.left + pen_w / 2, rc.top, NULL);
            LineTo(hdc, rc.left + pen_w / 2, rc.bottom);
            /* Right */
            MoveToEx(hdc, rc.right - 1 - pen_w / 2, rc.top, NULL);
            LineTo(hdc, rc.right - 1 - pen_w / 2, rc.bottom);
            SelectObject(hdc, old_pen);
            DeleteObject(pen);

            EndPaint(hwnd, &ps);
        }
        return 0;
    }
    return CallWindowProcW(g_old_title_proc, hwnd, msg, wParam, lParam);
}

static LRESULT CALLBACK ListSubclassProc(HWND hwnd,
                                         UINT msg,
                                         WPARAM wParam,
                                         LPARAM lParam,
                                         WNDPROC old_proc)
{
    AppState *app = App_FromChild(hwnd);

    if (msg == WM_CONTEXTMENU)
    {
        if (app != NULL)
        {
            App_ShowCopyContextMenu(app, hwnd, GET_X_LPARAM(lParam), GET_Y_LPARAM(lParam));
        }
        return 0;
    }

    /* Header NM_CUSTOMDRAW is delivered to the ListView (header parent). */
    if (msg == WM_NOTIFY)
    {
        NMHDR *nm = (NMHDR *)lParam;
        HWND header = ListView_GetHeader(hwnd);
        if (app != NULL && nm != NULL && header != NULL && nm->hwndFrom == header &&
            nm->code == NM_CUSTOMDRAW)
        {
            return App_HeaderCustomDraw(app, (NMCUSTOMDRAW *)lParam, FALSE);
        }
    }

    {
        LRESULT r = CallWindowProcW(old_proc, hwnd, msg, wParam, lParam);
        if (app != NULL && (msg == WM_VSCROLL || msg == WM_MOUSEWHEEL || msg == WM_MOUSEHWHEEL ||
                            msg == WM_KEYDOWN))
        {
            App_SyncVerticalScroll(app, hwnd);
        }
        return r;
    }
}

static LRESULT CALLBACK FrozenSubclass(HWND hwnd, UINT msg, WPARAM wParam, LPARAM lParam)
{
    return ListSubclassProc(hwnd, msg, wParam, lParam, g_old_frozen_proc);
}

static LRESULT CALLBACK ScrollSubclass(HWND hwnd, UINT msg, WPARAM wParam, LPARAM lParam)
{
    return ListSubclassProc(hwnd, msg, wParam, lParam, g_old_scroll_proc);
}

static void App_RequestClose(AppState *app)
{
    if (app == NULL || app->hwnd_main == NULL)
    {
        return;
    }
    if (app->loading || app->scanning)
    {
        app->close_pending = TRUE;
        InterlockedExchange(&app->load_cancel, 1);
        InterlockedExchange(&app->scan_cancel, 1);
        App_SetStatus(app, app->scanning ? L"Cancelling scan…" : L"Cancelling load…");
        return;
    }
    DestroyWindow(app->hwnd_main);
}

static void App_ExitAll(void)
{
    HWND hwnds[64];
    int n = 0;
    int i;
    AppState *p;

    for (p = g_viewers; p != NULL && n < (int)ARRAYSIZE(hwnds); p = p->next)
    {
        hwnds[n++] = p->hwnd_main;
    }
    for (i = 0; i < n; i++)
    {
        if (hwnds[i] != NULL && IsWindow(hwnds[i]))
        {
            SendMessageW(hwnds[i], WM_CLOSE, 0, 0);
        }
    }
    /* Close standalone CVR windows too (snapshot first — each close mutates the list). */
    {
        HWND cvr[EE_MAX_CVR_WINDOWS];
        int c = 0;
        int k;
        for (k = 0; k < g_cvr_window_count && c < (int)ARRAYSIZE(cvr); k++)
        {
            cvr[c++] = g_cvr_windows[k];
        }
        for (k = 0; k < c; k++)
        {
            if (cvr[k] != NULL && IsWindow(cvr[k]))
            {
                SendMessageW(cvr[k], WM_CLOSE, 0, 0);
            }
        }
    }
}

static AppState *App_CreateViewer(HINSTANCE instance,
                                  int nCmdShow,
                                  HWND offset_from,
                                  const AppState *prefs)
{
    return App_CreateViewerEx(instance, nCmdShow, offset_from, prefs, FALSE);
}

/* Create a viewer window; @p roster makes it a single-pane Voter Roster window. */
static AppState *App_CreateViewerEx(HINSTANCE instance,
                                    int nCmdShow,
                                    HWND offset_from,
                                    const AppState *prefs,
                                    BOOL roster)
{
    AppState *app;
    HWND hwnd;
    int x = CW_USEDEFAULT;
    int y = CW_USEDEFAULT;
    int w = k_DefaultWidth;
    int h = k_DefaultHeight;

    app = (AppState *)HeapAlloc(GetProcessHeap(), HEAP_ZERO_MEMORY, sizeof(AppState));
    if (app == NULL)
    {
        return NULL;
    }
    App_InitViewerState(app, instance, prefs);
    app->is_roster = roster; /* before WM_CREATE: it picks the menu */

    if (offset_from != NULL)
    {
        RECT wr;
        if (GetWindowRect(offset_from, &wr))
        {
            x = wr.left + 32;
            y = wr.top + 32;
            w = wr.right - wr.left;
            h = wr.bottom - wr.top;
        }
    }

    hwnd = CreateWindowExW(0,
                           k_WindowClassName,
                           k_WindowTitle,
                           WS_OVERLAPPEDWINDOW,
                           x,
                           y,
                           w,
                           h,
                           NULL,
                           NULL,
                           instance,
                           app);
    if (hwnd == NULL)
    {
        /* WM_DESTROY frees app if WM_CREATE ran; otherwise this is a rare leak. */
        return NULL;
    }

    App_LinkViewer(app);
    App_UpdateDpiMetrics(app, GetDpiForWindow(hwnd));
    App_ApplyFont(app);
    if (offset_from == NULL)
    {
        RECT wr;
        GetWindowRect(hwnd, &wr);
        SetWindowPos(hwnd,
                     NULL,
                     wr.left,
                     wr.top,
                     Scale(app, k_DefaultWidth),
                     Scale(app, k_DefaultHeight),
                     SWP_NOZORDER | SWP_NOMOVE);
    }
    App_Layout(app);
    App_SubclassViewer(app);
    ShowWindow(hwnd, nCmdShow);
    UpdateWindow(hwnd);
    return app;
}

/* -------------------------------------------------------------------------- */
/* Cast Vote Record viewer window                                             */
/* -------------------------------------------------------------------------- */

typedef struct CvrReportWindow CvrReportWindow;
typedef struct CvrValueReportWindow CvrValueReportWindow;
typedef struct CvrRcvWindow CvrRcvWindow;

/* Per-column value reports opened from the CVR Reports menu (Batch / Precinct /
 * Ballot Style, plus the Hart PDF report's Polling Place / Device Serial / Voting
 * Type). Indexed by these kinds; one window of each kind per CVR window. */
enum
{
    EE_CVRREP_BATCH = 0,
    EE_CVRREP_PRECINCT = 1,
    EE_CVRREP_BALLOTSTYLE = 2,
    EE_CVRREP_POLLINGPLACE = 3,
    EE_CVRREP_DEVICESERIAL = 4,
    EE_CVRREP_VOTINGTYPE = 5,
    EE_CVRREP_COUNT = 6
};

typedef struct CvrWindow
{
    AppState *app;
    HWND hwnd;
    HWND list;    /* owner-data report list view */
    HWND status;  /* bottom status bar */
    EeCvrTable table;
    int sort_col; /* -1 = unsorted */
    BOOL sort_asc;
    BOOL multi_card;         /* CVR looks like one row per ballot card/page */
    EeCvrCardCounts cards;   /* its ballots and ballot sheets (EeCvr_CardCounts) */
    CvrReportWindow *report; /* tabulation report tied to this window, or NULL */
    CvrRcvWindow *rcv_report; /* ranked-choice rounds report, or NULL */
    uint32_t rcv_count;       /* ranked-choice contests in the table (set at load) */
    EeFilterSet filters;     /* applied filter rules (is / is not, include/exclude) */
    uint32_t *disp;          /* filtered physical rows in sort order (when filtered) */
    uint32_t disp_count;
    BOOL filt_active;        /* a filter is narrowing the view */
    HWND hwnd_filter;        /* open CVR filter window for this CVR window, or NULL */
    CvrValueReportWindow *vreports[EE_CVRREP_COUNT]; /* per-column value reports */
    /* Per-column report availability, computed once at load (the data never changes
     * afterward) so WM_INITMENUPOPUP need not re-scan the table on every menu open. */
    BOOL vreport_avail[EE_CVRREP_COUNT];
    uint32_t vreport_col[EE_CVRREP_COUNT];
    wchar_t base_name[192]; /* initial CVR file base name (no extension), for exports */
    HWND cmp_targets[32];   /* Compare menu: results windows by command offset */
} CvrWindow;

static void App_ShowCvrReport(CvrWindow *cw, BOOL filtered);
static void App_ShowCvrRcvReport(CvrWindow *cw, BOOL filtered);
static void App_ShowCvrValueReport(CvrWindow *cw, int kind);
static void App_ShowCvrOptions(CvrWindow *cw);
static BOOL App_ShowCvrFilter(CvrWindow *cw);
static void Cvr_ApplyFilter(CvrWindow *cw);
static void Cvr_ResetFilter(CvrWindow *cw);
static void App_ExportCvrWindow(CvrWindow *cw, BOOL selection_only);
static void App_ExportCvrTabReport(CvrReportWindow *rw, BOOL selection_only);
static void App_ExportCvrValueReport(CvrValueReportWindow *rw, BOOL selection_only);

struct CvrReportWindow
{
    CvrWindow *owner;    /* the CVR window this report is tied to */
    AppState *app;       /* for fonts / header brushes / DPI      */
    HWND hwnd;           /* this report's top-level window         */
    HWND list;           /* owner-data report list view           */
    HWND status;         /* bottom status bar                     */
    EeCvrTally *items;   /* tabulated (contest, selection, count) */
    uint32_t count;
    BOOL filtered;       /* TRUE = tabulate only the filtered rows */
};

/* Per-column value report (Batch / Precinct / Ballot Style / Polling Place / Device
 * Serial / Voting Type): one value per row with
 * its ballot-record count, like the voter-list Precinct/Address reports. */
struct CvrValueReportWindow
{
    CvrWindow *owner;        /* CVR window (owns the table + filters) */
    AppState *app;
    HWND hwnd;
    HWND list;
    HWND status;
    int kind;                /* EE_CVRREP_* (slot in owner->vreports)  */
    uint32_t column;         /* source CVR column                      */
    const wchar_t *label;    /* "Batch" / "Precinct" / "Polling Place" ... */
    EeCvrValueCount *items;  /* value + count, in current sort order    */
    uint32_t count;
    BOOL has_blank;          /* a "(blank)" row is present in items      */
    BOOL numeric;            /* sort the value column numerically        */
    int sort_col;            /* 0 = value, 1 = count                     */
    BOOL sort_asc;
};

/* Ranked-choice (instant-runoff) rounds report: one block of rows per contest. */
struct CvrRcvWindow
{
    CvrWindow *owner;
    AppState *app;
    HWND hwnd;
    HWND list;
    HWND status;
    EeRcvResult *res;    /* one per ranked-choice contest */
    uint32_t nres;
    uint32_t *row_res;   /* display row -> contest (index into res) */
    int32_t *row_kind;   /* display row -> candidate index (>= 0) or RCVROW_* */
    uint32_t nrows;
    uint32_t max_rounds; /* round columns shown */
    BOOL filtered;       /* TRUE = tabulate only the filtered rows */
};

static void Cvr_UpdateStatus(CvrWindow *cw)
{
    wchar_t buf[160];
    if (cw->status == NULL)
    {
        return;
    }
    if (cw->filt_active)
    {
        StringCchPrintfW(buf,
                         ARRAYSIZE(buf),
                         L"%u of %u ballot records  \x2022  %u columns  \x2022  filtered",
                         cw->disp_count,
                         cw->table.nrows,
                         cw->table.ncols);
    }
    else
    {
        StringCchPrintfW(buf,
                         ARRAYSIZE(buf),
                         L"%u ballot records  \x2022  %u columns",
                         cw->table.nrows,
                         cw->table.ncols);
    }
    if (cw->multi_card)
    {
        wchar_t mc[96];
        StringCchPrintfW(mc, ARRAYSIZE(mc), L"  \x2022  multi-card: %u ballots on %u sheets", cw->cards.ballots,
                         cw->cards.rows);
        StringCchCatW(buf, ARRAYSIZE(buf), mc);
    }
    SendMessageW(cw->status, SB_SETTEXTW, 0, (LPARAM)buf);
}

static void Cvr_LayoutList(CvrWindow *cw, int width, int height)
{
    int sb_h = 0;
    if (cw->status != NULL)
    {
        RECT sb;
        SendMessageW(cw->status, WM_SIZE, 0, 0);
        if (GetWindowRect(cw->status, &sb))
        {
            sb_h = sb.bottom - sb.top;
        }
    }
    if (cw->list == NULL)
    {
        return;
    }
    height = (height > sb_h) ? height - sb_h : 0;
    MoveWindow(cw->list, 0, 0, width, height, TRUE);
}

/* Menu bar for a CVR window: File (mirrors the voter window, but "Close Cast Vote
 * Records") + Edit (Copy). */
static HMENU App_CreateCvrMenu(void)
{
    HMENU menu = CreateMenu();
    HMENU file_menu = CreatePopupMenu();
    HMENU edit_menu = CreatePopupMenu();
    HMENU filter_menu = CreatePopupMenu();
    HMENU reports_menu = CreatePopupMenu();
    HMENU compare_menu = CreatePopupMenu();
    HMENU help_menu = CreatePopupMenu();
    if (menu == NULL || file_menu == NULL || edit_menu == NULL || filter_menu == NULL ||
        reports_menu == NULL || compare_menu == NULL || help_menu == NULL)
    {
        if (compare_menu != NULL)
        {
            DestroyMenu(compare_menu);
        }
        if (file_menu != NULL)
        {
            DestroyMenu(file_menu);
        }
        if (edit_menu != NULL)
        {
            DestroyMenu(edit_menu);
        }
        if (filter_menu != NULL)
        {
            DestroyMenu(filter_menu);
        }
        if (reports_menu != NULL)
        {
            DestroyMenu(reports_menu);
        }
        if (help_menu != NULL)
        {
            DestroyMenu(help_menu);
        }
        if (menu != NULL)
        {
            DestroyMenu(menu);
        }
        return NULL;
    }
    AppendMenuW(file_menu, MF_STRING, IDM_FILE_OPEN_VOTER_LIST, L"&Load Voter List…\tCtrl+O");
    AppendMenuW(file_menu, MF_STRING, IDM_FILE_OPEN_CVR, L"Load Cast &Vote Records…");
    AppendMenuW(file_menu, MF_STRING, IDM_FILE_OPEN_ROSTER, L"Load Voter &Roster…");
    AppendMenuW(file_menu, MF_STRING, IDM_FILE_OPEN_RESULTS, L"Load Election Re&sults…");
    AppendMenuW(file_menu, MF_STRING, IDM_FILE_OPEN_AUDIT, L"Load &Audit Logs…");
    AppendMenuW(file_menu, MF_STRING, IDM_FILE_CLOSE_CVR, L"&Close Cast Vote Records");
    AppendMenuW(file_menu, MF_STRING, IDM_FILE_EXPORT_CVR, L"&Export Cast Vote Records…");
    AppendMenuW(file_menu, MF_SEPARATOR, 0, NULL);
    AppendMenuW(file_menu, MF_STRING, IDM_FILE_EXIT, L"E&xit");
    AppendMenuW(edit_menu, MF_STRING, IDM_EDIT_COPY, L"&Copy\tCtrl+C");
    AppendMenuW(edit_menu, MF_SEPARATOR, 0, NULL);
    AppendMenuW(edit_menu, MF_STRING, IDM_CVR_OPTIONS, L"&Options…");
    AppendMenuW(filter_menu, MF_STRING, IDM_CVR_FILTER, L"&Filter…\tCtrl+L");
    AppendMenuW(filter_menu, MF_STRING, IDM_CVR_FILTER_RESET, L"&Reset Filter");
    AppendMenuW(reports_menu, MF_STRING, IDM_CVR_TABULATE, L"&Tabulate All CVR Votes…");
    AppendMenuW(reports_menu, MF_STRING, IDM_CVR_TABULATE_FILTERED,
                L"Tabulate F&iltered CVR Votes…");
    AppendMenuW(reports_menu,
                MF_STRING,
                IDM_CVR_TABULATE_RCV,
                L"Tabulate &Ranked-Choice Contests…");
    AppendMenuW(reports_menu,
                MF_STRING,
                IDM_CVR_TABULATE_RCV_FILTERED,
                L"Tabulate Filtered Ra&nked-Choice Contests…");
    AppendMenuW(reports_menu, MF_SEPARATOR, 0, NULL);
    AppendMenuW(reports_menu, MF_STRING, IDM_CVR_REPORT_BATCH, L"Display &Batch Report…");
    AppendMenuW(reports_menu, MF_STRING, IDM_CVR_REPORT_PRECINCT, L"Display &Precinct Report…");
    AppendMenuW(reports_menu, MF_STRING, IDM_CVR_REPORT_BALLOTSTYLE,
                L"Display Ballot &Style Report…");
    AppendMenuW(reports_menu, MF_STRING, IDM_CVR_REPORT_POLLINGPLACE,
                L"Display P&olling Place Report…");
    AppendMenuW(reports_menu, MF_STRING, IDM_CVR_REPORT_DEVICESERIAL,
                L"Display &Device Serial Report…");
    AppendMenuW(reports_menu, MF_STRING, IDM_CVR_REPORT_VOTINGTYPE,
                L"Display &Voting Type Report…");
    AppendMenuW(help_menu, MF_STRING, IDM_HELP_LOADING, L"&Loading Cast Vote Records");
    AppendMenuW(help_menu, MF_STRING, IDM_HELP_OPTIONS, L"&Options");
    AppendMenuW(help_menu, MF_STRING, IDM_HELP_FILTERS, L"&Filters");
    AppendMenuW(help_menu, MF_STRING, IDM_HELP_REPORTS, L"&Reports");
    AppendMenuW(help_menu, MF_STRING, IDM_HELP_COMPARE, L"&Compare");
    AppendMenuW(help_menu, MF_STRING, IDM_HELP_EXPORT, L"&Export");
    AppendMenuW(help_menu, MF_SEPARATOR, 0, NULL);
    AppendMenuW(help_menu, MF_STRING, IDM_HELP_ABOUT, L"&About Election Explorer…");
    AppendMenuW(menu, MF_POPUP, (UINT_PTR)file_menu, L"&File");
    AppendMenuW(menu, MF_POPUP, (UINT_PTR)edit_menu, L"&Edit");
    AppendMenuW(menu, MF_POPUP, (UINT_PTR)filter_menu, L"F&ilter");
    AppendMenuW(menu, MF_POPUP, (UINT_PTR)reports_menu, L"&Reports");
    AppendMenuW(compare_menu, MF_STRING | MF_GRAYED, 0, L"(open a results window to compare)");
    AppendMenuW(menu, MF_POPUP, (UINT_PTR)compare_menu, L"&Compare");
    AppendMenuW(menu, MF_POPUP, (UINT_PTR)help_menu, L"&Help");
    return menu;
}

/* Subclass so the CVR list draws its header bold on the grey header background,
 * like the voter list (header NM_CUSTOMDRAW is delivered to the list, the header's
 * parent). */
static WNDPROC g_old_cvr_list_proc = NULL;

static LRESULT CALLBACK CvrListSubclass(HWND hwnd, UINT msg, WPARAM wParam, LPARAM lParam)
{
    if (msg == WM_NOTIFY)
    {
        NMHDR *nm = (NMHDR *)lParam;
        HWND header = ListView_GetHeader(hwnd);
        CvrWindow *cw = (CvrWindow *)GetWindowLongPtrW(GetParent(hwnd), GWLP_USERDATA);
        if (cw != NULL && nm != NULL && header != NULL && nm->hwndFrom == header &&
            nm->code == NM_CUSTOMDRAW)
        {
            return App_HeaderCustomDraw(cw->app, (NMCUSTOMDRAW *)lParam, FALSE);
        }
    }
    return CallWindowProcW(g_old_cvr_list_proc, hwnd, msg, wParam, lParam);
}

/* Grow @p *buf (wide chars) so it holds at least @p need elements. */
static BOOL cvr_wbuf_reserve(wchar_t **buf, size_t *cap, size_t need)
{
    size_t nc;
    wchar_t *nb;
    if (need <= *cap)
    {
        return TRUE;
    }
    nc = (*cap == 0) ? 4096 : *cap;
    while (nc < need)
    {
        if (nc > ((size_t)-1) / (2 * sizeof(wchar_t)))
        {
            return FALSE;
        }
        nc *= 2;
    }
    nb = (wchar_t *)realloc(*buf, nc * sizeof(wchar_t));
    if (nb == NULL)
    {
        return FALSE;
    }
    *buf = nb;
    *cap = nc;
    return TRUE;
}

/* Copy the selected ballots to the clipboard as tab-separated UTF-8, one row per
 * ballot with every column (frozen keys + contests), CR/LF between rows. */
static void Cvr_CopySelected(CvrWindow *cw)
{
    int i;
    wchar_t *buf = NULL;
    size_t cap = 0;
    size_t len = 0;
    wchar_t cell[512];
    char *utf8 = NULL;
    int u8len;

    if (cw == NULL || cw->list == NULL || cw->table.ncols == 0)
    {
        return;
    }
    i = ListView_GetNextItem(cw->list, -1, LVNI_SELECTED);
    while (i >= 0)
    {
        uint32_t c;
        /* Map the selected display row to a physical row through the filter. */
        uint32_t phys = cw->filt_active ? (((uint32_t)i < cw->disp_count) ? cw->disp[i] : 0)
                                        : cw->table.view_index[i];
        for (c = 0; c < cw->table.ncols; c++)
        {
            size_t n;
            cell[0] = L'\0';
            EeCvr_GetCellW(&cw->table, phys, c, cell, ARRAYSIZE(cell));
            n = wcslen(cell);
            if (!cvr_wbuf_reserve(&buf, &cap, len + n + 2))
            {
                free(buf);
                return;
            }
            if (n > 0)
            {
                memcpy(buf + len, cell, n * sizeof(wchar_t));
                len += n;
            }
            if (c + 1 < cw->table.ncols)
            {
                buf[len++] = L'\t';
            }
            else
            {
                buf[len++] = L'\r';
                buf[len++] = L'\n';
            }
        }
        i = ListView_GetNextItem(cw->list, i, LVNI_SELECTED);
    }
    if (buf == NULL || len == 0)
    {
        free(buf);
        return;
    }
    if (!cvr_wbuf_reserve(&buf, &cap, len + 1))
    {
        free(buf);
        return;
    }
    buf[len] = L'\0';

    u8len = WideCharToMultiByte(CP_UTF8, 0, buf, -1, NULL, 0, NULL, NULL);
    if (u8len > 0)
    {
        utf8 = (char *)malloc((size_t)u8len);
        if (utf8 != NULL &&
            WideCharToMultiByte(CP_UTF8, 0, buf, -1, utf8, u8len, NULL, NULL) > 0)
        {
            App_SetClipboardUtf8(cw->hwnd, utf8);
        }
    }
    free(utf8);
    free(buf);
}

/* Right-click: select the row if needed, then a Copy menu. */
static void Cvr_OnContextMenu(CvrWindow *cw, int item, POINT screen)
{
    HMENU m;
    UINT cmd;

    if (cw == NULL || cw->list == NULL)
    {
        return;
    }
    if (item >= 0 && !(ListView_GetItemState(cw->list, item, LVIS_SELECTED) & LVIS_SELECTED))
    {
        ListView_SetItemState(cw->list, -1, 0, LVIS_SELECTED | LVIS_FOCUSED);
        ListView_SetItemState(cw->list,
                              item,
                              LVIS_SELECTED | LVIS_FOCUSED,
                              LVIS_SELECTED | LVIS_FOCUSED);
    }
    m = CreatePopupMenu();
    if (m == NULL)
    {
        return;
    }
    AppendMenuW(m, MF_STRING, IDM_EDIT_COPY, L"&Copy");
    AppendMenuW(m, MF_SEPARATOR, 0, NULL);
    AppendMenuW(m, MF_STRING, IDM_EXPORT_SELECTED, L"Export &Selected…");
    cmd = (UINT)
        TrackPopupMenu(m, TPM_RIGHTBUTTON | TPM_RETURNCMD, screen.x, screen.y, 0, cw->hwnd, NULL);
    DestroyMenu(m);
    if (cmd == IDM_EDIT_COPY)
    {
        Cvr_CopySelected(cw);
    }
    else if (cmd == IDM_EXPORT_SELECTED)
    {
        App_ExportCvrWindow(cw, TRUE);
    }
}

/* -------------------------------------------------------------------------- */
/* CVR filter (is / is not, include/exclude; value chosen from a list)         */
/* -------------------------------------------------------------------------- */

/* Match one "is"/"is not" rule against a physical CVR row (case-insensitive). */
static BOOL Cvr_RuleMatches(const EeFilterRule *r, const EeCvrTable *t, uint32_t phys)
{
    wchar_t cell[EE_FILTER_VALUE_CCH];
    BOOL eq;
    EeCvr_GetCellW(t, phys, r->column, cell, ARRAYSIZE(cell));
    eq = (CompareStringOrdinal(cell, -1, r->value, -1, TRUE) == CSTR_EQUAL);
    return (r->relation == EeRel_IsNot) ? !eq : eq;
}

/* ProcMon visibility: same-column includes OR, different columns AND; any matching
 * exclude hides the row. Mirrors EeFilter_AcceptsViewRow but for the CVR table. */
static BOOL Cvr_FilterAccepts(const EeFilterSet *set, const EeCvrTable *t, uint32_t phys)
{
    uint32_t i;
    uint32_t j;
    BOOL any_include = FALSE;

    if (set == NULL)
    {
        return TRUE;
    }
    for (i = 0; i < set->count; i++)
    {
        if (set->rules[i].enabled && set->rules[i].action == EeFilt_Exclude &&
            Cvr_RuleMatches(&set->rules[i], t, phys))
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
        BOOL seen = FALSE;
        if (!set->rules[i].enabled || set->rules[i].action != EeFilt_Include)
        {
            continue;
        }
        col = set->rules[i].column;
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
        matched = FALSE;
        for (j = i; j < set->count; j++)
        {
            if (set->rules[j].enabled && set->rules[j].action == EeFilt_Include &&
                set->rules[j].column == col && Cvr_RuleMatches(&set->rules[j], t, phys))
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

/* Rebuild the filtered display (physical rows in current sort order) and refresh. */
static void Cvr_ApplyFilter(CvrWindow *cw)
{
    int count;
    if (cw == NULL)
    {
        return;
    }
    free(cw->disp);
    cw->disp = NULL;
    cw->disp_count = 0;
    cw->filt_active = FALSE;

    if (EeFilter_HasEnabled(&cw->filters) && cw->table.nrows > 0)
    {
        uint32_t *map = (uint32_t *)malloc((size_t)cw->table.nrows * sizeof(uint32_t));
        if (map != NULL)
        {
            uint32_t v;
            uint32_t n = 0;
            for (v = 0; v < cw->table.nrows; v++)
            {
                uint32_t phys = cw->table.view_index[v];
                if (Cvr_FilterAccepts(&cw->filters, &cw->table, phys))
                {
                    map[n++] = phys;
                }
            }
            cw->disp = map;
            cw->disp_count = n;
            cw->filt_active = TRUE;
        }
    }
    if (cw->list != NULL)
    {
        count = cw->filt_active ? (int)cw->disp_count : (int)cw->table.nrows;
        ListView_SetItemCountEx(cw->list, count, LVSICF_NOINVALIDATEALL);
        if (count > 0)
        {
            ListView_RedrawItems(cw->list, 0, count);
        }
        InvalidateRect(cw->list, NULL, FALSE);
    }
    Cvr_UpdateStatus(cw);
}

static void Cvr_ResetFilter(CvrWindow *cw)
{
    if (cw == NULL)
    {
        return;
    }
    EeFilter_Clear(&cw->filters);
    Cvr_ApplyFilter(cw);
}

/* Header title (and display label) of the CVR column a value-report kind reports on. */
static const wchar_t *Cvr_ReportColumnTitle(int kind)
{
    switch (kind)
    {
        case EE_CVRREP_BATCH:
            return L"Batch";
        case EE_CVRREP_PRECINCT:
            return L"Precinct";
        case EE_CVRREP_BALLOTSTYLE:
            return L"Ballot Style";
        case EE_CVRREP_POLLINGPLACE:
            return L"Polling Place";
        case EE_CVRREP_DEVICESERIAL:
            return L"Device Serial";
        case EE_CVRREP_VOTINGTYPE:
            return L"Voting Type";
        default:
            return L"";
    }
}

/* Find the CVR column a value-report kind reports on. Vendors name some key columns
 * differently: ES&S calls the batch column "Batch", Hart calls it "Batch Number", so
 * the Batch report accepts either (the report keeps the "Batch" label); Dominion's
 * names are mapped below. */
static BOOL Cvr_FindReportColumn(const EeCvrTable *t, int kind, uint32_t *col)
{
    if (EeCvr_FindColumnByTitle(t, Cvr_ReportColumnTitle(kind), col))
    {
        return TRUE;
    }
    if (kind == EE_CVRREP_BATCH)
    {
        return EeCvr_FindColumnByTitle(t, L"Batch Number", col);
    }
    /* Dominion names its precinct, ballot style and voting-method columns after its
     * manifests: Precinct Portion, Ballot Type, Counting Group. */
    if (kind == EE_CVRREP_PRECINCT)
    {
        return EeCvr_FindColumnByTitle(t, L"Precinct Portion", col);
    }
    if (kind == EE_CVRREP_BALLOTSTYLE)
    {
        return EeCvr_FindColumnByTitle(t, L"Ballot Type", col);
    }
    if (kind == EE_CVRREP_VOTINGTYPE)
    {
        return EeCvr_FindColumnByTitle(t, L"Counting Group", col);
    }
    return FALSE;
}

/* Command IDs for the CVR value reports, indexed by EE_CVRREP_*. */
static const int k_CvrReportIds[EE_CVRREP_COUNT] = {IDM_CVR_REPORT_BATCH,
                                                    IDM_CVR_REPORT_PRECINCT,
                                                    IDM_CVR_REPORT_BALLOTSTYLE,
                                                    IDM_CVR_REPORT_POLLINGPLACE,
                                                    IDM_CVR_REPORT_DEVICESERIAL,
                                                    IDM_CVR_REPORT_VOTINGTYPE};

static LRESULT CALLBACK CvrWndProc(HWND hwnd, UINT msg, WPARAM wParam, LPARAM lParam)
{
    CvrWindow *cw = (CvrWindow *)GetWindowLongPtrW(hwnd, GWLP_USERDATA);

    switch (msg)
    {
        case WM_CREATE:
        {
            CREATESTRUCTW *cs = (CREATESTRUCTW *)lParam;
            RECT rc;
            uint32_t i;
            cw = (CvrWindow *)cs->lpCreateParams;
            SetWindowLongPtrW(hwnd, GWLP_USERDATA, (LONG_PTR)cw);
            cw->hwnd = hwnd;

            GetClientRect(hwnd, &rc);
            cw->list = CreateWindowExW(0,
                                       WC_LISTVIEWW,
                                       L"",
                                       WS_CHILD | WS_VISIBLE | LVS_REPORT | LVS_OWNERDATA |
                                           LVS_SHOWSELALWAYS,
                                       0,
                                       0,
                                       rc.right,
                                       rc.bottom,
                                       hwnd,
                                       NULL,
                                       cw->app->instance,
                                       NULL);
            if (cw->list == NULL)
            {
                return -1;
            }
            /* Subclass so the header draws bold/grey like the voter list. */
            {
                WNDPROC old =
                    (WNDPROC)SetWindowLongPtrW(cw->list, GWLP_WNDPROC, (LONG_PTR)CvrListSubclass);
                if (g_old_cvr_list_proc == NULL)
                {
                    g_old_cvr_list_proc = old;
                }
            }
            ListView_SetExtendedListViewStyle(cw->list,
                                              LVS_EX_FULLROWSELECT | LVS_EX_DOUBLEBUFFER |
                                                  LVS_EX_GRIDLINES);
            if (cw->app->font_ui)
            {
                SendMessageW(cw->list, WM_SETFONT, (WPARAM)cw->app->font_ui, TRUE);
            }
            /* A report-mode ListView header desyncs and stops hit-testing once the
             * cumulative column width passes the Win32 16-bit limit (32,767 px): the
             * body keeps scrolling but far-right column headers can't be clicked and the
             * view jumps back to the start. A wide CVR (e.g. ~188 contest columns) blows
             * past it, so clamp per-column width to keep the total under the limit. */
            {
                int cap = (cw->table.ncols > 0) ? (32000 / (int)cw->table.ncols) : 0;
                int floor_w = Scale(cw->app, 20);
                for (i = 0; i < cw->table.ncols; i++)
                {
                    LVCOLUMNW col;
                    int cx = Scale(cw->app, (i < cw->table.frozen_count) ? 130 : 190);
                    if (cx < floor_w)
                    {
                        cx = floor_w;
                    }
                    if (cap > 0 && cx > cap)
                    {
                        cx = cap; /* the limit always wins */
                    }
                    ZeroMemory(&col, sizeof(col));
                    col.mask = LVCF_TEXT | LVCF_WIDTH | LVCF_FMT;
                    col.fmt = LVCFMT_LEFT;
                    col.pszText = cw->table.col_titles[i];
                    col.cx = cx;
                    ListView_InsertColumn(cw->list, (int)i, &col);
                }
            }

            cw->status = CreateWindowExW(0,
                                         STATUSCLASSNAMEW,
                                         NULL,
                                         WS_CHILD | WS_VISIBLE | SBARS_SIZEGRIP,
                                         0,
                                         0,
                                         0,
                                         0,
                                         hwnd,
                                         NULL,
                                         cw->app->instance,
                                         NULL);
            if (cw->status != NULL && cw->app->font_ui)
            {
                SendMessageW(cw->status, WM_SETFONT, (WPARAM)cw->app->font_ui, TRUE);
            }
            Cvr_UpdateStatus(cw);

            ListView_SetItemCountEx(cw->list, (int)cw->table.nrows, LVSICF_NOINVALIDATEALL);
            Cvr_LayoutList(cw, rc.right, rc.bottom);
            return 0;
        }

        case WM_SIZE:
            if (cw != NULL)
            {
                Cvr_LayoutList(cw, LOWORD(lParam), HIWORD(lParam));
            }
            return 0;

        case WM_SETFOCUS:
            if (cw != NULL && cw->list != NULL)
            {
                SetFocus(cw->list);
            }
            return 0;

        case WM_INITMENUPOPUP:
            /* Grey a per-column report unless its column exists and holds real
             * (non-blank, non-redacted) data. Availability is cached at load, so this
             * is O(1) per menu open. Enabling by command on other popups is harmless
             * (the items are not present there). */
            if (cw != NULL)
            {
                int k;
                for (k = 0; k < EE_CVRREP_COUNT; k++)
                {
                    EnableMenuItem((HMENU)wParam,
                                   (UINT)k_CvrReportIds[k],
                                   MF_BYCOMMAND | (cw->vreport_avail[k] ? MF_ENABLED : MF_GRAYED));
                }
                /* Filtered tabulation is available only while a filter is narrowing
                 * the view. */
                EnableMenuItem((HMENU)wParam,
                               IDM_CVR_TABULATE_FILTERED,
                               MF_BYCOMMAND | (cw->filt_active ? MF_ENABLED : MF_GRAYED));
                /* Ranked-choice rounds need a ranked-choice contest (and, for the
                 * filtered form, an active filter). */
                EnableMenuItem((HMENU)wParam,
                               IDM_CVR_TABULATE_RCV,
                               MF_BYCOMMAND | (cw->rcv_count > 0 ? MF_ENABLED : MF_GRAYED));
                EnableMenuItem((HMENU)wParam,
                               IDM_CVR_TABULATE_RCV_FILTERED,
                               MF_BYCOMMAND |
                                   (cw->rcv_count > 0 && cw->filt_active ? MF_ENABLED : MF_GRAYED));
                /* Compare (File, Edit, Filter, Reports, Compare, Help): open results windows. */
                if ((HMENU)wParam == GetSubMenu(GetMenu(hwnd), 4))
                {
                    Res_FillCompareMenu((HMENU)wParam, hwnd, FALSE, cw->cmp_targets,
                                        (int)ARRAYSIZE(cw->cmp_targets));
                }
            }
            break;

        case WM_COMMAND:
            if (cw == NULL)
            {
                break;
            }
            if (LOWORD(wParam) >= IDM_COMPARE_WITH_FIRST &&
                LOWORD(wParam) < IDM_COMPARE_WITH_FIRST + ARRAYSIZE(cw->cmp_targets))
            {
                HWND target = cw->cmp_targets[LOWORD(wParam) - IDM_COMPARE_WITH_FIRST];
                if (target != NULL && IsWindow(target) && Aud_IsAuditWindow(target))
                {
                    App_RunBallotRecon(target, hwnd);
                }
                else if (target != NULL && IsWindow(target))
                {
                    App_RunResultsCompare(hwnd, target);
                }
                return 0;
            }
            switch (LOWORD(wParam))
            {
                case IDM_EDIT_COPY: /* also Ctrl+C via the shared accelerator table */
                    Cvr_CopySelected(cw);
                    return 0;
                case IDM_CVR_TABULATE:
                    App_ShowCvrReport(cw, FALSE);
                    return 0;
                case IDM_CVR_TABULATE_FILTERED:
                    App_ShowCvrReport(cw, TRUE);
                    return 0;
                case IDM_CVR_TABULATE_RCV:
                    App_ShowCvrRcvReport(cw, FALSE);
                    return 0;
                case IDM_CVR_TABULATE_RCV_FILTERED:
                    App_ShowCvrRcvReport(cw, TRUE);
                    return 0;
                case IDM_CVR_REPORT_BATCH:
                    App_ShowCvrValueReport(cw, EE_CVRREP_BATCH);
                    return 0;
                case IDM_CVR_REPORT_PRECINCT:
                    App_ShowCvrValueReport(cw, EE_CVRREP_PRECINCT);
                    return 0;
                case IDM_CVR_REPORT_BALLOTSTYLE:
                    App_ShowCvrValueReport(cw, EE_CVRREP_BALLOTSTYLE);
                    return 0;
                case IDM_CVR_REPORT_POLLINGPLACE:
                    App_ShowCvrValueReport(cw, EE_CVRREP_POLLINGPLACE);
                    return 0;
                case IDM_CVR_REPORT_DEVICESERIAL:
                    App_ShowCvrValueReport(cw, EE_CVRREP_DEVICESERIAL);
                    return 0;
                case IDM_CVR_REPORT_VOTINGTYPE:
                    App_ShowCvrValueReport(cw, EE_CVRREP_VOTINGTYPE);
                    return 0;
                case IDM_CVR_OPTIONS:
                    App_ShowCvrOptions(cw);
                    return 0;
                case IDM_CVR_FILTER:
                case IDM_FILTER_EDIT: /* Ctrl+L via the shared accelerator table */
                    App_ShowCvrFilter(cw);
                    return 0;
                case IDM_CVR_FILTER_RESET:
                    Cvr_ResetFilter(cw);
                    return 0;
                case IDM_FILE_OPEN_VOTER_LIST:
                    App_BeginOpenVoterList(cw->app);
                    return 0;
                case IDM_FILE_OPEN_CVR:
                    App_BeginOpenCvr(cw->app);
                    return 0;
                case IDM_FILE_OPEN_ROSTER:
                    App_BeginOpenRoster(cw->app);
                    return 0;
                case IDM_FILE_OPEN_RESULTS:
                    App_BeginOpenResults(cw->app);
                    return 0;
                case IDM_FILE_OPEN_AUDIT:
                    App_BeginOpenAudit(cw->app);
                    return 0;
                case IDM_FILE_CLOSE_CVR:
                    DestroyWindow(hwnd);
                    return 0;
                case IDM_FILE_EXPORT_CVR:
                    App_ExportCvrWindow(cw, FALSE);
                    return 0;
                case IDM_FILE_EXIT:
                    App_ExitAll();
                    return 0;
                case IDM_HELP_LOADING:
                    App_ShowHelpTopicOn(cw->app, hwnd, L"Help — Loading Cast Vote Records",
                                        k_CvrHelpLoading);
                    return 0;
                case IDM_HELP_OPTIONS:
                    App_ShowHelpTopicOn(cw->app, hwnd, L"Help — Options", k_CvrHelpOptions);
                    return 0;
                case IDM_HELP_FILTERS:
                    App_ShowHelpTopicOn(cw->app, hwnd, L"Help — Filters", k_CvrHelpFilters);
                    return 0;
                case IDM_HELP_REPORTS:
                    App_ShowHelpTopicOn(cw->app, hwnd, L"Help — Reports", k_CvrHelpReports);
                    return 0;
                case IDM_HELP_EXPORT:
                    App_ShowHelpTopicOn(cw->app, hwnd, L"Help — Export", k_CvrHelpExport);
                    return 0;
                case IDM_HELP_COMPARE:
                    App_ShowResultsCompareHelp(cw->app, hwnd);
                    return 0;
                case IDM_HELP_ABOUT:
                    App_ShowAboutOn(cw->app, hwnd);
                    return 0;
                default:
                    break;
            }
            break;

        case WM_NOTIFY:
        {
            NMHDR *hdr = (NMHDR *)lParam;
            if (cw == NULL || hdr->hwndFrom != cw->list)
            {
                break;
            }
            if (hdr->code == NM_RCLICK)
            {
                LPNMITEMACTIVATE ia = (LPNMITEMACTIVATE)lParam;
                POINT screen = ia->ptAction;
                ClientToScreen(cw->list, &screen);
                Cvr_OnContextMenu(cw, ia->iItem, screen);
                return 0;
            }
            if (hdr->code == LVN_GETDISPINFOW)
            {
                NMLVDISPINFOW *di = (NMLVDISPINFOW *)lParam;
                if ((di->item.mask & LVIF_TEXT) && di->item.iItem >= 0)
                {
                    uint32_t i = (uint32_t)di->item.iItem;
                    if (di->item.cchTextMax > 0)
                    {
                        di->item.pszText[0] = L'\0';
                    }
                    if (cw->filt_active)
                    {
                        if (i < cw->disp_count)
                        {
                            EeCvr_GetCellW(&cw->table,
                                           cw->disp[i],
                                           (uint32_t)di->item.iSubItem,
                                           di->item.pszText,
                                           (size_t)di->item.cchTextMax);
                        }
                    }
                    else
                    {
                        EeCvr_GetViewCellW(&cw->table,
                                           i,
                                           (uint32_t)di->item.iSubItem,
                                           di->item.pszText,
                                           (size_t)di->item.cchTextMax);
                    }
                }
                return 0;
            }
            if (hdr->code == LVN_COLUMNCLICK)
            {
                NMLISTVIEW *nlv = (NMLISTVIEW *)lParam;
                uint32_t col = (uint32_t)nlv->iSubItem;
                if (nlv->iSubItem == cw->sort_col)
                {
                    cw->sort_asc = !cw->sort_asc;
                }
                else
                {
                    cw->sort_col = nlv->iSubItem;
                    cw->sort_asc = TRUE;
                }
                EeCvr_SortByColumn(&cw->table, col, cw->sort_asc);
                /* Rebuild the filtered display in the new order (also redraws). */
                Cvr_ApplyFilter(cw);
                return 0;
            }
            break;
        }

        case WM_CLOSE:
            DestroyWindow(hwnd);
            return 0;

        case WM_DESTROY:
            if (cw != NULL)
            {
                /* Close the tabulation report + filter window tied to this window
                 * (their WM_DESTROY clears cw->report / cw->hwnd_filter) before we
                 * free the table they read from. */
                if (cw->report != NULL)
                {
                    DestroyWindow(cw->report->hwnd);
                }
                if (cw->rcv_report != NULL)
                {
                    DestroyWindow(cw->rcv_report->hwnd);
                }
                if (cw->hwnd_filter != NULL)
                {
                    DestroyWindow(cw->hwnd_filter);
                }
                {
                    int k;
                    for (k = 0; k < EE_CVRREP_COUNT; k++)
                    {
                        if (cw->vreports[k] != NULL)
                        {
                            DestroyWindow(cw->vreports[k]->hwnd);
                        }
                    }
                }
                EeFilter_Clear(&cw->filters);
                free(cw->disp);
                EeCvr_Clear(&cw->table);
                /* Release this window's own resource-only AppState (fonts/brushes),
                 * created in App_CreateCvrWindow. Sub-windows above have already
                 * closed, so nothing references it any more. */
                App_FreeCvrUi(cw->app);
                free(cw);
                CvrWindow_Unregister(hwnd);
                SetWindowLongPtrW(hwnd, GWLP_USERDATA, 0);
                App_MaybeQuit();
            }
            return 0;

        default:
            break;
    }
    return DefWindowProcW(hwnd, msg, wParam, lParam);
}

/* Free a CVR window's resource-only AppState (created in App_CreateCvrWindow). */
static void App_FreeCvrUi(AppState *ui)
{
    if (ui == NULL)
    {
        return;
    }
    if (ui->font_ui)
    {
        DeleteObject(ui->font_ui);
    }
    if (ui->font_grid)
    {
        DeleteObject(ui->font_grid);
    }
    if (ui->font_header)
    {
        DeleteObject(ui->font_header);
    }
    if (ui->brush_header)
    {
        DeleteObject(ui->brush_header);
    }
    if (ui->pen_header_line)
    {
        DeleteObject(ui->pen_header_line);
    }
    EeFilter_Clear(&ui->filters);
    EeVoterTable_Clear(&ui->table);
    DeleteCriticalSection(&ui->progress_lock);
    HeapFree(GetProcessHeap(), 0, ui);
}

/* Create a CVR window, taking ownership of @p table (moved; the source is reset
 * to empty). The window is a first-class top-level window that owns its own
 * resource-only AppState (@p app is only used to seed it and for its instance), so
 * it survives the voter window that launched it and keeps the app alive on its own. */
static void App_CreateCvrWindow(AppState *app,
                                EeCvrTable *table,
                                const wchar_t *title,
                                const wchar_t *first_path)
{
    CvrWindow *cw = (CvrWindow *)calloc(1, sizeof(CvrWindow));
    AppState *ui;
    HWND h;
    if (cw == NULL)
    {
        EeCvr_Clear(table);
        MessageBoxW(app->hwnd_main, L"Out of memory.", k_WindowTitle, MB_ICONERROR | MB_OK);
        return;
    }
    /* This window's own resources (fonts, header brush, DPI, instance), independent
     * of the launching voter window's AppState so it can outlive it. */
    ui = (AppState *)HeapAlloc(GetProcessHeap(), HEAP_ZERO_MEMORY, sizeof(AppState));
    if (ui == NULL)
    {
        EeCvr_Clear(table);
        free(cw);
        MessageBoxW(app->hwnd_main, L"Out of memory.", k_WindowTitle, MB_ICONERROR | MB_OK);
        return;
    }
    App_InitViewerState(ui, app->instance, app); /* seed zoom/map prefs from launcher */
    ui->is_cvr_ui = TRUE;
    App_UpdateDpiMetrics(ui, app->dpi ? app->dpi : 96u); /* build fonts/brushes now */

    cw->app = ui;
    cw->table = *table; /* move */
    EeCvr_Init(table);
    cw->sort_col = -1;
    cw->sort_asc = TRUE;
    if (!EeCvr_CardCounts(&cw->table, NULL, 0, &cw->cards))
    {
        cw->cards.rows = cw->cards.ballots = cw->table.nrows;
        cw->cards.continuation = 0;
        cw->cards.method = EE_CVR_CARDS_ONE_PER_ROW;
    }
    cw->multi_card = cw->cards.continuation > 0;
    cw->rcv_count = EeCvr_FindRcvContests(&cw->table, NULL, 0);
    App_BaseNameNoExt(first_path, cw->base_name, ARRAYSIZE(cw->base_name));

    /* Resolve which per-column reports are available once, up front. */
    {
        int k;
        for (k = 0; k < EE_CVRREP_COUNT; k++)
        {
            uint32_t col = 0;
            cw->vreport_avail[k] = Cvr_FindReportColumn(&cw->table, k, &col) &&
                                   EeCvr_ColumnHasReportableData(&cw->table, col);
            cw->vreport_col[k] = col;
        }
    }

    /* Independent top-level window (owner NULL) so the voter list can overlap it,
     * rather than the CVR window always staying above its opener. A live window owns
     * (and destroys) its menu; if creation fails we must destroy it ourselves. */
    {
        HMENU menu = App_CreateCvrMenu();
        h = CreateWindowExW(0,
                            k_CvrClassName,
                            (title != NULL) ? title : L"Cast Vote Records",
                            WS_OVERLAPPEDWINDOW | WS_CLIPCHILDREN,
                            CW_USEDEFAULT,
                            CW_USEDEFAULT,
                            Scale(ui, 900),
                            Scale(ui, 560),
                            NULL,
                            menu,
                            ui->instance,
                            cw);
        if (h == NULL && menu != NULL)
        {
            DestroyMenu(menu);
        }
    }
    if (h == NULL)
    {
        EeCvr_Clear(&cw->table);
        App_FreeCvrUi(ui);
        free(cw);
        MessageBoxW(app->hwnd_main,
                    L"Could not create the CVR window.",
                    k_WindowTitle,
                    MB_ICONERROR | MB_OK);
        return;
    }
    ui->hwnd_main = h; /* the CVR window is this AppState's "main" window */
    CvrWindow_Register(h);
    ShowWindow(h, SW_SHOWNORMAL);
    UpdateWindow(h);
    if (cw->table.load_note != NULL)
    {
        /* e.g. the OCR summary of a scanned Hart PDF: what was repaired / needs review */
        MessageBoxW(h, cw->table.load_note, L"Cast Vote Records Loaded",
                    MB_ICONINFORMATION | MB_OK);
    }
}

/* -------------------------------------------------------------------------- */
/* CVR tabulation report (Phase 2)                                            */
/* -------------------------------------------------------------------------- */

/* Subclass so the report list draws its header bold on the grey band. */
static WNDPROC g_old_cvr_report_list_proc = NULL;

static LRESULT CALLBACK CvrReportListSubclass(HWND hwnd, UINT msg, WPARAM wParam, LPARAM lParam)
{
    if (msg == WM_NOTIFY)
    {
        NMHDR *nm = (NMHDR *)lParam;
        HWND header = ListView_GetHeader(hwnd);
        CvrReportWindow *rw = (CvrReportWindow *)GetWindowLongPtrW(GetParent(hwnd), GWLP_USERDATA);
        if (rw != NULL && nm != NULL && header != NULL && nm->hwndFrom == header &&
            nm->code == NM_CUSTOMDRAW)
        {
            return App_HeaderCustomDraw(rw->app, (NMCUSTOMDRAW *)lParam, FALSE);
        }
    }
    return CallWindowProcW(g_old_cvr_report_list_proc, hwnd, msg, wParam, lParam);
}

static void CvrReport_Layout(CvrReportWindow *rw, int width, int height)
{
    int sb_h = 0;
    int avail;
    int count_w;
    int sel_w;
    int contest_w;
    if (rw->status != NULL)
    {
        RECT sb;
        SendMessageW(rw->status, WM_SIZE, 0, 0);
        if (GetWindowRect(rw->status, &sb))
        {
            sb_h = sb.bottom - sb.top;
        }
    }
    if (rw->list == NULL)
    {
        return;
    }
    height = (height > sb_h) ? height - sb_h : 0;
    MoveWindow(rw->list, 0, 0, width, height, TRUE);

    /* Fit the three columns inside the list's visible width. Reserve the vertical
     * scrollbar so a horizontal scrollbar never appears (it would push the
     * right-aligned Votes column out of view). Contest takes the slack, Selection is
     * ~75% of an even split, and Votes stays a fixed narrow column. */
    avail = width - GetSystemMetrics(SM_CXVSCROLL) - Scale(rw->app, 4);
    if (avail < Scale(rw->app, 220))
    {
        avail = Scale(rw->app, 220);
    }
    count_w = Scale(rw->app, 90);
    sel_w = ((avail - count_w) / 2) * 3 / 4; /* 75% of the previous even split */
    if (sel_w < Scale(rw->app, 90))
    {
        sel_w = Scale(rw->app, 90);
    }
    contest_w = avail - count_w - sel_w;
    if (contest_w < Scale(rw->app, 160))
    {
        contest_w = Scale(rw->app, 160);
    }
    ListView_SetColumnWidth(rw->list, 0, contest_w);
    ListView_SetColumnWidth(rw->list, 1, sel_w);
    ListView_SetColumnWidth(rw->list, 2, count_w);
}

/* Copy the selected rows as tab-separated UTF-8 (Contest, Selection, Count). */
static void CvrReport_CopySelected(CvrReportWindow *rw)
{
    int i;
    size_t total = 0;
    wchar_t *buf;
    wchar_t *p;
    char *utf8 = NULL;
    int u8len;

    if (rw == NULL || rw->list == NULL)
    {
        return;
    }
    i = ListView_GetNextItem(rw->list, -1, LVNI_SELECTED);
    while (i >= 0)
    {
        if ((uint32_t)i < rw->count)
        {
            wchar_t num[16];
            StringCchPrintfW(num, ARRAYSIZE(num), L"%u", rw->items[i].count);
            total += wcslen(rw->items[i].contest) + wcslen(rw->items[i].selection) + wcslen(num) +
                     4; /* 2 tabs + CR + LF */
        }
        i = ListView_GetNextItem(rw->list, i, LVNI_SELECTED);
    }
    if (total == 0)
    {
        return;
    }
    buf = (wchar_t *)malloc((total + 1) * sizeof(wchar_t));
    if (buf == NULL)
    {
        return;
    }
    p = buf;
    i = ListView_GetNextItem(rw->list, -1, LVNI_SELECTED);
    while (i >= 0)
    {
        if ((uint32_t)i < rw->count)
        {
            const wchar_t *fields[3];
            wchar_t num[16];
            int f;
            StringCchPrintfW(num, ARRAYSIZE(num), L"%u", rw->items[i].count);
            fields[0] = rw->items[i].contest;
            fields[1] = rw->items[i].selection;
            fields[2] = num;
            for (f = 0; f < 3; f++)
            {
                const wchar_t *s = fields[f];
                while (*s)
                {
                    *p++ = *s++;
                }
                *p++ = (f < 2) ? L'\t' : L'\r';
            }
            *p++ = L'\n';
        }
        i = ListView_GetNextItem(rw->list, i, LVNI_SELECTED);
    }
    *p = L'\0';

    u8len = WideCharToMultiByte(CP_UTF8, 0, buf, -1, NULL, 0, NULL, NULL);
    if (u8len > 0)
    {
        utf8 = (char *)malloc((size_t)u8len);
        if (utf8 != NULL && WideCharToMultiByte(CP_UTF8, 0, buf, -1, utf8, u8len, NULL, NULL) > 0)
        {
            App_SetClipboardUtf8(rw->hwnd, utf8);
        }
    }
    free(utf8);
    free(buf);
}

/* Right-click: select the row if needed, then a Copy menu. */
static void CvrReport_OnContextMenu(CvrReportWindow *rw, int item, POINT screen)
{
    HMENU m;
    UINT cmd;

    if (rw == NULL || rw->list == NULL)
    {
        return;
    }
    if (item >= 0 && !(ListView_GetItemState(rw->list, item, LVIS_SELECTED) & LVIS_SELECTED))
    {
        ListView_SetItemState(rw->list, -1, 0, LVIS_SELECTED | LVIS_FOCUSED);
        ListView_SetItemState(rw->list,
                              item,
                              LVIS_SELECTED | LVIS_FOCUSED,
                              LVIS_SELECTED | LVIS_FOCUSED);
    }
    m = CreatePopupMenu();
    if (m == NULL)
    {
        return;
    }
    AppendMenuW(m, MF_STRING, IDM_EDIT_COPY, L"&Copy");
    AppendMenuW(m, MF_SEPARATOR, 0, NULL);
    AppendMenuW(m, MF_STRING, IDM_EXPORT_SELECTED, L"Export &Selected…");
    AppendMenuW(m, MF_STRING, IDM_EXPORT_ALL, L"Export &All…");
    cmd = (UINT)
        TrackPopupMenu(m, TPM_RIGHTBUTTON | TPM_RETURNCMD, screen.x, screen.y, 0, rw->hwnd, NULL);
    DestroyMenu(m);
    if (cmd == IDM_EDIT_COPY)
    {
        CvrReport_CopySelected(rw);
    }
    else if (cmd == IDM_EXPORT_SELECTED)
    {
        App_ExportCvrTabReport(rw, TRUE);
    }
    else if (cmd == IDM_EXPORT_ALL)
    {
        App_ExportCvrTabReport(rw, FALSE);
    }
}

static LRESULT CALLBACK CvrReportWndProc(HWND hwnd, UINT msg, WPARAM wParam, LPARAM lParam)
{
    CvrReportWindow *rw = (CvrReportWindow *)GetWindowLongPtrW(hwnd, GWLP_USERDATA);

    switch (msg)
    {
        case WM_CREATE:
        {
            CREATESTRUCTW *cs = (CREATESTRUCTW *)lParam;
            LVCOLUMNW col;
            RECT rc;
            wchar_t st[96];
            rw = (CvrReportWindow *)cs->lpCreateParams;
            SetWindowLongPtrW(hwnd, GWLP_USERDATA, (LONG_PTR)rw);
            rw->hwnd = hwnd;

            GetClientRect(hwnd, &rc);
            rw->list = CreateWindowExW(0,
                                       WC_LISTVIEWW,
                                       L"",
                                       WS_CHILD | WS_VISIBLE | LVS_REPORT | LVS_OWNERDATA |
                                           LVS_SHOWSELALWAYS,
                                       0,
                                       0,
                                       rc.right,
                                       rc.bottom,
                                       hwnd,
                                       NULL,
                                       rw->app->instance,
                                       NULL);
            if (rw->list == NULL)
            {
                return -1;
            }
            {
                WNDPROC old = (WNDPROC)SetWindowLongPtrW(rw->list,
                                                         GWLP_WNDPROC,
                                                         (LONG_PTR)CvrReportListSubclass);
                if (g_old_cvr_report_list_proc == NULL)
                {
                    g_old_cvr_report_list_proc = old;
                }
            }
            ListView_SetExtendedListViewStyle(rw->list,
                                              LVS_EX_FULLROWSELECT | LVS_EX_DOUBLEBUFFER |
                                                  LVS_EX_GRIDLINES);
            if (rw->app->font_ui)
            {
                SendMessageW(rw->list, WM_SETFONT, (WPARAM)rw->app->font_ui, TRUE);
            }
            ZeroMemory(&col, sizeof(col));
            col.mask = LVCF_TEXT | LVCF_WIDTH | LVCF_FMT;
            col.fmt = LVCFMT_LEFT;
            col.pszText = L"Contest";
            col.cx = Scale(rw->app, 300);
            ListView_InsertColumn(rw->list, 0, &col);
            col.fmt = LVCFMT_LEFT;
            col.pszText = L"Selection";
            col.cx = Scale(rw->app, 240);
            ListView_InsertColumn(rw->list, 1, &col);
            col.fmt = LVCFMT_RIGHT;
            col.pszText = L"Votes";
            col.cx = Scale(rw->app, 90);
            ListView_InsertColumn(rw->list, 2, &col);

            rw->status = CreateWindowExW(0,
                                         STATUSCLASSNAMEW,
                                         NULL,
                                         WS_CHILD | WS_VISIBLE | SBARS_SIZEGRIP,
                                         0,
                                         0,
                                         0,
                                         0,
                                         hwnd,
                                         NULL,
                                         rw->app->instance,
                                         NULL);
            if (rw->status != NULL)
            {
                if (rw->app->font_ui)
                {
                    SendMessageW(rw->status, WM_SETFONT, (WPARAM)rw->app->font_ui, TRUE);
                }
                StringCchPrintfW(st, ARRAYSIZE(st), L"%u rows — right-click to copy", rw->count);
                SendMessageW(rw->status, SB_SETTEXTW, 0, (LPARAM)st);
            }

            ListView_SetItemCountEx(rw->list, (int)rw->count, LVSICF_NOINVALIDATEALL);
            CvrReport_Layout(rw, rc.right, rc.bottom);
            return 0;
        }

        case WM_SIZE:
            if (rw != NULL)
            {
                CvrReport_Layout(rw, LOWORD(lParam), HIWORD(lParam));
            }
            return 0;

        case WM_SETFOCUS:
            if (rw != NULL && rw->list != NULL)
            {
                SetFocus(rw->list);
            }
            return 0;

        case WM_COMMAND:
            /* Ctrl+C is routed here by the shared accelerator table. */
            if (rw != NULL && LOWORD(wParam) == IDM_EDIT_COPY)
            {
                CvrReport_CopySelected(rw);
                return 0;
            }
            break;

        case WM_NOTIFY:
        {
            NMHDR *hdr = (NMHDR *)lParam;
            if (rw == NULL || hdr->hwndFrom != rw->list)
            {
                break;
            }
            if (hdr->code == LVN_GETDISPINFOW)
            {
                NMLVDISPINFOW *di = (NMLVDISPINFOW *)lParam;
                int idx = di->item.iItem;
                if ((di->item.mask & LVIF_TEXT) && idx >= 0 && (uint32_t)idx < rw->count)
                {
                    const wchar_t *text = L"";
                    if (di->item.iSubItem == 0)
                    {
                        text = rw->items[idx].contest;
                    }
                    else if (di->item.iSubItem == 1)
                    {
                        text = rw->items[idx].selection;
                    }
                    if (di->item.iSubItem == 2)
                    {
                        StringCchPrintfW(di->item.pszText,
                                         di->item.cchTextMax,
                                         L"%u",
                                         rw->items[idx].count);
                    }
                    else
                    {
                        StringCchCopyW(di->item.pszText, di->item.cchTextMax, text);
                    }
                }
                return 0;
            }
            if (hdr->code == NM_RCLICK)
            {
                LPNMITEMACTIVATE ia = (LPNMITEMACTIVATE)lParam;
                POINT screen = ia->ptAction;
                ClientToScreen(rw->list, &screen);
                CvrReport_OnContextMenu(rw, ia->iItem, screen);
                return 0;
            }
            break;
        }

        case WM_CLOSE:
            DestroyWindow(hwnd);
            return 0;

        case WM_DESTROY:
            if (rw != NULL)
            {
                if (rw->owner != NULL && rw->owner->report == rw)
                {
                    rw->owner->report = NULL;
                }
                EeCvr_FreeTally(rw->items, rw->count);
                free(rw);
                SetWindowLongPtrW(hwnd, GWLP_USERDATA, 0);
            }
            return 0;

        default:
            break;
    }
    return DefWindowProcW(hwnd, msg, wParam, lParam);
}

/* Tabulate the CVR and open (or re-focus) the report window tied to @p cw. */
/* Tabulate for @p rw honoring its filtered mode, the merge-write-ins setting, and the
 * primary party-first ordering. Returns FALSE on OOM (out params left NULL/0). */
static BOOL Cvr_TabulateReport(CvrReportWindow *rw, EeCvrTally **out_items, uint32_t *out_count)
{
    CvrWindow *cw = rw->owner;
    BOOL ok;
    if (rw->filtered && cw->filt_active)
    {
        ok = EeCvr_TabulateRows(&cw->table, cw->disp, cw->disp_count,
                                g_settings.cvr_merge_writeins, out_items, out_count);
    }
    else
    {
        ok = EeCvr_Tabulate(&cw->table, g_settings.cvr_merge_writeins, out_items, out_count);
    }
    if (ok)
    {
        EeCvr_ReorderTallyByParty(*out_items, *out_count, g_settings.cvr_tab_party_first);
    }
    return ok;
}

/* Compose the tabulation window title into @p title. */
static void Cvr_ReportTitle(CvrWindow *cw, BOOL filtered, wchar_t *title, size_t cch)
{
    wchar_t base[128];
    base[0] = L'\0';
    GetWindowTextW(cw->hwnd, base, ARRAYSIZE(base));
    if (base[0] != L'\0')
    {
        StringCchPrintfW(title, cch, filtered ? L"CVR Tabulation (Filtered) — %s"
                                              : L"CVR Tabulation — %s",
                         base);
    }
    else
    {
        StringCchCopyW(title, cch, filtered ? L"CVR Tabulation (Filtered)" : L"CVR Tabulation");
    }
}

/* Re-tabulate an open report (current merge/party settings + its filtered mode) and
 * refresh its list + title. */
static void App_RefreshCvrReport(CvrReportWindow *rw)
{
    EeCvrTally *items = NULL;
    uint32_t count = 0;
    HCURSOR old_cursor;

    if (rw == NULL || rw->owner == NULL)
    {
        return;
    }
    old_cursor = SetCursor(LoadCursorW(NULL, IDC_WAIT));
    if (!Cvr_TabulateReport(rw, &items, &count))
    {
        SetCursor(old_cursor);
        return; /* keep the existing tally on failure */
    }
    SetCursor(old_cursor);
    EeCvr_FreeTally(rw->items, rw->count);
    rw->items = items;
    rw->count = count;
    {
        wchar_t title[MAX_PATH + 64];
        Cvr_ReportTitle(rw->owner, rw->filtered, title, ARRAYSIZE(title));
        SetWindowTextW(rw->hwnd, title);
    }
    if (rw->list != NULL)
    {
        ListView_SetItemCountEx(rw->list, (int)count, LVSICF_NOINVALIDATEALL);
        ListView_RedrawItems(rw->list, 0, (int)count);
        InvalidateRect(rw->list, NULL, TRUE);
    }
    if (rw->status != NULL)
    {
        wchar_t st[96];
        StringCchPrintfW(st, ARRAYSIZE(st), L"%u rows — right-click to copy", count);
        SendMessageW(rw->status, SB_SETTEXTW, 0, (LPARAM)st);
    }
}

static void App_ShowCvrReport(CvrWindow *cw, BOOL filtered)
{
    EeCvrTally *items = NULL;
    uint32_t count = 0;
    CvrReportWindow *rw;
    wchar_t title[MAX_PATH + 64];
    HCURSOR old_cursor;
    RECT pr;
    int x = CW_USEDEFAULT;
    int y = CW_USEDEFAULT;

    if (cw == NULL)
    {
        return;
    }
    if (cw->table.nrows == 0 || cw->table.ncols <= cw->table.frozen_count)
    {
        MessageBoxW(cw->hwnd, L"There are no contest columns to tabulate.",
                    L"Tabulate CVR Votes", MB_ICONINFORMATION | MB_OK);
        return;
    }
    /* One tabulation report per CVR window: if it is already open, just switch it to
     * the requested (all vs filtered) mode, re-tabulate, and bring it forward. */
    if (cw->report != NULL)
    {
        cw->report->filtered = filtered;
        App_RefreshCvrReport(cw->report);
        SetForegroundWindow(cw->report->hwnd);
        return;
    }

    rw = (CvrReportWindow *)calloc(1, sizeof(CvrReportWindow));
    if (rw == NULL)
    {
        return;
    }
    rw->owner = cw;
    rw->app = cw->app;
    rw->filtered = filtered;

    old_cursor = SetCursor(LoadCursorW(NULL, IDC_WAIT));
    if (!Cvr_TabulateReport(rw, &items, &count))
    {
        SetCursor(old_cursor);
        free(rw);
        MessageBoxW(cw->hwnd, L"Out of memory while tabulating the Cast Vote Records.",
                    L"Tabulate CVR Votes", MB_ICONERROR | MB_OK);
        return;
    }
    SetCursor(old_cursor);
    if (count == 0)
    {
        EeCvr_FreeTally(items, count);
        free(rw);
        MessageBoxW(cw->hwnd, L"No selections were found to tabulate.", L"Tabulate CVR Votes",
                    MB_ICONINFORMATION | MB_OK);
        return;
    }
    rw->items = items;
    rw->count = count;

    Cvr_ReportTitle(cw, filtered, title, ARRAYSIZE(title));
    if (GetWindowRect(cw->hwnd, &pr))
    {
        x = pr.left + Scale(cw->app, 48);
        y = pr.top + Scale(cw->app, 48);
    }

    /* Unowned top-level window (like the voter-list reports) so the CVR window can
     * cover it; tracked in cw->report and closed when the CVR window closes. */
    rw->hwnd = CreateWindowExW(0,
                               k_CvrReportClassName,
                               title,
                               WS_OVERLAPPEDWINDOW | WS_CLIPCHILDREN,
                               x,
                               y,
                               Scale(cw->app, 680),
                               Scale(cw->app, 600),
                               NULL,
                               NULL,
                               cw->app->instance,
                               rw);
    if (rw->hwnd == NULL)
    {
        EeCvr_FreeTally(items, count);
        free(rw);
        return;
    }
    cw->report = rw;
    ShowWindow(rw->hwnd, SW_SHOW);
    SetForegroundWindow(rw->hwnd);
}

/* -------------------------------------------------------------------------- */
/* CVR ranked-choice (instant-runoff) rounds report                           */
/* -------------------------------------------------------------------------- */

/* Display rows per contest: one per candidate (in finishing order), then these totals,
 * matching the rows of the official Dominion RCV reports. */
enum
{
    RCVROW_CONTINUING = -1,
    RCVROW_BLANKS = -2,
    RCVROW_EXHAUSTED = -3,
    RCVROW_OVERVOTES = -4,
    RCVROW_NONTRANSFER = -5
};

#define RCV_FIXED_COLS 3 /* Contest | Candidate | Result, then Round 1..N */

static WNDPROC g_old_cvr_rcv_list_proc = NULL;

static void CvrRcv_FreeResults(CvrRcvWindow *rw)
{
    uint32_t i;
    for (i = 0; i < rw->nres; i++)
    {
        EeCvr_FreeRcvResult(&rw->res[i]);
    }
    free(rw->res);
    free(rw->row_res);
    free(rw->row_kind);
    rw->res = NULL;
    rw->row_res = NULL;
    rw->row_kind = NULL;
    rw->nres = 0;
    rw->nrows = 0;
    rw->max_rounds = 0;
}

/* Run the instant runoff for every ranked-choice contest (all rows, or the filtered
 * rows) and build the display rows. Returns FALSE on OOM (rw left empty). */
static BOOL CvrRcv_Compute(CvrRcvWindow *rw)
{
    CvrWindow *cw = rw->owner;
    uint32_t n = EeCvr_FindRcvContests(&cw->table, NULL, 0);
    uint32_t *cols;
    const uint32_t *rows = NULL;
    uint32_t nrows = 0;
    uint32_t i;
    uint32_t total = 0;

    CvrRcv_FreeResults(rw);
    if (n == 0)
    {
        return TRUE;
    }
    if (rw->filtered && cw->filt_active)
    {
        rows = cw->disp;
        nrows = cw->disp_count;
    }
    cols = (uint32_t *)malloc(n * sizeof(uint32_t));
    rw->res = (EeRcvResult *)calloc(n, sizeof(EeRcvResult));
    if (cols == NULL || rw->res == NULL)
    {
        free(cols);
        CvrRcv_FreeResults(rw);
        return FALSE;
    }
    EeCvr_FindRcvContests(&cw->table, cols, n);
    for (i = 0; i < n; i++)
    {
        EeRcvResult *r = &rw->res[i];
        BOOL ok;
        if (rows != NULL)
        {
            ok = EeCvr_TabulateRcv(&cw->table, cols[i], rows, nrows, r);
        }
        else
        {
            ok = EeCvr_TabulateRcv(&cw->table, cols[i], NULL, 0, r);
        }
        if (!ok)
        {
            free(cols);
            CvrRcv_FreeResults(rw);
            return FALSE;
        }
        rw->nres++;
        total += r->ncand + 5;
        if (r->nrounds > rw->max_rounds)
        {
            rw->max_rounds = r->nrounds;
        }
    }
    free(cols);
    rw->row_res = (uint32_t *)malloc((total ? total : 1) * sizeof(uint32_t));
    rw->row_kind = (int32_t *)malloc((total ? total : 1) * sizeof(int32_t));
    if (rw->row_res == NULL || rw->row_kind == NULL)
    {
        CvrRcv_FreeResults(rw);
        return FALSE;
    }
    for (i = 0; i < rw->nres; i++)
    {
        uint32_t c;
        int32_t k;
        for (c = 0; c < rw->res[i].ncand; c++)
        {
            rw->row_res[rw->nrows] = i;
            rw->row_kind[rw->nrows++] = (int32_t)c;
        }
        for (k = RCVROW_CONTINUING; k >= RCVROW_NONTRANSFER; k--)
        {
            rw->row_res[rw->nrows] = i;
            rw->row_kind[rw->nrows++] = k;
        }
    }
    return TRUE;
}

/* Text of display row @p row, column @p col (0 Contest, 1 Candidate, 2 Result, 3.. the
 * rounds). Used for the list, Copy and Export alike. */
static void CvrRcv_CellText(const CvrRcvWindow *rw,
                            uint32_t row,
                            uint32_t col,
                            wchar_t *buf,
                            size_t cch)
{
    const EeRcvResult *r;
    int32_t kind;
    buf[0] = L'\0';
    if (row >= rw->nrows)
    {
        return;
    }
    r = &rw->res[rw->row_res[row]];
    kind = rw->row_kind[row];
    if (col == 0)
    {
        StringCchCopyW(buf, cch, r->contest);
        return;
    }
    if (col == 1)
    {
        static const wchar_t *const k_Totals[] = {L"Continuing Ballots Total",
                                                  L"Blanks",
                                                  L"Exhausted",
                                                  L"Overvotes",
                                                  L"Non Transferable Total"};
        StringCchCopyW(buf, cch, (kind >= 0) ? r->cand[kind] : k_Totals[-kind - 1]);
        return;
    }
    if (col == 2)
    {
        uint32_t k;
        if (kind < 0)
        {
            return;
        }
        if (kind == r->winner)
        {
            if (r->majority_round > 0)
            {
                StringCchPrintfW(buf, cch, L"Winner (majority in round %u)", r->majority_round);
            }
            else
            {
                StringCchCopyW(buf, cch, L"Winner");
            }
            return;
        }
        for (k = 0; k < r->nrounds; k++)
        {
            if (r->eliminated[k] == kind)
            {
                StringCchPrintfW(buf,
                                 cch,
                                 r->elim_tie[k] ? L"Eliminated round %u (tie)"
                                                : L"Eliminated round %u",
                                 k + 1);
                return;
            }
        }
        StringCchCopyW(buf, cch, L"Runner-up");
        return;
    }
    {
        uint32_t rnd = col - RCV_FIXED_COLS;
        uint32_t v;
        if (rnd >= r->nrounds)
        {
            return;
        }
        if (kind >= 0)
        {
            uint32_t k;
            /* Leave a candidate's cells blank after the round they were eliminated. */
            for (k = 0; k < rnd; k++)
            {
                if (r->eliminated[k] == kind)
                {
                    return;
                }
            }
            v = r->votes[(size_t)rnd * r->ncand + (uint32_t)kind];
        }
        else if (kind == RCVROW_CONTINUING)
        {
            v = r->continuing[rnd];
        }
        else if (kind == RCVROW_BLANKS)
        {
            v = r->blanks[rnd];
        }
        else if (kind == RCVROW_EXHAUSTED)
        {
            v = r->exhausted[rnd];
        }
        else if (kind == RCVROW_OVERVOTES)
        {
            v = r->overvotes[rnd];
        }
        else
        {
            v = r->blanks[rnd] + r->exhausted[rnd] + r->overvotes[rnd];
        }
        StringCchPrintfW(buf, cch, L"%u", v);
    }
}

static LRESULT CALLBACK CvrRcvListSubclass(HWND hwnd, UINT msg, WPARAM wParam, LPARAM lParam)
{
    if (msg == WM_NOTIFY)
    {
        NMHDR *nm = (NMHDR *)lParam;
        HWND header = ListView_GetHeader(hwnd);
        CvrRcvWindow *rw = (CvrRcvWindow *)GetWindowLongPtrW(GetParent(hwnd), GWLP_USERDATA);
        if (rw != NULL && nm != NULL && header != NULL && nm->hwndFrom == header &&
            nm->code == NM_CUSTOMDRAW)
        {
            return App_HeaderCustomDraw(rw->app, (NMCUSTOMDRAW *)lParam, FALSE);
        }
    }
    return CallWindowProcW(g_old_cvr_rcv_list_proc, hwnd, msg, wParam, lParam);
}

/* (Re)build the list columns: Contest | Candidate | Result | Round 1..max_rounds. */
static void CvrRcv_BuildColumns(CvrRcvWindow *rw)
{
    LVCOLUMNW col;
    uint32_t i;
    HWND header = ListView_GetHeader(rw->list);
    int existing = (header != NULL) ? Header_GetItemCount(header) : 0;
    while (existing-- > 0)
    {
        ListView_DeleteColumn(rw->list, 0);
    }
    ZeroMemory(&col, sizeof(col));
    col.mask = LVCF_TEXT | LVCF_WIDTH | LVCF_FMT;
    col.fmt = LVCFMT_LEFT;
    col.pszText = L"Contest";
    col.cx = Scale(rw->app, 230);
    ListView_InsertColumn(rw->list, 0, &col);
    col.pszText = L"Candidate";
    col.cx = Scale(rw->app, 210);
    ListView_InsertColumn(rw->list, 1, &col);
    col.pszText = L"Result";
    col.cx = Scale(rw->app, 190);
    ListView_InsertColumn(rw->list, 2, &col);
    col.fmt = LVCFMT_RIGHT;
    col.cx = Scale(rw->app, 78);
    for (i = 0; i < rw->max_rounds; i++)
    {
        wchar_t title[24];
        StringCchPrintfW(title, ARRAYSIZE(title), L"Round %u", i + 1);
        col.pszText = title;
        ListView_InsertColumn(rw->list, (int)(RCV_FIXED_COLS + i), &col);
    }
}

static void CvrRcv_UpdateStatus(CvrRcvWindow *rw)
{
    wchar_t st[160];
    if (rw->status == NULL)
    {
        return;
    }
    StringCchPrintfW(st,
                     ARRAYSIZE(st),
                     L"%u ranked-choice contest%s — instant runoff, single elimination — "
                     L"right-click to copy",
                     rw->nres,
                     rw->nres == 1 ? L"" : L"s");
    SendMessageW(rw->status, SB_SETTEXTW, 0, (LPARAM)st);
}

static void CvrRcv_Layout(CvrRcvWindow *rw, int width, int height)
{
    int sb_h = 0;
    if (rw->status != NULL)
    {
        RECT sb;
        SendMessageW(rw->status, WM_SIZE, 0, 0);
        if (GetWindowRect(rw->status, &sb))
        {
            sb_h = sb.bottom - sb.top;
        }
    }
    if (rw->list != NULL)
    {
        MoveWindow(rw->list, 0, 0, width, (height > sb_h) ? height - sb_h : 0, TRUE);
    }
}

/* Copy the selected rows as tab-separated UTF-8 (every column). */
static void CvrRcv_CopySelected(CvrRcvWindow *rw)
{
    Utf8Export ex;
    int i;
    BOOL ok = TRUE;
    uint32_t ncols = RCV_FIXED_COLS + rw->max_rounds;

    if (rw == NULL || rw->list == NULL)
    {
        return;
    }
    ZeroMemory(&ex, sizeof(ex));
    i = ListView_GetNextItem(rw->list, -1, LVNI_SELECTED);
    while (i >= 0 && ok)
    {
        uint32_t c;
        for (c = 0; c < ncols && ok; c++)
        {
            wchar_t cell[512];
            CvrRcv_CellText(rw, (uint32_t)i, c, cell, ARRAYSIZE(cell));
            if (c > 0)
            {
                ok = Utf8Export_AppendChar(&ex, '\t');
            }
            ok = ok && Utf8Export_AppendFieldW(&ex, cell, '\t');
        }
        ok = ok && Utf8Export_Append(&ex, "\r\n", 2);
        i = ListView_GetNextItem(rw->list, i, LVNI_SELECTED);
    }
    if (ok && ex.data != NULL && Utf8Export_AppendChar(&ex, '\0'))
    {
        App_SetClipboardUtf8(rw->hwnd, ex.data);
    }
    free(ex.data);
}

/* Cell provider for the RCV report export. */
typedef struct CvrRcvExportCtx
{
    CvrRcvWindow *rw;
    const uint32_t *idx;
} CvrRcvExportCtx;

static void CvrRcv_ExportCell(void *user, uint32_t row, uint32_t col, wchar_t *buf, size_t cch)
{
    CvrRcvExportCtx *c = (CvrRcvExportCtx *)user;
    CvrRcv_CellText(c->rw, (c->idx != NULL) ? c->idx[row] : row, col, buf, cch);
}

static void App_ExportCvrRcvReport(CvrRcvWindow *rw, BOOL selection_only)
{
    uint32_t ncols;
    wchar_t **headers = NULL;
    wchar_t suggested[256];
    uint32_t *idx = NULL;
    uint32_t n = 0;
    uint32_t i;
    CvrRcvExportCtx ctx;

    if (rw == NULL || rw->owner == NULL || rw->nrows == 0)
    {
        return;
    }
    if (selection_only)
    {
        idx = App_CollectSelectedIndices(rw->list, rw->nrows, &n);
        if (n == 0)
        {
            free(idx);
            return;
        }
    }
    else
    {
        n = rw->nrows;
    }
    ncols = RCV_FIXED_COLS + rw->max_rounds;
    headers = (wchar_t **)calloc(ncols, sizeof(wchar_t *));
    if (headers == NULL)
    {
        free(idx);
        return;
    }
    for (i = 0; i < ncols; i++)
    {
        headers[i] = (wchar_t *)malloc(24 * sizeof(wchar_t));
        if (headers[i] == NULL)
        {
            goto done;
        }
        if (i < RCV_FIXED_COLS)
        {
            static const wchar_t *const k_Fixed[RCV_FIXED_COLS] = {L"Contest",
                                                                   L"Candidate",
                                                                   L"Result"};
            StringCchCopyW(headers[i], 24, k_Fixed[i]);
        }
        else
        {
            StringCchPrintfW(headers[i], 24, L"Round %u", i - RCV_FIXED_COLS + 1);
        }
    }
    ctx.rw = rw;
    ctx.idx = idx;
    StringCchPrintfW(suggested,
                     ARRAYSIZE(suggested),
                     L"%s-%s_RCV_Rounds",
                     rw->owner->base_name,
                     selection_only ? L"Selected" : L"All");
    App_ExportReportModel(rw->hwnd,
                          suggested,
                          ncols,
                          (const wchar_t *const *)headers,
                          n,
                          CvrRcv_ExportCell,
                          &ctx);
done:
    for (i = 0; i < ncols; i++)
    {
        free(headers[i]);
    }
    free(headers);
    free(idx);
}

static void CvrRcv_OnContextMenu(CvrRcvWindow *rw, int item, POINT screen)
{
    HMENU m;
    UINT cmd;

    if (rw == NULL || rw->list == NULL)
    {
        return;
    }
    if (item >= 0 && !(ListView_GetItemState(rw->list, item, LVIS_SELECTED) & LVIS_SELECTED))
    {
        ListView_SetItemState(rw->list, -1, 0, LVIS_SELECTED | LVIS_FOCUSED);
        ListView_SetItemState(rw->list,
                              item,
                              LVIS_SELECTED | LVIS_FOCUSED,
                              LVIS_SELECTED | LVIS_FOCUSED);
    }
    m = CreatePopupMenu();
    if (m == NULL)
    {
        return;
    }
    AppendMenuW(m, MF_STRING, IDM_EDIT_COPY, L"&Copy");
    AppendMenuW(m, MF_SEPARATOR, 0, NULL);
    AppendMenuW(m, MF_STRING, IDM_EXPORT_SELECTED, L"Export &Selected…");
    AppendMenuW(m, MF_STRING, IDM_EXPORT_ALL, L"Export &All…");
    cmd = (UINT)
        TrackPopupMenu(m, TPM_RIGHTBUTTON | TPM_RETURNCMD, screen.x, screen.y, 0, rw->hwnd, NULL);
    DestroyMenu(m);
    if (cmd == IDM_EDIT_COPY)
    {
        CvrRcv_CopySelected(rw);
    }
    else if (cmd == IDM_EXPORT_SELECTED)
    {
        App_ExportCvrRcvReport(rw, TRUE);
    }
    else if (cmd == IDM_EXPORT_ALL)
    {
        App_ExportCvrRcvReport(rw, FALSE);
    }
}

static LRESULT CALLBACK CvrRcvWndProc(HWND hwnd, UINT msg, WPARAM wParam, LPARAM lParam)
{
    CvrRcvWindow *rw = (CvrRcvWindow *)GetWindowLongPtrW(hwnd, GWLP_USERDATA);

    switch (msg)
    {
        case WM_CREATE:
        {
            CREATESTRUCTW *cs = (CREATESTRUCTW *)lParam;
            RECT rc;
            rw = (CvrRcvWindow *)cs->lpCreateParams;
            SetWindowLongPtrW(hwnd, GWLP_USERDATA, (LONG_PTR)rw);
            rw->hwnd = hwnd;

            GetClientRect(hwnd, &rc);
            rw->list = CreateWindowExW(0,
                                       WC_LISTVIEWW,
                                       L"",
                                       WS_CHILD | WS_VISIBLE | LVS_REPORT | LVS_OWNERDATA |
                                           LVS_SHOWSELALWAYS,
                                       0,
                                       0,
                                       rc.right,
                                       rc.bottom,
                                       hwnd,
                                       NULL,
                                       rw->app->instance,
                                       NULL);
            if (rw->list == NULL)
            {
                return -1;
            }
            {
                WNDPROC old = (WNDPROC)SetWindowLongPtrW(rw->list,
                                                         GWLP_WNDPROC,
                                                         (LONG_PTR)CvrRcvListSubclass);
                if (g_old_cvr_rcv_list_proc == NULL)
                {
                    g_old_cvr_rcv_list_proc = old;
                }
            }
            ListView_SetExtendedListViewStyle(rw->list,
                                              LVS_EX_FULLROWSELECT | LVS_EX_DOUBLEBUFFER |
                                                  LVS_EX_GRIDLINES);
            if (rw->app->font_ui)
            {
                SendMessageW(rw->list, WM_SETFONT, (WPARAM)rw->app->font_ui, TRUE);
            }
            CvrRcv_BuildColumns(rw);

            rw->status = CreateWindowExW(0,
                                         STATUSCLASSNAMEW,
                                         NULL,
                                         WS_CHILD | WS_VISIBLE | SBARS_SIZEGRIP,
                                         0,
                                         0,
                                         0,
                                         0,
                                         hwnd,
                                         NULL,
                                         rw->app->instance,
                                         NULL);
            if (rw->status != NULL && rw->app->font_ui)
            {
                SendMessageW(rw->status, WM_SETFONT, (WPARAM)rw->app->font_ui, TRUE);
            }
            CvrRcv_UpdateStatus(rw);
            ListView_SetItemCountEx(rw->list, (int)rw->nrows, LVSICF_NOINVALIDATEALL);
            CvrRcv_Layout(rw, rc.right, rc.bottom);
            return 0;
        }

        case WM_SIZE:
            if (rw != NULL)
            {
                CvrRcv_Layout(rw, LOWORD(lParam), HIWORD(lParam));
            }
            return 0;

        case WM_SETFOCUS:
            if (rw != NULL && rw->list != NULL)
            {
                SetFocus(rw->list);
            }
            return 0;

        case WM_COMMAND:
            /* Ctrl+C is routed here by the shared accelerator table. */
            if (rw != NULL && LOWORD(wParam) == IDM_EDIT_COPY)
            {
                CvrRcv_CopySelected(rw);
                return 0;
            }
            break;

        case WM_NOTIFY:
        {
            NMHDR *hdr = (NMHDR *)lParam;
            if (rw == NULL || hdr->hwndFrom != rw->list)
            {
                break;
            }
            if (hdr->code == LVN_GETDISPINFOW)
            {
                NMLVDISPINFOW *di = (NMLVDISPINFOW *)lParam;
                if ((di->item.mask & LVIF_TEXT) && di->item.iItem >= 0 && di->item.iSubItem >= 0)
                {
                    CvrRcv_CellText(rw,
                                    (uint32_t)di->item.iItem,
                                    (uint32_t)di->item.iSubItem,
                                    di->item.pszText,
                                    (size_t)di->item.cchTextMax);
                }
                return 0;
            }
            if (hdr->code == NM_RCLICK)
            {
                LPNMITEMACTIVATE ia = (LPNMITEMACTIVATE)lParam;
                POINT screen = ia->ptAction;
                ClientToScreen(rw->list, &screen);
                CvrRcv_OnContextMenu(rw, ia->iItem, screen);
                return 0;
            }
            break;
        }

        case WM_CLOSE:
            DestroyWindow(hwnd);
            return 0;

        case WM_DESTROY:
            if (rw != NULL)
            {
                if (rw->owner != NULL && rw->owner->rcv_report == rw)
                {
                    rw->owner->rcv_report = NULL;
                }
                CvrRcv_FreeResults(rw);
                free(rw);
                SetWindowLongPtrW(hwnd, GWLP_USERDATA, 0);
            }
            return 0;

        default:
            break;
    }
    return DefWindowProcW(hwnd, msg, wParam, lParam);
}

static void Cvr_RcvReportTitle(CvrWindow *cw, BOOL filtered, wchar_t *title, size_t cch)
{
    wchar_t base[128];
    base[0] = L'\0';
    GetWindowTextW(cw->hwnd, base, ARRAYSIZE(base));
    if (base[0] != L'\0')
    {
        StringCchPrintfW(title,
                         cch,
                         filtered ? L"RCV Tabulation (Filtered) — %s" : L"RCV Tabulation — %s",
                         base);
    }
    else
    {
        StringCchCopyW(title, cch, filtered ? L"RCV Tabulation (Filtered)" : L"RCV Tabulation");
    }
}

/* Run the ranked-choice tabulation and open (or refresh + re-focus) the RCV report tied
 * to @p cw. */
static void App_ShowCvrRcvReport(CvrWindow *cw, BOOL filtered)
{
    CvrRcvWindow *rw;
    wchar_t title[MAX_PATH + 64];
    HCURSOR old_cursor;
    RECT pr;
    int x = CW_USEDEFAULT;
    int y = CW_USEDEFAULT;
    BOOL ok;

    if (cw == NULL)
    {
        return;
    }
    if (cw->rcv_count == 0)
    {
        MessageBoxW(cw->hwnd,
                    L"These Cast Vote Records have no ranked-choice contests.",
                    L"Tabulate Ranked-Choice Contests",
                    MB_ICONINFORMATION | MB_OK);
        return;
    }
    rw = cw->rcv_report;
    if (rw == NULL)
    {
        rw = (CvrRcvWindow *)calloc(1, sizeof(CvrRcvWindow));
        if (rw == NULL)
        {
            return;
        }
        rw->owner = cw;
        rw->app = cw->app;
    }
    rw->filtered = filtered;
    old_cursor = SetCursor(LoadCursorW(NULL, IDC_WAIT));
    ok = CvrRcv_Compute(rw);
    SetCursor(old_cursor);
    if (!ok)
    {
        MessageBoxW(cw->hwnd,
                    L"Out of memory while tabulating the ranked-choice contests.",
                    L"Tabulate Ranked-Choice Contests",
                    MB_ICONERROR | MB_OK);
        if (cw->rcv_report == NULL)
        {
            free(rw);
        }
        else
        {
            ListView_SetItemCountEx(rw->list, 0, 0);
        }
        return;
    }
    Cvr_RcvReportTitle(cw, filtered, title, ARRAYSIZE(title));

    if (cw->rcv_report != NULL)
    {
        /* Already open: show the new results in place. */
        SetWindowTextW(rw->hwnd, title);
        CvrRcv_BuildColumns(rw);
        ListView_SetItemCountEx(rw->list, (int)rw->nrows, 0);
        InvalidateRect(rw->list, NULL, TRUE);
        CvrRcv_UpdateStatus(rw);
        SetForegroundWindow(rw->hwnd);
        return;
    }

    if (GetWindowRect(cw->hwnd, &pr))
    {
        x = pr.left + Scale(cw->app, 64);
        y = pr.top + Scale(cw->app, 64);
    }
    /* Unowned top-level window like the tabulation report; tracked in cw->rcv_report and
     * closed when the CVR window closes. */
    rw->hwnd = CreateWindowExW(0,
                               k_CvrRcvClassName,
                               title,
                               WS_OVERLAPPEDWINDOW | WS_CLIPCHILDREN,
                               x,
                               y,
                               Scale(cw->app, 1000),
                               Scale(cw->app, 600),
                               NULL,
                               NULL,
                               cw->app->instance,
                               rw);
    if (rw->hwnd == NULL)
    {
        CvrRcv_FreeResults(rw);
        free(rw);
        return;
    }
    cw->rcv_report = rw;
    ShowWindow(rw->hwnd, SW_SHOW);
    SetForegroundWindow(rw->hwnd);
}

/* -------------------------------------------------------------------------- */
/* CVR per-column value reports (Batch / Precinct / Ballot Style)             */
/* -------------------------------------------------------------------------- */

static WNDPROC g_old_cvr_vreport_list_proc = NULL;

/* Draw the report list header bold on the grey band, like the other windows. */
static LRESULT CALLBACK CvrValueReportListSubclass(HWND hwnd, UINT msg, WPARAM wParam, LPARAM lParam)
{
    if (msg == WM_NOTIFY)
    {
        NMHDR *nm = (NMHDR *)lParam;
        HWND header = ListView_GetHeader(hwnd);
        CvrValueReportWindow *rw =
            (CvrValueReportWindow *)GetWindowLongPtrW(GetParent(hwnd), GWLP_USERDATA);
        if (rw != NULL && nm != NULL && header != NULL && nm->hwndFrom == header &&
            nm->code == NM_CUSTOMDRAW)
        {
            return App_HeaderCustomDraw(rw->app, (NMCUSTOMDRAW *)lParam, FALSE);
        }
    }
    return CallWindowProcW(g_old_cvr_vreport_list_proc, hwnd, msg, wParam, lParam);
}

static void CvrValueReport_Layout(CvrValueReportWindow *rw, int width, int height)
{
    int sb_h = 0;
    int avail;
    int count_w;
    int value_w;
    if (rw->status != NULL)
    {
        RECT sb;
        SendMessageW(rw->status, WM_SIZE, 0, 0);
        if (GetWindowRect(rw->status, &sb))
        {
            sb_h = sb.bottom - sb.top;
        }
    }
    if (rw->list == NULL)
    {
        return;
    }
    height = (height > sb_h) ? height - sb_h : 0;
    MoveWindow(rw->list, 0, 0, width, height, TRUE);

    /* Reserve the vertical scrollbar so no horizontal scrollbar hides the count. */
    avail = width - GetSystemMetrics(SM_CXVSCROLL) - Scale(rw->app, 4);
    if (avail < Scale(rw->app, 240))
    {
        avail = Scale(rw->app, 240);
    }
    count_w = Scale(rw->app, 170);
    value_w = avail - count_w;
    if (value_w < Scale(rw->app, 140))
    {
        value_w = Scale(rw->app, 140);
    }
    ListView_SetColumnWidth(rw->list, 0, value_w);
    ListView_SetColumnWidth(rw->list, 1, count_w);
}

typedef struct CvrValueSortCtx
{
    int sort_col;
    BOOL asc;
    BOOL numeric;
} CvrValueSortCtx;

static int __cdecl cvr_vreport_sort_cmp(void *ctxv, const void *a, const void *b)
{
    const CvrValueSortCtx *ctx = (const CvrValueSortCtx *)ctxv;
    const EeCvrValueCount *pa = (const EeCvrValueCount *)a;
    const EeCvrValueCount *pb = (const EeCvrValueCount *)b;
    const wchar_t *va = pa->value ? pa->value : L"";
    const wchar_t *vb = pb->value ? pb->value : L"";
    int c;

    if (ctx->sort_col == 1)
    {
        c = (pa->count < pb->count) ? -1 : (pa->count > pb->count ? 1 : 0);
        if (c == 0)
        {
            c = _wcsicmp(va, vb);
        }
    }
    else if (ctx->numeric)
    {
        unsigned long na = wcstoul(va, NULL, 10);
        unsigned long nb = wcstoul(vb, NULL, 10);
        c = (na < nb) ? -1 : (na > nb ? 1 : 0);
        if (c == 0)
        {
            c = _wcsicmp(va, vb);
        }
    }
    else
    {
        c = _wcsicmp(va, vb);
    }
    return ctx->asc ? c : -c;
}

static void CvrValueReport_Sort(CvrValueReportWindow *rw)
{
    CvrValueSortCtx ctx;
    ctx.sort_col = rw->sort_col;
    ctx.asc = rw->sort_asc;
    ctx.numeric = rw->numeric;
    if (rw->items != NULL && rw->count > 1)
    {
        qsort_s(rw->items, rw->count, sizeof(EeCvrValueCount), cvr_vreport_sort_cmp, &ctx);
    }
    if (rw->list != NULL)
    {
        ListView_RedrawItems(rw->list, 0, (int)rw->count);
        InvalidateRect(rw->list, NULL, FALSE);
    }
}

/* Copy the selected rows as tab-separated UTF-8 (value, count). */
static void CvrValueReport_CopySelected(CvrValueReportWindow *rw)
{
    int i;
    size_t total = 0;
    wchar_t *buf;
    wchar_t *p;
    char *utf8 = NULL;
    int u8len;

    if (rw == NULL || rw->list == NULL)
    {
        return;
    }
    i = ListView_GetNextItem(rw->list, -1, LVNI_SELECTED);
    while (i >= 0)
    {
        if ((uint32_t)i < rw->count)
        {
            const wchar_t *v = report_display_value(rw->items[i].value);
            wchar_t num[16];
            StringCchPrintfW(num, ARRAYSIZE(num), L"%u", rw->items[i].count);
            total += wcslen(v) + wcslen(num) + 3; /* tab + CR + LF */
        }
        i = ListView_GetNextItem(rw->list, i, LVNI_SELECTED);
    }
    if (total == 0)
    {
        return;
    }
    buf = (wchar_t *)malloc((total + 1) * sizeof(wchar_t));
    if (buf == NULL)
    {
        return;
    }
    p = buf;
    i = ListView_GetNextItem(rw->list, -1, LVNI_SELECTED);
    while (i >= 0)
    {
        if ((uint32_t)i < rw->count)
        {
            const wchar_t *v = report_display_value(rw->items[i].value);
            const wchar_t *n;
            wchar_t num[16];
            StringCchPrintfW(num, ARRAYSIZE(num), L"%u", rw->items[i].count);
            while (*v)
            {
                *p++ = *v++;
            }
            *p++ = L'\t';
            for (n = num; *n; n++)
            {
                *p++ = *n;
            }
            *p++ = L'\r';
            *p++ = L'\n';
        }
        i = ListView_GetNextItem(rw->list, i, LVNI_SELECTED);
    }
    *p = L'\0';

    u8len = WideCharToMultiByte(CP_UTF8, 0, buf, -1, NULL, 0, NULL, NULL);
    if (u8len > 0)
    {
        utf8 = (char *)malloc((size_t)u8len);
        if (utf8 != NULL && WideCharToMultiByte(CP_UTF8, 0, buf, -1, utf8, u8len, NULL, NULL) > 0)
        {
            App_SetClipboardUtf8(rw->hwnd, utf8);
        }
    }
    free(utf8);
    free(buf);
}

/* Add an "is" Include/Exclude rule for each selected value to the CVR window's
 * filter, then re-apply it (matches the voter report's Include/Exclude). */
static void CvrValueReport_FilterSelected(CvrValueReportWindow *rw, EeFilterAction action)
{
    int i;
    BOOL any = FALSE;

    if (rw == NULL || rw->list == NULL || rw->owner == NULL)
    {
        return;
    }
    i = ListView_GetNextItem(rw->list, -1, LVNI_SELECTED);
    while (i >= 0)
    {
        if ((uint32_t)i < rw->count && rw->items[i].value != NULL)
        {
            EeFilterRule r;
            ZeroMemory(&r, sizeof(r));
            r.column = rw->column;
            r.relation = EeRel_Is;
            r.action = action;
            r.enabled = TRUE;
            /* Empty value -> "is (blank)"; Cvr_RuleMatches compares against "". */
            StringCchCopyW(r.value, ARRAYSIZE(r.value), rw->items[i].value);
            if (EeFilter_Add(&rw->owner->filters, &r))
            {
                any = TRUE;
            }
        }
        i = ListView_GetNextItem(rw->list, i, LVNI_SELECTED);
    }
    if (any)
    {
        Cvr_ApplyFilter(rw->owner);
    }
}

static void CvrValueReport_OnContextMenu(CvrValueReportWindow *rw, int item, POINT screen)
{
    HMENU m;
    UINT cmd;

    if (rw == NULL || rw->list == NULL || item < 0 || (uint32_t)item >= rw->count)
    {
        return;
    }
    if (!(ListView_GetItemState(rw->list, item, LVIS_SELECTED) & LVIS_SELECTED))
    {
        ListView_SetItemState(rw->list, -1, 0, LVIS_SELECTED | LVIS_FOCUSED);
        ListView_SetItemState(rw->list,
                              item,
                              LVIS_SELECTED | LVIS_FOCUSED,
                              LVIS_SELECTED | LVIS_FOCUSED);
    }
    m = CreatePopupMenu();
    if (m == NULL)
    {
        return;
    }
    {
        wchar_t shown[48];
        wchar_t inc[96];
        wchar_t exc[96];
        const wchar_t *val = report_display_value(rw->items[item].value);
        StringCchCopyW(shown, ARRAYSIZE(shown), val);
        if (wcslen(val) >= 40)
        {
            shown[36] = L'.';
            shown[37] = L'.';
            shown[38] = L'.';
            shown[39] = L'\0';
        }
        StringCchPrintfW(inc, ARRAYSIZE(inc), L"&Include \"%s\"", shown);
        StringCchPrintfW(exc, ARRAYSIZE(exc), L"&Exclude \"%s\"", shown);
        AppendMenuW(m, MF_STRING, IDM_EDIT_COPY, L"&Copy");
        AppendMenuW(m, MF_SEPARATOR, 0, NULL);
        AppendMenuW(m, MF_STRING, IDM_FILTER_INCLUDE, inc);
        AppendMenuW(m, MF_STRING, IDM_FILTER_EXCLUDE, exc);
        AppendMenuW(m, MF_SEPARATOR, 0, NULL);
        AppendMenuW(m, MF_STRING, IDM_EXPORT_SELECTED, L"Export &Selected…");
        AppendMenuW(m, MF_STRING, IDM_EXPORT_ALL, L"Export &All…");
    }
    cmd = (UINT)
        TrackPopupMenu(m, TPM_RIGHTBUTTON | TPM_RETURNCMD, screen.x, screen.y, 0, rw->hwnd, NULL);
    DestroyMenu(m);
    switch (cmd)
    {
        case IDM_EDIT_COPY:
            CvrValueReport_CopySelected(rw);
            break;
        case IDM_EXPORT_SELECTED:
            App_ExportCvrValueReport(rw, TRUE);
            break;
        case IDM_EXPORT_ALL:
            App_ExportCvrValueReport(rw, FALSE);
            break;
        case IDM_FILTER_INCLUDE:
            CvrValueReport_FilterSelected(rw, EeFilt_Include);
            break;
        case IDM_FILTER_EXCLUDE:
            CvrValueReport_FilterSelected(rw, EeFilt_Exclude);
            break;
        default:
            break;
    }
}

static LRESULT CALLBACK CvrValueReportWndProc(HWND hwnd, UINT msg, WPARAM wParam, LPARAM lParam)
{
    CvrValueReportWindow *rw = (CvrValueReportWindow *)GetWindowLongPtrW(hwnd, GWLP_USERDATA);

    switch (msg)
    {
        case WM_CREATE:
        {
            CREATESTRUCTW *cs = (CREATESTRUCTW *)lParam;
            LVCOLUMNW col;
            RECT rc;
            wchar_t hdr[64];
            wchar_t st[96];
            rw = (CvrValueReportWindow *)cs->lpCreateParams;
            SetWindowLongPtrW(hwnd, GWLP_USERDATA, (LONG_PTR)rw);
            rw->hwnd = hwnd;

            GetClientRect(hwnd, &rc);
            rw->list = CreateWindowExW(0,
                                       WC_LISTVIEWW,
                                       L"",
                                       WS_CHILD | WS_VISIBLE | LVS_REPORT | LVS_OWNERDATA |
                                           LVS_SHOWSELALWAYS,
                                       0,
                                       0,
                                       rc.right,
                                       rc.bottom,
                                       hwnd,
                                       NULL,
                                       rw->app->instance,
                                       NULL);
            if (rw->list == NULL)
            {
                return -1;
            }
            {
                WNDPROC old = (WNDPROC)SetWindowLongPtrW(rw->list,
                                                         GWLP_WNDPROC,
                                                         (LONG_PTR)CvrValueReportListSubclass);
                if (g_old_cvr_vreport_list_proc == NULL)
                {
                    g_old_cvr_vreport_list_proc = old;
                }
            }
            ListView_SetExtendedListViewStyle(rw->list,
                                              LVS_EX_FULLROWSELECT | LVS_EX_DOUBLEBUFFER |
                                                  LVS_EX_GRIDLINES);
            if (rw->app->font_ui)
            {
                SendMessageW(rw->list, WM_SETFONT, (WPARAM)rw->app->font_ui, TRUE);
            }
            ZeroMemory(&col, sizeof(col));
            col.mask = LVCF_TEXT | LVCF_WIDTH | LVCF_FMT;
            col.fmt = LVCFMT_LEFT;
            StringCchCopyW(hdr, ARRAYSIZE(hdr), rw->label);
            col.pszText = hdr;
            col.cx = Scale(rw->app, 260);
            ListView_InsertColumn(rw->list, 0, &col);
            col.fmt = LVCFMT_RIGHT;
            col.pszText = L"Number of Ballot Records";
            col.cx = Scale(rw->app, 170);
            ListView_InsertColumn(rw->list, 1, &col);

            rw->status = CreateWindowExW(0,
                                         STATUSCLASSNAMEW,
                                         NULL,
                                         WS_CHILD | WS_VISIBLE | SBARS_SIZEGRIP,
                                         0,
                                         0,
                                         0,
                                         0,
                                         hwnd,
                                         NULL,
                                         rw->app->instance,
                                         NULL);
            if (rw->status != NULL)
            {
                if (rw->app->font_ui)
                {
                    SendMessageW(rw->status, WM_SETFONT, (WPARAM)rw->app->font_ui, TRUE);
                }
                StringCchPrintfW(st,
                                 ARRAYSIZE(st),
                                 L"%u rows — right-click to Copy or Include/Exclude",
                                 rw->count);
                SendMessageW(rw->status, SB_SETTEXTW, 0, (LPARAM)st);
            }

            ListView_SetItemCountEx(rw->list, (int)rw->count, LVSICF_NOINVALIDATEALL);
            CvrValueReport_Sort(rw); /* initial ascending by value */
            CvrValueReport_Layout(rw, rc.right, rc.bottom);
            return 0;
        }

        case WM_SIZE:
            if (rw != NULL)
            {
                CvrValueReport_Layout(rw, LOWORD(lParam), HIWORD(lParam));
            }
            return 0;

        case WM_SETFOCUS:
            if (rw != NULL && rw->list != NULL)
            {
                SetFocus(rw->list);
            }
            return 0;

        case WM_COMMAND:
            /* Ctrl+C is routed here by the shared accelerator table. */
            if (rw != NULL && LOWORD(wParam) == IDM_EDIT_COPY)
            {
                CvrValueReport_CopySelected(rw);
                return 0;
            }
            break;

        case WM_NOTIFY:
        {
            NMHDR *hdr = (NMHDR *)lParam;
            if (rw == NULL || hdr->hwndFrom != rw->list)
            {
                break;
            }
            if (hdr->code == LVN_GETDISPINFOW)
            {
                NMLVDISPINFOW *di = (NMLVDISPINFOW *)lParam;
                int idx = di->item.iItem;
                if ((di->item.mask & LVIF_TEXT) && idx >= 0 && (uint32_t)idx < rw->count)
                {
                    if (di->item.iSubItem == 1)
                    {
                        StringCchPrintfW(di->item.pszText,
                                         di->item.cchTextMax,
                                         L"%u",
                                         rw->items[idx].count);
                    }
                    else
                    {
                        StringCchCopyW(di->item.pszText,
                                       di->item.cchTextMax,
                                       report_display_value(rw->items[idx].value));
                    }
                }
                return 0;
            }
            if (hdr->code == LVN_COLUMNCLICK)
            {
                NMLISTVIEW *nlv = (NMLISTVIEW *)lParam;
                if (nlv->iSubItem == rw->sort_col)
                {
                    rw->sort_asc = !rw->sort_asc;
                }
                else
                {
                    rw->sort_col = nlv->iSubItem;
                    rw->sort_asc = TRUE;
                }
                CvrValueReport_Sort(rw);
                return 0;
            }
            if (hdr->code == NM_RCLICK)
            {
                LPNMITEMACTIVATE ia = (LPNMITEMACTIVATE)lParam;
                POINT screen = ia->ptAction;
                ClientToScreen(rw->list, &screen);
                CvrValueReport_OnContextMenu(rw, ia->iItem, screen);
                return 0;
            }
            break;
        }

        case WM_CLOSE:
            DestroyWindow(hwnd);
            return 0;

        case WM_DESTROY:
            if (rw != NULL)
            {
                if (rw->owner != NULL && rw->kind >= 0 && rw->kind < EE_CVRREP_COUNT &&
                    rw->owner->vreports[rw->kind] == rw)
                {
                    rw->owner->vreports[rw->kind] = NULL;
                }
                EeCvr_FreeColumnCounts(rw->items, rw->count);
                free(rw);
                SetWindowLongPtrW(hwnd, GWLP_USERDATA, 0);
            }
            return 0;

        default:
            break;
    }
    return DefWindowProcW(hwnd, msg, wParam, lParam);
}

/* Open (or re-focus) the Batch/Precinct/Ballot Style value report for @p cw. */
static void App_ShowCvrValueReport(CvrWindow *cw, int kind)
{
    const wchar_t *label;
    uint32_t column = 0;
    EeCvrValueCount *items = NULL;
    uint32_t count = 0;
    uint32_t blank = 0;
    CvrValueReportWindow *rw;
    wchar_t title[MAX_PATH + 64];
    wchar_t base[192];
    wchar_t none_msg[96];
    HCURSOR old_cursor;
    RECT pr;
    int x = CW_USEDEFAULT;
    int y = CW_USEDEFAULT;
    uint32_t i;
    BOOL numeric;

    if (cw == NULL || kind < 0 || kind >= EE_CVRREP_COUNT)
    {
        return;
    }
    if (cw->vreports[kind] != NULL)
    {
        SetForegroundWindow(cw->vreports[kind]->hwnd);
        return;
    }
    label = Cvr_ReportColumnTitle(kind);
    if (!cw->vreport_avail[kind])
    {
        return; /* menu is greyed in this case; guard anyway */
    }
    column = cw->vreport_col[kind];

    old_cursor = SetCursor(LoadCursorW(NULL, IDC_WAIT));
    if (!EeCvr_CollectColumnCounts(&cw->table, column, &items, &count, &blank))
    {
        SetCursor(old_cursor);
        MessageBoxW(cw->hwnd,
                    L"Out of memory while building the report.",
                    L"Report",
                    MB_ICONERROR | MB_OK);
        return;
    }
    SetCursor(old_cursor);
    if (count == 0)
    {
        EeCvr_FreeColumnCounts(items, count);
        StringCchPrintfW(none_msg, ARRAYSIZE(none_msg), L"No %s information available.", label);
        MessageBoxW(cw->hwnd, none_msg, L"Report", MB_ICONINFORMATION | MB_OK);
        return;
    }

    /* Append a "(blank)" row for records with no value in this column. */
    if (blank > 0)
    {
        EeCvrValueCount *grown =
            (EeCvrValueCount *)realloc(items, ((size_t)count + 1) * sizeof(EeCvrValueCount));
        wchar_t *empty = (grown != NULL) ? (wchar_t *)calloc(1, sizeof(wchar_t)) : NULL;
        if (grown != NULL && empty != NULL)
        {
            grown[count].value = empty;
            grown[count].count = blank;
            items = grown;
            count++;
        }
        else if (grown != NULL)
        {
            items = grown; /* keep the resized buffer; just no blank row */
        }
    }

    /* Numeric sort when every non-blank value is a run of digits (typical for
     * Batch / Precinct / Ballot Style codes). */
    numeric = TRUE;
    for (i = 0; i < count; i++)
    {
        const wchar_t *v = items[i].value;
        if (v == NULL || v[0] == L'\0')
        {
            continue; /* blank row does not disqualify numeric sort */
        }
        for (; *v != L'\0'; v++)
        {
            if (*v < L'0' || *v > L'9')
            {
                numeric = FALSE;
                break;
            }
        }
        if (!numeric)
        {
            break;
        }
    }

    rw = (CvrValueReportWindow *)calloc(1, sizeof(CvrValueReportWindow));
    if (rw == NULL)
    {
        EeCvr_FreeColumnCounts(items, count);
        return;
    }
    rw->owner = cw;
    rw->app = cw->app;
    rw->kind = kind;
    rw->column = column;
    rw->label = label;
    rw->items = items;
    rw->count = count;
    rw->has_blank = (blank > 0);
    rw->numeric = numeric;
    rw->sort_col = 0;
    rw->sort_asc = TRUE;

    /* Title "<label> Report - <filename>": derive the filename from the CVR window
     * title by stripping its " — Cast Vote Records" suffix. */
    base[0] = L'\0';
    GetWindowTextW(cw->hwnd, base, ARRAYSIZE(base));
    {
        wchar_t *suffix = wcsstr(base, L" \x2014 Cast Vote Records");
        if (suffix != NULL)
        {
            *suffix = L'\0';
        }
    }
    if (base[0] != L'\0')
    {
        StringCchPrintfW(title, ARRAYSIZE(title), L"%s Report - %s", label, base);
    }
    else
    {
        StringCchPrintfW(title, ARRAYSIZE(title), L"%s Report", label);
    }

    if (GetWindowRect(cw->hwnd, &pr))
    {
        x = pr.left + Scale(cw->app, 48);
        y = pr.top + Scale(cw->app, 48);
    }

    /* Unowned top-level window (like the voter-list reports) so the CVR window can
     * cover it; tracked in cw->vreports[kind] and closed when the CVR window closes. */
    rw->hwnd = CreateWindowExW(0,
                               k_CvrValueReportClassName,
                               title,
                               WS_OVERLAPPEDWINDOW | WS_CLIPCHILDREN,
                               x,
                               y,
                               Scale(cw->app, 440),
                               Scale(cw->app, 600),
                               NULL,
                               NULL,
                               cw->app->instance,
                               rw);
    if (rw->hwnd == NULL)
    {
        EeCvr_FreeColumnCounts(items, count);
        free(rw);
        return;
    }
    cw->vreports[kind] = rw;
    ShowWindow(rw->hwnd, SW_SHOW);
    SetForegroundWindow(rw->hwnd);
}

/* Export CVR ballot rows (all visible/filtered, or just selected) as CSV/TSV, in the
 * current display order, with a header row of column titles. */
static void App_ExportCvrWindow(CvrWindow *cw, BOOL selection_only)
{
    uint32_t *rows = NULL;
    uint32_t n = 0;
    BOOL filtered;
    wchar_t suggested[256];
    wchar_t path[MAX_PATH];
    char delim = ',';
    char *text = NULL;
    size_t len = 0;

    if (cw == NULL || cw->table.nrows == 0 || cw->table.ncols == 0)
    {
        return;
    }
    filtered = cw->filt_active;

    if (selection_only)
    {
        int i = ListView_GetNextItem(cw->list, -1, LVNI_SELECTED);
        uint32_t cap = 0;
        while (i >= 0)
        {
            uint32_t phys = cw->filt_active
                                ? (((uint32_t)i < cw->disp_count) ? cw->disp[i] : 0)
                                : cw->table.view_index[i];
            if (n == cap)
            {
                uint32_t nc = cap ? cap * 2u : 64u;
                uint32_t *g = (uint32_t *)realloc(rows, (size_t)nc * sizeof(uint32_t));
                if (g == NULL)
                {
                    free(rows);
                    return;
                }
                rows = g;
                cap = nc;
            }
            rows[n++] = phys;
            i = ListView_GetNextItem(cw->list, i, LVNI_SELECTED);
        }
        if (n == 0)
        {
            return;
        }
    }
    else
    {
        uint32_t total = cw->filt_active ? cw->disp_count : cw->table.nrows;
        uint32_t d;
        if (total == 0)
        {
            return;
        }
        rows = (uint32_t *)malloc((size_t)total * sizeof(uint32_t));
        if (rows == NULL)
        {
            return;
        }
        for (d = 0; d < total; d++)
        {
            rows[d] = cw->filt_active ? cw->disp[d] : cw->table.view_index[d];
        }
        n = total;
    }

    StringCchPrintfW(suggested,
                     ARRAYSIZE(suggested),
                     L"%s%s",
                     cw->base_name,
                     selection_only ? L"-Selected_Records"
                                     : (filtered ? L"-Filtered_Records" : L"-All_Records"));
    if (!App_PromptExportPath(cw->hwnd, suggested, path, ARRAYSIZE(path), &delim))
    {
        free(rows);
        return;
    }
    if (!EeCvr_FormatDelimitedUtf8(&cw->table, rows, n, delim, TRUE, &text, &len))
    {
        free(rows);
        MessageBoxW(cw->hwnd, L"Out of memory building the export.", L"Export",
                    MB_ICONERROR | MB_OK);
        return;
    }
    App_WriteExportUtf8(cw->hwnd, path, text, len);
    free(text);
    free(rows);
}

/* Cell provider for the CVR tabulation report export (Contest | Selection | Votes). */
typedef struct CvrTabExportCtx
{
    CvrReportWindow *rw;
    const uint32_t *idx;
} CvrTabExportCtx;

static void CvrTab_ExportCell(void *user, uint32_t row, uint32_t col, wchar_t *buf, size_t cch)
{
    CvrTabExportCtx *c = (CvrTabExportCtx *)user;
    uint32_t item = (c->idx != NULL) ? c->idx[row] : row;
    if (item >= c->rw->count)
    {
        buf[0] = L'\0';
        return;
    }
    if (col == 0)
    {
        StringCchCopyW(buf, cch, c->rw->items[item].contest);
    }
    else if (col == 1)
    {
        StringCchCopyW(buf, cch, c->rw->items[item].selection);
    }
    else
    {
        StringCchPrintfW(buf, cch, L"%u", c->rw->items[item].count);
    }
}

static void App_ExportCvrTabReport(CvrReportWindow *rw, BOOL selection_only)
{
    const wchar_t *headers[3] = {L"Contest", L"Selection", L"Votes"};
    uint32_t *idx = NULL;
    uint32_t n = 0;
    CvrTabExportCtx ctx;
    wchar_t suggested[256];
    const wchar_t *base;

    if (rw == NULL || rw->owner == NULL || rw->count == 0)
    {
        return;
    }
    if (selection_only)
    {
        idx = App_CollectSelectedIndices(rw->list, rw->count, &n);
        if (n == 0)
        {
            free(idx);
            return;
        }
    }
    else
    {
        n = rw->count;
    }
    ctx.rw = rw;
    ctx.idx = idx;
    base = rw->owner->base_name;
    StringCchPrintfW(suggested,
                     ARRAYSIZE(suggested),
                     L"%s-%s_Contests",
                     base,
                     selection_only ? L"Selected" : L"All");
    App_ExportReportModel(rw->hwnd, suggested, 3, headers, n, CvrTab_ExportCell, &ctx);
    free(idx);
}

/* Cell provider for a CVR value report export (value | Number of Ballot Records). */
typedef struct CvrValExportCtx
{
    CvrValueReportWindow *rw;
    const uint32_t *idx;
} CvrValExportCtx;

static void CvrVal_ExportCell(void *user, uint32_t row, uint32_t col, wchar_t *buf, size_t cch)
{
    CvrValExportCtx *c = (CvrValExportCtx *)user;
    uint32_t item = (c->idx != NULL) ? c->idx[row] : row;
    if (item >= c->rw->count)
    {
        buf[0] = L'\0';
        return;
    }
    if (col == 0)
    {
        StringCchCopyW(buf, cch, report_display_value(c->rw->items[item].value));
    }
    else
    {
        StringCchPrintfW(buf, cch, L"%u", c->rw->items[item].count);
    }
}

static void App_ExportCvrValueReport(CvrValueReportWindow *rw, BOOL selection_only)
{
    const wchar_t *headers[2];
    const wchar_t *plural;
    uint32_t *idx = NULL;
    uint32_t n = 0;
    CvrValExportCtx ctx;
    wchar_t suggested[256];

    if (rw == NULL || rw->owner == NULL || rw->count == 0)
    {
        return;
    }
    switch (rw->kind)
    {
        case EE_CVRREP_BATCH:
            plural = L"Batches";
            break;
        case EE_CVRREP_BALLOTSTYLE:
            plural = L"Ballot_Styles";
            break;
        case EE_CVRREP_POLLINGPLACE:
            plural = L"Polling_Places";
            break;
        case EE_CVRREP_DEVICESERIAL:
            plural = L"Device_Serials";
            break;
        case EE_CVRREP_VOTINGTYPE:
            plural = L"Voting_Types";
            break;
        default:
            plural = L"Precincts";
            break;
    }
    headers[0] = rw->label;
    headers[1] = L"Number of Ballot Records";

    if (selection_only)
    {
        idx = App_CollectSelectedIndices(rw->list, rw->count, &n);
        if (n == 0)
        {
            free(idx);
            return;
        }
    }
    else
    {
        n = rw->count;
    }
    ctx.rw = rw;
    ctx.idx = idx;
    StringCchPrintfW(suggested,
                     ARRAYSIZE(suggested),
                     L"%s-%s_%s",
                     rw->owner->base_name,
                     selection_only ? L"Selected" : L"All",
                     plural);
    App_ExportReportModel(rw->hwnd, suggested, 2, headers, n, CvrVal_ExportCell, &ctx);
    free(idx);
}

typedef struct CvrOptData
{
    AppState *app;
    HWND owner; /* CVR window the dialog is modal to / centered over */
    BOOL merge;
    int party_first; /* EE_TAB_PARTY_REP / EE_TAB_PARTY_DEM */
} CvrOptData;

static INT_PTR CALLBACK CvrOptionsDlgProc(HWND dlg, UINT msg, WPARAM wParam, LPARAM lParam)
{
    CvrOptData *d = (CvrOptData *)GetWindowLongPtrW(dlg, GWLP_USERDATA);

    switch (msg)
    {
        case WM_INITDIALOG:
        {
            AppState *app;
            RECT rc;
            RECT owner;
            int margin;
            int btn_w;
            int btn_h;
            int gap;
            int cx;
            int cy;
            int ow;
            int oh;
            int x = CW_USEDEFAULT;
            int y = CW_USEDEFAULT;
            HWND chk;
            HWND lbl;
            HWND rep;
            HWND dem;
            HWND ok;
            HWND cancel;

            d = (CvrOptData *)lParam;
            SetWindowLongPtrW(dlg, GWLP_USERDATA, (LONG_PTR)d);
            app = d->app;

            cx = Scale(app, 340);
            cy = Scale(app, 172);
            rc.left = 0;
            rc.top = 0;
            rc.right = cx;
            rc.bottom = cy;
            AdjustWindowRectEx(&rc,
                               (DWORD)GetWindowLongPtrW(dlg, GWL_STYLE),
                               FALSE,
                               (DWORD)GetWindowLongPtrW(dlg, GWL_EXSTYLE));
            ow = rc.right - rc.left;
            oh = rc.bottom - rc.top;
            if (d->owner != NULL && GetWindowRect(d->owner, &owner))
            {
                x = owner.left + ((owner.right - owner.left) - ow) / 2;
                y = owner.top + ((owner.bottom - owner.top) - oh) / 2;
            }
            SetWindowPos(dlg, NULL, x, y, ow, oh, SWP_NOZORDER | SWP_NOACTIVATE);

            GetClientRect(dlg, &rc);
            margin = Scale(app, 14);
            btn_w = Scale(app, 84);
            btn_h = Scale(app, 26);
            gap = Scale(app, 8);

            chk = CreateWindowExW(0,
                                  L"BUTTON",
                                  L"Merge image and text write-ins",
                                  WS_CHILD | WS_VISIBLE | WS_TABSTOP | BS_AUTOCHECKBOX,
                                  margin,
                                  margin,
                                  rc.right - 2 * margin,
                                  Scale(app, 24),
                                  dlg,
                                  (HMENU)(INT_PTR)IDC_CVR_MERGE_WRITEINS,
                                  app->instance,
                                  NULL);
            SendMessageW(chk, BM_SETCHECK, d->merge ? BST_CHECKED : BST_UNCHECKED, 0);

            lbl = CreateWindowExW(0,
                                  L"STATIC",
                                  L"Party to display first for tabulation:",
                                  WS_CHILD | WS_VISIBLE,
                                  margin,
                                  margin + Scale(app, 34),
                                  rc.right - 2 * margin,
                                  Scale(app, 18),
                                  dlg,
                                  NULL,
                                  app->instance,
                                  NULL);
            rep = CreateWindowExW(0,
                                  L"BUTTON",
                                  L"Republican",
                                  WS_CHILD | WS_VISIBLE | WS_TABSTOP | WS_GROUP | BS_AUTORADIOBUTTON,
                                  margin + Scale(app, 10),
                                  margin + Scale(app, 56),
                                  Scale(app, 130),
                                  Scale(app, 22),
                                  dlg,
                                  (HMENU)(INT_PTR)IDC_CVR_PARTY_REP,
                                  app->instance,
                                  NULL);
            dem = CreateWindowExW(0,
                                  L"BUTTON",
                                  L"Democratic",
                                  WS_CHILD | WS_VISIBLE | WS_TABSTOP | BS_AUTORADIOBUTTON,
                                  margin + Scale(app, 150),
                                  margin + Scale(app, 56),
                                  Scale(app, 130),
                                  Scale(app, 22),
                                  dlg,
                                  (HMENU)(INT_PTR)IDC_CVR_PARTY_DEM,
                                  app->instance,
                                  NULL);
            SendMessageW(d->party_first == EE_TAB_PARTY_DEM ? dem : rep, BM_SETCHECK, BST_CHECKED,
                         0);
            ok = CreateWindowExW(0,
                                 L"BUTTON",
                                 L"OK",
                                 WS_CHILD | WS_VISIBLE | WS_TABSTOP | BS_DEFPUSHBUTTON,
                                 rc.right - margin - 2 * btn_w - gap,
                                 rc.bottom - margin - btn_h,
                                 btn_w,
                                 btn_h,
                                 dlg,
                                 (HMENU)(INT_PTR)IDOK,
                                 app->instance,
                                 NULL);
            cancel = CreateWindowExW(0,
                                     L"BUTTON",
                                     L"Cancel",
                                     WS_CHILD | WS_VISIBLE | WS_TABSTOP,
                                     rc.right - margin - btn_w,
                                     rc.bottom - margin - btn_h,
                                     btn_w,
                                     btn_h,
                                     dlg,
                                     (HMENU)(INT_PTR)IDCANCEL,
                                     app->instance,
                                     NULL);
            if (app->font_ui != NULL)
            {
                SendMessageW(chk, WM_SETFONT, (WPARAM)app->font_ui, TRUE);
                SendMessageW(lbl, WM_SETFONT, (WPARAM)app->font_ui, TRUE);
                SendMessageW(rep, WM_SETFONT, (WPARAM)app->font_ui, TRUE);
                SendMessageW(dem, WM_SETFONT, (WPARAM)app->font_ui, TRUE);
                SendMessageW(ok, WM_SETFONT, (WPARAM)app->font_ui, TRUE);
                SendMessageW(cancel, WM_SETFONT, (WPARAM)app->font_ui, TRUE);
            }
            SetFocus(chk);
            return (INT_PTR)FALSE;
        }

        case WM_COMMAND:
        {
            WORD id = LOWORD(wParam);
            if (id == IDOK)
            {
                if (d != NULL)
                {
                    d->merge = (SendMessageW(GetDlgItem(dlg, IDC_CVR_MERGE_WRITEINS),
                                             BM_GETCHECK,
                                             0,
                                             0) == BST_CHECKED);
                    d->party_first =
                        (SendMessageW(GetDlgItem(dlg, IDC_CVR_PARTY_DEM), BM_GETCHECK, 0, 0) ==
                         BST_CHECKED)
                            ? EE_TAB_PARTY_DEM
                            : EE_TAB_PARTY_REP;
                }
                EndDialog(dlg, 1);
                return (INT_PTR)TRUE;
            }
            if (id == IDCANCEL)
            {
                EndDialog(dlg, 0);
                return (INT_PTR)TRUE;
            }
            break;
        }

        case WM_CLOSE:
            EndDialog(dlg, 0);
            return (INT_PTR)TRUE;

        default:
            break;
    }
    return (INT_PTR)FALSE;
}

/* Edit -> Options… for a CVR window: toggle "Merge image and text write-ins",
 * persist it, and re-tabulate an open report so the change shows immediately. */
static void App_ShowCvrOptions(CvrWindow *cw)
{
    CvrOptData d;
    DWORD buf[64]; /* DWORD-aligned for DLGTEMPLATE */
    DLGTEMPLATE *dt = (DLGTEMPLATE *)buf;
    BYTE *p;
    static const wchar_t k_caption[] = L"CVR Report Options";
    size_t clen = ARRAYSIZE(k_caption) - 1;

    if (cw == NULL)
    {
        return;
    }
    d.app = cw->app;
    d.owner = cw->hwnd;
    d.merge = g_settings.cvr_merge_writeins;
    d.party_first = g_settings.cvr_tab_party_first;

    ZeroMemory(buf, sizeof(buf));
    dt->style = WS_POPUP | WS_CAPTION | WS_SYSMENU | DS_MODALFRAME;
    dt->cx = 200;
    dt->cy = 80;
    p = (BYTE *)buf + sizeof(DLGTEMPLATE);
    *(WORD *)p = 0; /* no menu */
    p += sizeof(WORD);
    *(WORD *)p = 0; /* default dialog class */
    p += sizeof(WORD);
    memcpy(p, k_caption, clen * sizeof(WCHAR));
    ((WCHAR *)p)[clen] = L'\0';

    if (DialogBoxIndirectParamW(cw->app->instance, dt, cw->hwnd, CvrOptionsDlgProc, (LPARAM)&d) == 1)
    {
        BOOL changed = ((d.merge != 0) != (g_settings.cvr_merge_writeins != 0)) ||
                       (d.party_first != g_settings.cvr_tab_party_first);
        if (changed)
        {
            g_settings.cvr_merge_writeins = d.merge;
            g_settings.cvr_tab_party_first = d.party_first;
            EeSettings_Save(&g_settings);
            if (cw->report != NULL)
            {
                App_RefreshCvrReport(cw->report);
            }
        }
    }
}

/* -------------------------------------------------------------------------- */
/* CVR filter dialog ("Election Explorer CVR Filter")                          */
/* -------------------------------------------------------------------------- */

/* State of an "is / is not" rule filter window. The CVR window and the Election Results
 * window both host it: the host describes its columns, collects a column's values and
 * applies the rules. */
typedef struct CvrFilterState
{
    AppState *app;                /* the host window's resources */
    HWND owner;                   /* host window (disabled while the filter is open) */
    EeFilterSet *applied;         /* the host's applied rules */
    HWND *slot;                   /* the host's "open filter window" field */
    const wchar_t *const *titles; /* filter column titles, indexed by column */
    const uint8_t *hidden;        /* ncols flags: column not offered (NULL = all offered) */
    uint32_t ncols;
    const wchar_t *base_name; /* suggested base name for Export Filter */
    const wchar_t *file_noun; /* fallback base name ("Cast Vote Record") */
    const wchar_t *caption;   /* window caption, also used for its messages */
    BOOL (*collect)(void *ctx, uint32_t column, wchar_t ***vals, uint32_t *n);
    void (*apply)(void *ctx);
    void *ctx;
    /* Optional, for hosts with more than "is" / "is not" (the Audit Log window): the
     * relations a column offers (1u << EeRel_*), their labels, and a check of a rule
     * before it is added. A column without "is" takes a typed value. */
    uint32_t (*relations)(void *ctx, uint32_t column);
    const wchar_t *(*relation_text)(void *ctx, uint32_t column, EeFilterRelation rel);
    BOOL (*validate)(void *ctx, const EeFilterRule *r, wchar_t *msg, size_t cch);
    uint32_t relation_mask; /* relations a loaded filter file may use; 0 = is / is not */
    EeFilterSet draft;
    int edit_index;
    BOOL refreshing;
    BOOL values_ready;
    uint32_t values_column;
} CvrFilterState;

/* Widen a combo's drop-down list (not the combo control) to fit its longest item
 * plus the vertical scrollbar, so long contest names aren't clipped. Capped so the
 * list never grows absurdly wide. */
static void Combo_AutosizeDropdown(HWND combo, HFONT font, int max_width)
{
    HDC dc;
    HFONT old = NULL;
    int count;
    int i;
    int maxw = 0;
    wchar_t buf[512];

    if (combo == NULL)
    {
        return;
    }
    dc = GetDC(combo);
    if (dc == NULL)
    {
        return;
    }
    if (font != NULL)
    {
        old = (HFONT)SelectObject(dc, font);
    }
    count = (int)SendMessageW(combo, CB_GETCOUNT, 0, 0);
    for (i = 0; i < count; i++)
    {
        int len = (int)SendMessageW(combo, CB_GETLBTEXTLEN, (WPARAM)i, 0);
        SIZE sz;
        if (len <= 0 || len >= (int)ARRAYSIZE(buf))
        {
            continue;
        }
        buf[0] = L'\0';
        SendMessageW(combo, CB_GETLBTEXT, (WPARAM)i, (LPARAM)buf);
        if (GetTextExtentPoint32W(dc, buf, (int)wcslen(buf), &sz) && sz.cx > maxw)
        {
            maxw = sz.cx;
        }
    }
    if (old != NULL)
    {
        SelectObject(dc, old);
    }
    ReleaseDC(combo, dc);
    if (maxw > 0)
    {
        int want = maxw + GetSystemMetrics(SM_CXVSCROLL) + 4 * GetSystemMetrics(SM_CXEDGE) + 8;
        RECT rc;
        int cur = 0;
        if (GetWindowRect(combo, &rc))
        {
            cur = rc.right - rc.left; /* never narrower than the combo itself */
        }
        if (want < cur)
        {
            want = cur;
        }
        if (max_width > 0 && want > max_width)
        {
            want = max_width;
        }
        SendMessageW(combo, CB_SETDROPPEDWIDTH, (WPARAM)want, 0);
    }
}

static void CvrFilt_PopulateColumns(HWND combo, const CvrFilterState *st)
{
    uint32_t i;
    SendMessageW(combo, CB_RESETCONTENT, 0, 0);
    for (i = 0; i < st->ncols; i++)
    {
        const wchar_t *title = st->titles[i] ? st->titles[i] : L"";
        if (st->hidden != NULL && st->hidden[i])
        {
            continue;
        }
        int idx = (int)SendMessageW(combo, CB_ADDSTRING, 0, (LPARAM)title);
        if (idx >= 0)
        {
            SendMessageW(combo, CB_SETITEMDATA, (WPARAM)idx, (LPARAM)i);
        }
    }
    SendMessageW(combo, CB_SETCURSEL, 0, 0);
}

/* The relations column @p column offers: "is" / "is not" unless the host says otherwise. */
static uint32_t CvrFilt_ColumnRelations(const CvrFilterState *st, uint32_t column)
{
    return st->relations != NULL ? st->relations(st->ctx, column) : EE_FILTER_REL_EQUALITY;
}

static const wchar_t *CvrFilt_RelText(const CvrFilterState *st, uint32_t column, EeFilterRelation rel)
{
    return st->relation_text != NULL ? st->relation_text(st->ctx, column, rel) : EeFilter_RelationText(rel);
}

/* Fill the relation combo with what @p column offers (CVR and results: "is" / "is not"). */
static void CvrFilt_PopulateRelations(HWND combo, const CvrFilterState *st, uint32_t column)
{
    static const EeFilterRelation k_order[] = {EeRel_Is,       EeRel_IsNot,    EeRel_Contains, EeRel_Excludes,
                                               EeRel_MoreThan, EeRel_LessThan, EeRel_BeginsWith, EeRel_EndsWith};
    uint32_t mask = CvrFilt_ColumnRelations(st, column);
    size_t k;
    SendMessageW(combo, CB_RESETCONTENT, 0, 0);
    for (k = 0; k < ARRAYSIZE(k_order); k++)
    {
        if (mask & (1u << k_order[k]))
        {
            int idx = (int)SendMessageW(combo, CB_ADDSTRING, 0, (LPARAM)CvrFilt_RelText(st, column, k_order[k]));
            SendMessageW(combo, CB_SETITEMDATA, (WPARAM)idx, (LPARAM)k_order[k]);
        }
    }
    SendMessageW(combo, CB_SETCURSEL, 0, 0);
}

/* TRUE if @p s is a (possibly signed) run of digits, e.g. a Cast Vote Record. */
static BOOL cvr_wstr_is_number(const wchar_t *s)
{
    BOOL any = FALSE;
    if (s == NULL)
    {
        return FALSE;
    }
    if (*s == L'-' || *s == L'+')
    {
        s++;
    }
    for (; *s != L'\0'; s++)
    {
        if (*s < L'0' || *s > L'9')
        {
            return FALSE;
        }
        any = TRUE;
    }
    return any;
}

/* TRUE if the value combo is currently the editable (CBS_DROPDOWN) kind. */
static BOOL CvrFilt_ValueComboEditable(HWND hwnd)
{
    HWND combo = GetDlgItem(hwnd, IDC_FLT_VALUE);
    if (combo == NULL)
    {
        return FALSE;
    }
    return ((DWORD)GetWindowLongPtrW(combo, GWL_STYLE) & 0x3u) == CBS_DROPDOWN;
}

/* Ensure the value combo is editable (typeable) or a fixed list, recreating it if
 * the style must change. Numeric columns (e.g. Cast Vote Record) are editable so a
 * value beyond the capped suggestion list can still be entered. Returns the combo. */
static HWND CvrFilt_EnsureValueCombo(HWND hwnd, CvrFilterState *st, BOOL editable)
{
    HWND old = GetDlgItem(hwnd, IDC_FLT_VALUE);
    AppState *app = st->app;
    HWND neww;
    RECT r;
    POINT tl;

    if (old == NULL || app == NULL)
    {
        return old;
    }
    if ((((DWORD)GetWindowLongPtrW(old, GWL_STYLE) & 0x3u) == CBS_DROPDOWN) == (editable != 0))
    {
        return old; /* already the right kind */
    }
    GetWindowRect(old, &r);
    tl.x = r.left;
    tl.y = r.top;
    ScreenToClient(hwnd, &tl);
    DestroyWindow(old);
    neww = CreateWindowExW(0, L"COMBOBOX", L"",
                           WS_CHILD | WS_VISIBLE | WS_TABSTOP | WS_CLIPSIBLINGS | WS_VSCROLL |
                               CBS_HASSTRINGS |
                               (editable ? (CBS_DROPDOWN | CBS_AUTOHSCROLL) : CBS_DROPDOWNLIST),
                           tl.x, tl.y, Scale(app, 180), Scale(app, 240), hwnd,
                           (HMENU)(INT_PTR)IDC_FLT_VALUE, app->instance, NULL);
    if (neww != NULL)
    {
        if (app->font_ui)
        {
            SendMessageW(neww, WM_SETFONT, (WPARAM)app->font_ui, TRUE);
        }
        /* Restore tab order: place right after the Relation combo. */
        SetWindowPos(neww, GetDlgItem(hwnd, IDC_FLT_RELATION), 0, 0, 0, 0,
                     SWP_NOMOVE | SWP_NOSIZE | SWP_NOACTIVATE);
        FilterDlg_Layout(hwnd, app);
    }
    return neww;
}

/* Fill the value control with the distinct selections for the chosen column. If the
 * column is numeric (all selections are numbers) the control becomes an editable
 * combo so any value can be typed even when the suggestion list is capped; otherwise
 * it is a fixed drop-down list defaulting to the first value. */
static void CvrFilt_FillValues(HWND hwnd, CvrFilterState *st, BOOL force)
{
    HWND combo;
    wchar_t **vals = NULL;
    uint32_t n = 0;
    uint32_t i;
    uint32_t column;
    BOOL numeric;
    HCURSOR prev;

    if (hwnd == NULL || st == NULL || st->collect == NULL)
    {
        return;
    }
    column = FilterDlg_ComboData(GetDlgItem(hwnd, IDC_FLT_COLUMN), 0);
    if (!force && st->values_ready && st->values_column == column)
    {
        return;
    }
    prev = SetCursor(LoadCursorW(NULL, IDC_WAIT));
    st->collect(st->ctx, column, &vals, &n);
    numeric = (n > 0);
    for (i = 0; i < n; i++)
    {
        if (!cvr_wstr_is_number(vals[i]))
        {
            numeric = FALSE;
            break;
        }
    }
    if (!(CvrFilt_ColumnRelations(st, column) & (1u << EeRel_Is)))
    {
        numeric = TRUE; /* typed: "contains" text or a date (suggestions only) */
    }
    combo = CvrFilt_EnsureValueCombo(hwnd, st, numeric);
    if (combo == NULL)
    {
        combo = GetDlgItem(hwnd, IDC_FLT_VALUE);
    }
    SendMessageW(combo, CB_RESETCONTENT, 0, 0);
    if (n > 0)
    {
        SendMessageW(combo, CB_INITSTORAGE, (WPARAM)n, (LPARAM)(n * 32u));
    }
    for (i = 0; i < n; i++)
    {
        SendMessageW(combo, CB_ADDSTRING, 0, (LPARAM)vals[i]);
        free(vals[i]);
    }
    free(vals);
    /* Fixed lists default to the first value; editable numeric boxes stay empty so
     * the user knows to type (the list is just a capped suggestion set). */
    if (n > 0 && !numeric)
    {
        SendMessageW(combo, CB_SETCURSEL, 0, 0);
    }
    Combo_AutosizeDropdown(combo, st->app->font_ui, Scale(st->app, 900));
    SetCursor(prev);
    st->values_ready = TRUE;
    st->values_column = column;
}

static void CvrFilt_RefreshList(HWND list, CvrFilterState *st)
{
    uint32_t i;
    if (list == NULL || st == NULL)
    {
        return;
    }
    st->refreshing = TRUE;
    ListView_DeleteAllItems(list);
    for (i = 0; i < st->draft.count; i++)
    {
        const EeFilterRule *r = &st->draft.rules[i];
        LVITEMW it;
        const wchar_t *colname = L"?";
        ZeroMemory(&it, sizeof(it));
        if (r->column < st->ncols && st->titles[r->column] != NULL)
        {
            colname = st->titles[r->column];
        }
        it.mask = LVIF_TEXT | LVIF_PARAM;
        it.iItem = (int)i;
        it.pszText = (LPWSTR)colname;
        it.lParam = (LPARAM)i;
        ListView_InsertItem(list, &it);
        ListView_SetItemText(list, (int)i, 1, (LPWSTR)CvrFilt_RelText(st, r->column, r->relation));
        ListView_SetItemText(list, (int)i, 2, (LPWSTR)r->value);
        ListView_SetItemText(list, (int)i, 3, (LPWSTR)EeFilter_ActionText(r->action));
        ListView_SetCheckState(list, (int)i, r->enabled);
    }
    st->refreshing = FALSE;
}

static void CvrFilt_LoadRuleToControls(HWND hwnd, CvrFilterState *st, const EeFilterRule *r)
{
    HWND col = GetDlgItem(hwnd, IDC_FLT_COLUMN);
    HWND rel = GetDlgItem(hwnd, IDC_FLT_RELATION);
    HWND act = GetDlgItem(hwnd, IDC_FLT_ACTION);
    int i;
    int n;

    n = (int)SendMessageW(col, CB_GETCOUNT, 0, 0);
    for (i = 0; i < n; i++)
    {
        if ((uint32_t)SendMessageW(col, CB_GETITEMDATA, (WPARAM)i, 0) == r->column)
        {
            SendMessageW(col, CB_SETCURSEL, (WPARAM)i, 0);
            break;
        }
    }
    st->values_ready = FALSE;
    CvrFilt_FillValues(hwnd, st, TRUE); /* may swap the value combo's editability */
    CvrFilt_PopulateRelations(rel, st, r->column);
    n = (int)SendMessageW(rel, CB_GETCOUNT, 0, 0);
    for (i = 0; i < n; i++)
    {
        if ((EeFilterRelation)SendMessageW(rel, CB_GETITEMDATA, (WPARAM)i, 0) == r->relation)
        {
            SendMessageW(rel, CB_SETCURSEL, (WPARAM)i, 0);
            break;
        }
    }
    {
        HWND val = GetDlgItem(hwnd, IDC_FLT_VALUE);
        if (CvrFilt_ValueComboEditable(hwnd))
        {
            /* Editable (numeric) column: set the text directly — the value may be
             * outside the capped suggestion list. */
            SetWindowTextW(val, r->value);
        }
        else
        {
            int sel = (int)SendMessageW(val, CB_FINDSTRINGEXACT, (WPARAM)-1, (LPARAM)r->value);
            if (sel >= 0)
            {
                SendMessageW(val, CB_SETCURSEL, (WPARAM)sel, 0);
            }
        }
    }
    SendMessageW(act, CB_SETCURSEL, (WPARAM)(r->action == EeFilt_Exclude ? 1 : 0), 0);
}

static void CvrFilt_EditSelected(HWND hwnd, CvrFilterState *st)
{
    HWND list;
    int i;
    if (st == NULL)
    {
        return;
    }
    list = GetDlgItem(hwnd, IDC_FLT_LIST);
    i = ListView_GetNextItem(list, -1, LVNI_SELECTED);
    if (i >= 0 && (uint32_t)i < st->draft.count)
    {
        st->edit_index = i;
        CvrFilt_LoadRuleToControls(hwnd, st, &st->draft.rules[i]);
        SetWindowTextW(GetDlgItem(hwnd, IDC_FLT_ADD), L"Update");
    }
}

static void CvrFilt_SetSelectedEnabled(HWND hwnd, CvrFilterState *st, BOOL enabled)
{
    HWND list;
    int i;
    if (st == NULL)
    {
        return;
    }
    list = GetDlgItem(hwnd, IDC_FLT_LIST);
    for (i = 0; i < (int)st->draft.count; i++)
    {
        if (ListView_GetItemState(list, i, LVIS_SELECTED) & LVIS_SELECTED)
        {
            st->draft.rules[i].enabled = enabled;
        }
    }
    CvrFilt_RefreshList(list, st);
}

static void CvrFilt_Apply(CvrFilterState *st)
{
    if (st == NULL || st->applied == NULL)
    {
        return;
    }
    if (!EeFilter_Copy(st->applied, &st->draft))
    {
        MessageBoxW(st->owner, L"Could not apply the filter.", k_WindowTitle,
                    MB_ICONERROR | MB_OK);
        return;
    }
    st->apply(st->ctx);
}

static void CvrFilt_Close(HWND hwnd, CvrFilterState *st)
{
    if (st != NULL && st->owner != NULL)
    {
        EnableWindow(st->owner, TRUE);
        SetForegroundWindow(st->owner);
    }
    DestroyWindow(hwnd);
}

static LRESULT CALLBACK CvrFilterWndProc(HWND hwnd, UINT msg, WPARAM wParam, LPARAM lParam)
{
    CvrFilterState *st = (CvrFilterState *)GetWindowLongPtrW(hwnd, GWLP_USERDATA);
    AppState *app = (st != NULL) ? st->app : NULL;

    switch (msg)
    {
        case WM_CREATE:
        {
            CREATESTRUCTW *cs = (CREATESTRUCTW *)lParam;
            st = (CvrFilterState *)cs->lpCreateParams;
            SetWindowLongPtrW(hwnd, GWLP_USERDATA, (LONG_PTR)st);
            return 0;
        }
        case WM_SIZE:
            if (app != NULL)
            {
                FilterDlg_Layout(hwnd, app);
            }
            return 0;
        case WM_GETMINMAXINFO:
            if (app != NULL)
            {
                MINMAXINFO *mm = (MINMAXINFO *)lParam;
                RECT wr;
                DWORD style = (DWORD)GetWindowLongPtrW(hwnd, GWL_STYLE);
                DWORD ex_style = (DWORD)GetWindowLongPtrW(hwnd, GWL_EXSTYLE);
                wr.left = 0;
                wr.top = 0;
                wr.right = FilterDlg_MinClientWidth(app);
                wr.bottom = FilterDlg_MinClientHeight(app);
                if (!AdjustWindowRectExForDpi(&wr, style, FALSE, ex_style, app->dpi))
                {
                    wr.right += Scale(app, 16);
                    wr.bottom += Scale(app, 40);
                }
                mm->ptMinTrackSize.x = wr.right - wr.left;
                mm->ptMinTrackSize.y = wr.bottom - wr.top;
                return 0;
            }
            break;
        case WM_COMMAND:
            if (st == NULL)
            {
                return 0;
            }
            switch (LOWORD(wParam))
            {
                case IDC_FLT_COLUMN:
                    if (HIWORD(wParam) == CBN_SELCHANGE)
                    {
                        st->values_ready = FALSE;
                        CvrFilt_PopulateRelations(GetDlgItem(hwnd, IDC_FLT_RELATION), st,
                                                  FilterDlg_ComboData(GetDlgItem(hwnd, IDC_FLT_COLUMN), 0));
                        CvrFilt_FillValues(hwnd, st, TRUE);
                    }
                    return 0;
                case IDC_FLT_VALUE:
                    if (HIWORD(wParam) == CBN_DROPDOWN)
                    {
                        CvrFilt_FillValues(hwnd, st, FALSE);
                    }
                    return 0;
                case IDC_FLT_ADD:
                {
                    EeFilterRule r;
                    if (FilterDlg_ReadControls(hwnd, &r))
                    {
                        if (r.value[0] == L'\0')
                        {
                            MessageBoxW(hwnd, L"Choose a value to filter on.", st->caption,
                                        MB_ICONINFORMATION | MB_OK);
                            return 0;
                        }
                        if (st->validate != NULL)
                        {
                            wchar_t why[256] = L"";
                            if (!st->validate(st->ctx, &r, why, ARRAYSIZE(why)))
                            {
                                MessageBoxW(hwnd, why, st->caption, MB_ICONINFORMATION | MB_OK);
                                return 0;
                            }
                        }
                        if (st->edit_index >= 0 && (uint32_t)st->edit_index < st->draft.count)
                        {
                            EeFilter_Set(&st->draft, (uint32_t)st->edit_index, &r);
                        }
                        else
                        {
                            EeFilter_Add(&st->draft, &r);
                        }
                        st->edit_index = -1;
                        SetWindowTextW(GetDlgItem(hwnd, IDC_FLT_ADD), L"Add");
                        CvrFilt_RefreshList(GetDlgItem(hwnd, IDC_FLT_LIST), st);
                    }
                    return 0;
                }
                case IDC_FLT_REMOVE:
                {
                    HWND list = GetDlgItem(hwnd, IDC_FLT_LIST);
                    int i;
                    for (i = ListView_GetItemCount(list) - 1; i >= 0; i--)
                    {
                        if (ListView_GetItemState(list, i, LVIS_SELECTED) & LVIS_SELECTED)
                        {
                            EeFilter_Remove(&st->draft, (uint32_t)i);
                        }
                    }
                    st->edit_index = -1;
                    SetWindowTextW(GetDlgItem(hwnd, IDC_FLT_ADD), L"Add");
                    CvrFilt_RefreshList(list, st);
                    return 0;
                }
                case IDC_FLT_EDIT:
                    CvrFilt_EditSelected(hwnd, st);
                    return 0;
                case IDC_FLT_ENABLE:
                    CvrFilt_SetSelectedEnabled(hwnd, st, TRUE);
                    return 0;
                case IDC_FLT_DISABLE:
                    CvrFilt_SetSelectedEnabled(hwnd, st, FALSE);
                    return 0;
                case IDC_FLT_RESET:
                    EeFilter_Clear(&st->draft);
                    st->edit_index = -1;
                    SetWindowTextW(GetDlgItem(hwnd, IDC_FLT_ADD), L"Add");
                    CvrFilt_RefreshList(GetDlgItem(hwnd, IDC_FLT_LIST), st);
                    return 0;
                case IDC_FLT_EXPORT:
                case IDC_FLT_LOAD:
                {
                    uint32_t ncols = st->ncols;
                    EeFilterColumnRef *cols =
                        (EeFilterColumnRef *)calloc(ncols ? ncols : 1, sizeof(EeFilterColumnRef));
                    uint32_t c;
                    if (cols == NULL)
                    {
                        return 0;
                    }
                    for (c = 0; c < ncols; c++)
                    {
                        cols[c].column = c;
                        cols[c].title = (st->titles[c] != NULL && !(st->hidden != NULL && st->hidden[c]))
                                            ? st->titles[c]
                                            : L"";
                    }
                    if (LOWORD(wParam) == IDC_FLT_EXPORT)
                    {
                        FilterFile_Export(hwnd, &st->draft, cols, ncols, st->base_name, st->file_noun);
                    }
                    else if (FilterFile_Load(hwnd, &st->draft, cols, ncols,
                                             st->relation_mask != 0 ? st->relation_mask : EE_FILTER_REL_EQUALITY,
                                             NULL))
                    {
                        st->edit_index = -1;
                        SetWindowTextW(GetDlgItem(hwnd, IDC_FLT_ADD), L"Add");
                        CvrFilt_RefreshList(GetDlgItem(hwnd, IDC_FLT_LIST), st);
                    }
                    free(cols);
                    return 0;
                }
                case IDC_FLT_APPLY:
                    CvrFilt_Apply(st);
                    return 0;
                case IDOK:
                    CvrFilt_Apply(st);
                    CvrFilt_Close(hwnd, st);
                    return 0;
                case IDCANCEL:
                    CvrFilt_Close(hwnd, st);
                    return 0;
                default:
                    break;
            }
            return 0;
        case WM_NOTIFY:
        {
            NMHDR *hdr = (NMHDR *)lParam;
            if (st != NULL && hdr != NULL && hdr->idFrom == IDC_FLT_LIST)
            {
                if (hdr->code == LVN_ITEMCHANGED && !st->refreshing)
                {
                    NMLISTVIEW *lv = (NMLISTVIEW *)hdr;
                    if (lv->uChanged & LVIF_STATE)
                    {
                        UINT ni = (lv->uNewState & LVIS_STATEIMAGEMASK) >> 12;
                        UINT oi = (lv->uOldState & LVIS_STATEIMAGEMASK) >> 12;
                        if (ni != oi && lv->iItem >= 0 && (uint32_t)lv->iItem < st->draft.count)
                        {
                            st->draft.rules[lv->iItem].enabled = (ni == 2);
                        }
                    }
                }
                if (hdr->code == NM_DBLCLK)
                {
                    CvrFilt_EditSelected(hwnd, st);
                }
                if (hdr->code == LVN_KEYDOWN)
                {
                    NMLVKEYDOWN *kd = (NMLVKEYDOWN *)hdr;
                    if (kd->wVKey == VK_DELETE)
                    {
                        SendMessageW(hwnd, WM_COMMAND, IDC_FLT_REMOVE, 0);
                    }
                }
            }
            return 0;
        }
        case WM_CONTEXTMENU:
            if (st != NULL)
            {
                POINT pt;
                HMENU menu;
                UINT cmd;
                pt.x = GET_X_LPARAM(lParam);
                pt.y = GET_Y_LPARAM(lParam);
                if (pt.x == -1 && pt.y == -1)
                {
                    GetCursorPos(&pt);
                }
                menu = CreatePopupMenu();
                if (menu)
                {
                    AppendMenuW(menu, MF_STRING, IDC_FLT_EDIT, L"&Edit");
                    AppendMenuW(menu, MF_STRING, IDC_FLT_REMOVE, L"&Remove");
                    AppendMenuW(menu, MF_SEPARATOR, 0, NULL);
                    AppendMenuW(menu, MF_STRING, IDC_FLT_ENABLE, L"E&nable");
                    AppendMenuW(menu, MF_STRING, IDC_FLT_DISABLE, L"&Disable");
                    cmd = (UINT)TrackPopupMenu(menu, TPM_RIGHTBUTTON | TPM_RETURNCMD, pt.x, pt.y, 0,
                                               hwnd, NULL);
                    DestroyMenu(menu);
                    if (cmd != 0)
                    {
                        SendMessageW(hwnd, WM_COMMAND, cmd, 0);
                    }
                }
            }
            return 0;
        case WM_CLOSE:
            CvrFilt_Close(hwnd, st);
            return 0;
        case WM_DESTROY:
            CvrFilter_Unregister(hwnd);
            if (st != NULL)
            {
                if (st->owner != NULL)
                {
                    EnableWindow(st->owner, TRUE);
                }
                if (st->slot != NULL)
                {
                    *st->slot = NULL;
                }
                EeFilter_Clear(&st->draft);
                HeapFree(GetProcessHeap(), 0, st);
                SetWindowLongPtrW(hwnd, GWLP_USERDATA, 0);
            }
            return 0;
        default:
            break;
    }
    return DefWindowProcW(hwnd, msg, wParam, lParam);
}

/* Create and show a rule filter window for a host described by @p st (which the window
 * then owns: it is freed in WM_DESTROY). @p prompt heads the window. */
static BOOL Filter_ShowRuleWindow(CvrFilterState *st, const wchar_t *prompt)
{
    AppState *app = st->app;
    const DWORD style = WS_POPUP | WS_CAPTION | WS_SYSMENU | WS_THICKFRAME | WS_MINIMIZEBOX |
                        WS_CLIPCHILDREN | WS_CLIPSIBLINGS;
    const DWORD ex_style = WS_EX_DLGMODALFRAME | WS_EX_CONTROLPARENT;
    RECT rc_wnd;
    RECT owner;
    int client_w;
    int client_h;
    int outer_w;
    int outer_h;
    int x = CW_USEDEFAULT;
    int y = CW_USEDEFAULT;
    HWND hwnd;
    HWND list;
    LVCOLUMNW col;
    static const struct
    {
        const wchar_t *cls;
        const wchar_t *text;
        DWORD extra;
        int id;
    } k_ctrls[] = {
        {L"STATIC", L"", SS_LEFT | SS_CENTERIMAGE, IDC_FLT_PROMPT},
        {L"COMBOBOX", L"", WS_VSCROLL | CBS_DROPDOWNLIST | CBS_HASSTRINGS, IDC_FLT_COLUMN},
        {L"COMBOBOX", L"", WS_VSCROLL | CBS_DROPDOWNLIST | CBS_HASSTRINGS, IDC_FLT_RELATION},
        {L"COMBOBOX", L"", WS_VSCROLL | CBS_DROPDOWNLIST | CBS_HASSTRINGS, IDC_FLT_VALUE},
        {L"COMBOBOX", L"", WS_VSCROLL | CBS_DROPDOWNLIST | CBS_HASSTRINGS, IDC_FLT_ACTION},
        {L"BUTTON", L"Add", BS_PUSHBUTTON, IDC_FLT_ADD},
        {L"BUTTON", L"Remove", BS_PUSHBUTTON, IDC_FLT_REMOVE},
        {L"BUTTON", L"Reset", BS_PUSHBUTTON, IDC_FLT_RESET},
        {L"BUTTON", L"Export Filter\x2026", BS_PUSHBUTTON, IDC_FLT_EXPORT},
        {L"BUTTON", L"Load Filter\x2026", BS_PUSHBUTTON, IDC_FLT_LOAD},
        {L"BUTTON", L"Apply", BS_PUSHBUTTON, IDC_FLT_APPLY},
        {L"BUTTON", L"OK", BS_DEFPUSHBUTTON, IDOK},
        {L"BUTTON", L"Cancel", BS_PUSHBUTTON, IDCANCEL},
    };
    int i;

    /* Default wider than the voter filter so long contest names fit in the rules
     * list and the wide column drop-down has room. */
    client_w = FilterDlg_MinClientWidth(app);
    if (client_w < Scale(app, 940))
    {
        client_w = Scale(app, 940);
    }
    client_h = Scale(app, 480);
    rc_wnd.left = 0;
    rc_wnd.top = 0;
    rc_wnd.right = client_w;
    rc_wnd.bottom = client_h;
    if (!AdjustWindowRectExForDpi(&rc_wnd, style, FALSE, ex_style, app->dpi))
    {
        rc_wnd.right = client_w + Scale(app, 16);
        rc_wnd.bottom = client_h + Scale(app, 40);
    }
    outer_w = rc_wnd.right - rc_wnd.left;
    outer_h = rc_wnd.bottom - rc_wnd.top;
    if (GetWindowRect(st->owner, &owner))
    {
        x = owner.left + ((owner.right - owner.left) - outer_w) / 2;
        y = owner.top + ((owner.bottom - owner.top) - outer_h) / 2;
    }

    hwnd = CreateWindowExW(ex_style, k_CvrFilterClassName, st->caption, style, x, y, outer_w,
                           outer_h, st->owner, NULL, app->instance, st);
    if (hwnd == NULL)
    {
        EeFilter_Clear(&st->draft);
        HeapFree(GetProcessHeap(), 0, st);
        return FALSE;
    }
    *st->slot = hwnd;
    CvrFilter_Register(hwnd);

    for (i = 0; i < (int)ARRAYSIZE(k_ctrls); i++)
    {
        DWORD base = WS_CHILD | WS_VISIBLE | WS_CLIPSIBLINGS;
        if (k_ctrls[i].id != IDC_FLT_PROMPT)
        {
            base |= WS_TABSTOP;
        }
        CreateWindowExW(0, k_ctrls[i].cls,
                        k_ctrls[i].id == IDC_FLT_PROMPT ? prompt : k_ctrls[i].text,
                        base | k_ctrls[i].extra, 0, 0, 0, 0, hwnd, (HMENU)(INT_PTR)k_ctrls[i].id,
                        app->instance, NULL);
    }
    list = CreateWindowExW(WS_EX_CLIENTEDGE, WC_LISTVIEWW, L"",
                           WS_CHILD | WS_VISIBLE | WS_TABSTOP | WS_CLIPSIBLINGS | LVS_REPORT |
                               LVS_SHOWSELALWAYS | LVS_SHAREIMAGELISTS,
                           0, 0, 0, 0, hwnd, (HMENU)(INT_PTR)IDC_FLT_LIST, app->instance, NULL);

    if (app->font_ui)
    {
        HWND child = GetWindow(hwnd, GW_CHILD);
        while (child)
        {
            SendMessageW(child, WM_SETFONT, (WPARAM)app->font_ui, TRUE);
            child = GetWindow(child, GW_HWNDNEXT);
        }
    }

    if (list != NULL)
    {
        ListView_SetExtendedListViewStyle(list, LVS_EX_CHECKBOXES | LVS_EX_FULLROWSELECT |
                                                    LVS_EX_DOUBLEBUFFER | LVS_EX_GRIDLINES);
        ZeroMemory(&col, sizeof(col));
        col.mask = LVCF_TEXT | LVCF_WIDTH;
        col.pszText = L"Column";
        col.cx = Scale(app, 340);
        ListView_InsertColumn(list, 0, &col);
        col.pszText = L"Relation";
        col.cx = Scale(app, 80);
        ListView_InsertColumn(list, 1, &col);
        col.pszText = L"Value";
        col.cx = Scale(app, 260);
        ListView_InsertColumn(list, 2, &col);
        col.pszText = L"Action";
        col.cx = Scale(app, 90);
        ListView_InsertColumn(list, 3, &col);
    }

    CvrFilt_PopulateColumns(GetDlgItem(hwnd, IDC_FLT_COLUMN), st);
    Combo_AutosizeDropdown(GetDlgItem(hwnd, IDC_FLT_COLUMN), app->font_ui, Scale(app, 900));
    CvrFilt_PopulateRelations(GetDlgItem(hwnd, IDC_FLT_RELATION), st,
                              FilterDlg_ComboData(GetDlgItem(hwnd, IDC_FLT_COLUMN), 0));
    FilterDlg_PopulateActions(GetDlgItem(hwnd, IDC_FLT_ACTION));
    CvrFilt_FillValues(hwnd, st, TRUE);
    CvrFilt_RefreshList(list, st);
    FilterDlg_Layout(hwnd, app);

    EnableWindow(st->owner, FALSE);
    ShowWindow(hwnd, SW_SHOW);
    UpdateWindow(hwnd);
    return TRUE;
}

/* A new filter state for a host (rules copied from @p applied), or NULL. */
static CvrFilterState *Filter_NewState(AppState *app, HWND owner, EeFilterSet *applied, HWND *slot)
{
    CvrFilterState *st =
        (CvrFilterState *)HeapAlloc(GetProcessHeap(), HEAP_ZERO_MEMORY, sizeof(CvrFilterState));
    if (st == NULL)
    {
        return NULL;
    }
    st->app = app;
    st->owner = owner;
    st->applied = applied;
    st->slot = slot;
    st->edit_index = -1;
    EeFilter_Init(&st->draft);
    if (!EeFilter_Copy(&st->draft, applied))
    {
        HeapFree(GetProcessHeap(), 0, st);
        return NULL;
    }
    return st;
}

static BOOL Cvr_FilterCollect(void *ctx, uint32_t column, wchar_t ***vals, uint32_t *n)
{
    return EeCvr_CollectColumnValues(&((CvrWindow *)ctx)->table, column, EE_FILTER_MAX_DISTINCT,
                                     vals, n);
}

static void Cvr_FilterApply(void *ctx)
{
    Cvr_ApplyFilter((CvrWindow *)ctx);
}

static BOOL App_ShowCvrFilter(CvrWindow *cw)
{
    CvrFilterState *st;

    if (cw == NULL)
    {
        return FALSE;
    }
    if (cw->hwnd_filter != NULL)
    {
        SetForegroundWindow(cw->hwnd_filter);
        return TRUE;
    }
    if (cw->table.nrows == 0 || cw->table.ncols == 0)
    {
        return TRUE;
    }
    st = Filter_NewState(cw->app, cw->hwnd, &cw->filters, &cw->hwnd_filter);
    if (st == NULL)
    {
        return FALSE;
    }
    st->titles = (const wchar_t *const *)cw->table.col_titles;
    st->ncols = cw->table.ncols;
    st->base_name = cw->base_name;
    st->file_noun = L"Cast Vote Record";
    st->caption = L"Election Explorer CVR Filter";
    st->collect = Cvr_FilterCollect;
    st->apply = Cvr_FilterApply;
    st->ctx = cw;
    return Filter_ShowRuleWindow(st, L"Display ballot records matching these conditions:");
}

/* -------------------------------------------------------------------------- */
/* CVR loading (worker thread behind a modal progress dialog)                 */
/* -------------------------------------------------------------------------- */

#define EEM_CVR_LOAD_DONE     (WM_APP + 11)
#define EEM_CVR_LOAD_PROGRESS (WM_APP + 12)

typedef struct CvrLoadJob
{
    AppState *app;
    const wchar_t *const *paths;
    int count;
    EeCvrTable *table;
    volatile LONG cancel;
    EeLoadStatus status;
    wchar_t err[512];
    HWND dlg;
    HWND count_label; /* shows "N ballot records" (or "Scanning ballots…") during load */
    HWND bar;         /* determinate progress bar (0..100) */
    volatile LONG ocr_pages_total; /* pages in the OCR pass of a scanned Hart PDF */
    HANDLE thread;
} CvrLoadJob;

/* Load progress: post the running ballot-record count, overall percent, and scan flag to
 * the load dialog. wParam = rows loaded; LOWORD(lParam) = percent (0..100);
 * HIWORD(lParam) = scanning (1 during the Hart discovery pass). */
static BOOL CvrLoadProgressCb(const EeLoadProgress *pr, void *user)
{
    CvrLoadJob *j = (CvrLoadJob *)user;
    if (j != NULL && j->dlg != NULL)
    {
        uint32_t pct = (pr->percent > 100u) ? 100u : pr->percent;
        WPARAM n = (WPARAM)pr->rows_loaded;
        if (pr->scanning == 2)
        {
            /* OCR pass of a scanned PDF: report the page being read instead of rows. */
            InterlockedExchange(&j->ocr_pages_total, (LONG)pr->bytes_total);
            n = (WPARAM)pr->bytes_read;
        }
        PostMessageW(j->dlg, EEM_CVR_LOAD_PROGRESS, n,
                     MAKELPARAM((WORD)pct, (WORD)(pr->scanning == 2 ? 2 : (pr->scanning ? 1 : 0))));
    }
    return TRUE; /* cancellation is driven by j->cancel, checked by the loader */
}

/* TRUE if @p path is a Hart CVR file: a ".zip" export (one XML per ballot sheet) or a
 * ".pdf" CVR Report (case-insensitive). */
static BOOL path_is_hart(const wchar_t *path)
{
    size_t n = (path != NULL) ? wcslen(path) : 0;
    return n >= 4 &&
           (_wcsicmp(path + (n - 4), L".zip") == 0 || _wcsicmp(path + (n - 4), L".pdf") == 0);
}

/* TRUE if @p path is a Dominion CVR export: a ".zip" holding ContestManifest.json and
 * CvrExport*.json files (a Hart zip holds per-sheet XML instead). */
static BOOL path_is_dominion(const wchar_t *path)
{
    size_t n = (path != NULL) ? wcslen(path) : 0;
    return n >= 4 && _wcsicmp(path + (n - 4), L".zip") == 0 && EeCvr_IsDominionZip(path);
}

static DWORD WINAPI CvrLoadThreadProc(void *param)
{
    CvrLoadJob *j = (CvrLoadJob *)param;
    int i, nhart = 0, ndominion = 0;
    /* Dominion CVRs come as .zip exports of JSON files; Hart CVRs as .zip (per-sheet XML)
     * and/or .pdf (loaded together: PDF-only, or ZIP votes decorated with the PDF's
     * device/polling-place fields); ES&S as .xlsx/.csv/.tsv. One vendor per load. */
    for (i = 0; i < j->count; i++)
    {
        if (path_is_dominion(j->paths[i]))
        {
            ndominion++;
        }
        else if (path_is_hart(j->paths[i]))
        {
            nhart++;
        }
    }
    if (ndominion > 0 && ndominion < j->count)
    {
        j->status = EeLoadStatus_Error;
        StringCchCopyW(j->err,
                       ARRAYSIZE(j->err),
                       L"Dominion CVR exports (.zip) cannot be loaded together with other "
                       L"vendors' files. Select files from one vendor.");
    }
    else if (ndominion > 0)
    {
        j->status = EeCvr_LoadFromDominionZips(j->paths,
                                               j->count,
                                               j->table,
                                               &j->cancel,
                                               CvrLoadProgressCb,
                                               j,
                                               j->err,
                                               ARRAYSIZE(j->err));
    }
    else if (nhart > 0 && nhart < j->count)
    {
        j->status = EeLoadStatus_Error;
        StringCchCopyW(j->err,
                       ARRAYSIZE(j->err),
                       L"Hart files (.zip, .pdf) and ES&S files (.xlsx, .csv, .tsv, .txt) "
                       L"cannot be loaded together. Select files from one vendor.");
    }
    else if (nhart > 0)
    {
        j->status = EeCvr_LoadFromHartFiles(j->paths,
                                            j->count,
                                            j->table,
                                            &j->cancel,
                                            CvrLoadProgressCb,
                                            j,
                                            j->err,
                                            ARRAYSIZE(j->err));
    }
    else
    {
        j->status = EeCvr_LoadFromFiles(j->paths,
                                        j->count,
                                        j->table,
                                        &j->cancel,
                                        CvrLoadProgressCb,
                                        j,
                                        j->err,
                                        ARRAYSIZE(j->err));
    }
    PostMessageW(j->dlg, EEM_CVR_LOAD_DONE, 0, 0);
    return 0;
}

static INT_PTR CALLBACK CvrLoadDlgProc(HWND dlg, UINT msg, WPARAM wParam, LPARAM lParam)
{
    CvrLoadJob *j = (CvrLoadJob *)GetWindowLongPtrW(dlg, GWLP_USERDATA);

    switch (msg)
    {
        case WM_INITDIALOG:
        {
            AppState *app;
            RECT rc;
            int margin;
            HWND label;
            HWND count;
            HWND bar;
            HWND cancel;
            j = (CvrLoadJob *)lParam;
            SetWindowLongPtrW(dlg, GWLP_USERDATA, (LONG_PTR)j);
            j->dlg = dlg;
            app = j->app;

            App_CenterModalClient(dlg, app, Scale(app, 340), Scale(app, 140));
            GetClientRect(dlg, &rc);
            margin = Scale(app, 14);
            label = CreateWindowExW(0,
                                    L"STATIC",
                                    L"Loading Cast Vote Records…",
                                    WS_CHILD | WS_VISIBLE | SS_LEFT,
                                    margin,
                                    margin,
                                    rc.right - 2 * margin,
                                    Scale(app, 20),
                                    dlg,
                                    NULL,
                                    app->instance,
                                    NULL);
            /* Running ballot-record count, like the voter-list load. */
            count = CreateWindowExW(0,
                                    L"STATIC",
                                    L"0 ballot records",
                                    WS_CHILD | WS_VISIBLE | SS_LEFT,
                                    margin,
                                    margin + Scale(app, 22),
                                    rc.right - 2 * margin,
                                    Scale(app, 18),
                                    dlg,
                                    NULL,
                                    app->instance,
                                    NULL);
            j->count_label = count;
            bar = CreateWindowExW(0,
                                  PROGRESS_CLASSW,
                                  NULL,
                                  WS_CHILD | WS_VISIBLE,
                                  margin,
                                  margin + Scale(app, 46),
                                  rc.right - 2 * margin,
                                  Scale(app, 16),
                                  dlg,
                                  NULL,
                                  app->instance,
                                  NULL);
            j->bar = bar;
            cancel = CreateWindowExW(0,
                                     L"BUTTON",
                                     L"Cancel",
                                     WS_CHILD | WS_VISIBLE | WS_TABSTOP,
                                     rc.right - margin - Scale(app, 84),
                                     rc.bottom - margin - Scale(app, 26),
                                     Scale(app, 84),
                                     Scale(app, 26),
                                     dlg,
                                     (HMENU)(INT_PTR)IDCANCEL,
                                     app->instance,
                                     NULL);
            if (app->font_ui)
            {
                SendMessageW(label, WM_SETFONT, (WPARAM)app->font_ui, TRUE);
                SendMessageW(count, WM_SETFONT, (WPARAM)app->font_ui, TRUE);
                SendMessageW(cancel, WM_SETFONT, (WPARAM)app->font_ui, TRUE);
            }
            if (bar != NULL)
            {
                SendMessageW(bar, PBM_SETRANGE32, 0, 100);
                SendMessageW(bar, PBM_SETPOS, 0, 0);
            }
            j->thread = CreateThread(NULL, 0, CvrLoadThreadProc, j, 0, NULL);
            if (j->thread == NULL)
            {
                j->status = EeLoadStatus_Error;
                StringCchCopyW(j->err, ARRAYSIZE(j->err), L"Could not start the load thread.");
                EndDialog(dlg, 0);
            }
            return (INT_PTR)FALSE;
        }

        case EEM_CVR_LOAD_PROGRESS:
            if (j != NULL)
            {
                unsigned long n = (unsigned long)wParam;
                int pct = (int)LOWORD(lParam);
                int mode = (int)HIWORD(lParam); /* 0 rows, 1 scanning, 2 OCR pages */
                if (j->bar != NULL)
                {
                    SendMessageW(j->bar, PBM_SETPOS, (WPARAM)pct, 0);
                }
                if (j->count_label != NULL)
                {
                    wchar_t txt[96];
                    if (mode == 2)
                    {
                        /* Scanned Hart PDF: OCR runs ~0.15 s per page. */
                        StringCchPrintfW(txt, ARRAYSIZE(txt),
                                         L"Reading scanned page %lu of %ld (OCR)…", n,
                                         (long)j->ocr_pages_total);
                    }
                    else if (mode == 1)
                    {
                        /* Discovery pass: no rows produced yet. */
                        StringCchCopyW(txt, ARRAYSIZE(txt), L"Scanning ballots…");
                    }
                    else
                    {
                        StringCchPrintfW(txt, ARRAYSIZE(txt), L"%lu ballot record%s", n,
                                         n == 1ul ? L"" : L"s");
                    }
                    SetWindowTextW(j->count_label, txt);
                }
            }
            return (INT_PTR)TRUE;

        case EEM_CVR_LOAD_DONE:
            if (j != NULL && j->thread != NULL)
            {
                WaitForSingleObject(j->thread, INFINITE);
                CloseHandle(j->thread);
                j->thread = NULL;
            }
            EndDialog(dlg, 1);
            return (INT_PTR)TRUE;

        case WM_COMMAND:
            if (j != NULL && LOWORD(wParam) == IDCANCEL)
            {
                HWND b = GetDlgItem(dlg, IDCANCEL);
                InterlockedExchange(&j->cancel, 1);
                if (b != NULL)
                {
                    EnableWindow(b, FALSE);
                }
                return (INT_PTR)TRUE;
            }
            break;

        case WM_CLOSE:
            if (j != NULL)
            {
                InterlockedExchange(&j->cancel, 1);
            }
            return (INT_PTR)TRUE;

        default:
            break;
    }
    return (INT_PTR)FALSE;
}

static EeLoadStatus App_RunCvrLoad(AppState *app,
                                   const wchar_t *const *paths,
                                   int count,
                                   EeCvrTable *table,
                                   wchar_t *err,
                                   size_t errcch)
{
    CvrLoadJob job;
    ZeroMemory(&job, sizeof(job));
    job.app = app;
    job.paths = paths;
    job.count = count;
    job.table = table;
    job.status = EeLoadStatus_Error;
    App_RunModalDialog(app, L"Loading Cast Vote Records", CvrLoadDlgProc, (LPARAM)&job);
    if (err != NULL && errcch > 0)
    {
        StringCchCopyW(err, errcch, job.err);
    }
    return job.status;
}

static void App_BeginOpenCvr(AppState *app)
{
    OPENFILENAMEW ofn;
    wchar_t *files;
    wchar_t *paths[64];
    int count = 0;
    int i;
    EeCvrTable table;
    EeLoadStatus s;
    wchar_t err[512];
    wchar_t title[160];

    if (app == NULL)
    {
        return;
    }
    files = (wchar_t *)malloc(32768 * sizeof(wchar_t));
    if (files == NULL)
    {
        return;
    }
    files[0] = L'\0';

    ZeroMemory(&ofn, sizeof(ofn));
    ofn.lStructSize = sizeof(ofn);
    ofn.hwndOwner = app->hwnd_main;
    ofn.lpstrFilter =
        L"Cast Vote Records (*.xlsx;*.csv;*.tsv;*.txt;*.zip;*.pdf)\0"
        L"*.xlsx;*.csv;*.tsv;*.txt;*.zip;*.pdf\0"
        L"ES&S (*.xlsx;*.csv;*.tsv;*.txt)\0*.xlsx;*.csv;*.tsv;*.txt\0"
        L"Hart (*.zip;*.pdf)\0*.zip;*.pdf\0"
        L"Dominion (*.zip)\0*.zip\0"
        L"All files (*.*)\0*.*\0";
    ofn.lpstrFile = files;
    ofn.nMaxFile = 32768;
    ofn.Flags = OFN_FILEMUSTEXIST | OFN_PATHMUSTEXIST | OFN_EXPLORER | OFN_ALLOWMULTISELECT;
    ofn.lpstrTitle = L"Load Cast Vote Records";

    if (!GetOpenFileNameW(&ofn))
    {
        free(files);
        return;
    }

    /* Parse the multi-select result: either a single full path, or a directory
     * followed by NUL-separated file names, terminated by a double NUL. */
    {
        const wchar_t *dir = files;
        size_t dlen = wcslen(dir);
        const wchar_t *p = files + dlen + 1;
        if (*p == L'\0')
        {
            paths[count++] = _wcsdup(files); /* single selection */
        }
        else
        {
            BOOL need_sep = (dlen > 0 && dir[dlen - 1] != L'\\');
            while (*p != L'\0' && count < (int)ARRAYSIZE(paths))
            {
                size_t need = dlen + 1 + wcslen(p) + 1;
                wchar_t *full = (wchar_t *)malloc(need * sizeof(wchar_t));
                if (full == NULL)
                {
                    break;
                }
                StringCchCopyW(full, need, dir);
                if (need_sep)
                {
                    StringCchCatW(full, need, L"\\");
                }
                StringCchCatW(full, need, p);
                paths[count++] = full;
                p += wcslen(p) + 1;
            }
        }
    }
    free(files);
    if (count == 0)
    {
        return;
    }

    EeCvr_Init(&table);
    err[0] = L'\0';
    s = App_RunCvrLoad(app, (const wchar_t *const *)paths, count, &table, err, ARRAYSIZE(err));

    if (s == EeLoadStatus_Ok)
    {
        const wchar_t *leaf = paths[0];
        const wchar_t *q;
        for (q = paths[0]; *q != L'\0'; q++)
        {
            if (*q == L'\\' || *q == L'/')
            {
                leaf = q + 1;
            }
        }
        if (count > 1)
        {
            StringCchPrintfW(title,
                             ARRAYSIZE(title),
                             L"%s (+%d more) — Cast Vote Records",
                             leaf,
                             count - 1);
        }
        else
        {
            StringCchPrintfW(title, ARRAYSIZE(title), L"%s — Cast Vote Records", leaf);
        }
        App_CreateCvrWindow(app, &table, title, paths[0]);
    }
    else
    {
        EeCvr_Clear(&table);
        if (s == EeLoadStatus_Error)
        {
            MessageBoxW(app->hwnd_main,
                        (err[0] != L'\0') ? err : L"Could not load the Cast Vote Records.",
                        k_WindowTitle,
                        MB_ICONERROR | MB_OK);
        }
    }

    for (i = 0; i < count; i++)
    {
        free(paths[i]);
    }
}

/* -------------------------------------------------------------------------- */
/* Election Results window (1.5)                                              */
/* -------------------------------------------------------------------------- */

/* Kinds of list column. */
enum
{
    RC_CONTEST = 0,
    RC_VOTEFOR,
    RC_CHOICE,
    RC_PARTY,
    RC_PRECINCT,
    RC_VT, /* one vote type (ResultsWindow.col_vt) */
    RC_TOTAL,
    RC_PERCENT
};

/* Kinds of list row. */
enum
{
    RR_CHOICE = 0,
    RR_OVER,
    RR_UNDER
};

#define RES_MAX_COLS 96
#define RES_ALL_PRECINCTS UINT32_MAX

/* Filter columns (EeFilterRule.column) of the results window. */
enum
{
    RF_PRECINCT = 0,
    RF_CONTEST,
    RF_CHOICE,
    RF_PARTY,
    RF_VOTETYPE,
    RF_COUNT
};

static const wchar_t *const k_ResFilterTitles[RF_COUNT] = {L"Precinct", L"Contest", L"Choice",
                                                          L"Party", L"Vote Type"};

typedef struct ResRow
{
    uint32_t contest;
    uint32_t choice; /* local choice index (RR_CHOICE) */
    uint32_t pre;    /* local precinct index in the contest, or RES_ALL_PRECINCTS */
    uint32_t seq;    /* position in display order, the sort's tie-break */
    uint8_t kind;    /* RR_* */
} ResRow;

typedef struct ResTurnoutWindow ResTurnoutWindow;
static void ResTurnout_Refresh(ResTurnoutWindow *tw);

typedef struct ResultsWindow
{
    AppState *app; /* this window's own resource-only AppState */
    HWND hwnd;
    HWND list;
    HWND status;
    EeResults res;
    BOOL by_precinct; /* View: By Precinct (else Totals) */
    BOOL show_vt;     /* one column per vote type */
    ResRow *rows;
    uint32_t nrows;
    int sort_col; /* -1 = display order */
    BOOL sort_asc;
    int ncols;
    uint8_t col_kind[RES_MAX_COLS];
    uint32_t col_vt[RES_MAX_COLS];
    const wchar_t *col_title[RES_MAX_COLS]; /* header text (static or a vote type name) */
    /* Tabulation (all precincts and vote types until the filter arrives). */
    int64_t *tot;    /* nchoices */
    int64_t *tot_vt; /* nchoices * nvt */
    int64_t *ovr;    /* ncontests */
    int64_t *und;    /* ncontests */
    int64_t *ctot;   /* ncontests: sum of the known choice totals, for percentages */
    BOOL tab_exact;  /* FALSE when a filtered total had to use unfiltered figures */
    /* Filter (step 3): is / is not rules on the RF_* columns. Precinct and vote type rules
     * choose what is counted; contest, choice and party rules hide rows. */
    EeFilterSet filters;
    HWND hwnd_filter;
    BOOL filt_active;
    uint8_t *pmask; /* nprecincts: precinct counted (NULL = all) */
    uint8_t *vmask; /* nvt: vote type counted (NULL = all) */
    uint32_t pre_included;
    uint8_t filt_hidden[RF_COUNT]; /* filter columns not offered (no precincts / vote types) */
    ResTurnoutWindow *turnout;     /* open turnout report, or NULL */
    HWND cmp_targets[32];          /* Compare menu: windows by command offset */
    wchar_t base_name[192]; /* first file's base name (no extension), for exports */
} ResultsWindow;

static void App_ExportResults(ResultsWindow *rw, BOOL selection_only);

static WNDPROC g_old_results_list_proc = NULL;

static const wchar_t k_ResHelpLoading[] =
    L"File \x2192 Load Election Results\x2026 opens election results files.\r\n\r\n"
    L"Clarity Elections results: select any of an election's ZIP files \x2014 summary.zip, "
    L"detailtxt.zip, detailxls.zip (or detailxlsx.zip) and detailxml.zip \x2014 or the files "
    L"inside them (summary.csv, detail.txt, detail.xls, detail.xlsx, detail.xml). An Election "
    L"Explorer results export (CSV or TSV) loads too.\r\n\r\n"
    L"Results report PDFs:\r\n"
    L"\x2022 ES&S Electionware: the summary (cumulative) results report and the precinct "
    L"report (\"Precinct Summary Results Report\", \"Precinct Results\", \"Canvass\"). Write-in "
    L"names are part of Write-In Totals. A report printed to PDF through a printer driver, "
    L"whose letters are drawn as shapes, is read with the OCR built into Windows (an OCR "
    L"language such as English must be installed); the Load Summary notes it, so check its "
    L"figures.\r\n"
    L"\x2022 Hart Verity: the cumulative results report and the canvass results report (a "
    L"county's ZIP of them loads too; the CSV files beside them are skipped). The canvass has "
    L"no vote-type breakdown.\r\n"
    L"\x2022 Hart reports from before Verity (the Hart Voting System, HVS 6.x, which some Texas "
    L"counties used through 2024): the cumulative report, the precinct report (by vote type) and "
    L"the canvass.\r\n"
    L"\x2022 Dominion Democracy Suite: the Election Summary Report, saved as XML, XLSX or PDF "
    L"(county-wide totals with ballots cast by counting group; a ranked-choice contest shows "
    L"its first choices; qualified write-in candidates are listed by name), and the Statement of "
    L"Votes Cast PDF, which gives every precinct by vote type. Where it prints \"****\" for a "
    L"count too small to keep the votes private, the precinct's Total still gives the whole "
    L"count, and the votes whose vote type is not printed are under the vote type \"Withheld\".\r\n"
    L"\x2022 Scanned ES&S reports (page images only): Electionware summary and precinct "
    L"reports, and the older monospaced reports (\"SUMMRY REPT-GROUP DETAIL\", \"PREC "
    L"REPORT-GROUP DETAIL\"), are read with the OCR built into Windows, about 0.3 seconds a "
    L"page. Each row's total must equal the sum of its vote types and each column the "
    L"contest's Total row: a count OCR left out or misread is filled or corrected from them, "
    L"and the Load Summary says how many rows still do not add up, so check those against the "
    L"report. A precinct whose name could not be read is named by its page.\r\n"
    L"Not read: scanned Hart and other reports, ES&S \"Statement of Votes Cast\" and custom "
    L"table reports, and password-protected PDFs.\r\n"
    L"A summary report gives totals only; the precinct report or canvass gives each "
    L"precinct. A county that posts a report per city or school district (McLennan, Cameron): "
    L"select them all, and the files of one election open as one window.\r\n\r\n"
    L"Several files of one election open as one window: the richest file is shown (XML, then "
    L"XLS or a precinct report, then TXT, then a summary) and every other file is checked "
    L"against it. Each "
    L"disagreement is listed in Reports \x2192 Display Load Summary\x2026, which opens by itself "
    L"when the files disagree. Files from different elections open in separate windows.\r\n\r\n"
    L"Clarity writes \"protected\" for counts a county withholds; they count as 0 in precinct "
    L"sums and are noted in the Load Summary.";

static const wchar_t k_ResHelpFilters[] =
    L"Filter \x2192 Filter\x2026 (Ctrl+L) chooses what the window counts and shows. Each rule "
    L"is a column, \"is\" or \"is not\", a value, and Include or Exclude:\r\n"
    L"\x2022 Precinct: only the included precincts are counted, so every total, percentage and "
    L"Over/Under Votes figure is recomputed from them. Contests with no included precinct are "
    L"hidden.\r\n"
    L"\x2022 Vote Type: only the included vote types are counted and shown as columns.\r\n"
    L"\x2022 Contest, Choice, Party: rows that do not pass are hidden; totals are unchanged.\r\n\r\n"
    L"Rules on the same column are alternatives (any included value passes); rules on "
    L"different columns all apply; a matching Exclude always wins. Check boxes turn rules on "
    L"and off. Export Filter\x2026 saves the rules to a file and Load Filter\x2026 reads them "
    L"back, also in another results window. Filter \x2192 Reset Filter removes every rule.\r\n\r\n"
    L"A precinct filter needs precinct detail (detail.txt, detail.xls(x), detail.xml, or a "
    L"precinct report or canvass PDF). For "
    L"contests that have only summary totals, or precinct counts the county withheld, the "
    L"status bar notes that some totals cannot be filtered.";

static const wchar_t k_ResHelpReports[] =
    L"Reports \x2192 Display Turnout Report\x2026 lists each precinct's registered voters, "
    L"ballots cast (by vote type when the files give it, the XLS turnout sheet for Clarity) "
    L"and turnout. The status bar totals the precincts shown; without a precinct filter it "
    L"shows the file's reported county totals. A precinct filter narrows the report. "
    L"Right-click to copy rows, include or exclude the selected precincts in the filter, or "
    L"export the rows to CSV / TSV.\r\n\r\n"
    L"Note: Clarity's precinct registered voters do not always add up to the county total in "
    L"detail.xml (in Travis County's primaries they add up to about twice it); the Load "
    L"Summary notes it.\r\n\r\n"
    L"Reports \x2192 Display Load Summary\x2026 lists the loaded files, the one shown, and every "
    L"note and difference found when the other files were checked against it.";

static const wchar_t k_ResHelpOptions[] =
    L"View \x2192 Totals shows one row per contest and choice. View \x2192 By Precinct shows the "
    L"same rows for each precinct in the contest's district.\r\n\r\n"
    L"Edit \x2192 Options\x2026:\r\n"
    L"\x2022 Zoom: the size of the list text.\r\n"
    L"\x2022 Show a column for each vote type: Ballot by Mail, Early Voting, Election Day, "
    L"provisional and so on, as the county names them.\r\n"
    L"\x2022 Party to display first: in a primary, the Republican or the Democratic contests come "
    L"first (shared with the CVR tabulation).\r\n\r\n"
    L"Click a column title to sort by it; click again to reverse. View \x2192 Reset View puts "
    L"the rows back in the order they had when loaded. Edit \x2192 Copy (Ctrl+C) or "
    L"right-click copies the selected rows as tab-separated text.";

/* Rank of a contest for the party-first order: 0 = the display-first party, 1 = the other
 * major party, 2 = everything else (like the CVR tabulation). */
static int Res_PartyRank(const EeResultsContest *c)
{
    BOOL rep = c->party != NULL && _wcsicmp(c->party, L"REP") == 0;
    BOOL dem = c->party != NULL && _wcsicmp(c->party, L"DEM") == 0;
    if (!rep && !dem)
    {
        return 2;
    }
    if (g_settings.cvr_tab_party_first == EE_TAB_PARTY_DEM)
    {
        return dem ? 0 : 1;
    }
    return rep ? 0 : 1;
}

static void Res_FreeTab(ResultsWindow *rw)
{
    free(rw->tot);
    free(rw->tot_vt);
    free(rw->ovr);
    free(rw->und);
    free(rw->ctot);
    rw->tot = rw->tot_vt = rw->ovr = rw->und = rw->ctot = NULL;
}

/* Tabulate every choice (filters arrive in a later step). */
static BOOL Res_Tabulate(ResultsWindow *rw)
{
    const EeResults *r = &rw->res;
    uint32_t c, k;
    Res_FreeTab(rw);
    rw->tot = (int64_t *)calloc((size_t)r->nchoices + 1, sizeof(int64_t));
    rw->tot_vt = (int64_t *)calloc((size_t)r->nchoices * EE_RESULTS_NVT(r) + 1, sizeof(int64_t));
    rw->ovr = (int64_t *)calloc((size_t)r->ncontests + 1, sizeof(int64_t));
    rw->und = (int64_t *)calloc((size_t)r->ncontests + 1, sizeof(int64_t));
    rw->ctot = (int64_t *)calloc((size_t)r->ncontests + 1, sizeof(int64_t));
    if (rw->tot == NULL || rw->tot_vt == NULL || rw->ovr == NULL || rw->und == NULL ||
        rw->ctot == NULL)
    {
        Res_FreeTab(rw);
        return FALSE;
    }
    rw->tab_exact = EeResults_Tabulate(r, rw->pmask, rw->vmask, rw->tot,
                                       r->nvt > 0 ? rw->tot_vt : NULL, rw->ovr, rw->und);
    for (c = 0; c < r->ncontests; c++)
    {
        const EeResultsContest *ct = &r->contests[c];
        for (k = 0; k < ct->nchoices; k++)
        {
            int64_t v = rw->tot[ct->first_choice + k];
            if (v != EE_RES_UNKNOWN)
            {
                rw->ctot[c] += v;
            }
        }
    }
    return TRUE;
}

/* Rows of the current view, in display order (contests ranked by party). */
/* ProcMon-style test of one column's rules on @p value: an enabled exclude that matches
 * rejects; when the column has enabled includes, one of them must match. Rules on other
 * columns are ignored (each results column filters its own dimension). */
static BOOL Filter_ColumnAccepts(const EeFilterSet *set, uint32_t column, const wchar_t *value)
{
    uint32_t i;
    BOOL any_include = FALSE, included = FALSE;
    for (i = 0; i < set->count; i++)
    {
        const EeFilterRule *r = &set->rules[i];
        BOOL eq;
        if (!r->enabled || r->column != column)
        {
            continue;
        }
        eq = CompareStringOrdinal(value, -1, r->value, -1, TRUE) == CSTR_EQUAL;
        if (r->relation == EeRel_IsNot)
        {
            eq = !eq;
        }
        if (r->action == EeFilt_Exclude)
        {
            if (eq)
            {
                return FALSE;
            }
        }
        else
        {
            any_include = TRUE;
            included = included || eq;
        }
    }
    return !any_include || included;
}

/* Is a row (contest, choice or Over/Under Votes) shown under the contest / choice / party
 * rules? */
static BOOL Res_RowPasses(const ResultsWindow *rw, uint32_t contest, uint8_t kind, uint32_t choice)
{
    const EeResultsContest *ct = &rw->res.contests[contest];
    const wchar_t *name;
    const wchar_t *party = L"";
    if (!rw->filt_active)
    {
        return TRUE;
    }
    if (kind == RR_OVER)
    {
        name = L"Over Votes";
    }
    else if (kind == RR_UNDER)
    {
        name = L"Under Votes";
    }
    else
    {
        name = rw->res.choices[ct->first_choice + choice].name;
        if (rw->res.choices[ct->first_choice + choice].party != NULL)
        {
            party = rw->res.choices[ct->first_choice + choice].party;
        }
    }
    return Filter_ColumnAccepts(&rw->filters, RF_CONTEST, ct->title) &&
           Filter_ColumnAccepts(&rw->filters, RF_CHOICE, name) &&
           Filter_ColumnAccepts(&rw->filters, RF_PARTY, party);
}

/* Does a contest's district include a counted precinct? (Always, for a contest without
 * precinct detail.) */
static BOOL Res_ContestHasPrecinct(const ResultsWindow *rw, const EeResultsContest *ct)
{
    uint32_t p;
    if (rw->pmask == NULL || ct->counts == NULL)
    {
        return TRUE;
    }
    for (p = 0; p < ct->npre; p++)
    {
        if (rw->pmask[ct->pre[p]])
        {
            return TRUE;
        }
    }
    return FALSE;
}

static BOOL Res_BuildRows(ResultsWindow *rw)
{
    const EeResults *r = &rw->res;
    size_t cap = 0, n = 0;
    ResRow *rows = NULL;
    int pass;
    uint32_t c;

    /* Size first: one pass to count, one to fill. */
    for (pass = 0; pass < 2; pass++)
    {
        int rank;
        n = 0;
        for (rank = 0; rank < 3; rank++)
        {
            for (c = 0; c < r->ncontests; c++)
            {
                const EeResultsContest *ct = &r->contests[c];
                uint32_t k, p;
                uint32_t npre = (rw->by_precinct && ct->counts != NULL) ? ct->npre : 1;
                if (Res_PartyRank(ct) != rank || !Res_ContestHasPrecinct(rw, ct))
                {
                    continue;
                }
                for (p = 0; p < npre; p++)
                {
                    uint32_t pre = (rw->by_precinct && ct->counts != NULL) ? p : RES_ALL_PRECINCTS;
                    uint32_t extra = 0;
                    if (pre != RES_ALL_PRECINCTS && rw->pmask != NULL && !rw->pmask[ct->pre[p]])
                    {
                        continue; /* a precinct the filter leaves out */
                    }
                    if (ct->has_over_under &&
                        (pre == RES_ALL_PRECINCTS || (ct->over != NULL && ct->under != NULL)))
                    {
                        extra = 2;
                    }
                    if (pass == 1)
                    {
                        for (k = 0; k < ct->nchoices; k++)
                        {
                            if (!Res_RowPasses(rw, c, RR_CHOICE, k))
                            {
                                continue;
                            }
                            rows[n].contest = c;
                            rows[n].choice = k;
                            rows[n].pre = pre;
                            rows[n].kind = RR_CHOICE;
                            rows[n].seq = (uint32_t)n;
                            n++;
                        }
                        for (k = 0; k < extra; k++)
                        {
                            if (!Res_RowPasses(rw, c, (uint8_t)(k == 0 ? RR_OVER : RR_UNDER), 0))
                            {
                                continue;
                            }
                            rows[n].contest = c;
                            rows[n].choice = 0;
                            rows[n].pre = pre;
                            rows[n].kind = (uint8_t)(k == 0 ? RR_OVER : RR_UNDER);
                            rows[n].seq = (uint32_t)n;
                            n++;
                        }
                    }
                    else
                    {
                        for (k = 0; k < ct->nchoices; k++)
                        {
                            n += Res_RowPasses(rw, c, RR_CHOICE, k) ? 1 : 0;
                        }
                        for (k = 0; k < extra; k++)
                        {
                            n += Res_RowPasses(rw, c, (uint8_t)(k == 0 ? RR_OVER : RR_UNDER), 0)
                                     ? 1
                                     : 0;
                        }
                    }
                }
            }
        }
        if (pass == 0)
        {
            cap = n;
            rows = (ResRow *)malloc((cap + 1) * sizeof(ResRow));
            if (rows == NULL)
            {
                return FALSE;
            }
        }
    }
    free(rw->rows);
    rw->rows = rows;
    rw->nrows = (uint32_t)n;
    return TRUE;
}

/* Votes of one row in one vote type (v < nvt), or over all vote types (v == UINT32_MAX).
 * Returns EE_RES_UNKNOWN for a withheld cell or total. */
static int64_t Res_RowVotes(const ResultsWindow *rw, const ResRow *row, uint32_t v)
{
    const EeResults *r = &rw->res;
    const EeResultsContest *ct = &r->contests[row->contest];
    uint32_t nvt = EE_RESULTS_NVT(r);
    if (row->pre == RES_ALL_PRECINCTS)
    {
        if (row->kind == RR_OVER)
        {
            return v == UINT32_MAX ? rw->ovr[row->contest] : EE_RES_UNKNOWN;
        }
        if (row->kind == RR_UNDER)
        {
            return v == UINT32_MAX ? rw->und[row->contest] : EE_RES_UNKNOWN;
        }
        if (v == UINT32_MAX)
        {
            return rw->tot[ct->first_choice + row->choice];
        }
        return (r->nvt > 0 && ct->counts != NULL)
                   ? rw->tot_vt[(size_t)(ct->first_choice + row->choice) * r->nvt + v]
                   : EE_RES_UNKNOWN;
    }
    if (row->kind == RR_OVER || row->kind == RR_UNDER)
    {
        const int32_t *a = row->kind == RR_OVER ? ct->over : ct->under;
        return (a != NULL && v == UINT32_MAX) ? a[row->pre] : EE_RES_UNKNOWN;
    }
    {
        size_t base = ((size_t)row->choice * ct->npre + row->pre) * nvt;
        if (v != UINT32_MAX)
        {
            if (ct->protected_cells != NULL && ct->protected_cells[base + v])
            {
                return EE_RES_UNKNOWN;
            }
            return ct->counts[base + v];
        }
        {
            int64_t s = 0;
            uint32_t q;
            for (q = 0; q < nvt; q++)
            {
                if (r->nvt == 0 || rw->vmask == NULL || rw->vmask[q])
                {
                    s += ct->counts[base + q];
                }
            }
            return s;
        }
    }
}

/* All the choices' votes of a row's contest in the row's precinct (for percentages). */
static int64_t Res_RowBase(const ResultsWindow *rw, const ResRow *row)
{
    const EeResults *r = &rw->res;
    const EeResultsContest *ct = &r->contests[row->contest];
    uint32_t nvt = EE_RESULTS_NVT(r);
    int64_t s = 0;
    uint32_t k, q;
    if (row->pre == RES_ALL_PRECINCTS)
    {
        return rw->ctot[row->contest];
    }
    for (k = 0; k < ct->nchoices; k++)
    {
        const int32_t *cell = ct->counts + ((size_t)k * ct->npre + row->pre) * nvt;
        for (q = 0; q < nvt; q++)
        {
            if (r->nvt == 0 || rw->vmask == NULL || rw->vmask[q])
            {
                s += cell[q];
            }
        }
    }
    return s;
}

static const wchar_t *Res_RowChoiceName(const ResultsWindow *rw, const ResRow *row)
{
    const EeResultsContest *ct = &rw->res.contests[row->contest];
    if (row->kind == RR_OVER)
    {
        return L"Over Votes";
    }
    if (row->kind == RR_UNDER)
    {
        return L"Under Votes";
    }
    return rw->res.choices[ct->first_choice + row->choice].name;
}

static const wchar_t *Res_RowPrecinct(const ResultsWindow *rw, const ResRow *row)
{
    const EeResultsContest *ct = &rw->res.contests[row->contest];
    if (row->pre == RES_ALL_PRECINCTS)
    {
        return L"";
    }
    return rw->res.precincts[ct->pre[row->pre]].name;
}

/* Text of one cell. */
static void Res_CellText(const ResultsWindow *rw, uint32_t ri, int col, wchar_t *out, size_t cch)
{
    const ResRow *row;
    const EeResultsContest *ct;
    int64_t v;
    if (cch == 0)
    {
        return;
    }
    out[0] = L'\0';
    if (ri >= rw->nrows || col < 0 || col >= rw->ncols)
    {
        return;
    }
    row = &rw->rows[ri];
    ct = &rw->res.contests[row->contest];
    switch (rw->col_kind[col])
    {
    case RC_CONTEST:
        StringCchCopyW(out, cch, ct->title);
        break;
    case RC_VOTEFOR:
        if (ct->vote_for > 0)
        {
            StringCchPrintfW(out, cch, L"%u", ct->vote_for);
        }
        break;
    case RC_CHOICE:
        StringCchCopyW(out, cch, Res_RowChoiceName(rw, row));
        break;
    case RC_PARTY:
        if (row->kind == RR_CHOICE)
        {
            const wchar_t *p = rw->res.choices[ct->first_choice + row->choice].party;
            StringCchCopyW(out, cch, p != NULL ? p : L"");
        }
        break;
    case RC_PRECINCT:
        StringCchCopyW(out, cch, row->pre == RES_ALL_PRECINCTS ? L"(all)" : Res_RowPrecinct(rw, row));
        break;
    case RC_VT:
    case RC_TOTAL:
        v = Res_RowVotes(rw, row, rw->col_kind[col] == RC_VT ? rw->col_vt[col] : UINT32_MAX);
        if (v != EE_RES_UNKNOWN)
        {
            StringCchPrintfW(out, cch, L"%lld", (long long)v);
        }
        else if (row->kind == RR_CHOICE && row->pre != RES_ALL_PRECINCTS &&
                 rw->col_kind[col] == RC_VT)
        {
            StringCchCopyW(out, cch, L"protected");
        }
        else if (row->kind == RR_CHOICE && rw->col_kind[col] == RC_TOTAL)
        {
            StringCchCopyW(out, cch, L"withheld");
        }
        break;
    case RC_PERCENT:
        if (row->kind == RR_CHOICE)
        {
            int64_t base = Res_RowBase(rw, row);
            v = Res_RowVotes(rw, row, UINT32_MAX);
            if (base > 0 && v != EE_RES_UNKNOWN)
            {
                StringCchPrintfW(out, cch, L"%.2f%%", (double)v * 100.0 / (double)base);
            }
        }
        break;
    default:
        break;
    }
}

/* Numeric value of a numeric column's cell, for sorting (-1 when blank). */
static double Res_CellNumber(const ResultsWindow *rw, const ResRow *row, int col)
{
    int64_t v;
    switch (rw->col_kind[col])
    {
    case RC_VOTEFOR:
        return (double)rw->res.contests[row->contest].vote_for;
    case RC_VT:
    case RC_TOTAL:
        v = Res_RowVotes(rw, row, rw->col_kind[col] == RC_VT ? rw->col_vt[col] : UINT32_MAX);
        return v == EE_RES_UNKNOWN ? -1.0 : (double)v;
    case RC_PERCENT:
        if (row->kind == RR_CHOICE)
        {
            int64_t base = Res_RowBase(rw, row);
            v = Res_RowVotes(rw, row, UINT32_MAX);
            if (base > 0 && v != EE_RES_UNKNOWN)
            {
                return (double)v * 100.0 / (double)base;
            }
        }
        return -1.0;
    default:
        return 0.0;
    }
}

static const wchar_t *Res_CellString(const ResultsWindow *rw, const ResRow *row, int col)
{
    const EeResultsContest *ct = &rw->res.contests[row->contest];
    switch (rw->col_kind[col])
    {
    case RC_CONTEST:
        return ct->title;
    case RC_CHOICE:
        return Res_RowChoiceName(rw, row);
    case RC_PARTY:
        if (row->kind == RR_CHOICE)
        {
            const wchar_t *p = rw->res.choices[ct->first_choice + row->choice].party;
            return p != NULL ? p : L"";
        }
        return L"";
    case RC_PRECINCT:
        return Res_RowPrecinct(rw, row);
    default:
        return L"";
    }
}

/* Compare two texts, numerically when both are whole numbers (precincts 99 < 100). */
static int Res_CompareText(const wchar_t *a, const wchar_t *b)
{
    wchar_t *ea, *eb;
    unsigned long long na, nb;
    if (*a != L'\0' && *b != L'\0' && iswdigit(*a) && iswdigit(*b))
    {
        na = wcstoull(a, &ea, 10);
        nb = wcstoull(b, &eb, 10);
        if (*ea == L'\0' && *eb == L'\0')
        {
            return na < nb ? -1 : (na > nb ? 1 : 0);
        }
    }
    return _wcsicmp(a, b);
}

typedef struct ResSortCtx
{
    const ResultsWindow *rw;
    int col;
    BOOL asc;
} ResSortCtx;

static int __cdecl Res_SortCmp(void *ctx, const void *pa, const void *pb)
{
    const ResSortCtx *sc = (const ResSortCtx *)ctx;
    const ResRow *a = (const ResRow *)pa;
    const ResRow *b = (const ResRow *)pb;
    int r = 0;
    uint8_t kind = sc->rw->col_kind[sc->col];
    if (kind == RC_VOTEFOR || kind == RC_VT || kind == RC_TOTAL || kind == RC_PERCENT)
    {
        double x = Res_CellNumber(sc->rw, a, sc->col);
        double y = Res_CellNumber(sc->rw, b, sc->col);
        r = x < y ? -1 : (x > y ? 1 : 0);
    }
    else
    {
        r = Res_CompareText(Res_CellString(sc->rw, a, sc->col), Res_CellString(sc->rw, b, sc->col));
    }
    if (!sc->asc)
    {
        r = -r;
    }
    if (r == 0)
    {
        r = a->seq < b->seq ? -1 : (a->seq > b->seq ? 1 : 0);
    }
    return r;
}

/* Put the rows in display order (seq), then apply the column sort. */
static int __cdecl Res_SeqCmp(const void *pa, const void *pb)
{
    const ResRow *a = (const ResRow *)pa;
    const ResRow *b = (const ResRow *)pb;
    return a->seq < b->seq ? -1 : (a->seq > b->seq ? 1 : 0);
}

static void Res_Sort(ResultsWindow *rw)
{
    if (rw->nrows < 2)
    {
        return;
    }
    if (rw->sort_col < 0 || rw->sort_col >= rw->ncols)
    {
        qsort(rw->rows, rw->nrows, sizeof(ResRow), Res_SeqCmp);
    }
    else
    {
        ResSortCtx sc;
        sc.rw = rw;
        sc.col = rw->sort_col;
        sc.asc = rw->sort_asc;
        qsort_s(rw->rows, rw->nrows, sizeof(ResRow), Res_SortCmp, &sc);
    }
}

/* (Re)create the list columns for the view and options. */
static void Res_BuildColumns(ResultsWindow *rw)
{
    const EeResults *r = &rw->res;
    struct
    {
        uint8_t kind;
        uint32_t vt;
        const wchar_t *title;
        int width;
        int fmt;
    } cols[RES_MAX_COLS];
    int n = 0, i;
    uint32_t v;
    HWND header;

    if (rw->list == NULL)
    {
        return;
    }
    header = ListView_GetHeader(rw->list);
    while (header != NULL && Header_GetItemCount(header) > 0)
    {
        ListView_DeleteColumn(rw->list, 0);
    }
#define RES_ADD_COL(k, t, w, f, vv)                                                                \
    do                                                                                             \
    {                                                                                              \
        if (n < RES_MAX_COLS)                                                                      \
        {                                                                                          \
            cols[n].kind = (uint8_t)(k);                                                           \
            cols[n].title = (t);                                                                   \
            cols[n].width = (w);                                                                   \
            cols[n].fmt = (f);                                                                     \
            cols[n].vt = (vv);                                                                     \
            n++;                                                                                   \
        }                                                                                          \
    } while (0)
    RES_ADD_COL(RC_CONTEST, L"Contest", 320, LVCFMT_LEFT, 0);
    RES_ADD_COL(RC_VOTEFOR, L"Vote For", 64, LVCFMT_RIGHT, 0);
    RES_ADD_COL(RC_CHOICE, L"Choice", 240, LVCFMT_LEFT, 0);
    RES_ADD_COL(RC_PARTY, L"Party", 60, LVCFMT_LEFT, 0);
    if (rw->by_precinct)
    {
        RES_ADD_COL(RC_PRECINCT, L"Precinct", 120, LVCFMT_LEFT, 0);
    }
    if (rw->show_vt)
    {
        for (v = 0; v < r->nvt; v++)
        {
            if (rw->vmask == NULL || rw->vmask[v])
            {
                RES_ADD_COL(RC_VT, r->vote_types[v], 100, LVCFMT_RIGHT, v);
            }
        }
    }
    RES_ADD_COL(RC_TOTAL, L"Total Votes", 96, LVCFMT_RIGHT, 0);
    RES_ADD_COL(RC_PERCENT, L"Percent", 72, LVCFMT_RIGHT, 0);
#undef RES_ADD_COL
    rw->ncols = n;
    for (i = 0; i < n; i++)
    {
        LVCOLUMNW col;
        rw->col_kind[i] = cols[i].kind;
        rw->col_vt[i] = cols[i].vt;
        rw->col_title[i] = cols[i].title;
        ZeroMemory(&col, sizeof(col));
        col.mask = LVCF_TEXT | LVCF_WIDTH | LVCF_FMT;
        col.fmt = i == 0 ? LVCFMT_LEFT : cols[i].fmt; /* column 0 is always left-aligned */
        col.pszText = (wchar_t *)cols[i].title;
        col.cx = ScaleDisplay(rw->app, cols[i].width);
        ListView_InsertColumn(rw->list, i, &col);
    }
}

static void Res_UpdateStatus(ResultsWindow *rw)
{
    const EeResults *r = &rw->res;
    wchar_t buf[600];
    wchar_t num[32];
    uint32_t i, nsrc_issues = 0;
    if (rw->status == NULL)
    {
        return;
    }
    buf[0] = L'\0';
    if (r->election_name != NULL)
    {
        StringCchCatW(buf, ARRAYSIZE(buf), r->election_name);
        if (r->election_date != NULL)
        {
            StringCchCatW(buf, ARRAYSIZE(buf), L" (");
            StringCchCatW(buf, ARRAYSIZE(buf), r->election_date);
            StringCchCatW(buf, ARRAYSIZE(buf), L")");
        }
        StringCchCatW(buf, ARRAYSIZE(buf), L"  \x2022  ");
    }
    StringCchPrintfW(num, ARRAYSIZE(num), L"%u contests", r->ncontests);
    StringCchCatW(buf, ARRAYSIZE(buf), num);
    if (r->nprecincts > 0)
    {
        StringCchPrintfW(num, ARRAYSIZE(num), L"  \x2022  %u precincts", r->nprecincts);
        StringCchCatW(buf, ARRAYSIZE(buf), num);
    }
    if (rw->by_precinct || rw->filt_active)
    {
        StringCchPrintfW(num, ARRAYSIZE(num), L"  \x2022  %u rows", rw->nrows);
        StringCchCatW(buf, ARRAYSIZE(buf), num);
    }
    if (rw->filt_active)
    {
        wchar_t f[160];
        if (rw->pmask != NULL)
        {
            StringCchPrintfW(f, ARRAYSIZE(f), L"  \x2022  filtered: %u of %u precincts",
                             rw->pre_included, r->nprecincts);
        }
        else
        {
            StringCchCopyW(f, ARRAYSIZE(f), L"  \x2022  filtered");
        }
        StringCchCatW(buf, ARRAYSIZE(buf), f);
        if (!rw->tab_exact)
        {
            StringCchCatW(buf, ARRAYSIZE(buf), L" (some totals cannot be filtered)");
        }
    }
    StringCchCatW(buf, ARRAYSIZE(buf), L"  \x2022  ");
    for (i = 0; i < r->nsources; i++)
    {
        if (i > 0)
        {
            StringCchCatW(buf, ARRAYSIZE(buf), L", ");
        }
        StringCchCatW(buf, ARRAYSIZE(buf), EeResults_FormatLabel(r->sources[i].format));
    }
    if (r->timestamp != NULL)
    {
        StringCchCatW(buf, ARRAYSIZE(buf), L"  \x2022  as of ");
        StringCchCatW(buf, ARRAYSIZE(buf), r->timestamp);
    }
    for (i = 0; i < r->nissues; i++)
    {
        if (r->issues[i].source != 0)
        {
            nsrc_issues++;
        }
    }
    if (nsrc_issues > 0)
    {
        StringCchPrintfW(num, ARRAYSIZE(num), L"  \x2022  %u differences", nsrc_issues);
        StringCchCatW(buf, ARRAYSIZE(buf), num);
    }
    else if (r->nissues > 0)
    {
        StringCchPrintfW(num, ARRAYSIZE(num), L"  \x2022  %u notes", r->nissues);
        StringCchCatW(buf, ARRAYSIZE(buf), num);
    }
    SendMessageW(rw->status, SB_SETTEXTW, 0, (LPARAM)buf);
}

/* Rebuild rows and columns after a view or option change, keeping the sort if its column
 * still exists. */
static void Res_Rebuild(ResultsWindow *rw)
{
    uint8_t sort_kind = (rw->sort_col >= 0 && rw->sort_col < rw->ncols)
                            ? rw->col_kind[rw->sort_col]
                            : 0xFF;
    uint32_t sort_vt = (rw->sort_col >= 0 && rw->sort_col < rw->ncols) ? rw->col_vt[rw->sort_col]
                                                                       : 0;
    int i;
    if (!Res_BuildRows(rw))
    {
        MessageBoxW(rw->hwnd, L"Out of memory.", k_WindowTitle, MB_ICONERROR | MB_OK);
        rw->nrows = 0;
    }
    SendMessageW(rw->list, WM_SETREDRAW, FALSE, 0);
    Res_BuildColumns(rw);
    rw->sort_col = -1;
    for (i = 0; i < rw->ncols && sort_kind != 0xFF; i++)
    {
        if (rw->col_kind[i] == sort_kind && (sort_kind != RC_VT || rw->col_vt[i] == sort_vt))
        {
            rw->sort_col = i;
            break;
        }
    }
    Res_Sort(rw);
    ListView_SetItemCountEx(rw->list, (int)rw->nrows, 0);
    SendMessageW(rw->list, WM_SETREDRAW, TRUE, 0);
    InvalidateRect(rw->list, NULL, TRUE);
    Res_UpdateStatus(rw);
    if (rw->turnout != NULL)
    {
        ResTurnout_Refresh(rw->turnout);
    }
}

static void Res_Layout(ResultsWindow *rw, int width, int height)
{
    int sb_h = 0;
    if (rw->status != NULL)
    {
        RECT sb;
        SendMessageW(rw->status, WM_SIZE, 0, 0);
        if (GetWindowRect(rw->status, &sb))
        {
            sb_h = sb.bottom - sb.top;
        }
    }
    if (rw->list != NULL)
    {
        MoveWindow(rw->list, 0, 0, width, height > sb_h ? height - sb_h : 0, TRUE);
    }
}

/* Copy the selected rows as tab-separated UTF-8, every column, CR/LF between rows. */
static void Res_CopySelected(ResultsWindow *rw)
{
    wchar_t *buf = NULL;
    size_t cap = 0, len = 0;
    wchar_t cell[1024];
    int i;
    if (rw == NULL || rw->list == NULL)
    {
        return;
    }
    for (i = ListView_GetNextItem(rw->list, -1, LVNI_SELECTED); i >= 0;
         i = ListView_GetNextItem(rw->list, i, LVNI_SELECTED))
    {
        int c;
        for (c = 0; c < rw->ncols; c++)
        {
            size_t n;
            Res_CellText(rw, (uint32_t)i, c, cell, ARRAYSIZE(cell));
            n = wcslen(cell);
            if (!cvr_wbuf_reserve(&buf, &cap, len + n + 3))
            {
                free(buf);
                return;
            }
            memcpy(buf + len, cell, n * sizeof(wchar_t));
            len += n;
            if (c + 1 < rw->ncols)
            {
                buf[len++] = L'\t';
            }
            else
            {
                buf[len++] = L'\r';
                buf[len++] = L'\n';
            }
        }
    }
    if (buf != NULL && len > 0)
    {
        int u8len;
        buf[len] = L'\0';
        u8len = WideCharToMultiByte(CP_UTF8, 0, buf, -1, NULL, 0, NULL, NULL);
        if (u8len > 0)
        {
            char *utf8 = (char *)malloc((size_t)u8len);
            if (utf8 != NULL && WideCharToMultiByte(CP_UTF8, 0, buf, -1, utf8, u8len, NULL, NULL) > 0)
            {
                App_SetClipboardUtf8(rw->hwnd, utf8);
            }
            free(utf8);
        }
    }
    free(buf);
}

static void Res_OnContextMenu(ResultsWindow *rw, int item, POINT screen)
{
    HMENU m;
    UINT cmd;
    if (item >= 0 && !(ListView_GetItemState(rw->list, item, LVIS_SELECTED) & LVIS_SELECTED))
    {
        ListView_SetItemState(rw->list, -1, 0, LVIS_SELECTED | LVIS_FOCUSED);
        ListView_SetItemState(rw->list, item, LVIS_SELECTED | LVIS_FOCUSED,
                              LVIS_SELECTED | LVIS_FOCUSED);
    }
    m = CreatePopupMenu();
    if (m == NULL)
    {
        return;
    }
    AppendMenuW(m, MF_STRING, IDM_EDIT_COPY, L"&Copy");
    AppendMenuW(m, MF_SEPARATOR, 0, NULL);
    AppendMenuW(m, MF_STRING, IDM_EXPORT_SELECTED, L"Export &Selected…");
    cmd = (UINT)TrackPopupMenu(m, TPM_RIGHTBUTTON | TPM_RETURNCMD, screen.x, screen.y, 0, rw->hwnd,
                               NULL);
    DestroyMenu(m);
    if (cmd == IDM_EDIT_COPY)
    {
        Res_CopySelected(rw);
    }
    else if (cmd == IDM_EXPORT_SELECTED)
    {
        App_ExportResults(rw, TRUE);
    }
}

/* Load Summary: the files, which one is shown, and every note and difference. */
static void App_ShowResultsLoadSummary(ResultsWindow *rw)
{
    const EeResults *r = &rw->res;
    wchar_t *text = NULL;
    size_t cap = 0, len = 0;
    uint32_t i, s;
#define RES_APPEND(str)                                                                            \
    do                                                                                             \
    {                                                                                              \
        const wchar_t *str_ = (str);                                                               \
        size_t n_ = wcslen(str_);                                                                  \
        if (cvr_wbuf_reserve(&text, &cap, len + n_ + 1))                                           \
        {                                                                                          \
            memcpy(text + len, str_, n_ * sizeof(wchar_t));                                        \
            len += n_;                                                                             \
            text[len] = L'\0';                                                                     \
        }                                                                                          \
    } while (0)
    RES_APPEND(L"Files:\r\n");
    for (s = 0; s < r->nsources; s++)
    {
        const EeResultsSource *src = &r->sources[s];
        wchar_t line[MAX_PATH + 300];
        StringCchPrintfW(line, ARRAYSIZE(line), L"%u. %s%s%s%s \x2014 %s: %u contests, %u choices, "
                                                L"%u precincts%s\r\n",
                         s + 1, App_PathBaseName(src->path), src->member != NULL ? L" (" : L"",
                         src->member != NULL ? src->member : L"", src->member != NULL ? L")" : L"",
                         EeResults_FormatLabel(src->format), src->ncontests, src->nchoices,
                         src->nprecincts, src->primary ? L" \x2014 shown" : L"");
        RES_APPEND(line);
    }
    for (s = 0; s < r->nsources; s++)
    {
        BOOL header = FALSE;
        for (i = 0; i < r->nissues; i++)
        {
            if (r->issues[i].source != s)
            {
                continue;
            }
            if (!header)
            {
                wchar_t line[MAX_PATH + 64];
                StringCchPrintfW(line, ARRAYSIZE(line),
                                 s == 0 ? L"\r\nNotes on file 1 (shown):\r\n"
                                        : L"\r\nFile %u compared with file 1:\r\n",
                                 s + 1);
                RES_APPEND(line);
                header = TRUE;
            }
            RES_APPEND(L"\x2022 ");
            RES_APPEND(r->issues[i].text);
            RES_APPEND(L"\r\n");
        }
        if (!header && s > 0)
        {
            wchar_t line[96];
            StringCchPrintfW(line, ARRAYSIZE(line),
                             L"\r\nFile %u agrees with file 1.\r\n", s + 1);
            RES_APPEND(line);
        }
    }
#undef RES_APPEND
    if (text != NULL)
    {
        App_ShowHelpTopicOn(rw->app, rw->hwnd, L"Election Results Load Summary", text);
    }
    free(text);
}

/* --- Filter --------------------------------------------------------------- */

static int __cdecl Res_ValueCmp(const void *pa, const void *pb)
{
    return Res_CompareText(*(const wchar_t *const *)pa, *(const wchar_t *const *)pb);
}

/* Add @p v to a value list unless present (case-insensitive). */
static BOOL Res_AddValue(wchar_t ***vals, uint32_t *n, size_t *cap, const wchar_t *v)
{
    uint32_t i;
    if (v == NULL || v[0] == L'\0' || *n >= EE_FILTER_MAX_DISTINCT)
    {
        return TRUE;
    }
    for (i = 0; i < *n; i++)
    {
        if (CompareStringOrdinal((*vals)[i], -1, v, -1, TRUE) == CSTR_EQUAL)
        {
            return TRUE;
        }
    }
    if (*n + 1 > *cap)
    {
        size_t nc = *cap ? *cap * 2 : 64;
        wchar_t **nv = (wchar_t **)realloc(*vals, nc * sizeof(wchar_t *));
        if (nv == NULL)
        {
            return FALSE;
        }
        *vals = nv;
        *cap = nc;
    }
    (*vals)[*n] = _wcsdup(v);
    if ((*vals)[*n] == NULL)
    {
        return FALSE;
    }
    (*n)++;
    return TRUE;
}

/* The distinct values of a results filter column, sorted (precincts numerically). */
static BOOL Res_FilterCollect(void *ctx, uint32_t column, wchar_t ***vals, uint32_t *n)
{
    ResultsWindow *rw = (ResultsWindow *)ctx;
    const EeResults *r = &rw->res;
    size_t cap = 0;
    uint32_t i;
    BOOL ok = TRUE;
    *vals = NULL;
    *n = 0;
    switch (column)
    {
    case RF_PRECINCT:
        for (i = 0; i < r->nprecincts && ok; i++)
        {
            ok = Res_AddValue(vals, n, &cap, r->precincts[i].name);
        }
        break;
    case RF_CONTEST:
        for (i = 0; i < r->ncontests && ok; i++)
        {
            ok = Res_AddValue(vals, n, &cap, r->contests[i].title);
        }
        break;
    case RF_CHOICE:
        for (i = 0; i < r->nchoices && ok; i++)
        {
            ok = Res_AddValue(vals, n, &cap, r->choices[i].name);
        }
        for (i = 0; i < r->ncontests && ok; i++)
        {
            if (r->contests[i].has_over_under)
            {
                ok = Res_AddValue(vals, n, &cap, L"Over Votes") &&
                     Res_AddValue(vals, n, &cap, L"Under Votes");
                break;
            }
        }
        break;
    case RF_PARTY:
        for (i = 0; i < r->nchoices && ok; i++)
        {
            ok = Res_AddValue(vals, n, &cap, r->choices[i].party);
        }
        break;
    case RF_VOTETYPE:
        for (i = 0; i < r->nvt && ok; i++)
        {
            ok = Res_AddValue(vals, n, &cap, r->vote_types[i]);
        }
        break;
    default:
        break;
    }
    if (*n > 1 && column != RF_VOTETYPE)
    {
        qsort(*vals, *n, sizeof(wchar_t *), Res_ValueCmp);
    }
    return ok;
}

/* Apply rw->filters: precinct / vote type masks, re-tabulation, rows. */
static void Res_ApplyFilter(ResultsWindow *rw)
{
    const EeResults *r = &rw->res;
    uint32_t i, j;
    BOOL has_pre = FALSE, has_vt = FALSE;
    free(rw->pmask);
    free(rw->vmask);
    rw->pmask = NULL;
    rw->vmask = NULL;
    rw->pre_included = r->nprecincts;
    rw->filt_active = EeFilter_HasEnabled(&rw->filters);
    for (i = 0; i < rw->filters.count; i++)
    {
        if (rw->filters.rules[i].enabled)
        {
            has_pre = has_pre || rw->filters.rules[i].column == RF_PRECINCT;
            has_vt = has_vt || rw->filters.rules[i].column == RF_VOTETYPE;
        }
    }
    if (has_pre && r->nprecincts > 0)
    {
        rw->pmask = (uint8_t *)malloc(r->nprecincts);
        if (rw->pmask != NULL)
        {
            rw->pre_included = 0;
            for (j = 0; j < r->nprecincts; j++)
            {
                rw->pmask[j] =
                    (uint8_t)Filter_ColumnAccepts(&rw->filters, RF_PRECINCT, r->precincts[j].name);
                rw->pre_included += rw->pmask[j];
            }
        }
    }
    if (has_vt && r->nvt > 0)
    {
        rw->vmask = (uint8_t *)malloc(r->nvt);
        if (rw->vmask != NULL)
        {
            for (j = 0; j < r->nvt; j++)
            {
                rw->vmask[j] =
                    (uint8_t)Filter_ColumnAccepts(&rw->filters, RF_VOTETYPE, r->vote_types[j]);
            }
        }
    }
    if (!Res_Tabulate(rw))
    {
        MessageBoxW(rw->hwnd, L"Out of memory.", k_WindowTitle, MB_ICONERROR | MB_OK);
        return;
    }
    Res_Rebuild(rw);
}

static void Res_FilterApplyCb(void *ctx)
{
    Res_ApplyFilter((ResultsWindow *)ctx);
}

static void Res_ResetFilter(ResultsWindow *rw)
{
    EeFilter_Clear(&rw->filters);
    Res_ApplyFilter(rw);
}

static BOOL App_ShowResultsFilter(ResultsWindow *rw)
{
    CvrFilterState *st;
    if (rw->hwnd_filter != NULL)
    {
        SetForegroundWindow(rw->hwnd_filter);
        return TRUE;
    }
    st = Filter_NewState(rw->app, rw->hwnd, &rw->filters, &rw->hwnd_filter);
    if (st == NULL)
    {
        return FALSE;
    }
    st->titles = k_ResFilterTitles;
    st->hidden = rw->filt_hidden;
    st->ncols = RF_COUNT;
    st->base_name = rw->base_name;
    st->file_noun = L"Election Results";
    st->caption = L"Election Explorer Results Filter";
    st->collect = Res_FilterCollect;
    st->apply = Res_FilterApplyCb;
    st->ctx = rw;
    return Filter_ShowRuleWindow(st, L"Count and display results matching these conditions:");
}

/* --- Turnout report ---------------------------------------------------------- */

/* Kinds of turnout report column. */
enum
{
    TC_PRECINCT = 0,
    TC_REGISTERED,
    TC_BALLOTS_VT, /* one vote type (ResTurnoutWindow.col_vt) */
    TC_BALLOTS,
    TC_TURNOUT
};

struct ResTurnoutWindow
{
    ResultsWindow *owner; /* the results window (owns the data and the filter) */
    AppState *app;
    HWND hwnd;
    HWND list;
    HWND status;
    uint32_t *rows; /* precinct indexes in display order */
    uint32_t nrows;
    int sort_col; /* -1 = file order */
    BOOL sort_asc;
    int ncols;
    uint8_t col_kind[RES_MAX_COLS];
    uint32_t col_vt[RES_MAX_COLS];
};

static WNDPROC g_old_res_turnout_list_proc = NULL;

/* 1234567 -> "1,234,567". */
static void fmt_count_w(int64_t v, wchar_t *out, size_t cch)
{
    wchar_t digits[32];
    wchar_t tmp[48];
    size_t n, i, k = 0;
    BOOL neg = v < 0;
    StringCchPrintfW(digits, ARRAYSIZE(digits), L"%lld", (long long)(neg ? -v : v));
    n = wcslen(digits);
    if (neg)
    {
        tmp[k++] = L'-';
    }
    for (i = 0; i < n; i++)
    {
        if (i > 0 && (n - i) % 3 == 0)
        {
            tmp[k++] = L',';
        }
        tmp[k++] = digits[i];
    }
    tmp[k] = L'\0';
    StringCchCopyW(out, cch, tmp);
}

static int64_t ResTurnout_Number(const ResTurnoutWindow *tw, uint32_t pi, int col)
{
    const EeResults *r = &tw->owner->res;
    const EeResultsPrecinct *p = &r->precincts[pi];
    switch (tw->col_kind[col])
    {
    case TC_REGISTERED:
        return p->registered;
    case TC_BALLOTS:
        return p->ballots;
    case TC_BALLOTS_VT:
        return r->prec_ballots_vt != NULL ? r->prec_ballots_vt[(size_t)pi * r->nvt + tw->col_vt[col]]
                                          : EE_RES_UNKNOWN;
    default:
        return EE_RES_UNKNOWN;
    }
}

/* Turnout of a precinct in basis points x 100 (percent * 100), or -1. */
static double ResTurnout_Pct(const EeResultsPrecinct *p)
{
    if (p->registered == EE_RES_UNKNOWN || p->ballots == EE_RES_UNKNOWN || p->registered <= 0)
    {
        return -1.0;
    }
    return (double)p->ballots * 100.0 / (double)p->registered;
}

static void ResTurnout_CellText(const ResTurnoutWindow *tw, uint32_t ri, int col, wchar_t *out,
                                size_t cch)
{
    uint32_t pi;
    if (cch == 0)
    {
        return;
    }
    out[0] = L'\0';
    if (ri >= tw->nrows || col < 0 || col >= tw->ncols)
    {
        return;
    }
    pi = tw->rows[ri];
    if (tw->col_kind[col] == TC_PRECINCT)
    {
        StringCchCopyW(out, cch, tw->owner->res.precincts[pi].name);
    }
    else if (tw->col_kind[col] == TC_TURNOUT)
    {
        double t = ResTurnout_Pct(&tw->owner->res.precincts[pi]);
        if (t >= 0.0)
        {
            StringCchPrintfW(out, cch, L"%.2f%%", t);
        }
    }
    else
    {
        int64_t v = ResTurnout_Number(tw, pi, col);
        if (v != EE_RES_UNKNOWN)
        {
            StringCchPrintfW(out, cch, L"%lld", (long long)v);
        }
    }
}

typedef struct ResTurnoutSortCtx
{
    const ResTurnoutWindow *tw;
    int col;
    BOOL asc;
} ResTurnoutSortCtx;

static int __cdecl ResTurnout_SortCmp(void *ctx, const void *pa, const void *pb)
{
    const ResTurnoutSortCtx *sc = (const ResTurnoutSortCtx *)ctx;
    uint32_t a = *(const uint32_t *)pa, b = *(const uint32_t *)pb;
    const EeResults *r = &sc->tw->owner->res;
    int c = 0;
    switch (sc->tw->col_kind[sc->col])
    {
    case TC_PRECINCT:
        c = Res_CompareText(r->precincts[a].name, r->precincts[b].name);
        break;
    case TC_TURNOUT:
    {
        double x = ResTurnout_Pct(&r->precincts[a]), y = ResTurnout_Pct(&r->precincts[b]);
        c = x < y ? -1 : (x > y ? 1 : 0);
        break;
    }
    default:
    {
        int64_t x = ResTurnout_Number(sc->tw, a, sc->col), y = ResTurnout_Number(sc->tw, b, sc->col);
        c = x < y ? -1 : (x > y ? 1 : 0);
        break;
    }
    }
    if (!sc->asc)
    {
        c = -c;
    }
    return c != 0 ? c : (a < b ? -1 : (a > b ? 1 : 0));
}

static int __cdecl ResTurnout_IndexCmp(const void *pa, const void *pb)
{
    uint32_t a = *(const uint32_t *)pa, b = *(const uint32_t *)pb;
    return a < b ? -1 : (a > b ? 1 : 0);
}

static void ResTurnout_Sort(ResTurnoutWindow *tw)
{
    if (tw->nrows < 2)
    {
        return;
    }
    if (tw->sort_col < 0 || tw->sort_col >= tw->ncols)
    {
        qsort(tw->rows, tw->nrows, sizeof(uint32_t), ResTurnout_IndexCmp);
    }
    else
    {
        ResTurnoutSortCtx sc;
        sc.tw = tw;
        sc.col = tw->sort_col;
        sc.asc = tw->sort_asc;
        qsort_s(tw->rows, tw->nrows, sizeof(uint32_t), ResTurnout_SortCmp, &sc);
    }
}

/* Status: precincts, registered voters, ballots cast and turnout of the rows shown. The
 * unfiltered totals are the file's reported ones (a Clarity primary's registered total is
 * not the precinct sum). */
static void ResTurnout_UpdateStatus(ResTurnoutWindow *tw)
{
    const ResultsWindow *rw = tw->owner;
    const EeResults *r = &rw->res;
    int64_t reg = 0, bal = 0;
    BOOL have_reg = TRUE, have_bal = TRUE;
    wchar_t buf[400], a[32], b[32];
    uint32_t i;
    for (i = 0; i < tw->nrows; i++)
    {
        const EeResultsPrecinct *p = &r->precincts[tw->rows[i]];
        if (p->registered == EE_RES_UNKNOWN)
        {
            have_reg = FALSE;
        }
        else
        {
            reg += p->registered;
        }
        if (p->ballots == EE_RES_UNKNOWN)
        {
            have_bal = FALSE;
        }
        else
        {
            bal += p->ballots;
        }
    }
    if (rw->pmask == NULL)
    {
        reg = r->registered_total;
        bal = r->ballots_total;
        have_reg = reg != EE_RES_UNKNOWN;
        have_bal = bal != EE_RES_UNKNOWN;
    }
    StringCchPrintfW(buf, ARRAYSIZE(buf), L"%u precincts%s", tw->nrows,
                     rw->pmask != NULL ? L" (filtered)" : L"");
    if (have_reg)
    {
        wchar_t part[64];
        fmt_count_w(reg, a, ARRAYSIZE(a));
        StringCchPrintfW(part, ARRAYSIZE(part), L"  \x2022  %s registered", a);
        StringCchCatW(buf, ARRAYSIZE(buf), part);
    }
    if (have_bal)
    {
        wchar_t part[64];
        fmt_count_w(bal, b, ARRAYSIZE(b));
        StringCchPrintfW(part, ARRAYSIZE(part), L"  \x2022  %s ballots cast", b);
        StringCchCatW(buf, ARRAYSIZE(buf), part);
    }
    if (have_reg && have_bal && reg > 0)
    {
        wchar_t part[64];
        StringCchPrintfW(part, ARRAYSIZE(part), L"  \x2022  %.2f%% turnout",
                         (double)bal * 100.0 / (double)reg);
        StringCchCatW(buf, ARRAYSIZE(buf), part);
    }
    StringCchCatW(buf, ARRAYSIZE(buf), L"  \x2022  right-click to Copy, Include/Exclude or Export");
    SendMessageW(tw->status, SB_SETTEXTW, 0, (LPARAM)buf);
}

/* Rebuild columns and rows from the owner (after a load, filter or option change). */
static void ResTurnout_Refresh(ResTurnoutWindow *tw)
{
    const ResultsWindow *rw;
    const EeResults *r;
    HWND header;
    uint32_t i, v, n = 0;
    if (tw == NULL || tw->list == NULL)
    {
        return;
    }
    rw = tw->owner;
    r = &rw->res;
    free(tw->rows);
    tw->rows = (uint32_t *)malloc(((size_t)r->nprecincts + 1) * sizeof(uint32_t));
    tw->nrows = 0;
    if (tw->rows != NULL)
    {
        for (i = 0; i < r->nprecincts; i++)
        {
            if (rw->pmask == NULL || rw->pmask[i])
            {
                tw->rows[n++] = i;
            }
        }
        tw->nrows = n;
    }
    SendMessageW(tw->list, WM_SETREDRAW, FALSE, 0);
    if (tw->app->font_grid != NULL)
    {
        SendMessageW(tw->list, WM_SETFONT, (WPARAM)tw->app->font_grid, FALSE); /* zoom */
    }
    header = ListView_GetHeader(tw->list);
    while (header != NULL && Header_GetItemCount(header) > 0)
    {
        ListView_DeleteColumn(tw->list, 0);
    }
    tw->ncols = 0;
#define TURN_ADD_COL(k, t, w, f, vv)                                                               \
    do                                                                                             \
    {                                                                                              \
        if (tw->ncols < RES_MAX_COLS)                                                              \
        {                                                                                          \
            LVCOLUMNW col_;                                                                        \
            ZeroMemory(&col_, sizeof(col_));                                                       \
            col_.mask = LVCF_TEXT | LVCF_WIDTH | LVCF_FMT;                                         \
            col_.fmt = tw->ncols == 0 ? LVCFMT_LEFT : (f);                                         \
            col_.pszText = (wchar_t *)(t);                                                         \
            col_.cx = ScaleDisplay(tw->app, (w));                                                  \
            tw->col_kind[tw->ncols] = (uint8_t)(k);                                                \
            tw->col_vt[tw->ncols] = (vv);                                                          \
            ListView_InsertColumn(tw->list, tw->ncols, &col_);                                     \
            tw->ncols++;                                                                           \
        }                                                                                          \
    } while (0)
    TURN_ADD_COL(TC_PRECINCT, L"Precinct", 160, LVCFMT_LEFT, 0);
    TURN_ADD_COL(TC_REGISTERED, L"Registered Voters", 120, LVCFMT_RIGHT, 0);
    if (r->prec_ballots_vt != NULL && rw->show_vt)
    {
        for (v = 0; v < r->nvt; v++)
        {
            if (rw->vmask == NULL || rw->vmask[v])
            {
                TURN_ADD_COL(TC_BALLOTS_VT, r->vote_types[v], 110, LVCFMT_RIGHT, v);
            }
        }
    }
    TURN_ADD_COL(TC_BALLOTS, L"Ballots Cast", 100, LVCFMT_RIGHT, 0);
    TURN_ADD_COL(TC_TURNOUT, L"Turnout", 80, LVCFMT_RIGHT, 0);
#undef TURN_ADD_COL
    if (tw->sort_col >= tw->ncols)
    {
        tw->sort_col = -1;
    }
    ResTurnout_Sort(tw);
    ListView_SetItemCountEx(tw->list, (int)tw->nrows, 0);
    SendMessageW(tw->list, WM_SETREDRAW, TRUE, 0);
    InvalidateRect(tw->list, NULL, TRUE);
    ResTurnout_UpdateStatus(tw);
}

static void ResTurnout_CopySelected(ResTurnoutWindow *tw)
{
    wchar_t *buf = NULL;
    size_t cap = 0, len = 0;
    wchar_t cell[512];
    int i;
    for (i = ListView_GetNextItem(tw->list, -1, LVNI_SELECTED); i >= 0;
         i = ListView_GetNextItem(tw->list, i, LVNI_SELECTED))
    {
        int c;
        for (c = 0; c < tw->ncols; c++)
        {
            size_t n;
            ResTurnout_CellText(tw, (uint32_t)i, c, cell, ARRAYSIZE(cell));
            n = wcslen(cell);
            if (!cvr_wbuf_reserve(&buf, &cap, len + n + 3))
            {
                free(buf);
                return;
            }
            memcpy(buf + len, cell, n * sizeof(wchar_t));
            len += n;
            if (c + 1 < tw->ncols)
            {
                buf[len++] = L'\t';
            }
            else
            {
                buf[len++] = L'\r';
                buf[len++] = L'\n';
            }
        }
    }
    if (buf != NULL && len > 0)
    {
        int u8len;
        buf[len] = L'\0';
        u8len = WideCharToMultiByte(CP_UTF8, 0, buf, -1, NULL, 0, NULL, NULL);
        if (u8len > 0)
        {
            char *utf8 = (char *)malloc((size_t)u8len);
            if (utf8 != NULL && WideCharToMultiByte(CP_UTF8, 0, buf, -1, utf8, u8len, NULL, NULL) > 0)
            {
                App_SetClipboardUtf8(tw->hwnd, utf8);
            }
            free(utf8);
        }
    }
    free(buf);
}

typedef struct ResTurnoutExportCtx
{
    const ResTurnoutWindow *tw;
    const uint32_t *idx; /* selected display rows, or NULL for all */
} ResTurnoutExportCtx;

static void ResTurnout_ExportCell(void *user, uint32_t row, uint32_t col, wchar_t *buf, size_t cch)
{
    const ResTurnoutExportCtx *x = (const ResTurnoutExportCtx *)user;
    ResTurnout_CellText(x->tw, x->idx != NULL ? x->idx[row] : row, (int)col, buf, cch);
}

static void App_ExportResTurnout(ResTurnoutWindow *tw, BOOL selection_only)
{
    const wchar_t *headers[RES_MAX_COLS];
    wchar_t titles[RES_MAX_COLS][96];
    uint32_t *idx = NULL;
    uint32_t n = 0;
    int c;
    ResTurnoutExportCtx ctx;
    wchar_t suggested[256];
    HWND header = ListView_GetHeader(tw->list);
    for (c = 0; c < tw->ncols; c++)
    {
        HDITEMW hi;
        ZeroMemory(&hi, sizeof(hi));
        hi.mask = HDI_TEXT;
        hi.pszText = titles[c];
        hi.cchTextMax = ARRAYSIZE(titles[c]);
        titles[c][0] = L'\0';
        Header_GetItem(header, c, &hi);
        headers[c] = titles[c];
    }
    if (selection_only)
    {
        idx = App_CollectSelectedIndices(tw->list, tw->nrows, &n);
        if (n == 0)
        {
            free(idx);
            return;
        }
    }
    else
    {
        n = tw->nrows;
    }
    ctx.tw = tw;
    ctx.idx = idx;
    StringCchPrintfW(suggested, ARRAYSIZE(suggested), L"%s-%s_Turnout", tw->owner->base_name,
                     selection_only ? L"Selected" : L"All");
    App_ExportReportModel(tw->hwnd, suggested, (uint32_t)tw->ncols, headers, n,
                          ResTurnout_ExportCell, &ctx);
    free(idx);
}

/* Add "Precinct is <selected>" rules (include or exclude) to the results filter. */
static void ResTurnout_FilterSelected(ResTurnoutWindow *tw, EeFilterAction action)
{
    ResultsWindow *rw = tw->owner;
    BOOL any = FALSE;
    int i;
    for (i = ListView_GetNextItem(tw->list, -1, LVNI_SELECTED); i >= 0;
         i = ListView_GetNextItem(tw->list, i, LVNI_SELECTED))
    {
        EeFilterRule r;
        if ((uint32_t)i >= tw->nrows)
        {
            continue;
        }
        ZeroMemory(&r, sizeof(r));
        r.column = RF_PRECINCT;
        r.relation = EeRel_Is;
        r.action = action;
        r.enabled = TRUE;
        StringCchCopyW(r.value, ARRAYSIZE(r.value), rw->res.precincts[tw->rows[i]].name);
        any = EeFilter_Add(&rw->filters, &r) || any;
    }
    if (any)
    {
        Res_ApplyFilter(rw); /* also refreshes this report */
    }
}

static void ResTurnout_OnContextMenu(ResTurnoutWindow *tw, int item, POINT screen)
{
    HMENU m;
    UINT cmd;
    if (item < 0 || (uint32_t)item >= tw->nrows)
    {
        return;
    }
    if (!(ListView_GetItemState(tw->list, item, LVIS_SELECTED) & LVIS_SELECTED))
    {
        ListView_SetItemState(tw->list, -1, 0, LVIS_SELECTED | LVIS_FOCUSED);
        ListView_SetItemState(tw->list, item, LVIS_SELECTED | LVIS_FOCUSED,
                              LVIS_SELECTED | LVIS_FOCUSED);
    }
    m = CreatePopupMenu();
    if (m == NULL)
    {
        return;
    }
    {
        wchar_t inc[96], exc[96];
        const wchar_t *name = tw->owner->res.precincts[tw->rows[item]].name;
        BOOL multi = ListView_GetSelectedCount(tw->list) > 1;
        if (multi)
        {
            StringCchCopyW(inc, ARRAYSIZE(inc), L"&Include the Selected Precincts");
            StringCchCopyW(exc, ARRAYSIZE(exc), L"&Exclude the Selected Precincts");
        }
        else
        {
            StringCchPrintfW(inc, ARRAYSIZE(inc), L"&Include Precinct \"%.40s\"", name);
            StringCchPrintfW(exc, ARRAYSIZE(exc), L"&Exclude Precinct \"%.40s\"", name);
        }
        AppendMenuW(m, MF_STRING, IDM_EDIT_COPY, L"&Copy");
        AppendMenuW(m, MF_SEPARATOR, 0, NULL);
        AppendMenuW(m, MF_STRING, IDM_FILTER_INCLUDE, inc);
        AppendMenuW(m, MF_STRING, IDM_FILTER_EXCLUDE, exc);
        AppendMenuW(m, MF_SEPARATOR, 0, NULL);
        AppendMenuW(m, MF_STRING, IDM_EXPORT_SELECTED, L"Export &Selected…");
        AppendMenuW(m, MF_STRING, IDM_EXPORT_ALL, L"Export &All…");
    }
    cmd = (UINT)TrackPopupMenu(m, TPM_RIGHTBUTTON | TPM_RETURNCMD, screen.x, screen.y, 0, tw->hwnd,
                               NULL);
    DestroyMenu(m);
    switch (cmd)
    {
    case IDM_EDIT_COPY:
        ResTurnout_CopySelected(tw);
        break;
    case IDM_FILTER_INCLUDE:
        ResTurnout_FilterSelected(tw, EeFilt_Include);
        break;
    case IDM_FILTER_EXCLUDE:
        ResTurnout_FilterSelected(tw, EeFilt_Exclude);
        break;
    case IDM_EXPORT_SELECTED:
        App_ExportResTurnout(tw, TRUE);
        break;
    case IDM_EXPORT_ALL:
        App_ExportResTurnout(tw, FALSE);
        break;
    default:
        break;
    }
}

static LRESULT CALLBACK ResTurnoutListSubclass(HWND hwnd, UINT msg, WPARAM wParam, LPARAM lParam)
{
    if (msg == WM_NOTIFY)
    {
        NMHDR *nm = (NMHDR *)lParam;
        HWND header = ListView_GetHeader(hwnd);
        ResTurnoutWindow *tw = (ResTurnoutWindow *)GetWindowLongPtrW(GetParent(hwnd), GWLP_USERDATA);
        if (tw != NULL && nm != NULL && header != NULL && nm->hwndFrom == header &&
            nm->code == NM_CUSTOMDRAW)
        {
            return App_HeaderCustomDraw(tw->app, (NMCUSTOMDRAW *)lParam, FALSE);
        }
    }
    return CallWindowProcW(g_old_res_turnout_list_proc, hwnd, msg, wParam, lParam);
}

static LRESULT CALLBACK ResTurnoutWndProc(HWND hwnd, UINT msg, WPARAM wParam, LPARAM lParam)
{
    ResTurnoutWindow *tw = (ResTurnoutWindow *)GetWindowLongPtrW(hwnd, GWLP_USERDATA);
    switch (msg)
    {
    case WM_CREATE:
    {
        CREATESTRUCTW *cs = (CREATESTRUCTW *)lParam;
        RECT rc;
        tw = (ResTurnoutWindow *)cs->lpCreateParams;
        SetWindowLongPtrW(hwnd, GWLP_USERDATA, (LONG_PTR)tw);
        tw->hwnd = hwnd;
        GetClientRect(hwnd, &rc);
        tw->list = CreateWindowExW(0, WC_LISTVIEWW, L"",
                                   WS_CHILD | WS_VISIBLE | LVS_REPORT | LVS_OWNERDATA |
                                       LVS_SHOWSELALWAYS,
                                   0, 0, rc.right, rc.bottom, hwnd, NULL, tw->app->instance, NULL);
        if (tw->list == NULL)
        {
            return -1;
        }
        {
            WNDPROC old = (WNDPROC)SetWindowLongPtrW(tw->list, GWLP_WNDPROC,
                                                     (LONG_PTR)ResTurnoutListSubclass);
            if (g_old_res_turnout_list_proc == NULL)
            {
                g_old_res_turnout_list_proc = old;
            }
        }
        ListView_SetExtendedListViewStyle(tw->list, LVS_EX_FULLROWSELECT | LVS_EX_DOUBLEBUFFER |
                                                        LVS_EX_GRIDLINES);
        if (tw->app->font_grid != NULL)
        {
            SendMessageW(tw->list, WM_SETFONT, (WPARAM)tw->app->font_grid, TRUE);
        }
        tw->status = CreateWindowExW(0, STATUSCLASSNAMEW, NULL,
                                     WS_CHILD | WS_VISIBLE | SBARS_SIZEGRIP, 0, 0, 0, 0, hwnd, NULL,
                                     tw->app->instance, NULL);
        if (tw->status != NULL && tw->app->font_ui != NULL)
        {
            SendMessageW(tw->status, WM_SETFONT, (WPARAM)tw->app->font_ui, TRUE);
        }
        ResTurnout_Refresh(tw);
        {
            int sb_h = 0;
            RECT sb;
            SendMessageW(tw->status, WM_SIZE, 0, 0);
            if (GetWindowRect(tw->status, &sb))
            {
                sb_h = sb.bottom - sb.top;
            }
            MoveWindow(tw->list, 0, 0, rc.right, rc.bottom > sb_h ? rc.bottom - sb_h : 0, TRUE);
        }
        return 0;
    }

    case WM_SIZE:
        if (tw != NULL)
        {
            int sb_h = 0;
            RECT sb;
            SendMessageW(tw->status, WM_SIZE, 0, 0);
            if (GetWindowRect(tw->status, &sb))
            {
                sb_h = sb.bottom - sb.top;
            }
            MoveWindow(tw->list, 0, 0, LOWORD(lParam),
                       HIWORD(lParam) > sb_h ? HIWORD(lParam) - sb_h : 0, TRUE);
        }
        return 0;

    case WM_SETFOCUS:
        if (tw != NULL && tw->list != NULL)
        {
            SetFocus(tw->list);
        }
        return 0;

    case WM_COMMAND:
        if (tw != NULL && LOWORD(wParam) == IDM_EDIT_COPY) /* Ctrl+C */
        {
            ResTurnout_CopySelected(tw);
            return 0;
        }
        break;

    case WM_NOTIFY:
    {
        NMHDR *hdr = (NMHDR *)lParam;
        if (tw == NULL || hdr->hwndFrom != tw->list)
        {
            break;
        }
        if (hdr->code == LVN_GETDISPINFOW)
        {
            NMLVDISPINFOW *di = (NMLVDISPINFOW *)lParam;
            if ((di->item.mask & LVIF_TEXT) && di->item.iItem >= 0 && di->item.cchTextMax > 0)
            {
                ResTurnout_CellText(tw, (uint32_t)di->item.iItem, di->item.iSubItem,
                                    di->item.pszText, (size_t)di->item.cchTextMax);
            }
            return 0;
        }
        if (hdr->code == LVN_COLUMNCLICK)
        {
            NMLISTVIEW *nlv = (NMLISTVIEW *)lParam;
            if (nlv->iSubItem == tw->sort_col)
            {
                tw->sort_asc = !tw->sort_asc;
            }
            else
            {
                tw->sort_col = nlv->iSubItem;
                tw->sort_asc = tw->col_kind[tw->sort_col] == TC_PRECINCT;
            }
            ResTurnout_Sort(tw);
            InvalidateRect(tw->list, NULL, FALSE);
            return 0;
        }
        if (hdr->code == NM_RCLICK)
        {
            LPNMITEMACTIVATE ia = (LPNMITEMACTIVATE)lParam;
            POINT screen = ia->ptAction;
            ClientToScreen(tw->list, &screen);
            ResTurnout_OnContextMenu(tw, ia->iItem, screen);
            return 0;
        }
        break;
    }

    case WM_CLOSE:
        DestroyWindow(hwnd);
        return 0;

    case WM_DESTROY:
        if (tw != NULL)
        {
            if (tw->owner != NULL && tw->owner->turnout == tw)
            {
                tw->owner->turnout = NULL;
            }
            free(tw->rows);
            free(tw);
            SetWindowLongPtrW(hwnd, GWLP_USERDATA, 0);
        }
        return 0;

    default:
        break;
    }
    return DefWindowProcW(hwnd, msg, wParam, lParam);
}

/* Open (or re-focus) the turnout report of a results window. */
static void App_ShowResultsTurnout(ResultsWindow *rw)
{
    ResTurnoutWindow *tw;
    wchar_t title[400];
    RECT pr;
    int x = CW_USEDEFAULT, y = CW_USEDEFAULT;
    int width;
    if (rw->turnout != NULL)
    {
        SetForegroundWindow(rw->turnout->hwnd);
        return;
    }
    if (rw->res.nprecincts == 0)
    {
        MessageBoxW(rw->hwnd, L"These results have no precinct turnout.", L"Report",
                    MB_ICONINFORMATION | MB_OK);
        return;
    }
    tw = (ResTurnoutWindow *)calloc(1, sizeof(ResTurnoutWindow));
    if (tw == NULL)
    {
        return;
    }
    tw->owner = rw;
    tw->app = rw->app;
    tw->sort_col = -1;
    tw->sort_asc = TRUE;
    StringCchPrintfW(title, ARRAYSIZE(title), L"Turnout Report - %s",
                     rw->res.election_name != NULL ? rw->res.election_name : rw->base_name);
    if (GetWindowRect(rw->hwnd, &pr))
    {
        x = pr.left + Scale(rw->app, 40);
        y = pr.top + Scale(rw->app, 40);
    }
    /* Wide enough for every column: precinct, registered, one per vote type, ballots, turnout. */
    width = 160 + 120 + 100 + 80 + 48;
    if (rw->res.prec_ballots_vt != NULL && rw->show_vt)
    {
        width += 110 * (int)rw->res.nvt;
    }
    if (width < 600)
    {
        width = 600;
    }
    if (CreateWindowExW(0, k_ResTurnoutClassName, title, WS_OVERLAPPEDWINDOW | WS_CLIPCHILDREN, x, y,
                        ScaleDisplay(rw->app, width), Scale(rw->app, 560), NULL, NULL,
                        rw->app->instance, tw) == NULL)
    {
        free(tw);
        return;
    }
    rw->turnout = tw;
    ShowWindow(tw->hwnd, SW_SHOWNORMAL);
    UpdateWindow(tw->hwnd);
}

/* --- Compare ----------------------------------------------------------------- */

/* Kinds of compare window column. */
enum
{
    CC_CONTEST = 0,
    CC_CHOICE,
    CC_PRECINCT,
    CC_VOTETYPE,
    CC_A,
    CC_B,
    CC_DIFF,
    CC_STATUS,
    CC_COUNT
};

typedef struct ResCompareWindow
{
    AppState *app; /* this window's own resource-only AppState */
    HWND hwnd;
    HWND list;
    HWND status;
    EeResultsDiff *d;
    uint32_t nd;
    uint32_t *rows; /* shown diff indexes */
    uint32_t nrows;
    BOOL diff_only;
    wchar_t label_a[160];
    wchar_t label_b[160];
    wchar_t note[256]; /* e.g. "filtered CVR ballots" */
    wchar_t base_name[192];
} ResCompareWindow;

static WNDPROC g_old_rescmp_list_proc = NULL;

static const wchar_t *ResCmp_StatusText(uint8_t st)
{
    switch (st)
    {
    case EE_DIFF_MATCH:
        return L"Match";
    case EE_DIFF_DIFFERS:
        return L"Differs";
    case EE_DIFF_ONLY_A:
        return L"Only in this window";
    case EE_DIFF_ONLY_B:
        return L"Only in the other window";
    default:
        return L"Not reported by one side";
    }
}

static void ResCmp_CellText(const ResCompareWindow *cw, uint32_t ri, int col, wchar_t *out,
                            size_t cch)
{
    const EeResultsDiff *d;
    if (cch == 0)
    {
        return;
    }
    out[0] = L'\0';
    if (ri >= cw->nrows)
    {
        return;
    }
    d = &cw->d[cw->rows[ri]];
    switch (col)
    {
    case CC_CONTEST:
        StringCchCopyW(out, cch, d->contest != NULL ? d->contest : L"(election)");
        break;
    case CC_CHOICE:
        StringCchCopyW(out, cch, d->choice != NULL ? d->choice : L"");
        break;
    case CC_PRECINCT:
        StringCchCopyW(out, cch, d->precinct != NULL ? d->precinct : L"");
        break;
    case CC_VOTETYPE:
        StringCchCopyW(out, cch, d->vote_type != NULL ? d->vote_type : L"");
        break;
    case CC_A:
        if (d->a != EE_RES_UNKNOWN)
        {
            StringCchPrintfW(out, cch, L"%lld", (long long)d->a);
        }
        break;
    case CC_B:
        if (d->b != EE_RES_UNKNOWN)
        {
            StringCchPrintfW(out, cch, L"%lld", (long long)d->b);
        }
        break;
    case CC_DIFF:
        if (d->a != EE_RES_UNKNOWN && d->b != EE_RES_UNKNOWN && d->a != d->b)
        {
            StringCchPrintfW(out, cch, L"%+lld", (long long)(d->b - d->a));
        }
        break;
    case CC_STATUS:
        StringCchCopyW(out, cch, ResCmp_StatusText(d->status));
        break;
    default:
        break;
    }
}

static void ResCmp_Rebuild(ResCompareWindow *cw)
{
    uint32_t i, n = 0;
    uint32_t cnt[5] = {0, 0, 0, 0, 0};
    uint32_t pre_diff = 0;
    wchar_t buf[512];
    free(cw->rows);
    cw->rows = (uint32_t *)malloc(((size_t)cw->nd + 1) * sizeof(uint32_t));
    if (cw->rows == NULL)
    {
        cw->nrows = 0;
        return;
    }
    for (i = 0; i < cw->nd; i++)
    {
        const EeResultsDiff *d = &cw->d[i];
        if (d->status < 5)
        {
            cnt[d->status]++;
        }
        if (d->precinct != NULL && d->status != EE_DIFF_MATCH)
        {
            pre_diff++;
        }
        if (!cw->diff_only || d->status != EE_DIFF_MATCH)
        {
            cw->rows[n++] = i;
        }
    }
    cw->nrows = n;
    ListView_SetItemCountEx(cw->list, (int)n, 0);
    InvalidateRect(cw->list, NULL, TRUE);
    if (cnt[EE_DIFF_DIFFERS] + cnt[EE_DIFF_ONLY_A] + cnt[EE_DIFF_ONLY_B] == 0)
    {
        StringCchPrintfW(buf, ARRAYSIZE(buf), L"Identical: %u figures match", cnt[EE_DIFF_MATCH]);
    }
    else
    {
        StringCchPrintfW(buf, ARRAYSIZE(buf),
                         L"%u match  \x2022  %u differ  \x2022  %u only in this window  \x2022  %u only "
                         L"in the other",
                         cnt[EE_DIFF_MATCH], cnt[EE_DIFF_DIFFERS], cnt[EE_DIFF_ONLY_A],
                         cnt[EE_DIFF_ONLY_B]);
    }
    if (cnt[EE_DIFF_NOT_REPORTED] > 0)
    {
        wchar_t part[96];
        StringCchPrintfW(part, ARRAYSIZE(part), L"  \x2022  %u not reported by one side",
                         cnt[EE_DIFF_NOT_REPORTED]);
        StringCchCatW(buf, ARRAYSIZE(buf), part);
    }
    if (pre_diff > 0)
    {
        wchar_t part[96];
        StringCchPrintfW(part, ARRAYSIZE(part), L"  \x2022  %u precinct / vote type rows differ",
                         pre_diff);
        StringCchCatW(buf, ARRAYSIZE(buf), part);
    }
    if (cw->note[0] != L'\0')
    {
        StringCchCatW(buf, ARRAYSIZE(buf), L"  \x2022  ");
        StringCchCatW(buf, ARRAYSIZE(buf), cw->note);
    }
    SendMessageW(cw->status, SB_SETTEXTW, 0, (LPARAM)buf);
}

static void ResCmp_CopySelected(ResCompareWindow *cw)
{
    wchar_t *buf = NULL;
    size_t cap = 0, len = 0;
    wchar_t cell[1024];
    int i;
    for (i = ListView_GetNextItem(cw->list, -1, LVNI_SELECTED); i >= 0;
         i = ListView_GetNextItem(cw->list, i, LVNI_SELECTED))
    {
        int c;
        for (c = 0; c < CC_COUNT; c++)
        {
            size_t n;
            ResCmp_CellText(cw, (uint32_t)i, c, cell, ARRAYSIZE(cell));
            n = wcslen(cell);
            if (!cvr_wbuf_reserve(&buf, &cap, len + n + 3))
            {
                free(buf);
                return;
            }
            memcpy(buf + len, cell, n * sizeof(wchar_t));
            len += n;
            if (c + 1 < CC_COUNT)
            {
                buf[len++] = L'\t';
            }
            else
            {
                buf[len++] = L'\r';
                buf[len++] = L'\n';
            }
        }
    }
    if (buf != NULL && len > 0)
    {
        int u8len;
        buf[len] = L'\0';
        u8len = WideCharToMultiByte(CP_UTF8, 0, buf, -1, NULL, 0, NULL, NULL);
        if (u8len > 0)
        {
            char *utf8 = (char *)malloc((size_t)u8len);
            if (utf8 != NULL && WideCharToMultiByte(CP_UTF8, 0, buf, -1, utf8, u8len, NULL, NULL) > 0)
            {
                App_SetClipboardUtf8(cw->hwnd, utf8);
            }
            free(utf8);
        }
    }
    free(buf);
}

typedef struct ResCmpExportCtx
{
    const ResCompareWindow *cw;
} ResCmpExportCtx;

static void ResCmp_ExportCell(void *user, uint32_t row, uint32_t col, wchar_t *buf, size_t cch)
{
    ResCmp_CellText(((const ResCmpExportCtx *)user)->cw, row, (int)col, buf, cch);
}

/* Export the shown rows (all, or the differences only). */
static void App_ExportResCompare(ResCompareWindow *cw)
{
    const wchar_t *headers[CC_COUNT];
    ResCmpExportCtx ctx;
    wchar_t suggested[256];
    headers[CC_CONTEST] = L"Contest";
    headers[CC_CHOICE] = L"Choice";
    headers[CC_PRECINCT] = L"Precinct";
    headers[CC_VOTETYPE] = L"Vote Type";
    headers[CC_A] = cw->label_a;
    headers[CC_B] = cw->label_b;
    headers[CC_DIFF] = L"Difference";
    headers[CC_STATUS] = L"Status";
    ctx.cw = cw;
    StringCchPrintfW(suggested, ARRAYSIZE(suggested), L"%s-%s", cw->base_name,
                     cw->diff_only ? L"Compare_Differences" : L"Compare");
    App_ExportReportModel(cw->hwnd, suggested, CC_COUNT, headers, cw->nrows, ResCmp_ExportCell,
                          &ctx);
}

static LRESULT ResCmp_CustomDraw(ResCompareWindow *cw, NMLVCUSTOMDRAW *cd)
{
    switch (cd->nmcd.dwDrawStage)
    {
    case CDDS_PREPAINT:
        return CDRF_NOTIFYITEMDRAW;
    case CDDS_ITEMPREPAINT:
    {
        uint32_t ri = (uint32_t)cd->nmcd.dwItemSpec;
        if (ri < cw->nrows)
        {
            switch (cw->d[cw->rows[ri]].status)
            {
            case EE_DIFF_DIFFERS:
                cd->clrTextBk = RGB(255, 222, 222);
                break;
            case EE_DIFF_ONLY_A:
            case EE_DIFF_ONLY_B:
                cd->clrTextBk = RGB(255, 243, 196);
                break;
            case EE_DIFF_NOT_REPORTED:
                cd->clrTextBk = RGB(236, 236, 236);
                break;
            default:
                break;
            }
        }
        return CDRF_DODEFAULT;
    }
    default:
        return CDRF_DODEFAULT;
    }
}

static LRESULT CALLBACK ResCmpListSubclass(HWND hwnd, UINT msg, WPARAM wParam, LPARAM lParam)
{
    if (msg == WM_NOTIFY)
    {
        NMHDR *nm = (NMHDR *)lParam;
        HWND header = ListView_GetHeader(hwnd);
        ResCompareWindow *cw = (ResCompareWindow *)GetWindowLongPtrW(GetParent(hwnd), GWLP_USERDATA);
        if (cw != NULL && nm != NULL && header != NULL && nm->hwndFrom == header &&
            nm->code == NM_CUSTOMDRAW)
        {
            return App_HeaderCustomDraw(cw->app, (NMCUSTOMDRAW *)lParam, FALSE);
        }
    }
    return CallWindowProcW(g_old_rescmp_list_proc, hwnd, msg, wParam, lParam);
}

static HMENU App_CreateResCompareMenu(void)
{
    HMENU menu = CreateMenu();
    HMENU file_menu = CreatePopupMenu();
    HMENU edit_menu = CreatePopupMenu();
    HMENU view_menu = CreatePopupMenu();
    if (menu == NULL || file_menu == NULL || edit_menu == NULL || view_menu == NULL)
    {
        if (file_menu != NULL)
            DestroyMenu(file_menu);
        if (edit_menu != NULL)
            DestroyMenu(edit_menu);
        if (view_menu != NULL)
            DestroyMenu(view_menu);
        if (menu != NULL)
            DestroyMenu(menu);
        return NULL;
    }
    AppendMenuW(file_menu, MF_STRING, IDM_RESCMP_EXPORT, L"&Export Compare…");
    AppendMenuW(file_menu, MF_SEPARATOR, 0, NULL);
    AppendMenuW(file_menu, MF_STRING, IDM_RESCMP_CLOSE, L"&Close Compare");
    AppendMenuW(edit_menu, MF_STRING, IDM_EDIT_COPY, L"&Copy\tCtrl+C");
    AppendMenuW(view_menu, MF_STRING, IDM_RESCMP_DIFFONLY, L"Show &Differences Only");
    AppendMenuW(menu, MF_POPUP, (UINT_PTR)file_menu, L"&File");
    AppendMenuW(menu, MF_POPUP, (UINT_PTR)edit_menu, L"&Edit");
    AppendMenuW(menu, MF_POPUP, (UINT_PTR)view_menu, L"&View");
    return menu;
}

static LRESULT CALLBACK ResCompareWndProc(HWND hwnd, UINT msg, WPARAM wParam, LPARAM lParam)
{
    ResCompareWindow *cw = (ResCompareWindow *)GetWindowLongPtrW(hwnd, GWLP_USERDATA);
    switch (msg)
    {
    case WM_CREATE:
    {
        CREATESTRUCTW *cs = (CREATESTRUCTW *)lParam;
        RECT rc;
        int c;
        static const struct
        {
            const wchar_t *title;
            int width;
            int fmt;
        } k_cols[CC_COUNT] = {{L"Contest", 260, LVCFMT_LEFT},  {L"Choice", 200, LVCFMT_LEFT},
                              {L"Precinct", 80, LVCFMT_LEFT},  {L"Vote Type", 90, LVCFMT_LEFT},
                              {L"", 130, LVCFMT_RIGHT},        {L"", 130, LVCFMT_RIGHT},
                              {L"Difference", 80, LVCFMT_RIGHT}, {L"Status", 160, LVCFMT_LEFT}};
        cw = (ResCompareWindow *)cs->lpCreateParams;
        SetWindowLongPtrW(hwnd, GWLP_USERDATA, (LONG_PTR)cw);
        cw->hwnd = hwnd;
        GetClientRect(hwnd, &rc);
        cw->list = CreateWindowExW(0, WC_LISTVIEWW, L"",
                                   WS_CHILD | WS_VISIBLE | LVS_REPORT | LVS_OWNERDATA |
                                       LVS_SHOWSELALWAYS,
                                   0, 0, rc.right, rc.bottom, hwnd, NULL, cw->app->instance, NULL);
        if (cw->list == NULL)
        {
            return -1;
        }
        {
            WNDPROC old =
                (WNDPROC)SetWindowLongPtrW(cw->list, GWLP_WNDPROC, (LONG_PTR)ResCmpListSubclass);
            if (g_old_rescmp_list_proc == NULL)
            {
                g_old_rescmp_list_proc = old;
            }
        }
        ListView_SetExtendedListViewStyle(cw->list, LVS_EX_FULLROWSELECT | LVS_EX_DOUBLEBUFFER |
                                                        LVS_EX_GRIDLINES);
        if (cw->app->font_grid != NULL)
        {
            SendMessageW(cw->list, WM_SETFONT, (WPARAM)cw->app->font_grid, TRUE);
        }
        for (c = 0; c < CC_COUNT; c++)
        {
            LVCOLUMNW col;
            ZeroMemory(&col, sizeof(col));
            col.mask = LVCF_TEXT | LVCF_WIDTH | LVCF_FMT;
            col.fmt = k_cols[c].fmt;
            col.pszText = (wchar_t *)(c == CC_A ? cw->label_a : c == CC_B ? cw->label_b : k_cols[c].title);
            col.cx = ScaleDisplay(cw->app, k_cols[c].width);
            ListView_InsertColumn(cw->list, c, &col);
        }
        cw->status = CreateWindowExW(0, STATUSCLASSNAMEW, NULL,
                                     WS_CHILD | WS_VISIBLE | SBARS_SIZEGRIP, 0, 0, 0, 0, hwnd, NULL,
                                     cw->app->instance, NULL);
        if (cw->status != NULL && cw->app->font_ui != NULL)
        {
            SendMessageW(cw->status, WM_SETFONT, (WPARAM)cw->app->font_ui, TRUE);
        }
        ResCmp_Rebuild(cw);
        SendMessageW(hwnd, WM_SIZE, 0, MAKELPARAM(rc.right, rc.bottom));
        return 0;
    }

    case WM_SIZE:
        if (cw != NULL)
        {
            int sb_h = 0;
            RECT sb, rc;
            GetClientRect(hwnd, &rc);
            SendMessageW(cw->status, WM_SIZE, 0, 0);
            if (GetWindowRect(cw->status, &sb))
            {
                sb_h = sb.bottom - sb.top;
            }
            MoveWindow(cw->list, 0, 0, rc.right, rc.bottom > sb_h ? rc.bottom - sb_h : 0, TRUE);
        }
        return 0;

    case WM_SETFOCUS:
        if (cw != NULL && cw->list != NULL)
        {
            SetFocus(cw->list);
        }
        return 0;

    case WM_INITMENUPOPUP:
        if (cw != NULL)
        {
            CheckMenuItem((HMENU)wParam, IDM_RESCMP_DIFFONLY,
                          MF_BYCOMMAND | (cw->diff_only ? MF_CHECKED : MF_UNCHECKED));
        }
        break;

    case WM_COMMAND:
        if (cw == NULL)
        {
            break;
        }
        switch (LOWORD(wParam))
        {
        case IDM_EDIT_COPY:
            ResCmp_CopySelected(cw);
            return 0;
        case IDM_RESCMP_DIFFONLY:
            cw->diff_only = !cw->diff_only;
            ResCmp_Rebuild(cw);
            return 0;
        case IDM_RESCMP_EXPORT:
            App_ExportResCompare(cw);
            return 0;
        case IDM_RESCMP_CLOSE:
            DestroyWindow(hwnd);
            return 0;
        default:
            break;
        }
        break;

    case WM_NOTIFY:
    {
        NMHDR *hdr = (NMHDR *)lParam;
        if (cw == NULL || hdr->hwndFrom != cw->list)
        {
            break;
        }
        if (hdr->code == LVN_GETDISPINFOW)
        {
            NMLVDISPINFOW *di = (NMLVDISPINFOW *)lParam;
            if ((di->item.mask & LVIF_TEXT) && di->item.iItem >= 0 && di->item.cchTextMax > 0)
            {
                ResCmp_CellText(cw, (uint32_t)di->item.iItem, di->item.iSubItem, di->item.pszText,
                                (size_t)di->item.cchTextMax);
            }
            return 0;
        }
        if (hdr->code == NM_CUSTOMDRAW)
        {
            return ResCmp_CustomDraw(cw, (NMLVCUSTOMDRAW *)lParam);
        }
        if (hdr->code == NM_RCLICK)
        {
            HMENU m = CreatePopupMenu();
            if (m != NULL)
            {
                POINT pt;
                UINT cmd;
                GetCursorPos(&pt);
                AppendMenuW(m, MF_STRING, IDM_EDIT_COPY, L"&Copy");
                AppendMenuW(m, MF_STRING, IDM_RESCMP_EXPORT, L"&Export Compare…");
                cmd = (UINT)TrackPopupMenu(m, TPM_RIGHTBUTTON | TPM_RETURNCMD, pt.x, pt.y, 0, hwnd,
                                           NULL);
                DestroyMenu(m);
                if (cmd != 0)
                {
                    SendMessageW(hwnd, WM_COMMAND, cmd, 0);
                }
            }
            return 0;
        }
        break;
    }

    case WM_CLOSE:
        DestroyWindow(hwnd);
        return 0;

    case WM_DESTROY:
        if (cw != NULL)
        {
            EeResults_FreeDiff(cw->d, cw->nd);
            free(cw->rows);
            App_FreeCvrUi(cw->app);
            free(cw);
            CvrWindow_Unregister(hwnd);
            SetWindowLongPtrW(hwnd, GWLP_USERDATA, 0);
            App_MaybeQuit();
        }
        return 0;

    default:
        break;
    }
    return DefWindowProcW(hwnd, msg, wParam, lParam);
}

/* Show a compare (takes ownership of @p d). */
static void App_CreateResCompareWindow(AppState *app, EeResultsDiff *d, uint32_t nd,
                                       const wchar_t *label_a, const wchar_t *label_b,
                                       const wchar_t *note, const wchar_t *base_name)
{
    ResCompareWindow *cw = (ResCompareWindow *)calloc(1, sizeof(ResCompareWindow));
    AppState *ui = NULL;
    wchar_t title[400];
    HMENU menu;
    HWND h;
    if (cw == NULL ||
        (ui = (AppState *)HeapAlloc(GetProcessHeap(), HEAP_ZERO_MEMORY, sizeof(AppState))) == NULL)
    {
        free(cw);
        EeResults_FreeDiff(d, nd);
        return;
    }
    App_InitViewerState(ui, app->instance, app);
    ui->is_cvr_ui = TRUE;
    App_UpdateDpiMetrics(ui, app->dpi ? app->dpi : 96u);
    cw->app = ui;
    cw->d = d;
    cw->nd = nd;
    StringCchCopyW(cw->label_a, ARRAYSIZE(cw->label_a), label_a);
    StringCchCopyW(cw->label_b, ARRAYSIZE(cw->label_b), label_b);
    StringCchCopyW(cw->note, ARRAYSIZE(cw->note), note != NULL ? note : L"");
    StringCchCopyW(cw->base_name, ARRAYSIZE(cw->base_name), base_name);
    StringCchPrintfW(title, ARRAYSIZE(title), L"Compare: %s \x2194 %s", label_a, label_b);
    menu = App_CreateResCompareMenu();
    h = CreateWindowExW(0, k_ResCompareClassName, title, WS_OVERLAPPEDWINDOW | WS_CLIPCHILDREN,
                        CW_USEDEFAULT, CW_USEDEFAULT, Scale(ui, 1200), Scale(ui, 600), NULL, menu,
                        ui->instance, cw);
    if (h == NULL)
    {
        if (menu != NULL)
        {
            DestroyMenu(menu);
        }
        EeResults_FreeDiff(d, nd);
        App_FreeCvrUi(ui);
        free(cw);
        return;
    }
    ui->hwnd_main = h;
    CvrWindow_Register(h);
    ShowWindow(h, SW_SHOWNORMAL);
    UpdateWindow(h);
}

/* A short label for a window: its title without the " — Election Results" / " — Cast Vote
 * Records" suffix. */
static void ResCmp_WindowLabel(HWND h, const wchar_t *kind, wchar_t *out, size_t cch)
{
    wchar_t t[300];
    wchar_t *dash;
    t[0] = L'\0';
    GetWindowTextW(h, t, ARRAYSIZE(t));
    dash = wcsstr(t, L" \x2014 ");
    if (dash != NULL)
    {
        *dash = L'\0';
    }
    StringCchPrintfW(out, cch, L"%s (%s)", t, kind);
}

/* Grow a tally item array. */
static BOOL res_grow_items(EeResultsTallyItem **items, size_t *cap, size_t need)
{
    if (need > *cap)
    {
        size_t nc = *cap ? *cap * 2 : 1024;
        EeResultsTallyItem *ni;
        while (nc < need)
        {
            nc *= 2;
        }
        ni = (EeResultsTallyItem *)realloc(*items, nc * sizeof(EeResultsTallyItem));
        if (ni == NULL)
        {
            return FALSE;
        }
        *items = ni;
        *cap = nc;
    }
    return TRUE;
}

/* Per-precinct row groups of a CVR: every distinct Precinct value, with its rows. */
typedef struct CvrPrecinctGroup
{
    wchar_t *name;
    uint32_t *rows;
    uint32_t n, cap;
} CvrPrecinctGroup;

/* Ballots among a CVR window's rows (all when @p rows is NULL): the rows, or for a multi-card
 * CVR the first cards (EeCvr_CardCounts). */
static int64_t Cvr_Ballots(const CvrWindow *cw, const uint32_t *rows, uint32_t nrows)
{
    EeCvrCardCounts cc;
    if (!cw->multi_card)
    {
        return rows != NULL ? (int64_t)nrows : (int64_t)cw->table.nrows;
    }
    if (rows == NULL)
    {
        return (int64_t)cw->cards.ballots;
    }
    return EeCvr_CardCounts(&cw->table, rows, nrows, &cc) ? (int64_t)cc.ballots : EE_RES_UNKNOWN;
}

/* Results from a CVR window's tabulation (all ballots, or the filtered ones): per precinct
 * when the CVR has a real Precinct column, else totals. With @p names (the results being
 * compared), a precinct value is filed under the precinct of @p names it stands for
 * (EeResults_MatchPrecinctName: a Hart CVR's "315BS14" counts for precinct 315). */
static BOOL Cvr_BuildResults(CvrWindow *cw, BOOL filtered, const EeResults *names, EeResults *out)
{
    const EeCvrTable *t = &cw->table;
    const uint32_t *rows = filtered ? cw->disp : NULL;
    uint32_t nrows = filtered ? cw->disp_count : t->nrows;
    int64_t ballots = Cvr_Ballots(cw, rows, nrows);
    EeResultsTallyItem *items = NULL;
    size_t nitems = 0, cap = 0;
    BOOL ok = TRUE;
    EeCvrTally *tal = NULL;
    uint32_t ntal = 0, i;

    if (!cw->vreport_avail[EE_CVRREP_PRECINCT])
    {
        ok = filtered ? EeCvr_TabulateRows(t, rows, nrows, g_settings.cvr_merge_writeins, &tal, &ntal)
                      : EeCvr_Tabulate(t, g_settings.cvr_merge_writeins, &tal, &ntal);
        if (!ok)
        {
            return FALSE;
        }
        items = (EeResultsTallyItem *)calloc((size_t)ntal + 1, sizeof(EeResultsTallyItem));
        if (items == NULL)
        {
            EeCvr_FreeTally(tal, ntal);
            return FALSE;
        }
        for (i = 0; i < ntal; i++)
        {
            items[i].contest_key = tal[i].group;
            items[i].contest = tal[i].contest;
            items[i].choice = tal[i].selection;
            items[i].votes = tal[i].count;
        }
        ok = EeResults_FromTally(items, ntal, ballots, out);
        free(items);
        EeCvr_FreeTally(tal, ntal);
        return ok;
    }

    /* Group the rows by precinct, then tabulate each group. */
    {
        uint32_t pcol = cw->vreport_col[EE_CVRREP_PRECINCT];
        CvrPrecinctGroup *groups = NULL;
        size_t ngroups = 0, gcap = 0;
        uint32_t r, g;
        uint32_t *slots = NULL; /* open-addressing hash: name -> group + 1 */
        uint32_t nslots = 0;
        wchar_t cell[128];
        for (r = 0; r < nrows && ok; r++)
        {
            uint32_t phys = rows != NULL ? rows[r] : r;
            uint32_t h = 2166136261u, s0;
            const wchar_t *q;
            cell[0] = L'\0';
            EeCvr_GetCellW(t, phys, pcol, cell, ARRAYSIZE(cell));
            if ((ngroups + 1) * 2 > nslots)
            {
                /* (re)build the hash at twice the size */
                uint32_t ns = nslots ? nslots * 2 : 4096, k;
                uint32_t *nt = (uint32_t *)calloc(ns, sizeof(uint32_t));
                if (nt == NULL)
                {
                    ok = FALSE;
                    break;
                }
                for (k = 0; k < ngroups; k++)
                {
                    uint32_t hh = 2166136261u, ss;
                    for (q = groups[k].name; *q != L'\0'; q++)
                    {
                        hh = (hh ^ (uint32_t)*q) * 16777619u;
                    }
                    for (ss = hh & (ns - 1); nt[ss] != 0; ss = (ss + 1) & (ns - 1))
                    {
                    }
                    nt[ss] = k + 1;
                }
                free(slots);
                slots = nt;
                nslots = ns;
            }
            for (q = cell; *q != L'\0'; q++)
            {
                h = (h ^ (uint32_t)*q) * 16777619u;
            }
            for (s0 = h & (nslots - 1); slots[s0] != 0; s0 = (s0 + 1) & (nslots - 1))
            {
                if (wcscmp(groups[slots[s0] - 1].name, cell) == 0)
                {
                    break;
                }
            }
            g = slots[s0] != 0 ? slots[s0] - 1 : (uint32_t)ngroups;
            if (g == ngroups)
            {
                if (ngroups + 1 > gcap)
                {
                    size_t nc = gcap ? gcap * 2 : 256;
                    CvrPrecinctGroup *ng =
                        (CvrPrecinctGroup *)realloc(groups, nc * sizeof(CvrPrecinctGroup));
                    if (ng == NULL)
                    {
                        ok = FALSE;
                        break;
                    }
                    groups = ng;
                    gcap = nc;
                }
                ZeroMemory(&groups[ngroups], sizeof(CvrPrecinctGroup));
                groups[ngroups].name = _wcsdup(cell);
                if (groups[ngroups].name == NULL)
                {
                    ok = FALSE;
                    break;
                }
                slots[s0] = (uint32_t)ngroups + 1;
                ngroups++;
            }
            if (groups[g].n + 1 > groups[g].cap)
            {
                uint32_t nc = groups[g].cap ? groups[g].cap * 2 : 64;
                uint32_t *nr = (uint32_t *)realloc(groups[g].rows, (size_t)nc * sizeof(uint32_t));
                if (nr == NULL)
                {
                    ok = FALSE;
                    break;
                }
                groups[g].rows = nr;
                groups[g].cap = nc;
            }
            groups[g].rows[groups[g].n++] = phys;
        }
        /* Tabulate each precinct; the tallies stay alive until the results are built. */
        {
            EeCvrTally **tals = (EeCvrTally **)calloc(ngroups + 1, sizeof(EeCvrTally *));
            uint32_t *ntals = (uint32_t *)calloc(ngroups + 1, sizeof(uint32_t));
            ok = ok && tals != NULL && ntals != NULL;
            for (g = 0; g < ngroups && ok; g++)
            {
                uint32_t k;
                ok = EeCvr_TabulateRows(t, groups[g].rows, groups[g].n, g_settings.cvr_merge_writeins,
                                        &tals[g], &ntals[g]);
                for (k = 0; ok && k < ntals[g]; k++)
                {
                    if (!res_grow_items(&items, &cap, nitems + 1))
                    {
                        ok = FALSE;
                        break;
                    }
                    items[nitems].contest_key = tals[g][k].group;
                    items[nitems].contest = tals[g][k].contest;
                    items[nitems].choice = tals[g][k].selection;
                    /* a blank precinct cell still counts, under "(blank)" */
                    items[nitems].precinct = groups[g].name[0] != L'\0' ? groups[g].name : L"(blank)";
                    if (names != NULL && groups[g].name[0] != L'\0')
                    {
                        const wchar_t *m = EeResults_MatchPrecinctName(names, groups[g].name);
                        if (m != NULL)
                        {
                            items[nitems].precinct = m;
                        }
                    }
                    items[nitems].votes = tals[g][k].count;
                    nitems++;
                }
            }
            if (ok)
            {
                ok = EeResults_FromTally(items, (uint32_t)nitems, ballots, out);
            }
            for (g = 0; g < ngroups; g++)
            {
                if (tals != NULL)
                {
                    EeCvr_FreeTally(tals[g], ntals != NULL ? ntals[g] : 0);
                }
            }
            free(tals);
            free(ntals);
        }
        for (g = 0; g < ngroups; g++)
        {
            free(groups[g].name);
            free(groups[g].rows);
        }
        free(groups);
        free(slots);
    }
    free(items);
    return ok;
}

static BOOL Res_IsResultsWindow(HWND h)
{
    wchar_t cls[64];
    return h != NULL && GetClassNameW(h, cls, ARRAYSIZE(cls)) > 0 &&
           wcscmp(cls, k_ResultsClassName) == 0;
}

static BOOL Res_IsCvrWindow(HWND h)
{
    wchar_t cls[64];
    return h != NULL && GetClassNameW(h, cls, ARRAYSIZE(cls)) > 0 && wcscmp(cls, k_CvrClassName) == 0;
}

/* Compare window @p from (a results or CVR window: "this window") with @p other. */
static void App_RunResultsCompare(HWND from, HWND other)
{
    ResultsWindow *ra = Res_IsResultsWindow(from) ? (ResultsWindow *)GetWindowLongPtrW(from, GWLP_USERDATA) : NULL;
    ResultsWindow *rb = Res_IsResultsWindow(other) ? (ResultsWindow *)GetWindowLongPtrW(other, GWLP_USERDATA) : NULL;
    CvrWindow *ca = Res_IsCvrWindow(from) ? (CvrWindow *)GetWindowLongPtrW(from, GWLP_USERDATA) : NULL;
    CvrWindow *cb = Res_IsCvrWindow(other) ? (CvrWindow *)GetWindowLongPtrW(other, GWLP_USERDATA) : NULL;
    CvrWindow *cvr = ca != NULL ? ca : cb;
    AppState *app = ra != NULL ? ra->app : (ca != NULL ? ca->app : NULL);
    EeResults cvr_res;
    const EeResults *a, *b;
    const uint8_t *apm = NULL, *avm = NULL, *bpm = NULL, *bvm = NULL;
    EeResultsDiff *d = NULL;
    uint32_t nd = 0;
    wchar_t la[160], lb[160], note[256];
    BOOL filtered = FALSE;
    HCURSOR old;

    if (app == NULL || (ra == NULL && ca == NULL) || (rb == NULL && cb == NULL) || (ca != NULL && cb != NULL))
    {
        return;
    }
    note[0] = L'\0';
    EeResults_Init(&cvr_res);
    if (cvr != NULL && cvr->filt_active)
    {
        wchar_t q[300];
        int ans;
        StringCchPrintfW(q, ARRAYSIZE(q),
                         L"The Cast Vote Records window is filtered (%u of %u ballot records).\n\n"
                         L"Compare the results with only the filtered ballots?\n\n"
                         L"Yes: the filtered ballots.   No: all ballots.",
                         cvr->disp_count, cvr->table.nrows);
        ans = MessageBoxW(from, q, L"Compare", MB_YESNOCANCEL | MB_ICONQUESTION);
        if (ans == IDCANCEL)
        {
            return;
        }
        filtered = ans == IDYES;
    }
    old = SetCursor(LoadCursorW(NULL, IDC_WAIT));
    if (cvr != NULL && !Cvr_BuildResults(cvr, filtered, ra != NULL ? &ra->res : (rb != NULL ? &rb->res : NULL), &cvr_res))
    {
        SetCursor(old);
        MessageBoxW(from, L"Out of memory while tabulating the Cast Vote Records.", L"Compare",
                    MB_ICONERROR | MB_OK);
        return;
    }
    if (ra != NULL)
    {
        a = &ra->res;
        apm = ra->pmask;
        avm = ra->vmask;
        ResCmp_WindowLabel(from, L"results", la, ARRAYSIZE(la));
    }
    else
    {
        a = &cvr_res;
        ResCmp_WindowLabel(from, filtered ? L"filtered CVR" : L"CVR", la, ARRAYSIZE(la));
    }
    if (rb != NULL)
    {
        b = &rb->res;
        bpm = rb->pmask;
        bvm = rb->vmask;
        ResCmp_WindowLabel(other, L"results", lb, ARRAYSIZE(lb));
    }
    else
    {
        b = &cvr_res;
        ResCmp_WindowLabel(other, filtered ? L"filtered CVR" : L"CVR", lb, ARRAYSIZE(lb));
    }
    if ((ra != NULL && ra->filt_active) || (rb != NULL && rb->filt_active))
    {
        StringCchCopyW(note, ARRAYSIZE(note), L"results filter applied");
    }
    if (cvr != NULL && !g_settings.cvr_merge_writeins)
    {
        StringCchCatW(note, ARRAYSIZE(note),
                      note[0] != L'\0' ? L"; CVR write-ins not merged" : L"CVR write-ins not merged");
    }
    if (cvr != NULL && cvr->multi_card)
    {
        StringCchCatW(note, ARRAYSIZE(note),
                      note[0] != L'\0' ? L"; multi-card CVR: ballots are its first cards"
                                       : L"multi-card CVR: ballots are its first cards");
    }
    if (!EeResults_Compare(a, apm, avm, b, bpm, bvm, &d, &nd))
    {
        SetCursor(old);
        EeResults_Clear(&cvr_res);
        MessageBoxW(from, L"Out of memory while comparing.", L"Compare", MB_ICONERROR | MB_OK);
        return;
    }
    SetCursor(old);
    App_CreateResCompareWindow(app, d, nd, la, lb, note,
                               ra != NULL ? ra->base_name : (rb != NULL ? rb->base_name : L"Compare"));
    EeResults_Clear(&cvr_res);
}

/* Fill a Compare popup with the windows @p self can be compared with: results windows,
 * and CVR windows when @p with_cvr. targets[] receives their handles (by command offset). */
static void Res_FillCompareMenu(HMENU popup, HWND self, BOOL with_cvr, HWND *targets, int max_targets)
{
    int i, n = 0;
    while (GetMenuItemCount(popup) > 0)
    {
        DeleteMenu(popup, 0, MF_BYPOSITION);
    }
    for (i = 0; i < g_cvr_window_count && n < max_targets &&
                n <= IDM_COMPARE_WITH_LAST - IDM_COMPARE_WITH_FIRST;
         i++)
    {
        HWND h = g_cvr_windows[i];
        wchar_t t[300], item[340];
        BOOL audit = Aud_IsAuditWindow(h);
        if (h == self || !(Res_IsResultsWindow(h) || (with_cvr && Res_IsCvrWindow(h)) || audit))
        {
            continue;
        }
        t[0] = L'\0';
        GetWindowTextW(h, t, ARRAYSIZE(t));
        StringCchPrintfW(item, ARRAYSIZE(item), audit ? L"Check Ballot Counts with %.200s" : L"Compare with %.200s", t);
        targets[n] = h;
        AppendMenuW(popup, MF_STRING, (UINT_PTR)(IDM_COMPARE_WITH_FIRST + n), item);
        n++;
    }
    if (n == 0)
    {
        AppendMenuW(popup, MF_STRING | MF_GRAYED, 0,
                    with_cvr ? L"(open another results window, a CVR window or an audit log window to compare)"
                             : L"(open a results window or an audit log window to compare)");
    }
}

static const wchar_t k_ResHelpCompare[] =
    L"Compare \x2192 Compare with\x2026 compares this window with another open window:\r\n"
    L"\x2022 another Election Results window, for example the same election loaded from its "
    L"export, or an earlier (unofficial) snapshot;\r\n"
    L"\x2022 a Cast Vote Records window, using its tabulation. If the CVR window is filtered you "
    L"choose its filtered ballots or all ballots. The CVR window's Compare menu does the same "
    L"from its side.\r\n\r\n"
    L"Contests are matched by name (repeated names in order) and choices by name, ignoring case "
    L"and extra spaces; a CVR's contest and choice ids (\"(45918)\", \"(EC12)\") are dropped. "
    L"Each choice's total is compared, then Over/Under Votes and Ballots Cast. When both sides "
    L"have precinct detail, every precinct is compared too (and every vote type when both have "
    L"the same ones); only the precincts that differ are listed. A results window's filter "
    L"applies to its side.\r\n\r\n"
    L"Rows are colored: red differs, yellow only one side has it, grey one side does not report "
    L"the figure (Travis County's results do not report over/under votes). View \x2192 Show "
    L"Differences Only hides the matches; File \x2192 Export Compare\x2026 saves the rows. For a "
    L"CVR, turn on the CVR window's \"Merge image and text write-ins\" option so its write-ins "
    L"add up like the results' single Write-in total.\r\n\r\n"
    L"Compare \x2192 Check Ballot Counts with\x2026 an open audit log window compares the ballots "
    L"cast with the ballots the county's ES&S Electionware audit log shows loaded into the EMS, "
    L"and each vote type with a lower bound (see Help \x2192 Compare in the audit log window).";

static void App_ShowResultsCompareHelp(AppState *app, HWND owner)
{
    App_ShowHelpTopicOn(app, owner, L"Help — Compare", k_ResHelpCompare);
}

/* --- Export -------------------------------------------------------------------- */

typedef struct ResExportCtx
{
    const ResultsWindow *rw;
    const uint32_t *idx; /* selected display rows, or NULL for all */
} ResExportCtx;

static void Res_ExportCell(void *user, uint32_t row, uint32_t col, wchar_t *buf, size_t cch)
{
    const ResExportCtx *x = (const ResExportCtx *)user;
    Res_CellText(x->rw, x->idx != NULL ? x->idx[row] : row, (int)col, buf, cch);
}

typedef struct ResExportOpt
{
    AppState *app;
    BOOL detail; /* TRUE = All detail (reloadable) */
    /* the texts, when not the results window's own (the audit log window's export) */
    const wchar_t *prompt;
    const wchar_t *view_text;
    const wchar_t *detail_text;
} ResExportOpt;

static INT_PTR CALLBACK ResExportDlgProc(HWND dlg, UINT msg, WPARAM wParam, LPARAM lParam)
{
    ResExportOpt *d = (ResExportOpt *)GetWindowLongPtrW(dlg, GWLP_USERDATA);
    switch (msg)
    {
    case WM_INITDIALOG:
    {
        AppState *app;
        RECT rc;
        int margin, btn_w, btn_h, gap, i, n = 0;
        HWND ctl[6];
        d = (ResExportOpt *)lParam;
        SetWindowLongPtrW(dlg, GWLP_USERDATA, (LONG_PTR)d);
        app = d->app;
        App_CenterModalClient(dlg, app, Scale(app, 420), Scale(app, 170));
        GetClientRect(dlg, &rc);
        margin = Scale(app, 14);
        btn_w = Scale(app, 84);
        btn_h = Scale(app, 26);
        gap = Scale(app, 8);
        ctl[n++] = CreateWindowExW(0, L"STATIC", d->prompt != NULL ? d->prompt : L"Export the election results as:",
                                   WS_CHILD | WS_VISIBLE, margin, margin, rc.right - 2 * margin,
                                   Scale(app, 18), dlg, NULL, app->instance, NULL);
        ctl[n++] = CreateWindowExW(0, L"BUTTON",
                                   d->view_text != NULL ? d->view_text
                                                        : L"Displayed view: the rows and columns shown (filter, view, sort)",
                                   WS_CHILD | WS_VISIBLE | WS_TABSTOP | WS_GROUP | BS_AUTORADIOBUTTON,
                                   margin + Scale(app, 8), margin + Scale(app, 26),
                                   rc.right - 2 * margin, Scale(app, 22), dlg,
                                   (HMENU)(INT_PTR)IDC_RES_EXPORT_VIEW, app->instance, NULL);
        ctl[n++] = CreateWindowExW(0, L"BUTTON",
                                   d->detail_text != NULL ? d->detail_text
                                                          : L"All detail (reloadable): every precinct and vote type",
                                   WS_CHILD | WS_VISIBLE | WS_TABSTOP | BS_AUTORADIOBUTTON,
                                   margin + Scale(app, 8), margin + Scale(app, 52),
                                   rc.right - 2 * margin, Scale(app, 22), dlg,
                                   (HMENU)(INT_PTR)IDC_RES_EXPORT_DETAIL, app->instance, NULL);
        SendMessageW(ctl[d->detail ? 2 : 1], BM_SETCHECK, BST_CHECKED, 0);
        ctl[n++] = CreateWindowExW(0, L"BUTTON", L"OK",
                                   WS_CHILD | WS_VISIBLE | WS_TABSTOP | WS_GROUP | BS_DEFPUSHBUTTON,
                                   rc.right - margin - 2 * btn_w - gap, rc.bottom - margin - btn_h,
                                   btn_w, btn_h, dlg, (HMENU)(INT_PTR)IDOK, app->instance, NULL);
        ctl[n++] = CreateWindowExW(0, L"BUTTON", L"Cancel", WS_CHILD | WS_VISIBLE | WS_TABSTOP,
                                   rc.right - margin - btn_w, rc.bottom - margin - btn_h, btn_w,
                                   btn_h, dlg, (HMENU)(INT_PTR)IDCANCEL, app->instance, NULL);
        for (i = 0; i < n; i++)
        {
            if (ctl[i] != NULL && app->font_ui != NULL)
            {
                SendMessageW(ctl[i], WM_SETFONT, (WPARAM)app->font_ui, TRUE);
            }
        }
        SetFocus(ctl[d->detail ? 2 : 1]);
        return (INT_PTR)FALSE;
    }
    case WM_COMMAND:
        if (LOWORD(wParam) == IDOK && d != NULL)
        {
            d->detail =
                SendMessageW(GetDlgItem(dlg, IDC_RES_EXPORT_DETAIL), BM_GETCHECK, 0, 0) == BST_CHECKED;
            EndDialog(dlg, 1);
            return (INT_PTR)TRUE;
        }
        if (LOWORD(wParam) == IDCANCEL)
        {
            EndDialog(dlg, 0);
            return (INT_PTR)TRUE;
        }
        break;
    case WM_CLOSE:
        EndDialog(dlg, 0);
        return (INT_PTR)TRUE;
    default:
        break;
    }
    return (INT_PTR)FALSE;
}

/* Export the results: the selected rows (displayed view), or everything after asking for
 * the layout. */
static void App_ExportResults(ResultsWindow *rw, BOOL selection_only)
{
    static BOOL s_detail = FALSE; /* the last layout chosen in this session */
    wchar_t suggested[300];
    const wchar_t *headers[RES_MAX_COLS];
    ResExportCtx ctx;
    uint32_t *idx = NULL;
    uint32_t n = 0;
    int c;

    if (!selection_only)
    {
        ResExportOpt opt;
        ZeroMemory(&opt, sizeof(opt));
        opt.app = rw->app;
        opt.detail = s_detail;
        if (App_RunModalDialogOwned(rw->app, rw->hwnd, L"Export Election Results", ResExportDlgProc,
                                    (LPARAM)&opt) != 1)
        {
            return;
        }
        s_detail = opt.detail;
        if (opt.detail)
        {
            wchar_t path[MAX_PATH];
            char delim = ',';
            char *buf = NULL;
            size_t len = 0;
            uint8_t *cmask = NULL;
            uint32_t ci;
            StringCchPrintfW(suggested, ARRAYSIZE(suggested), L"%s-%s", rw->base_name,
                             rw->filt_active ? L"Filtered_Detail" : L"All_Detail");
            if (!App_PromptExportPath(rw->hwnd, suggested, path, ARRAYSIZE(path), &delim))
            {
                return;
            }
            if (rw->filt_active)
            {
                /* the contests with a row the filter shows */
                cmask = (uint8_t *)calloc((size_t)rw->res.ncontests + 1, 1);
                for (ci = 0; cmask != NULL && ci < rw->res.ncontests; ci++)
                {
                    const EeResultsContest *ct = &rw->res.contests[ci];
                    uint32_t k;
                    if (!Res_ContestHasPrecinct(rw, ct))
                    {
                        continue;
                    }
                    for (k = 0; k < ct->nchoices && !cmask[ci]; k++)
                    {
                        cmask[ci] = (uint8_t)Res_RowPasses(rw, ci, RR_CHOICE, k);
                    }
                    if (ct->has_over_under && !cmask[ci])
                    {
                        cmask[ci] = (uint8_t)(Res_RowPasses(rw, ci, RR_OVER, 0) ||
                                              Res_RowPasses(rw, ci, RR_UNDER, 0));
                    }
                }
            }
            {
                HCURSOR old = SetCursor(LoadCursorW(NULL, IDC_WAIT));
                BOOL ok = EeResults_FormatDetailUtf8(&rw->res, delim, rw->pmask, rw->vmask, cmask,
                                                     &buf, &len);
                SetCursor(old);
                if (!ok)
                {
                    MessageBoxW(rw->hwnd, L"Out of memory building the export.", L"Export",
                                MB_ICONERROR | MB_OK);
                }
                else
                {
                    App_WriteExportUtf8(rw->hwnd, path, buf, len);
                }
            }
            free(buf);
            free(cmask);
            return;
        }
    }
    else
    {
        idx = App_CollectSelectedIndices(rw->list, rw->nrows, &n);
        if (n == 0)
        {
            free(idx);
            return;
        }
    }
    if (!selection_only)
    {
        n = rw->nrows;
    }
    for (c = 0; c < rw->ncols; c++)
    {
        headers[c] = rw->col_title[c];
    }
    ctx.rw = rw;
    ctx.idx = idx;
    StringCchPrintfW(suggested, ARRAYSIZE(suggested), L"%s-%s%s", rw->base_name,
                     selection_only ? L"Selected_Results"
                                    : (rw->filt_active ? L"Filtered_Results" : L"Results"),
                     rw->by_precinct ? L"_By_Precinct" : L"");
    App_ExportReportModel(rw->hwnd, suggested, (uint32_t)rw->ncols, headers, n, Res_ExportCell, &ctx);
    free(idx);
}

static const wchar_t k_ResHelpExport[] =
    L"File \x2192 Export Election Results\x2026 saves the results as UTF-8 CSV (or TSV, chosen in "
    L"the Save dialog). Choose the layout:\r\n"
    L"\x2022 Displayed view: exactly the columns and rows shown \x2014 Totals or By Precinct, the "
    L"filter, the vote type columns and the sort. Easy to read in a spreadsheet.\r\n"
    L"\x2022 All detail (reloadable): every precinct, vote type and turnout figure, one record per "
    L"line, with the election name, timestamp and each contest's order. Loading it back gives "
    L"exactly the same results (compare the two windows to see). A filter limits it to the "
    L"counted precincts and vote types and the contests with a row shown.\r\n\r\n"
    L"Both layouts load back with File \x2192 Load Election Results\x2026. A Displayed-view file "
    L"keeps only what it shows (a Totals file has no precinct detail), and two contests with the "
    L"same name are told apart only while the file is in the window's own order; use All detail "
    L"to reload exactly.\r\n\r\n"
    L"Right-click \x2192 Export Selected\x2026 saves the selected rows in the displayed view.";

/* --- Options --------------------------------------------------------------- */

typedef struct ResOptData
{
    AppState *app;
    HWND owner;
    int zoom;
    BOOL show_vt;
    int party_first;
} ResOptData;

static INT_PTR CALLBACK ResOptionsDlgProc(HWND dlg, UINT msg, WPARAM wParam, LPARAM lParam)
{
    ResOptData *d = (ResOptData *)GetWindowLongPtrW(dlg, GWLP_USERDATA);
    switch (msg)
    {
    case WM_INITDIALOG:
    {
        AppState *app;
        RECT rc;
        int margin, btn_w, btn_h, gap, y;
        HWND ctl[9];
        int nctl = 0, i;
        wchar_t ztxt[16];
        d = (ResOptData *)lParam;
        SetWindowLongPtrW(dlg, GWLP_USERDATA, (LONG_PTR)d);
        app = d->app;
        App_CenterModalClient(dlg, app, Scale(app, 360), Scale(app, 200));
        GetClientRect(dlg, &rc);
        margin = Scale(app, 14);
        btn_w = Scale(app, 84);
        btn_h = Scale(app, 26);
        gap = Scale(app, 8);
        y = margin;
        ctl[nctl++] = CreateWindowExW(0, L"STATIC", L"Zoom (%):", WS_CHILD | WS_VISIBLE, margin,
                                      y + Scale(app, 3), Scale(app, 80), Scale(app, 18), dlg, NULL,
                                      app->instance, NULL);
        StringCchPrintfW(ztxt, ARRAYSIZE(ztxt), L"%d", d->zoom);
        ctl[nctl++] = CreateWindowExW(WS_EX_CLIENTEDGE, L"EDIT", ztxt,
                                      WS_CHILD | WS_VISIBLE | WS_TABSTOP | ES_NUMBER,
                                      margin + Scale(app, 84), y, Scale(app, 64), Scale(app, 22),
                                      dlg, (HMENU)(INT_PTR)IDC_RES_ZOOM, app->instance, NULL);
        ctl[nctl++] = CreateWindowExW(0, UPDOWN_CLASSW, NULL,
                                      WS_CHILD | WS_VISIBLE | UDS_SETBUDDYINT | UDS_ALIGNRIGHT |
                                          UDS_ARROWKEYS | UDS_NOTHOUSANDS,
                                      0, 0, 0, 0, dlg, (HMENU)(INT_PTR)IDC_RES_ZOOM_SPIN,
                                      app->instance, NULL);
        SendMessageW(ctl[nctl - 1], UDM_SETBUDDY, (WPARAM)ctl[nctl - 2], 0);
        SendMessageW(ctl[nctl - 1], UDM_SETRANGE32, (WPARAM)k_ZoomMin, (LPARAM)k_ZoomMax);
        SendMessageW(ctl[nctl - 1], UDM_SETPOS32, 0, (LPARAM)d->zoom);
        y += Scale(app, 34);
        ctl[nctl++] = CreateWindowExW(0, L"BUTTON", L"Show a column for each vote type",
                                      WS_CHILD | WS_VISIBLE | WS_TABSTOP | BS_AUTOCHECKBOX, margin,
                                      y, rc.right - 2 * margin, Scale(app, 22), dlg,
                                      (HMENU)(INT_PTR)IDC_RES_SHOW_VT, app->instance, NULL);
        SendMessageW(ctl[nctl - 1], BM_SETCHECK, d->show_vt ? BST_CHECKED : BST_UNCHECKED, 0);
        y += Scale(app, 32);
        ctl[nctl++] = CreateWindowExW(0, L"STATIC", L"Party to display first in a primary:",
                                      WS_CHILD | WS_VISIBLE, margin, y, rc.right - 2 * margin,
                                      Scale(app, 18), dlg, NULL, app->instance, NULL);
        y += Scale(app, 22);
        ctl[nctl++] = CreateWindowExW(0, L"BUTTON", L"Republican",
                                      WS_CHILD | WS_VISIBLE | WS_TABSTOP | WS_GROUP |
                                          BS_AUTORADIOBUTTON,
                                      margin + Scale(app, 10), y, Scale(app, 130), Scale(app, 22),
                                      dlg, (HMENU)(INT_PTR)IDC_RES_PARTY_REP, app->instance, NULL);
        ctl[nctl++] = CreateWindowExW(0, L"BUTTON", L"Democratic",
                                      WS_CHILD | WS_VISIBLE | WS_TABSTOP | BS_AUTORADIOBUTTON,
                                      margin + Scale(app, 150), y, Scale(app, 130), Scale(app, 22),
                                      dlg, (HMENU)(INT_PTR)IDC_RES_PARTY_DEM, app->instance, NULL);
        SendMessageW(d->party_first == EE_TAB_PARTY_DEM ? ctl[nctl - 1] : ctl[nctl - 2],
                     BM_SETCHECK, BST_CHECKED, 0);
        ctl[nctl++] = CreateWindowExW(0, L"BUTTON", L"OK",
                                      WS_CHILD | WS_VISIBLE | WS_TABSTOP | WS_GROUP |
                                          BS_DEFPUSHBUTTON,
                                      rc.right - margin - 2 * btn_w - gap,
                                      rc.bottom - margin - btn_h, btn_w, btn_h, dlg,
                                      (HMENU)(INT_PTR)IDOK, app->instance, NULL);
        ctl[nctl++] = CreateWindowExW(0, L"BUTTON", L"Cancel", WS_CHILD | WS_VISIBLE | WS_TABSTOP,
                                      rc.right - margin - btn_w, rc.bottom - margin - btn_h, btn_w,
                                      btn_h, dlg, (HMENU)(INT_PTR)IDCANCEL, app->instance, NULL);
        for (i = 0; i < nctl; i++)
        {
            if (ctl[i] != NULL && app->font_ui != NULL)
            {
                SendMessageW(ctl[i], WM_SETFONT, (WPARAM)app->font_ui, TRUE);
            }
        }
        SetFocus(GetDlgItem(dlg, IDC_RES_ZOOM));
        SendMessageW(GetDlgItem(dlg, IDC_RES_ZOOM), EM_SETSEL, 0, -1);
        return (INT_PTR)FALSE;
    }

    case WM_COMMAND:
        if (LOWORD(wParam) == IDOK && d != NULL)
        {
            BOOL parsed = FALSE;
            UINT z = GetDlgItemInt(dlg, IDC_RES_ZOOM, &parsed, FALSE);
            if (!parsed || (int)z < k_ZoomMin || (int)z > k_ZoomMax)
            {
                wchar_t msg_text[96];
                StringCchPrintfW(msg_text, ARRAYSIZE(msg_text),
                                 L"Zoom must be a number from %d to %d.", k_ZoomMin, k_ZoomMax);
                MessageBoxW(dlg, msg_text, k_WindowTitle, MB_ICONWARNING | MB_OK);
                SetFocus(GetDlgItem(dlg, IDC_RES_ZOOM));
                return (INT_PTR)TRUE;
            }
            d->zoom = (int)z;
            d->show_vt =
                SendMessageW(GetDlgItem(dlg, IDC_RES_SHOW_VT), BM_GETCHECK, 0, 0) == BST_CHECKED;
            d->party_first =
                SendMessageW(GetDlgItem(dlg, IDC_RES_PARTY_DEM), BM_GETCHECK, 0, 0) == BST_CHECKED
                    ? EE_TAB_PARTY_DEM
                    : EE_TAB_PARTY_REP;
            EndDialog(dlg, 1);
            return (INT_PTR)TRUE;
        }
        if (LOWORD(wParam) == IDCANCEL)
        {
            EndDialog(dlg, 0);
            return (INT_PTR)TRUE;
        }
        break;

    case WM_CLOSE:
        EndDialog(dlg, 0);
        return (INT_PTR)TRUE;

    default:
        break;
    }
    return (INT_PTR)FALSE;
}

static void App_ShowResultsOptions(ResultsWindow *rw)
{
    ResOptData d;
    BOOL rebuild;
    d.app = rw->app;
    d.owner = rw->hwnd;
    d.zoom = rw->app->zoom_percent > 0 ? rw->app->zoom_percent : k_ZoomDefault;
    d.show_vt = rw->show_vt;
    d.party_first = g_settings.cvr_tab_party_first;
    if (App_RunModalDialogOwned(rw->app, rw->hwnd, L"Election Results Options", ResOptionsDlgProc,
                                (LPARAM)&d) != 1)
    {
        return;
    }
    rebuild = d.show_vt != rw->show_vt || d.party_first != g_settings.cvr_tab_party_first;
    if (d.zoom != rw->app->zoom_percent)
    {
        rw->app->zoom_percent = App_ClampZoom(d.zoom);
        App_UpdateDpiMetrics(rw->app, rw->app->dpi); /* rebuilds the zoomed grid font */
        if (rw->app->font_grid != NULL)
        {
            SendMessageW(rw->list, WM_SETFONT, (WPARAM)rw->app->font_grid, TRUE);
        }
        g_settings.zoom_percent = rw->app->zoom_percent;
        rebuild = TRUE; /* column widths follow the zoom */
    }
    rw->show_vt = d.show_vt;
    g_settings.results_show_vote_types = d.show_vt;
    g_settings.cvr_tab_party_first = d.party_first;
    EeSettings_Save(&g_settings);
    if (rebuild)
    {
        Res_Rebuild(rw);
    }
}

/* --- Window ---------------------------------------------------------------- */

static HMENU App_CreateResultsMenu(void)
{
    HMENU menu = CreateMenu();
    HMENU file_menu = CreatePopupMenu();
    HMENU edit_menu = CreatePopupMenu();
    HMENU view_menu = CreatePopupMenu();
    HMENU filter_menu = CreatePopupMenu();
    HMENU reports_menu = CreatePopupMenu();
    HMENU compare_menu = CreatePopupMenu();
    HMENU help_menu = CreatePopupMenu();
    if (menu == NULL || file_menu == NULL || edit_menu == NULL || view_menu == NULL ||
        filter_menu == NULL || reports_menu == NULL || compare_menu == NULL || help_menu == NULL)
    {
        HMENU all[8];
        int i;
        all[0] = file_menu;
        all[1] = edit_menu;
        all[2] = view_menu;
        all[3] = filter_menu;
        all[4] = reports_menu;
        all[5] = compare_menu;
        all[6] = help_menu;
        all[7] = menu;
        for (i = 0; i < 8; i++)
        {
            if (all[i] != NULL)
            {
                DestroyMenu(all[i]);
            }
        }
        return NULL;
    }
    AppendMenuW(file_menu, MF_STRING, IDM_FILE_OPEN_VOTER_LIST, L"&Load Voter List…\tCtrl+O");
    AppendMenuW(file_menu, MF_STRING, IDM_FILE_OPEN_CVR, L"Load Cast &Vote Records…");
    AppendMenuW(file_menu, MF_STRING, IDM_FILE_OPEN_ROSTER, L"Load Voter &Roster…");
    AppendMenuW(file_menu, MF_STRING, IDM_FILE_OPEN_RESULTS, L"Load Election Re&sults…");
    AppendMenuW(file_menu, MF_STRING, IDM_FILE_OPEN_AUDIT, L"Load &Audit Logs…");
    AppendMenuW(file_menu, MF_STRING, IDM_FILE_CLOSE_RESULTS, L"&Close Election Results");
    AppendMenuW(file_menu, MF_STRING, IDM_FILE_EXPORT_RESULTS, L"&Export Election Results…");
    AppendMenuW(file_menu, MF_SEPARATOR, 0, NULL);
    AppendMenuW(file_menu, MF_STRING, IDM_FILE_EXIT, L"E&xit");
    AppendMenuW(edit_menu, MF_STRING, IDM_EDIT_COPY, L"&Copy\tCtrl+C");
    AppendMenuW(edit_menu, MF_SEPARATOR, 0, NULL);
    AppendMenuW(edit_menu, MF_STRING, IDM_RES_OPTIONS, L"&Options…");
    AppendMenuW(view_menu, MF_STRING, IDM_RES_VIEW_TOTALS, L"&Totals");
    AppendMenuW(view_menu, MF_STRING, IDM_RES_VIEW_PRECINCT, L"By &Precinct");
    AppendMenuW(view_menu, MF_SEPARATOR, 0, NULL);
    AppendMenuW(view_menu, MF_STRING, IDM_RES_VIEW_RESET, L"&Reset View");
    AppendMenuW(filter_menu, MF_STRING, IDM_FILTER_EDIT, L"&Filter…\tCtrl+L");
    AppendMenuW(filter_menu, MF_STRING, IDM_FILTER_RESET, L"&Reset Filter");
    AppendMenuW(reports_menu, MF_STRING, IDM_RES_TURNOUT, L"Display &Turnout Report…");
    AppendMenuW(reports_menu, MF_SEPARATOR, 0, NULL);
    AppendMenuW(reports_menu, MF_STRING, IDM_RES_LOAD_SUMMARY, L"Display &Load Summary…");
    AppendMenuW(help_menu, MF_STRING, IDM_HELP_LOADING, L"&Loading Election Results");
    AppendMenuW(help_menu, MF_STRING, IDM_HELP_OPTIONS, L"&Views and Options");
    AppendMenuW(help_menu, MF_STRING, IDM_HELP_FILTERS, L"&Filters");
    AppendMenuW(help_menu, MF_STRING, IDM_HELP_REPORTS, L"&Reports");
    AppendMenuW(help_menu, MF_STRING, IDM_HELP_COMPARE, L"&Compare");
    AppendMenuW(help_menu, MF_STRING, IDM_HELP_EXPORT, L"&Export");
    AppendMenuW(help_menu, MF_SEPARATOR, 0, NULL);
    AppendMenuW(help_menu, MF_STRING, IDM_HELP_ABOUT, L"&About Election Explorer…");
    AppendMenuW(compare_menu, MF_STRING | MF_GRAYED, 0, L"(no other window to compare)");
    AppendMenuW(menu, MF_POPUP, (UINT_PTR)file_menu, L"&File");
    AppendMenuW(menu, MF_POPUP, (UINT_PTR)edit_menu, L"&Edit");
    AppendMenuW(menu, MF_POPUP, (UINT_PTR)view_menu, L"&View");
    AppendMenuW(menu, MF_POPUP, (UINT_PTR)filter_menu, L"F&ilter");
    AppendMenuW(menu, MF_POPUP, (UINT_PTR)reports_menu, L"&Reports");
    AppendMenuW(menu, MF_POPUP, (UINT_PTR)compare_menu, L"&Compare");
    AppendMenuW(menu, MF_POPUP, (UINT_PTR)help_menu, L"&Help");
    return menu;
}

/* Subclass so the list draws its header bold on the grey band, like the voter list. */
static LRESULT CALLBACK ResultsListSubclass(HWND hwnd, UINT msg, WPARAM wParam, LPARAM lParam)
{
    if (msg == WM_NOTIFY)
    {
        NMHDR *nm = (NMHDR *)lParam;
        HWND header = ListView_GetHeader(hwnd);
        ResultsWindow *rw = (ResultsWindow *)GetWindowLongPtrW(GetParent(hwnd), GWLP_USERDATA);
        if (rw != NULL && nm != NULL && header != NULL && nm->hwndFrom == header &&
            nm->code == NM_CUSTOMDRAW)
        {
            return App_HeaderCustomDraw(rw->app, (NMCUSTOMDRAW *)lParam, FALSE);
        }
    }
    return CallWindowProcW(g_old_results_list_proc, hwnd, msg, wParam, lParam);
}

/* TRUE when a file other than the shown one disagrees with it. */
static BOOL Res_HasDifferences(const EeResults *r)
{
    uint32_t i;
    for (i = 0; i < r->nissues; i++)
    {
        if (r->issues[i].source != 0)
        {
            return TRUE;
        }
    }
    return FALSE;
}

static LRESULT CALLBACK ResultsWndProc(HWND hwnd, UINT msg, WPARAM wParam, LPARAM lParam)
{
    ResultsWindow *rw = (ResultsWindow *)GetWindowLongPtrW(hwnd, GWLP_USERDATA);

    switch (msg)
    {
    case WM_CREATE:
    {
        CREATESTRUCTW *cs = (CREATESTRUCTW *)lParam;
        RECT rc;
        rw = (ResultsWindow *)cs->lpCreateParams;
        SetWindowLongPtrW(hwnd, GWLP_USERDATA, (LONG_PTR)rw);
        rw->hwnd = hwnd;
        GetClientRect(hwnd, &rc);
        rw->list = CreateWindowExW(0, WC_LISTVIEWW, L"",
                                   WS_CHILD | WS_VISIBLE | LVS_REPORT | LVS_OWNERDATA |
                                       LVS_SHOWSELALWAYS,
                                   0, 0, rc.right, rc.bottom, hwnd, NULL, rw->app->instance, NULL);
        if (rw->list == NULL)
        {
            return -1;
        }
        {
            WNDPROC old =
                (WNDPROC)SetWindowLongPtrW(rw->list, GWLP_WNDPROC, (LONG_PTR)ResultsListSubclass);
            if (g_old_results_list_proc == NULL)
            {
                g_old_results_list_proc = old;
            }
        }
        ListView_SetExtendedListViewStyle(rw->list, LVS_EX_FULLROWSELECT | LVS_EX_DOUBLEBUFFER |
                                                        LVS_EX_GRIDLINES);
        if (rw->app->font_grid != NULL)
        {
            SendMessageW(rw->list, WM_SETFONT, (WPARAM)rw->app->font_grid, TRUE);
        }
        rw->status = CreateWindowExW(0, STATUSCLASSNAMEW, NULL,
                                     WS_CHILD | WS_VISIBLE | SBARS_SIZEGRIP, 0, 0, 0, 0, hwnd, NULL,
                                     rw->app->instance, NULL);
        if (rw->status != NULL && rw->app->font_ui != NULL)
        {
            SendMessageW(rw->status, WM_SETFONT, (WPARAM)rw->app->font_ui, TRUE);
        }
        Res_Rebuild(rw);
        Res_Layout(rw, rc.right, rc.bottom);
        return 0;
    }

    case WM_SIZE:
        if (rw != NULL)
        {
            Res_Layout(rw, LOWORD(lParam), HIWORD(lParam));
        }
        return 0;

    case WM_SETFOCUS:
        if (rw != NULL && rw->list != NULL)
        {
            SetFocus(rw->list);
        }
        return 0;

    case WM_INITMENUPOPUP:
        if (rw != NULL)
        {
            HMENU m = (HMENU)wParam;
            BOOL detail = EeResults_HasPrecinctDetail(&rw->res);
            CheckMenuItem(m, IDM_RES_VIEW_TOTALS,
                          MF_BYCOMMAND | (rw->by_precinct ? MF_UNCHECKED : MF_CHECKED));
            CheckMenuItem(m, IDM_RES_VIEW_PRECINCT,
                          MF_BYCOMMAND | (rw->by_precinct ? MF_CHECKED : MF_UNCHECKED));
            EnableMenuItem(m, IDM_RES_VIEW_PRECINCT, MF_BYCOMMAND | (detail ? MF_ENABLED : MF_GRAYED));
            EnableMenuItem(m, IDM_RES_VIEW_RESET,
                           MF_BYCOMMAND | (rw->sort_col >= 0 ? MF_ENABLED : MF_GRAYED));
            EnableMenuItem(m, IDM_FILTER_RESET,
                           MF_BYCOMMAND | (rw->filters.count > 0 ? MF_ENABLED : MF_GRAYED));
            EnableMenuItem(m, IDM_RES_TURNOUT,
                           MF_BYCOMMAND | (rw->res.nprecincts > 0 ? MF_ENABLED : MF_GRAYED));
            /* Compare (File, Edit, View, Filter, Reports, Compare, Help). */
            if (m == GetSubMenu(GetMenu(hwnd), 5))
            {
                Res_FillCompareMenu(m, hwnd, TRUE, rw->cmp_targets, (int)ARRAYSIZE(rw->cmp_targets));
            }
        }
        break;

    case WM_COMMAND:
        if (rw == NULL)
        {
            break;
        }
        if (LOWORD(wParam) >= IDM_COMPARE_WITH_FIRST &&
            LOWORD(wParam) < IDM_COMPARE_WITH_FIRST + ARRAYSIZE(rw->cmp_targets))
        {
            HWND target = rw->cmp_targets[LOWORD(wParam) - IDM_COMPARE_WITH_FIRST];
            if (target != NULL && IsWindow(target) && Aud_IsAuditWindow(target))
            {
                App_RunBallotRecon(target, hwnd);
            }
            else if (target != NULL && IsWindow(target))
            {
                App_RunResultsCompare(hwnd, target);
            }
            return 0;
        }
        switch (LOWORD(wParam))
        {
        case IDM_EDIT_COPY: /* also Ctrl+C */
            Res_CopySelected(rw);
            return 0;
        case IDM_RES_OPTIONS:
            App_ShowResultsOptions(rw);
            return 0;
        case IDM_RES_VIEW_TOTALS:
        case IDM_RES_VIEW_PRECINCT:
        {
            BOOL want = LOWORD(wParam) == IDM_RES_VIEW_PRECINCT;
            if (want && !EeResults_HasPrecinctDetail(&rw->res))
            {
                return 0;
            }
            if (want != rw->by_precinct)
            {
                rw->by_precinct = want;
                Res_Rebuild(rw);
            }
            return 0;
        }
        case IDM_RES_VIEW_RESET:
            /* back to the order the rows had when loaded */
            if (rw->sort_col >= 0)
            {
                rw->sort_col = -1;
                Res_Sort(rw);
                InvalidateRect(rw->list, NULL, FALSE);
            }
            return 0;
        case IDM_RES_LOAD_SUMMARY:
            App_ShowResultsLoadSummary(rw);
            return 0;
        case IDM_RES_TURNOUT:
            App_ShowResultsTurnout(rw);
            return 0;
        case IDM_FILTER_EDIT: /* also Ctrl+L */
            App_ShowResultsFilter(rw);
            return 0;
        case IDM_FILTER_RESET:
            Res_ResetFilter(rw);
            return 0;
        case IDM_HELP_FILTERS:
            App_ShowHelpTopicOn(rw->app, hwnd, L"Help — Filters", k_ResHelpFilters);
            return 0;
        case IDM_HELP_REPORTS:
            App_ShowHelpTopicOn(rw->app, hwnd, L"Help — Reports", k_ResHelpReports);
            return 0;
        case IDM_HELP_COMPARE:
            App_ShowResultsCompareHelp(rw->app, hwnd);
            return 0;
        case IDM_FILE_OPEN_VOTER_LIST:
            App_BeginOpenVoterList(rw->app);
            return 0;
        case IDM_FILE_OPEN_CVR:
            App_BeginOpenCvr(rw->app);
            return 0;
        case IDM_FILE_OPEN_ROSTER:
            App_BeginOpenRoster(rw->app);
            return 0;
        case IDM_FILE_OPEN_RESULTS:
            App_BeginOpenResults(rw->app);
            return 0;
        case IDM_FILE_OPEN_AUDIT:
            App_BeginOpenAudit(rw->app);
            return 0;
        case IDM_FILE_CLOSE_RESULTS:
            DestroyWindow(hwnd);
            return 0;
        case IDM_FILE_EXPORT_RESULTS:
            App_ExportResults(rw, FALSE);
            return 0;
        case IDM_HELP_EXPORT:
            App_ShowHelpTopicOn(rw->app, hwnd, L"Help — Export", k_ResHelpExport);
            return 0;
        case IDM_FILE_EXIT:
            App_ExitAll();
            return 0;
        case IDM_HELP_LOADING:
            App_ShowHelpTopicOn(rw->app, hwnd, L"Help — Loading Election Results",
                                k_ResHelpLoading);
            return 0;
        case IDM_HELP_OPTIONS:
            App_ShowHelpTopicOn(rw->app, hwnd, L"Help — Views and Options", k_ResHelpOptions);
            return 0;
        case IDM_HELP_ABOUT:
            App_ShowAboutOn(rw->app, hwnd);
            return 0;
        default:
            break;
        }
        break;

    case WM_NOTIFY:
    {
        NMHDR *hdr = (NMHDR *)lParam;
        if (rw == NULL || hdr->hwndFrom != rw->list)
        {
            break;
        }
        if (hdr->code == LVN_GETDISPINFOW)
        {
            NMLVDISPINFOW *di = (NMLVDISPINFOW *)lParam;
            if ((di->item.mask & LVIF_TEXT) && di->item.iItem >= 0 && di->item.cchTextMax > 0)
            {
                Res_CellText(rw, (uint32_t)di->item.iItem, di->item.iSubItem, di->item.pszText,
                             (size_t)di->item.cchTextMax);
            }
            return 0;
        }
        if (hdr->code == LVN_COLUMNCLICK)
        {
            NMLISTVIEW *nlv = (NMLISTVIEW *)lParam;
            if (nlv->iSubItem == rw->sort_col)
            {
                rw->sort_asc = !rw->sort_asc;
            }
            else
            {
                rw->sort_col = nlv->iSubItem;
                /* counts and percentages sort largest first */
                rw->sort_asc = !(rw->col_kind[rw->sort_col] == RC_VT ||
                                 rw->col_kind[rw->sort_col] == RC_TOTAL ||
                                 rw->col_kind[rw->sort_col] == RC_PERCENT);
            }
            Res_Sort(rw);
            InvalidateRect(rw->list, NULL, FALSE);
            return 0;
        }
        if (hdr->code == NM_RCLICK)
        {
            LPNMITEMACTIVATE ia = (LPNMITEMACTIVATE)lParam;
            POINT screen = ia->ptAction;
            ClientToScreen(rw->list, &screen);
            Res_OnContextMenu(rw, ia->iItem, screen);
            return 0;
        }
        break;
    }

    case WM_CLOSE:
        DestroyWindow(hwnd);
        return 0;

    case WM_DESTROY:
        if (rw != NULL)
        {
            if (rw->hwnd_filter != NULL)
            {
                DestroyWindow(rw->hwnd_filter); /* clears rw->hwnd_filter */
            }
            if (rw->turnout != NULL)
            {
                DestroyWindow(rw->turnout->hwnd); /* clears rw->turnout */
            }
            EeFilter_Clear(&rw->filters);
            free(rw->pmask);
            free(rw->vmask);
            Res_FreeTab(rw);
            free(rw->rows);
            EeResults_Clear(&rw->res);
            App_FreeCvrUi(rw->app);
            free(rw);
            CvrWindow_Unregister(hwnd);
            SetWindowLongPtrW(hwnd, GWLP_USERDATA, 0);
            App_MaybeQuit();
        }
        return 0;

    default:
        break;
    }
    return DefWindowProcW(hwnd, msg, wParam, lParam);
}

/* Create a results window, taking ownership of @p res (moved; the source is reset). Like
 * a CVR window it is a first-class top-level window with its own resource-only AppState,
 * so it outlives the window that loaded it. */
static void App_CreateResultsWindow(AppState *app, EeResults *res, const wchar_t *first_path)
{
    ResultsWindow *rw = (ResultsWindow *)calloc(1, sizeof(ResultsWindow));
    AppState *ui;
    HWND h = NULL;
    wchar_t title[320];
    if (rw == NULL ||
        (ui = (AppState *)HeapAlloc(GetProcessHeap(), HEAP_ZERO_MEMORY, sizeof(AppState))) == NULL)
    {
        free(rw);
        EeResults_Clear(res);
        MessageBoxW(app->hwnd_main, L"Out of memory.", k_WindowTitle, MB_ICONERROR | MB_OK);
        return;
    }
    App_InitViewerState(ui, app->instance, app); /* seed zoom from the launcher */
    ui->is_cvr_ui = TRUE;                        /* resource-only, like a CVR window's */
    App_UpdateDpiMetrics(ui, app->dpi ? app->dpi : 96u);
    rw->app = ui;
    rw->res = *res; /* move */
    EeResults_Init(res);
    rw->sort_col = -1;
    rw->sort_asc = TRUE;
    rw->show_vt = g_settings.results_show_vote_types;
    EeFilter_Init(&rw->filters);
    rw->filt_hidden[RF_PRECINCT] = (uint8_t)!EeResults_HasPrecinctDetail(&rw->res);
    rw->filt_hidden[RF_VOTETYPE] = (uint8_t)(rw->res.nvt == 0);
    App_BaseNameNoExt(first_path, rw->base_name, ARRAYSIZE(rw->base_name));
    if (!Res_Tabulate(rw))
    {
        EeResults_Clear(&rw->res);
        App_FreeCvrUi(ui);
        free(rw);
        MessageBoxW(app->hwnd_main, L"Out of memory.", k_WindowTitle, MB_ICONERROR | MB_OK);
        return;
    }
    StringCchPrintfW(title, ARRAYSIZE(title), L"%s \x2014 Election Results",
                     rw->res.election_name != NULL ? rw->res.election_name
                                                   : App_PathBaseName(first_path));
    {
        HMENU menu = App_CreateResultsMenu();
        h = CreateWindowExW(0, k_ResultsClassName, title, WS_OVERLAPPEDWINDOW | WS_CLIPCHILDREN,
                            CW_USEDEFAULT, CW_USEDEFAULT, Scale(ui, 1000), Scale(ui, 600), NULL,
                            menu, ui->instance, rw);
        if (h == NULL && menu != NULL)
        {
            DestroyMenu(menu);
        }
    }
    if (h == NULL)
    {
        Res_FreeTab(rw);
        free(rw->rows);
        EeResults_Clear(&rw->res);
        App_FreeCvrUi(ui);
        free(rw);
        MessageBoxW(app->hwnd_main, L"Could not create the Election Results window.",
                    k_WindowTitle, MB_ICONERROR | MB_OK);
        return;
    }
    ui->hwnd_main = h;
    CvrWindow_Register(h); /* counts toward App_MaybeQuit and closes on Exit */
    ShowWindow(h, SW_SHOWNORMAL);
    UpdateWindow(h);
    if (Res_HasDifferences(&rw->res))
    {
        App_ShowResultsLoadSummary(rw);
    }
}

/* --- Loading (worker thread behind a modal progress dialog) --------------- */

#define EEM_RES_LOAD_DONE     (WM_APP + 15)
#define EEM_RES_LOAD_PROGRESS (WM_APP + 16)

typedef struct ResLoadJob
{
    AppState *app;
    const wchar_t *const *paths;
    int count;
    EeResults *results; /* out: one per election */
    uint32_t nresults;
    volatile LONG cancel;
    EeLoadStatus status;
    wchar_t err[512];
    HWND dlg;
    HWND bar;
    HANDLE thread;
} ResLoadJob;

static BOOL ResLoadProgressCb(const EeLoadProgress *pr, void *user)
{
    ResLoadJob *j = (ResLoadJob *)user;
    if (j != NULL && j->dlg != NULL)
    {
        PostMessageW(j->dlg, EEM_RES_LOAD_PROGRESS, (WPARAM)(pr->percent > 100u ? 100u : pr->percent),
                     0);
    }
    return j == NULL || j->cancel == 0;
}

static DWORD WINAPI ResLoadThreadProc(void *param)
{
    ResLoadJob *j = (ResLoadJob *)param;
    j->status = EeResults_LoadFiles(j->paths, (uint32_t)j->count, &j->results, &j->nresults,
                                    &j->cancel, ResLoadProgressCb, j, j->err, ARRAYSIZE(j->err));
    PostMessageW(j->dlg, EEM_RES_LOAD_DONE, 0, 0);
    return 0;
}

static INT_PTR CALLBACK ResLoadDlgProc(HWND dlg, UINT msg, WPARAM wParam, LPARAM lParam)
{
    ResLoadJob *j = (ResLoadJob *)GetWindowLongPtrW(dlg, GWLP_USERDATA);
    switch (msg)
    {
    case WM_INITDIALOG:
    {
        AppState *app;
        RECT rc;
        int margin;
        HWND label, cancel;
        j = (ResLoadJob *)lParam;
        SetWindowLongPtrW(dlg, GWLP_USERDATA, (LONG_PTR)j);
        j->dlg = dlg;
        app = j->app;
        App_CenterModalClient(dlg, app, Scale(app, 340), Scale(app, 120));
        GetClientRect(dlg, &rc);
        margin = Scale(app, 14);
        label = CreateWindowExW(0, L"STATIC", L"Loading election results…", WS_CHILD | WS_VISIBLE,
                                margin, margin, rc.right - 2 * margin, Scale(app, 20), dlg, NULL,
                                app->instance, NULL);
        j->bar = CreateWindowExW(0, PROGRESS_CLASSW, NULL, WS_CHILD | WS_VISIBLE, margin,
                                 margin + Scale(app, 26), rc.right - 2 * margin, Scale(app, 16), dlg,
                                 NULL, app->instance, NULL);
        cancel = CreateWindowExW(0, L"BUTTON", L"Cancel", WS_CHILD | WS_VISIBLE | WS_TABSTOP,
                                 rc.right - margin - Scale(app, 84),
                                 rc.bottom - margin - Scale(app, 26), Scale(app, 84),
                                 Scale(app, 26), dlg, (HMENU)(INT_PTR)IDCANCEL, app->instance,
                                 NULL);
        if (app->font_ui != NULL)
        {
            SendMessageW(label, WM_SETFONT, (WPARAM)app->font_ui, TRUE);
            SendMessageW(cancel, WM_SETFONT, (WPARAM)app->font_ui, TRUE);
        }
        if (j->bar != NULL)
        {
            SendMessageW(j->bar, PBM_SETRANGE32, 0, 100);
        }
        j->thread = CreateThread(NULL, 0, ResLoadThreadProc, j, 0, NULL);
        if (j->thread == NULL)
        {
            j->status = EeLoadStatus_Error;
            StringCchCopyW(j->err, ARRAYSIZE(j->err), L"Could not start loading.");
            EndDialog(dlg, 0);
        }
        return (INT_PTR)TRUE;
    }

    case EEM_RES_LOAD_PROGRESS:
        if (j != NULL && j->bar != NULL)
        {
            SendMessageW(j->bar, PBM_SETPOS, wParam, 0);
        }
        return (INT_PTR)TRUE;

    case EEM_RES_LOAD_DONE:
        if (j != NULL && j->thread != NULL)
        {
            WaitForSingleObject(j->thread, INFINITE);
            CloseHandle(j->thread);
            j->thread = NULL;
        }
        EndDialog(dlg, 1);
        return (INT_PTR)TRUE;

    case WM_COMMAND:
        if (j != NULL && LOWORD(wParam) == IDCANCEL)
        {
            InterlockedExchange(&j->cancel, 1);
            EnableWindow(GetDlgItem(dlg, IDCANCEL), FALSE);
            return (INT_PTR)TRUE;
        }
        break;

    case WM_CLOSE:
        if (j != NULL)
        {
            InterlockedExchange(&j->cancel, 1);
        }
        return (INT_PTR)TRUE;

    default:
        break;
    }
    return (INT_PTR)FALSE;
}

static void App_BeginOpenResults(AppState *app)
{
    OPENFILENAMEW ofn;
    wchar_t *files;
    wchar_t *paths[64];
    int count = 0;
    int i;
    ResLoadJob job;

    if (app == NULL)
    {
        return;
    }
    files = (wchar_t *)malloc(32768 * sizeof(wchar_t));
    if (files == NULL)
    {
        return;
    }
    files[0] = L'\0';
    ZeroMemory(&ofn, sizeof(ofn));
    ofn.lStructSize = sizeof(ofn);
    ofn.hwndOwner = app->hwnd_main;
    ofn.lpstrFilter = L"Election Results (*.zip;*.csv;*.tsv;*.txt;*.xml;*.xls;*.xlsx;*.pdf)\0"
                      L"*.zip;*.csv;*.tsv;*.txt;*.xml;*.xls;*.xlsx;*.pdf\0"
                      L"Clarity ZIP files (*.zip)\0*.zip\0"
                      L"Results report PDFs (*.pdf)\0*.pdf\0"
                      L"All files (*.*)\0*.*\0";
    ofn.lpstrFile = files;
    ofn.nMaxFile = 32768;
    ofn.Flags = OFN_FILEMUSTEXIST | OFN_PATHMUSTEXIST | OFN_EXPLORER | OFN_ALLOWMULTISELECT;
    ofn.lpstrTitle = L"Load Election Results";
    if (!GetOpenFileNameW(&ofn))
    {
        free(files);
        return;
    }
    /* A single full path, or a directory followed by NUL-separated names. */
    {
        const wchar_t *dir = files;
        size_t dlen = wcslen(dir);
        const wchar_t *p = files + dlen + 1;
        if (*p == L'\0')
        {
            paths[count++] = _wcsdup(files);
        }
        else
        {
            BOOL need_sep = (dlen > 0 && dir[dlen - 1] != L'\\');
            while (*p != L'\0' && count < (int)ARRAYSIZE(paths))
            {
                size_t need = dlen + 1 + wcslen(p) + 1;
                wchar_t *full = (wchar_t *)malloc(need * sizeof(wchar_t));
                if (full == NULL)
                {
                    break;
                }
                StringCchCopyW(full, need, dir);
                if (need_sep)
                {
                    StringCchCatW(full, need, L"\\");
                }
                StringCchCatW(full, need, p);
                paths[count++] = full;
                p += wcslen(p) + 1;
            }
        }
    }
    free(files);
    if (count == 0 || paths[0] == NULL)
    {
        return;
    }

    ZeroMemory(&job, sizeof(job));
    job.app = app;
    job.paths = (const wchar_t *const *)paths;
    job.count = count;
    job.status = EeLoadStatus_Error;
    App_RunModalDialog(app, L"Loading Election Results", ResLoadDlgProc, (LPARAM)&job);
    if (job.status == EeLoadStatus_Ok)
    {
        uint32_t g;
        for (g = 0; g < job.nresults; g++)
        {
            const wchar_t *first = paths[0];
            if (job.results[g].nsources > 0 && job.results[g].sources[0].path != NULL)
            {
                first = job.results[g].sources[0].path;
            }
            {
                wchar_t first_copy[MAX_PATH * 2];
                StringCchCopyW(first_copy, ARRAYSIZE(first_copy), first);
                App_CreateResultsWindow(app, &job.results[g], first_copy);
            }
        }
        EeResults_FreeArray(job.results, job.nresults);
    }
    else if (job.status == EeLoadStatus_Error)
    {
        MessageBoxW(app->hwnd_main, job.err[0] != L'\0' ? job.err : L"Could not load the results.",
                    k_WindowTitle, MB_ICONERROR | MB_OK);
    }
    for (i = 0; i < count; i++)
    {
        free(paths[i]);
    }
}

/* -------------------------------------------------------------------------- */
/* Audit Log window (1.6)                                                     */
/* -------------------------------------------------------------------------- */

/* Columns of the Events view. */
enum
{
    AC_TIMESTAMP = 0,
    AC_LOG,
    AC_EVENT,
    AC_MODULE,
    AC_USER,
    AC_DESC,
    AC_COPIES,
    AC_COUNT
};

/* Columns of the Ballot Batches view (AuditWindow.col_kind holds either kind). */
enum
{
    BC_COMMITTED = 16,
    BC_ID,
    BC_NAME,
    BC_PLACE,
    BC_MEDIA,
    BC_VOTETYPE,
    BC_BALLOTS,
    BC_USER,
    BC_STATUS,
    BC_END
};

#define AUD_MAX_COLS     16
#define AUD_MERGED       (-1) /* AuditWindow.show_copy: every copy merged */
#define AUD_VIEW_EVENTS  0
#define AUD_VIEW_BATCHES 1

typedef struct AudRow
{
    uint32_t idx; /* merged event of the log, the event of the copy shown, or a batch */
    uint32_t seq; /* position in display order, the sort's tie-break */
    uint8_t log;  /* index in EeAuditElection.logs (0 = Election, 1 = Admin, 2.. Hart Verity logs) */
} AudRow;

typedef struct AudReportWindow AudReportWindow;
static void AudRep_Refresh(AudReportWindow *r);

/* Report windows (AuditWindow.reps). */
#define AUD_REP_COPIES   0
#define AUD_REP_ACCT     1
#define AUD_REP_TIMELINE 2
#define AUD_REP_NOTABLE  3
#define AUD_REP_USERS    4
#define AUD_REP_RECON    5
#define AUD_REP_COUNT    6

static const wchar_t *const k_AudRepTitles[AUD_REP_COUNT] = {L"Copies Check",         L"Ballot Accounting",
                                                             L"L&A Testing Timeline", L"Notable Events",
                                                             L"Users and Workstations", L"Ballot Reconciliation"};

typedef struct AuditWindow
{
    AppState *app; /* this window's own resource-only AppState */
    HWND hwnd;
    HWND list;
    HWND status;
    EeAuditElection *el; /* one election (an array of 1, freed with EeAudit_FreeElections) */
    EeAuditAccounting acct; /* the ballot accounting (Election log, or Hart VerityCount log) */
    BOOL have_acct;
    int show_log;       /* View -> Log: 0 = all, else the kind shown (EE_AUDIT_*) */
    int show_copy_log;  /* View -> Copy: the log (index in el->logs) of the copy shown */
    int show_copy;      /* the copy shown as printed, or AUD_MERGED */
    int view;           /* AUD_VIEW_* */
    int session;        /* the accounting session the Ballot Batches view shows; -1 none */
    AudRow *rows;
    uint32_t nrows;
    uint32_t nrows_all; /* rows of the view before the filter */
    int sort_col; /* -1 = display order (newest first, as printed; batches in load order) */
    BOOL sort_asc;
    int ncols;
    uint8_t col_kind[AUD_MAX_COLS];
    uint32_t lacking[EE_AUDIT_NLOGS]; /* merged events each log's final copy lacks */
    AudReportWindow *reps[AUD_REP_COUNT]; /* open report windows (AUD_REP_*), or NULL */
    EeAuditNotes notes;                   /* notable events */
    BOOL have_notes;
    EeAuditTimeline tl; /* L&A testing timeline */
    BOOL have_tl;
    /* Filter: the rules (EeFilterRule.column is an AF_* column), compiled for matching. */
    EeFilterSet filters;
    HWND hwnd_filter;
    BOOL filt_active;
    struct AudRule *rules;
    uint32_t nrules;
    wchar_t base_name[192]; /* first copy's file name without extension, for filter files */
    HWND cmp_targets[32];   /* Compare menu: windows by command offset */
} AuditWindow;

/* Filter columns (EeFilterRule.column) of the audit window. */
enum
{
    AF_TIMESTAMP = 0,
    AF_LOG,
    AF_EVENT,
    AF_MODULE,
    AF_USER,
    AF_DESC,
    AF_VOTETYPE, /* Ballot Batches view */
    AF_COUNT
};

static const wchar_t *const k_AudFilterTitles[AF_COUNT] = {L"Timestamp", L"Log",         L"Event",    L"Module",
                                                           L"User Id",   L"Description", L"Vote Type"};

/* An enabled rule, ready to match: its value in UTF-8 (text) or as a time. */
typedef struct AudRule
{
    uint32_t column;
    EeFilterRelation relation;
    EeFilterAction action;
    char u8[EE_FILTER_VALUE_CCH * 3];
    int64_t ts;
} AudRule;

static WNDPROC g_old_audit_list_proc = NULL;

static const wchar_t k_AudHelpLoading[] =
    L"File \x2192 Load Audit Logs\x2026 opens ES&S Electionware audit log PDFs: the \"Election "
    L"Audit Events Report\" (one election's database) and the \"Admin Audit Events Report\" (the "
    L"whole system). Select every copy of an election's logs at once \x2014 counties print the "
    L"logs several times during an election, and each copy is a separate PDF.\r\n\r\n"
    L"The copies are grouped by county and election date: each election opens in its own "
    L"window, holding both of its logs and all their copies. The copies of a log are put in the "
    L"order they were printed (the print time in each report's header) and merged, so every "
    L"event appears once, with the number of copies that hold it.\r\n\r\n"
    L"The final copy of a log should hold every event of the copies printed before it. An "
    L"event the final copy lacks is shown in bold red, and the status bar counts them. A copy "
    L"printed with a start date lacks the earlier events on purpose.\r\n\r\n"
    L"A reloadable export (File \x2192 Export\x2026, Reloadable) loads back the same way, without "
    L"the PDFs, and can be selected with them.\r\n\r\n"
    L"Hart Verity \"Audit Log Report\" PDFs load the same way, one per component: VerityCount "
    L"(tabulation), VerityCentral (central scanning) and VerityData (the election definition). Select "
    L"an election's component logs together and they open in one window. A .zip holding the report "
    L"(Hays County posts it with an XML signature) can be selected as it is. The report numbers its "
    L"rows; the Copies Check says whether any row number is missing.\r\n\r\n"
    L"Other PDFs, including Electionware and Verity machine (tabulator) logs, are not read; the "
    L"message names the file. The logs hold county employees' user IDs, never voter data.";

static const wchar_t k_AudHelpOptions[] =
    L"The list shows one row per event, newest first, as the reports print them:\r\n"
    L"\x2022 Timestamp, Event (Info, Warning, Error ...), Module, User Id and Description, as "
    L"printed.\r\n"
    L"\x2022 Log: Election or Admin.\r\n"
    L"\x2022 Copies: how many of the log's copies hold the event (\"3 of 5\"). The status bar "
    L"names the first copy that holds the selected event, and its page.\r\n\r\n"
    L"View \x2192 Log shows both logs, or only the Election or the Admin log. View \x2192 Copy "
    L"shows one copy exactly as printed instead of the merged log.\r\n\r\n"
    L"The title bar shows the county, election and date. The status bar shows the copies "
    L"loaded, the events shown, and the ballots the Election log shows loaded into the EMS in the election "
    L"session (the last session after a full clear that loaded ballots other than logic and "
    L"accuracy test decks).\r\n\r\n"
    L"View \x2192 Ballot Batches lists the ballot batches loaded into the EMS in one session of "
    L"the Election log (the election session unless another is chosen in Reports \x2192 Select "
    L"Session\x2026): when each was committed, its batch ID and name, the poll place and media "
    L"its name gives, the vote type the poll place name points to, its Number of Ballots "
    L"(ballots, not ballot sheets), who loaded it, and its status:\r\n"
    L"\x2022 Counted: still loaded when the session ended.\r\n"
    L"\x2022 Removed by media clear: a \"Results successfully cleared\" for its media removed it.\r\n"
    L"\x2022 Replaced: its batch ID was committed again later in the session.\r\n"
    L"\x2022 L&A test deck: a logic and accuracy test batch (its poll place name has \"Logic\", "
    L"\"LA\", \"L&A\", \"Accuracy\" or \"Test\").\r\n"
    L"Batches that no longer count are grey. Double-click a batch, or right-click \x2192 Go to "
    L"Event, to see its commit in the Events view.\r\n\r\n"
    L"Hart Verity logs: the columns are the report's (Log Date & Time, Event, Device ID, User, Event "
    L"Data), and View \x2192 vDrive Reads takes the place of Ballot Batches: each vDrive read into a "
    L"task (State Read) with its polling place, vDrive ID and Ballot count. A task is a results "
    L"container that keeps its vDrives each time it is opened again; a vDrive read into the same task "
    L"again replaces its earlier read.\r\n\r\n"
    L"Edit \x2192 Options\x2026 sets the zoom (the size of the list text) and the words that make a "
    L"Hart task a test task (LAT, L&A, Test, Training, Logic; \"Pre-LAT\", \"PLAT\" and \"LAT2a\" "
    L"match).\r\n\r\n"
    L"Click a column title to sort by it; click again to reverse. View \x2192 Reset View puts "
    L"the rows back in printed order (batches in load order). Edit \x2192 Copy (Ctrl+C) or "
    L"right-click copies the selected rows as tab-separated text.";

static const wchar_t k_AudHelpReports[] =
    L"Reports \x2192 Copies Check\x2026 compares each log's copies, in the order they were "
    L"printed: each copy's file, print time, the election its header names, its first and last "
    L"event, its events, the events no earlier copy holds, and the earlier copies' events it "
    L"lacks, split into those outside its own time span (a copy printed with a start date) and "
    L"those inside it (missing, shown in red). The lines above the list say whether the final "
    L"copy holds every event of the copies before it.\r\n\r\n"
    L"Reports \x2192 Ballot Accounting\x2026 replays the Election log's batch loads, oldest "
    L"first:\r\n"
    L"\x2022 Sessions: a full clear (\"All Electionware election results will be cleared\" and "
    L"\"Results successfully cleared\") starts a session. The election session is the last one "
    L"that loaded batches other than L&A test decks; the others are L&A tests. Double-click a "
    L"session, or right-click \x2192 Use This Session, to show it in the Ballot Batches view and "
    L"the other tabs. Reports \x2192 Select Session\x2026 opens this tab.\r\n"
    L"\x2022 Poll Places and Vote Types: the session's counted batches and ballots.\r\n"
    L"\x2022 Media Clears: each clear of one media (\"Central Count Media-24\"). Poll places "
    L"number their media apart, so a clear removes the batches on that media of the poll place "
    L"loaded next, and only of the device type it names. When the next load does not tell which "
    L"poll place was meant, nothing is removed; the last two columns give the session's total "
    L"had the clear removed nothing, or every poll place's batches on that media.\r\n\r\n"
    L"Reports \x2192 L&A Testing Timeline\x2026 lists the logic and accuracy tests and the counting "
    L"periods in time order. A test is a session of test decks in the Election log, or a test copy of the "
    L"election restored or opened in the Admin log (a name with \"Test\", \"L&A\" or \"Logic\", restored "
    L"right after a backup of this election; other elections' test copies are left out). A counting "
    L"period is the loads of ballots (not test decks) from one test to the next; each shows its pauses "
    L"of 2 hours or more (and whether the election was closed meanwhile) and the nearest test before "
    L"and after it. A period with no test found before or after it is red. A test copy's own log is "
    L"not loaded, so its test shows only as the Admin log's restore and open.\r\n\r\n"
    L"Reports \x2192 Notable Events\x2026 sorts events into categories, most serious first:\r\n"
    L"\x2022 Red flag \x2014 results output before Election Day: a results report printed, saved to a "
    L"file or exported (cumulative, canvass, precinct and summary reports, text exports, cast vote "
    L"record exports) before Election Day while ballots (not test decks, more than 0) were loaded. "
    L"Ballots may be tabulated early (in a county of 100,000 or more, from 3 days before Election "
    L"Day), but results are not to be printed or released before Election Day; on Election Day they "
    L"are kept confidential until the polls close. The same output with only test decks or no ballots "
    L"loaded (logic and accuracy tests, zero reports) is listed as information.\r\n"
    L"\x2022 For review: ballots loaded more than 3 days before Election Day, elections deleted, failed "
    L"restores and backups, results validation errors, other errors, ballot sheet load errors, media "
    L"clears, clock anomalies.\r\n"
    L"\x2022 Information: full clears, zero-ballot batches, previously loaded batches, restores, user "
    L"accounts.\r\n"
    L"Election log events while another election was open (a database copied from an earlier "
    L"election keeps its log) are not checked. Double-click a category to list its events and an event "
    L"to see it in the Events view.\r\n\r\n"
    L"Reports \x2192 Users and Workstations\x2026 lists each user's events in both logs, first and last "
    L"event, modules, and the workstations the events name.\r\n\r\n"
    L"Hart Verity logs: Ballot Accounting lists the VerityCount log's tasks (official, or test by name), "
    L"their vDrives and ballots; the election task is the official task read into last. Its Central "
    L"Transfers tab matches each vDrive VerityCentral wrote (\"CVRs written to vDrive\", Total ballots) "
    L"with VerityCount's read of it by vDrive ID (or, when no \"vDrive closed\" was logged, by ballots "
    L"and time) and shows any difference in red. The timeline's tests are the test tasks' reads, one per "
    L"task and day. Notable Events adds Central to Count differences, manual vote records, reporting "
    L"status overrides, vDrives rejected and registered voter changes; a results report printed or "
    L"exported before Election Day is a red flag when ballots were read into the official task open at "
    L"the time.\r\n\r\n"
    L"The reports show what the logs show; they do not judge compliance. Right-click a report to copy "
    L"its rows; Ctrl+C copies the selected rows.";

static const wchar_t k_AudHelpFilters[] =
    L"Filter \x2192 Filter\x2026 (Ctrl+L) chooses which events and ballot batches the window "
    L"shows. Each rule is a column, a relation, a value, and Include or Exclude:\r\n"
    L"\x2022 Log, Event, Module, User Id: is / is not one of the values in the logs.\r\n"
    L"\x2022 Description: contains / does not contain the text typed (capital letters do not "
    L"matter), for example \"Committing new Batch\", \"cleared\" or \"error\".\r\n"
    L"\x2022 Timestamp: on or after / before a date (MM/DD/YYYY) or a date and time "
    L"(11/05/2024 7:00 PM, or 19:00). A date alone means midnight at its start.\r\n"
    L"\x2022 Vote Type: is / is not, in the Ballot Batches view only.\r\n\r\n"
    L"In the Ballot Batches view a batch is matched by its commit event (its time, user and "
    L"description) and its vote type.\r\n\r\n"
    L"Rules on the same column are alternatives (any included value passes), except Timestamp "
    L"rules, which all apply: \"on or after 11/05/2024\" and \"before 11/06/2024\" show that "
    L"day. Rules on different columns all apply, and a matching Exclude always hides the row. "
    L"Check boxes turn rules on and off. Export Filter\x2026 saves the rules to a file and Load "
    L"Filter\x2026 reads them back, also in another audit window. Filter \x2192 Reset Filter "
    L"removes every rule.\r\n\r\n"
    L"The filter changes what the list shows; the reports always count every event and batch.";

static const wchar_t k_AudHelpCompare[] =
    L"Compare \x2192 Check Ballot Counts with\x2026 compares the ballots the Election log shows loaded "
    L"into the EMS with an open Cast Vote Records or Election Results window, in the Ballot "
    L"Reconciliation report. The CVR and results windows' Compare menus offer the same check.\r\n\r\n"
    L"The EMS side is the election session (or the session chosen in Reports \x2192 Select "
    L"Session\x2026): the Number of Ballots of each batch still loaded at its end. These are ballots, "
    L"not ballot sheets.\r\n\r\n"
    L"\x2022 A CVR records one row per ballot sheet (card). For a multi-card election (Dallas County's "
    L"November 2024 ballots had two cards in some styles) its ballots are its first cards: a Hart or "
    L"Dominion CVR numbers each row's sheet or card; for an ES&S CVR a first card is a row with a "
    L"top-of-ballot contest (President, U.S. Senator, Governor) or with most of its ballot style's "
    L"common contests. The report shows the rows, the ballots and the continuation sheets.\r\n"
    L"\x2022 Election results: ballots cast is compared with the EMS ballots. When the results give "
    L"ballots cast by vote type, each vote type is compared; when they give only votes by vote type, "
    L"each vote type is a lower bound \x2014 the EMS ballots of that type must be at least the votes of "
    L"the Vote-For-1 contest with the most votes of that type. The EMS vote type comes from each "
    L"batch's poll place name (Early Vote Center, Vote Center, Mail, Provisional).\r\n\r\n"
    L"Hart Verity logs: the EMS side is the task chosen (the election task unless another is chosen in "
    L"Reports \x2192 Select Task\x2026): the Ballot count of each vDrive read into it. Only totals are "
    L"compared: a central count vDrive names no polling place, so it gives no vote type.\r\n\r\n"
    L"The report also gives the log's print time and newest event, and the results file's timestamp: "
    L"a log printed before the last ballots were loaded (for example before the final canvass) does not "
    L"show them, so its total is short of the results.\r\n\r\n"
    L"Statuses: Match, Differs (red), Lower bound met, Lower bound not met (red), Not reported. The "
    L"report is a snapshot taken when it is opened: open it again after loading other files. Filters "
    L"in the other windows are not applied.";

static const wchar_t k_AudHelpExport[] =
    L"File \x2192 Export\x2026 saves the list as UTF-8 CSV (or TSV, chosen in the Save dialog). In "
    L"the Events view choose:\r\n"
    L"\x2022 Displayed view: the rows and columns shown \x2014 the filter, the log, the copy and the "
    L"sort.\r\n"
    L"\x2022 Reloadable: every event the filter shows, in printed order (newest first), with a Record "
    L"Type column. \"Copy\" records describe each copy (print time, election, file, pages, county and "
    L"election date); each \"Event\" record lists the copies that hold it (\"1-3,5 of 5\"), the first "
    L"of them and its page there. File \x2192 Load Audit Logs\x2026 loads the file back without the "
    L"PDFs, and the merged log, the Copies Check, the ballot accounting and the reports come out as "
    L"before. A filtered file holds only the events it shows. Keep its rows in order if you edit it "
    L"in a spreadsheet.\r\n\r\n"
    L"In the Ballot Batches view, File \x2192 Export\x2026 saves the batches shown, with their session "
    L"as the last column.\r\n\r\n"
    L"Right-click \x2192 Export Selected\x2026 saves the selected rows as displayed. In a report "
    L"window, right-click \x2192 Export Selected\x2026 or Export All\x2026 saves its rows (of the tab "
    L"shown).";

static BOOL Aud_IsAuditWindow(HWND h)
{
    wchar_t cls[64];
    return h != NULL && GetClassNameW(h, cls, ARRAYSIZE(cls)) > 0 && wcscmp(cls, k_AuditClassName) == 0;
}

static const EeAuditLog *Aud_Log(const AuditWindow *aw, int li)
{
    return &aw->el->logs[li];
}

/* The event of row @p row and the copy whose strings it uses. */
static const EeAuditEvent *Aud_RowEvent(const AuditWindow *aw, const AudRow *row, const EeAuditCopy **copy)
{
    const EeAuditLog *log = Aud_Log(aw, row->log);
    if (aw->show_copy != AUD_MERGED)
    {
        *copy = &log->copies[aw->show_copy];
        return &(*copy)->events[row->idx];
    }
    return EeAudit_MergedEvent(log, row->idx, copy);
}

static uint32_t Aud_PopCount(uint64_t v)
{
    uint32_t n = 0;
    while (v != 0)
    {
        v &= v - 1;
        n++;
    }
    return n;
}

/* Copies of its log that hold row @p row's event (merged Events view), else 0. */
static uint32_t Aud_RowCopies(const AuditWindow *aw, const AudRow *row)
{
    return aw->view == AUD_VIEW_EVENTS && aw->show_copy == AUD_MERGED
               ? Aud_PopCount(Aud_Log(aw, row->log)->events[row->idx].copies)
               : 0;
}

/* TRUE when the final copy of its log lacks row @p row's event (merged Events view). */
static BOOL Aud_RowLacking(const AuditWindow *aw, const AudRow *row)
{
    const EeAuditLog *log = Aud_Log(aw, row->log);
    return aw->view == AUD_VIEW_EVENTS && aw->show_copy == AUD_MERGED && log->ncopies > 1 &&
           (log->events[row->idx].copies & (1ull << (log->ncopies - 1))) == 0;
}

/* UTF-8 @p s into @p out, cut to fit. */
static void Aud_Widen(const char *s, wchar_t *out, size_t cch)
{
    int n;
    if (cch == 0)
    {
        return;
    }
    out[0] = L'\0';
    if (s == NULL || *s == '\0')
    {
        return;
    }
    if (MultiByteToWideChar(CP_UTF8, 0, s, -1, out, (int)cch) > 0)
    {
        return;
    }
    n = MultiByteToWideChar(CP_UTF8, 0, s, -1, NULL, 0);
    if (n > 0)
    {
        wchar_t *tmp = (wchar_t *)malloc((size_t)n * sizeof(wchar_t));
        if (tmp != NULL && MultiByteToWideChar(CP_UTF8, 0, s, -1, tmp, n) > 0)
        {
            StringCchCopyW(out, cch, tmp); /* cut to fit */
        }
        free(tmp);
    }
}

static const wchar_t *Aud_LogName(int kind)
{
    return EeAudit_LogName(kind);
}

/* TRUE for Hart Verity logs: the ballot batches are vDrive reads and the sessions are tasks. */
static BOOL Aud_Hart(const AuditWindow *aw)
{
    return aw->have_acct ? aw->acct.hart : EeAudit_IsHart(aw->el);
}

/* View -> Log's item for a log kind. */
static UINT Aud_LogMenuId(int kind)
{
    switch (kind)
    {
    case EE_AUDIT_ELECTION:
        return IDM_AUD_LOG_ELECTION;
    case EE_AUDIT_ADMIN:
        return IDM_AUD_LOG_ADMIN;
    case EE_AUDIT_HART_COUNT:
        return IDM_AUD_LOG_HART_COUNT;
    case EE_AUDIT_HART_CENTRAL:
        return IDM_AUD_LOG_HART_CENTRAL;
    default:
        return IDM_AUD_LOG_HART_DATA;
    }
}

/* The logs loaded (with copies). */
static int Aud_NumLogs(const AuditWindow *aw)
{
    int li, n = 0;
    for (li = 0; li < EE_AUDIT_NLOGS; li++)
    {
        n += aw->el->logs[li].ncopies > 0;
    }
    return n;
}

/* The log the ballot batches come from: "Election" or "VerityCount". */
static const wchar_t *Aud_AcctLogName(const AuditWindow *aw)
{
    return Aud_Hart(aw) ? L"VerityCount" : L"Election";
}

/* @p ts as the report prints it, "11/05/2024 09:25:36PM". */
static void Aud_TimeText(int64_t ts, wchar_t *out, size_t cch)
{
    char t[48];
    EeAudit_FormatTime(ts, t, sizeof(t));
    Aud_Widen(t, out, cch);
}

/* --- Ballot batches (the Election log's accounting) --------------------- */

/* The session shown at first: the election session, else the last one; -1 when the Election
 * log has none. */
static int Aud_DefaultSession(const AuditWindow *aw)
{
    if (!aw->have_acct || aw->acct.nsessions == 0)
    {
        return -1;
    }
    return aw->acct.election_session >= 0 ? aw->acct.election_session : (int)aw->acct.nsessions - 1;
}

/* "Election session", "L&A test", "Other (not test decks)" (a session whose batches are not
 * named as test decks, before the election session) or "No batches". Hart: "Election task",
 * "Test task", "Official task" (another task with no test word) or "No vDrives read". */
static const wchar_t *Aud_SessionKind(const AuditWindow *aw, uint32_t si)
{
    const EeAuditSession *s = &aw->acct.sessions[si];
    if (aw->acct.hart)
    {
        return (int)si == aw->acct.election_session ? L"Election task"
               : s->test                            ? L"Test task"
               : s->nbatches == 0                   ? L"No vDrives read"
                                                    : L"Official task";
    }
    if ((int)si == aw->acct.election_session)
    {
        return L"Election session";
    }
    if (s->nbatches == 0)
    {
        return L"No batches";
    }
    return s->test ? L"L&A test" : L"Other (not test decks)";
}

/* A batch's commit event (Hart: its vDrive read) in the merged log, and the copy holding its
 * strings. */
static const EeAuditEvent *Aud_BatchEvent(const AuditWindow *aw, const EeAuditBatch *b, const EeAuditCopy **copy)
{
    return EeAudit_MergedEvent(Aud_Log(aw, aw->acct.log), b->event, copy);
}

static void Aud_BatchStatus(const AuditWindow *aw, const EeAuditBatch *b, wchar_t *out, size_t cch)
{
    wchar_t st[160];
    if (b->status == EE_AUDIT_BATCH_CLEARED && b->clear < aw->acct.nclears)
    {
        wchar_t t[48];
        Aud_TimeText(aw->acct.clears[b->clear].ts, t, ARRAYSIZE(t));
        StringCchPrintfW(st, ARRAYSIZE(st), L"Removed by media clear at %s", t);
    }
    else if (b->status == EE_AUDIT_BATCH_REPLACED)
    {
        StringCchCopyW(st, ARRAYSIZE(st),
                       aw->acct.hart ? L"Replaced: the vDrive was read into the task again"
                                     : L"Replaced: its batch ID was committed again");
    }
    else
    {
        StringCchCopyW(st, ARRAYSIZE(st), L"Counted");
    }
    if (b->test)
    {
        StringCchPrintfW(out, cch, aw->acct.hart ? L"Test task \x2014 %s" : L"L&A test deck \x2014 %s", st);
    }
    else
    {
        StringCchCopyW(out, cch, st);
    }
}

static void Aud_BatchCellText(const AuditWindow *aw, const EeAuditBatch *b, uint8_t kind, wchar_t *out, size_t cch)
{
    const EeAuditCopy *copy = NULL;
    switch (kind)
    {
    case BC_COMMITTED:
        Aud_TimeText(b->ts, out, cch);
        break;
    case BC_ID:
        if (b->revision > 0)
        {
            StringCchPrintfW(out, cch, L"%u (revision %u)", b->id, b->revision);
        }
        else
        {
            StringCchPrintfW(out, cch, L"%u", b->id);
        }
        break;
    case BC_NAME:
        Aud_Widen(b->name, out, cch);
        break;
    case BC_PLACE:
        Aud_Widen(b->place, out, cch);
        break;
    case BC_MEDIA:
        if (b->device != NULL)
        {
            Aud_Widen(b->device, out, cch); /* Hart: the vDrive ID */
        }
        else if (b->media >= 0)
        {
            StringCchPrintfW(out, cch, b->central ? L"%d (central)" : L"%d", b->media);
        }
        break;
    case BC_VOTETYPE:
        StringCchCopyW(out, cch, EeAudit_VoteTypeName(b->vote_type));
        break;
    case BC_BALLOTS:
        fmt_count_w(b->ballots, out, cch);
        break;
    case BC_USER:
    {
        const EeAuditEvent *ev = Aud_BatchEvent(aw, b, &copy);
        Aud_Widen(EeAudit_Str(copy, ev->user), out, cch);
        break;
    }
    case BC_STATUS:
        Aud_BatchStatus(aw, b, out, cch);
        break;
    default:
        break;
    }
}

static void Aud_CellText(const AuditWindow *aw, uint32_t ri, int col, wchar_t *out, size_t cch)
{
    const AudRow *row;
    const EeAuditCopy *copy = NULL;
    const EeAuditEvent *ev;
    char t[48];
    if (cch == 0)
    {
        return;
    }
    out[0] = L'\0';
    if (ri >= aw->nrows || col < 0 || col >= aw->ncols)
    {
        return;
    }
    row = &aw->rows[ri];
    if (aw->view == AUD_VIEW_BATCHES)
    {
        Aud_BatchCellText(aw, &aw->acct.batches[row->idx], aw->col_kind[col], out, cch);
        return;
    }
    ev = Aud_RowEvent(aw, row, &copy);
    switch (aw->col_kind[col])
    {
    case AC_TIMESTAMP:
        EeAudit_FormatTime(ev->ts, t, sizeof(t));
        Aud_Widen(t, out, cch);
        break;
    case AC_LOG:
        StringCchCopyW(out, cch, Aud_LogName(Aud_Log(aw, row->log)->kind));
        break;
    case AC_EVENT:
        Aud_Widen(EeAudit_Str(copy, ev->level), out, cch);
        break;
    case AC_MODULE:
        Aud_Widen(EeAudit_Str(copy, ev->module), out, cch);
        break;
    case AC_USER:
        Aud_Widen(EeAudit_Str(copy, ev->user), out, cch);
        break;
    case AC_DESC:
        Aud_Widen(EeAudit_Desc(copy, ev), out, cch);
        break;
    case AC_COPIES:
        StringCchPrintfW(out, cch, L"%u of %u", Aud_RowCopies(aw, row), Aud_Log(aw, row->log)->ncopies);
        break;
    default:
        break;
    }
}

/* The UTF-8 text a text column sorts by. */
static const char *Aud_SortText(const AuditWindow *aw, const AudRow *row, uint8_t kind)
{
    const EeAuditCopy *copy = NULL;
    const EeAuditEvent *ev = Aud_RowEvent(aw, row, &copy);
    switch (kind)
    {
    case AC_EVENT:
        return EeAudit_Str(copy, ev->level);
    case AC_MODULE:
        return EeAudit_Str(copy, ev->module);
    case AC_USER:
        return EeAudit_Str(copy, ev->user);
    case AC_DESC:
        return EeAudit_Desc(copy, ev);
    default:
        return "";
    }
}

typedef struct AudSortCtx
{
    const AuditWindow *aw;
    uint8_t kind;
    BOOL asc;
} AudSortCtx;

static int __cdecl Aud_SortCmp(void *ctx, const void *pa, const void *pb)
{
    const AudSortCtx *sc = (const AudSortCtx *)ctx;
    const AudRow *a = (const AudRow *)pa;
    const AudRow *b = (const AudRow *)pb;
    int r = 0;
    switch (sc->kind)
    {
    case AC_TIMESTAMP:
    {
        const EeAuditCopy *ca = NULL, *cb = NULL;
        int64_t x = Aud_RowEvent(sc->aw, a, &ca)->ts;
        int64_t y = Aud_RowEvent(sc->aw, b, &cb)->ts;
        r = x < y ? -1 : (x > y ? 1 : 0);
        if (r == 0)
        {
            /* one second: printed order is newest first */
            r = a->seq > b->seq ? -1 : (a->seq < b->seq ? 1 : 0);
        }
        break;
    }
    case AC_LOG:
        r = (int)a->log - (int)b->log;
        break;
    case AC_COPIES:
    {
        uint32_t x = Aud_RowCopies(sc->aw, a), y = Aud_RowCopies(sc->aw, b);
        r = x < y ? -1 : (x > y ? 1 : 0);
        break;
    }
    default:
        r = _stricmp(Aud_SortText(sc->aw, a, sc->kind), Aud_SortText(sc->aw, b, sc->kind));
        break;
    }
    if (!sc->asc)
    {
        r = -r;
    }
    if (r == 0)
    {
        r = a->seq < b->seq ? -1 : (a->seq > b->seq ? 1 : 0);
    }
    return r;
}

static int Aud_CmpI64(int64_t x, int64_t y)
{
    return x < y ? -1 : (x > y ? 1 : 0);
}

/* Ballot Batches view: numbers and times by value, text A-Z, status by kind then time. */
static int __cdecl Aud_BatchSortCmp(void *ctx, const void *pa, const void *pb)
{
    const AudSortCtx *sc = (const AudSortCtx *)ctx;
    const AudRow *ra = (const AudRow *)pa;
    const AudRow *rb = (const AudRow *)pb;
    const EeAuditBatch *a = &sc->aw->acct.batches[ra->idx];
    const EeAuditBatch *b = &sc->aw->acct.batches[rb->idx];
    int r = 0;
    switch (sc->kind)
    {
    case BC_COMMITTED:
        r = Aud_CmpI64(a->ts, b->ts);
        break;
    case BC_ID:
        r = Aud_CmpI64(a->id, b->id);
        break;
    case BC_NAME:
        r = _stricmp(a->name, b->name);
        break;
    case BC_PLACE:
        r = _stricmp(a->place, b->place);
        break;
    case BC_MEDIA:
        if (a->device != NULL && b->device != NULL)
        {
            r = strcmp(a->device, b->device);
            break;
        }
        r = Aud_CmpI64(a->media, b->media);
        if (r == 0)
        {
            r = (int)a->central - (int)b->central;
        }
        break;
    case BC_VOTETYPE:
        r = a->vote_type - b->vote_type;
        break;
    case BC_BALLOTS:
        r = Aud_CmpI64(a->ballots, b->ballots);
        break;
    case BC_USER:
    {
        const EeAuditCopy *ca = NULL, *cb = NULL;
        const EeAuditEvent *ea = Aud_BatchEvent(sc->aw, a, &ca);
        const EeAuditEvent *eb = Aud_BatchEvent(sc->aw, b, &cb);
        r = _stricmp(EeAudit_Str(ca, ea->user), EeAudit_Str(cb, eb->user));
        break;
    }
    case BC_STATUS:
        r = (int)a->test - (int)b->test;
        if (r == 0)
        {
            r = a->status - b->status;
        }
        if (r == 0 && a->status == EE_AUDIT_BATCH_CLEARED)
        {
            r = Aud_CmpI64(a->clear, b->clear);
        }
        break;
    default:
        break;
    }
    if (!sc->asc)
    {
        r = -r;
    }
    if (r == 0)
    {
        r = ra->seq < rb->seq ? -1 : (ra->seq > rb->seq ? 1 : 0);
    }
    return r;
}

/* --- Filter: matching --------------------------------------------------- */

/* A typed time: "MM/DD/YYYY", optionally followed by "hh:mm[:ss]" and AM / PM (24-hour without),
 * or "YYYY-MM-DD[ hh:mm[:ss]]". FALSE when it is not one. */
static BOOL Aud_ParseTime(const wchar_t *s, int64_t *out)
{
    unsigned y = 0, mo = 0, d = 0, h = 0, mi = 0, sec = 0;
    int used = 0, n;
    const wchar_t *p;
    while (*s == L' ')
    {
        s++;
    }
    if (swscanf_s(s, L"%4u-%2u-%2u%n", &y, &mo, &d, &used) == 3 ||
        swscanf_s(s, L"%2u/%2u/%4u%n", &mo, &d, &y, &used) == 3)
    {
        p = s + used;
    }
    else
    {
        return FALSE;
    }
    if (y < 1900 || y > 2199 || mo < 1 || mo > 12 || d < 1 || d > 31)
    {
        return FALSE;
    }
    while (*p == L' ')
    {
        p++;
    }
    if (*p != L'\0')
    {
        used = 0;
        n = swscanf_s(p, L"%2u:%2u%n", &h, &mi, &used);
        if (n != 2)
        {
            return FALSE;
        }
        p += used;
        if (*p == L':')
        {
            used = 0;
            if (swscanf_s(p + 1, L"%2u%n", &sec, &used) != 1)
            {
                return FALSE;
            }
            p += 1 + used;
        }
        while (*p == L' ')
        {
            p++;
        }
        if ((p[0] == L'A' || p[0] == L'a' || p[0] == L'P' || p[0] == L'p') && (p[1] == L'M' || p[1] == L'm'))
        {
            if (h < 1 || h > 12)
            {
                return FALSE;
            }
            h = (h % 12) + ((p[0] == L'P' || p[0] == L'p') ? 12u : 0u);
            p += 2;
        }
        while (*p == L' ')
        {
            p++;
        }
        if (*p != L'\0' || h > 23 || mi > 59 || sec > 59)
        {
            return FALSE;
        }
    }
    *out = EeAudit_MakeTime((int)y, (int)mo, (int)d, (int)h, (int)mi, (int)sec);
    return TRUE;
}

static char Aud_Lower(char c)
{
    return (c >= 'A' && c <= 'Z') ? (char)(c - 'A' + 'a') : c;
}

/* @p needle in @p hay, ASCII letters compared without case. */
static BOOL Aud_ContainsI(const char *hay, const char *needle)
{
    size_t i, k;
    if (needle[0] == '\0')
    {
        return TRUE;
    }
    for (i = 0; hay[i] != '\0'; i++)
    {
        for (k = 0; needle[k] != '\0' && hay[i + k] != '\0' && Aud_Lower(hay[i + k]) == Aud_Lower(needle[k]); k++)
        {
        }
        if (needle[k] == '\0')
        {
            return TRUE;
        }
    }
    return FALSE;
}

static BOOL Aud_RuleMatches(const AudRule *r, const char *text, int64_t ts)
{
    switch (r->relation)
    {
    case EeRel_IsNot:
        return _stricmp(text, r->u8) != 0;
    case EeRel_Contains:
        return Aud_ContainsI(text, r->u8);
    case EeRel_Excludes:
        return !Aud_ContainsI(text, r->u8);
    case EeRel_MoreThan: /* on or after */
        return ts >= r->ts;
    case EeRel_LessThan: /* before */
        return ts < r->ts;
    default:
        return _stricmp(text, r->u8) == 0;
    }
}

/* A row's fields (@p text indexed by AF_*; NULL = not in this view) pass the rules: a matching
 * Exclude hides it; Include rules on one column are alternatives, except Timestamp rules,
 * which all apply (so "on or after" and "before" give a range); columns all apply. */
static BOOL Aud_FieldsPass(const AuditWindow *aw, const char *const *text, int64_t ts)
{
    BOOL any_inc[AF_COUNT] = {FALSE}, inc[AF_COUNT] = {FALSE};
    uint32_t i;
    int c;
    for (i = 0; i < aw->nrules; i++)
    {
        const AudRule *r = &aw->rules[i];
        BOOL m;
        if (r->column >= AF_COUNT || text[r->column] == NULL)
        {
            continue; /* a rule for the other view */
        }
        m = Aud_RuleMatches(r, text[r->column], ts);
        if (r->action == EeFilt_Exclude)
        {
            if (m)
            {
                return FALSE;
            }
        }
        else if (r->column == AF_TIMESTAMP)
        {
            if (!m)
            {
                return FALSE;
            }
        }
        else
        {
            any_inc[r->column] = TRUE;
            inc[r->column] = inc[r->column] || m;
        }
    }
    for (c = 0; c < AF_COUNT; c++)
    {
        if (any_inc[c] && !inc[c])
        {
            return FALSE;
        }
    }
    return TRUE;
}

static BOOL Aud_EventPasses(const AuditWindow *aw, int li, const EeAuditCopy *copy, const EeAuditEvent *ev)
{
    const char *text[AF_COUNT];
    text[AF_TIMESTAMP] = "";
    text[AF_LOG] = EeAudit_LogNameA(Aud_Log(aw, li)->kind);
    text[AF_EVENT] = EeAudit_Str(copy, ev->level);
    text[AF_MODULE] = EeAudit_Str(copy, ev->module);
    text[AF_USER] = EeAudit_Str(copy, ev->user);
    text[AF_DESC] = EeAudit_Desc(copy, ev);
    text[AF_VOTETYPE] = NULL;
    return Aud_FieldsPass(aw, text, ev->ts);
}

/* A batch passes by its commit event's fields and its vote type. */
static BOOL Aud_BatchPasses(const AuditWindow *aw, const EeAuditBatch *b)
{
    static const char *const k_vt[EE_AUDIT_VT_COUNT] = {"Other", "Early voting", "Election Day", "Mail",
                                                        "Provisional"};
    const EeAuditCopy *copy = NULL;
    const EeAuditEvent *ev = Aud_BatchEvent(aw, b, &copy);
    const char *text[AF_COUNT];
    text[AF_TIMESTAMP] = "";
    text[AF_LOG] = EeAudit_LogNameA(aw->acct.log + 1);
    text[AF_EVENT] = EeAudit_Str(copy, ev->level);
    text[AF_MODULE] = EeAudit_Str(copy, ev->module);
    text[AF_USER] = EeAudit_Str(copy, ev->user);
    text[AF_DESC] = EeAudit_Desc(copy, ev);
    text[AF_VOTETYPE] = (b->vote_type >= 0 && b->vote_type < EE_AUDIT_VT_COUNT) ? k_vt[b->vote_type] : "Other";
    return Aud_FieldsPass(aw, text, b->ts);
}

/* Compile the enabled rules of aw->filters into aw->rules. */
static BOOL Aud_CompileFilter(AuditWindow *aw)
{
    uint32_t i;
    free(aw->rules);
    aw->rules = NULL;
    aw->nrules = 0;
    aw->filt_active = EeFilter_HasEnabled(&aw->filters);
    if (!aw->filt_active)
    {
        return TRUE;
    }
    aw->rules = (AudRule *)calloc(aw->filters.count, sizeof(AudRule));
    if (aw->rules == NULL)
    {
        aw->filt_active = FALSE;
        return FALSE;
    }
    for (i = 0; i < aw->filters.count; i++)
    {
        const EeFilterRule *f = &aw->filters.rules[i];
        AudRule *r = &aw->rules[aw->nrules];
        if (!f->enabled || f->column >= AF_COUNT)
        {
            continue;
        }
        r->column = f->column;
        r->relation = f->relation;
        r->action = f->action;
        if (f->column == AF_TIMESTAMP)
        {
            if (!Aud_ParseTime(f->value, &r->ts))
            {
                continue; /* refused when added; a loaded file's bad time is skipped */
            }
        }
        else if (WideCharToMultiByte(CP_UTF8, 0, f->value, -1, r->u8, (int)sizeof(r->u8), NULL, NULL) <= 0)
        {
            continue;
        }
        aw->nrules++;
    }
    return TRUE;
}

static int __cdecl Aud_SeqCmp(const void *pa, const void *pb)
{
    const AudRow *a = (const AudRow *)pa;
    const AudRow *b = (const AudRow *)pb;
    return a->seq < b->seq ? -1 : (a->seq > b->seq ? 1 : 0);
}

static void Aud_Sort(AuditWindow *aw)
{
    if (aw->nrows < 2)
    {
        return;
    }
    if (aw->sort_col < 0 || aw->sort_col >= aw->ncols)
    {
        qsort(aw->rows, aw->nrows, sizeof(AudRow), Aud_SeqCmp);
    }
    else
    {
        AudSortCtx sc;
        sc.aw = aw;
        sc.kind = aw->col_kind[aw->sort_col];
        sc.asc = aw->sort_asc;
        qsort_s(aw->rows, aw->nrows, sizeof(AudRow), aw->view == AUD_VIEW_BATCHES ? Aud_BatchSortCmp : Aud_SortCmp,
                &sc);
    }
}

/* The rows of the view in printed order: the session's batches in load order, one copy as
 * printed, or the merged events of the logs shown, the logs interleaved newest first. */
static BOOL Aud_BuildRows(AuditWindow *aw)
{
    uint32_t n = 0, cap;
    free(aw->rows);
    aw->rows = NULL;
    aw->nrows = 0;
    aw->nrows_all = 0;
    if (aw->view == AUD_VIEW_BATCHES)
    {
        const EeAuditSession *s;
        uint32_t i;
        if (aw->session < 0 || (uint32_t)aw->session >= aw->acct.nsessions)
        {
            return TRUE;
        }
        s = &aw->acct.sessions[aw->session];
        aw->rows = (AudRow *)malloc(((size_t)s->nbatches + 1) * sizeof(AudRow));
        if (aw->rows == NULL)
        {
            return FALSE;
        }
        for (i = 0; i < s->nbatches; i++)
        {
            aw->rows[n].idx = s->first_batch + i;
            aw->rows[n].log = (uint8_t)aw->acct.log;
            aw->rows[n].seq = n;
            n++;
        }
    }
    else if (aw->show_copy != AUD_MERGED)
    {
        const EeAuditCopy *c = &Aud_Log(aw, aw->show_copy_log)->copies[aw->show_copy];
        uint32_t i;
        cap = c->nevents;
        aw->rows = (AudRow *)malloc(((size_t)cap + 1) * sizeof(AudRow));
        if (aw->rows == NULL)
        {
            return FALSE;
        }
        for (i = 0; i < c->nevents; i++)
        {
            aw->rows[n].idx = i;
            aw->rows[n].log = (uint8_t)aw->show_copy_log;
            aw->rows[n].seq = n;
            n++;
        }
    }
    else
    {
        uint32_t next[EE_AUDIT_NLOGS], ne[EE_AUDIT_NLOGS];
        int li;
        cap = 0;
        for (li = 0; li < EE_AUDIT_NLOGS; li++)
        {
            const EeAuditLog *log = Aud_Log(aw, li);
            next[li] = 0;
            ne[li] = aw->show_log == 0 || aw->show_log == log->kind ? log->nevents : 0;
            cap += ne[li];
        }
        aw->rows = (AudRow *)malloc(((size_t)cap + 1) * sizeof(AudRow));
        if (aw->rows == NULL)
        {
            return FALSE;
        }
        for (;;)
        {
            /* the newest next event of the logs (the lower log on a tie) */
            int take = -1;
            int64_t best = 0;
            for (li = 0; li < EE_AUDIT_NLOGS; li++)
            {
                if (next[li] < ne[li])
                {
                    const EeAuditCopy *c = NULL;
                    int64_t ts = EeAudit_MergedEvent(Aud_Log(aw, li), next[li], &c)->ts;
                    if (take < 0 || ts > best)
                    {
                        take = li;
                        best = ts;
                    }
                }
            }
            if (take < 0)
            {
                break;
            }
            aw->rows[n].idx = next[take]++;
            aw->rows[n].log = (uint8_t)take;
            aw->rows[n].seq = n;
            n++;
        }
    }
    aw->nrows_all = n;
    if (aw->filt_active)
    {
        /* keep the rows that pass the filter, in order */
        uint32_t i, k = 0;
        for (i = 0; i < n; i++)
        {
            const AudRow *row = &aw->rows[i];
            BOOL pass;
            if (aw->view == AUD_VIEW_BATCHES)
            {
                pass = Aud_BatchPasses(aw, &aw->acct.batches[row->idx]);
            }
            else
            {
                const EeAuditCopy *copy = NULL;
                const EeAuditEvent *ev = Aud_RowEvent(aw, row, &copy);
                pass = Aud_EventPasses(aw, row->log, copy, ev);
            }
            if (pass)
            {
                aw->rows[k++] = *row;
            }
        }
        n = k;
    }
    aw->nrows = n;
    return TRUE;
}

typedef struct AudColDef
{
    uint8_t kind;
    const wchar_t *title;
    int width;
    int fmt;
} AudColDef;

static void Aud_BuildColumns(AuditWindow *aw)
{
    static const AudColDef k_events[AC_COUNT] = {
        {AC_TIMESTAMP, L"Timestamp", 150, LVCFMT_LEFT}, {AC_LOG, L"Log", 64, LVCFMT_LEFT},
        {AC_EVENT, L"Event", 64, LVCFMT_LEFT},          {AC_MODULE, L"Module", 140, LVCFMT_LEFT},
        {AC_USER, L"User Id", 110, LVCFMT_LEFT},        {AC_DESC, L"Description", 480, LVCFMT_LEFT},
        {AC_COPIES, L"Copies", 70, LVCFMT_LEFT}};
    static const AudColDef k_batches[BC_END - BC_COMMITTED] = {
        {BC_COMMITTED, L"Committed", 150, LVCFMT_LEFT}, {BC_ID, L"Batch ID", 70, LVCFMT_RIGHT},
        {BC_NAME, L"Batch Name", 220, LVCFMT_LEFT},     {BC_PLACE, L"Poll Place", 180, LVCFMT_LEFT},
        {BC_MEDIA, L"Media", 80, LVCFMT_RIGHT},         {BC_VOTETYPE, L"Vote Type", 96, LVCFMT_LEFT},
        {BC_BALLOTS, L"Ballots", 70, LVCFMT_RIGHT},     {BC_USER, L"User Id", 100, LVCFMT_LEFT},
        {BC_STATUS, L"Status", 260, LVCFMT_LEFT}};
    /* Hart: the Event Data, the Device ID; a batch is a vDrive read */
    static const AudColDef k_hart_events[AC_COUNT] = {
        {AC_TIMESTAMP, L"Log Date & Time", 150, LVCFMT_LEFT}, {AC_LOG, L"Log", 96, LVCFMT_LEFT},
        {AC_EVENT, L"Event", 170, LVCFMT_LEFT},               {AC_MODULE, L"Device ID", 100, LVCFMT_LEFT},
        {AC_USER, L"User", 90, LVCFMT_LEFT},                  {AC_DESC, L"Event Data", 520, LVCFMT_LEFT},
        {AC_COPIES, L"Copies", 70, LVCFMT_LEFT}};
    static const AudColDef k_hart_batches[] = {
        {BC_COMMITTED, L"Read", 150, LVCFMT_LEFT},       {BC_ID, L"Read No.", 64, LVCFMT_RIGHT},
        {BC_PLACE, L"Polling Place", 220, LVCFMT_LEFT},  {BC_MEDIA, L"vDrive ID", 190, LVCFMT_LEFT},
        {BC_VOTETYPE, L"Vote Type", 96, LVCFMT_LEFT},    {BC_BALLOTS, L"Ballots", 70, LVCFMT_RIGHT},
        {BC_USER, L"User", 100, LVCFMT_LEFT},            {BC_STATUS, L"Status", 260, LVCFMT_LEFT}};
    BOOL hart = Aud_Hart(aw);
    const AudColDef *defs = aw->view == AUD_VIEW_BATCHES ? (hart ? k_hart_batches : k_batches)
                            : hart                       ? k_hart_events
                                                         : k_events;
    int ndefs = aw->view == AUD_VIEW_BATCHES ? (hart ? (int)ARRAYSIZE(k_hart_batches) : (int)ARRAYSIZE(k_batches))
                                             : (int)ARRAYSIZE(k_events);
    HWND header;
    int i, n = 0;
    if (aw->list == NULL)
    {
        return;
    }
    header = ListView_GetHeader(aw->list);
    while (header != NULL && Header_GetItemCount(header) > 0)
    {
        ListView_DeleteColumn(aw->list, 0);
    }
    for (i = 0; i < ndefs; i++)
    {
        LVCOLUMNW col;
        if (defs[i].kind == AC_COPIES && aw->show_copy != AUD_MERGED)
        {
            continue; /* one copy as printed */
        }
        if (defs[i].kind == AC_LOG && (aw->show_copy != AUD_MERGED || aw->show_log != 0 || Aud_NumLogs(aw) < 2))
        {
            continue; /* one log */
        }
        aw->col_kind[n] = defs[i].kind;
        ZeroMemory(&col, sizeof(col));
        col.mask = LVCF_TEXT | LVCF_WIDTH | LVCF_FMT;
        col.fmt = n == 0 ? LVCFMT_LEFT : defs[i].fmt; /* column 0 is always left-aligned */
        col.pszText = (wchar_t *)defs[i].title;
        col.cx = ScaleDisplay(aw->app, defs[i].width);
        ListView_InsertColumn(aw->list, n, &col);
        n++;
    }
    aw->ncols = n;
}

/* "05/26/2026" from yyyymmdd. */
static void Aud_FormatDate(int yyyymmdd, wchar_t *out, size_t cch)
{
    if (yyyymmdd <= 0)
    {
        StringCchCopyW(out, cch, L"date unknown");
        return;
    }
    StringCchPrintfW(out, cch, L"%02d/%02d/%04d", yyyymmdd / 100 % 100, yyyymmdd % 100, yyyymmdd / 10000);
}

/* The window's election, as "Travis County — 2026 Primary Runoff (05/26/2026)". */
static void Aud_ElectionLabel(const AuditWindow *aw, wchar_t *out, size_t cch)
{
    wchar_t cust[128], name[160], date[32];
    Aud_Widen(aw->el->customer, cust, ARRAYSIZE(cust));
    Aud_Widen(aw->el->election, name, ARRAYSIZE(name));
    Aud_FormatDate(aw->el->election_date, date, ARRAYSIZE(date));
    StringCchPrintfW(out, cch, L"%s%s%s (%s)", cust, cust[0] != L'\0' && name[0] != L'\0' ? L" \x2014 " : L"",
                     name, date);
}

/* A copy's label for the View -> Copy menu and the status bar: "Election log, copy 2 of 5,
 * printed 05/27/2026 09:15AM (file.pdf)". */
static void Aud_CopyLabel(const AuditWindow *aw, int li, uint32_t ci, wchar_t *out, size_t cch)
{
    const EeAuditLog *log = Aud_Log(aw, li);
    const EeAuditCopy *c = &log->copies[ci];
    wchar_t printed[48] = L"print time unknown";
    if (c->printed != 0)
    {
        char t[48];
        wchar_t w[48];
        EeAudit_FormatTime(c->printed, t, sizeof(t));
        Aud_Widen(t, w, ARRAYSIZE(w));
        StringCchPrintfW(printed, ARRAYSIZE(printed), L"printed %s", w);
    }
    StringCchPrintfW(out, cch, L"%s log, copy %u of %u, %s (%s)", Aud_LogName(log->kind), ci + 1, log->ncopies,
                     printed, App_PathBaseName(c->path));
}

/* "Session 14 of 16 (election session), 11/02/2024 11:27:24AM to 11/22/2024 09:51:26AM";
 * Hart: "Task 1 of 4, Official 2026 Primary (election task), 03/03/2026 ... to ...". */
static void Aud_SessionLabel(const AuditWindow *aw, uint32_t si, wchar_t *out, size_t cch)
{
    const EeAuditSession *s = &aw->acct.sessions[si];
    wchar_t t0[48], t1[48];
    Aud_TimeText(s->start, t0, ARRAYSIZE(t0));
    Aud_TimeText(s->end, t1, ARRAYSIZE(t1));
    if (aw->acct.hart)
    {
        wchar_t name[128];
        Aud_Widen(s->name, name, ARRAYSIZE(name));
        StringCchPrintfW(out, cch, L"Task %u of %u, %s (%s), %s to %s", si + 1, aw->acct.nsessions, name,
                         Aud_SessionKind(aw, si), t0, t1);
        return;
    }
    StringCchPrintfW(out, cch, L"Session %u of %u (%s), %s to %s", si + 1, aw->acct.nsessions,
                     Aud_SessionKind(aw, si), t0, t1);
}

/* Ballot Batches view: the session shown and its totals. */
static void Aud_UpdateBatchStatus(AuditWindow *aw)
{
    wchar_t buf[900], n1[32], n2[32], n3[32];
    const EeAuditSession *s;
    BOOL hart = Aud_Hart(aw);
    if (!aw->have_acct || Aud_Log(aw, aw->acct.log)->ncopies == 0)
    {
        StringCchCopyW(buf, ARRAYSIZE(buf),
                       hart ? L"No VerityCount log is loaded, so there are no vDrive reads."
                            : L"No Election log is loaded, so there are no ballot batches.");
    }
    else if (aw->session < 0)
    {
        StringCchCopyW(buf, ARRAYSIZE(buf),
                       hart ? L"The VerityCount log shows no vDrive reads." : L"The Election log shows no ballot batches.");
    }
    else
    {
        s = &aw->acct.sessions[aw->session];
        /* the session's times are in Ballot Accounting, so the rest fits */
        if (hart)
        {
            wchar_t name[128];
            Aud_Widen(s->name, name, ARRAYSIZE(name));
            StringCchPrintfW(buf, ARRAYSIZE(buf), L"Task %d of %u, %s (%s)", aw->session + 1, aw->acct.nsessions, name,
                             Aud_SessionKind(aw, (uint32_t)aw->session));
        }
        else
        {
            StringCchPrintfW(buf, ARRAYSIZE(buf), L"Session %d of %u (%s)", aw->session + 1, aw->acct.nsessions,
                             Aud_SessionKind(aw, (uint32_t)aw->session));
        }
        fmt_count_w((int64_t)s->nbatches, n1, ARRAYSIZE(n1));
        fmt_count_w((int64_t)s->counted, n2, ARRAYSIZE(n2));
        fmt_count_w(s->ballots, n3, ARRAYSIZE(n3));
        StringCchCatW(buf, ARRAYSIZE(buf), L"  \x2022  ");
        StringCchCatW(buf, ARRAYSIZE(buf), n1);
        StringCchCatW(buf, ARRAYSIZE(buf), hart ? L" vDrive reads, " : L" batches, ");
        StringCchCatW(buf, ARRAYSIZE(buf), n2);
        StringCchCatW(buf, ARRAYSIZE(buf), hart ? L" counted  \x2022  ballots: " : L" counted  \x2022  EMS ballots: ");
        StringCchCatW(buf, ARRAYSIZE(buf), n3);
        if (s->nclears > 0)
        {
            StringCchPrintfW(n1, ARRAYSIZE(n1), L"  \x2022  %u media clear%s", s->nclears, s->nclears == 1 ? L"" : L"s");
            StringCchCatW(buf, ARRAYSIZE(buf), n1);
        }
        if (aw->filt_active)
        {
            fmt_count_w((int64_t)aw->nrows, n1, ARRAYSIZE(n1));
            StringCchCatW(buf, ARRAYSIZE(buf), L"  \x2022  filtered: ");
            StringCchCatW(buf, ARRAYSIZE(buf), n1);
            StringCchCatW(buf, ARRAYSIZE(buf), L" shown");
        }
    }
    SendMessageW(aw->status, SB_SETTEXTW, 0, (LPARAM)buf);
}

static void Aud_UpdateStatus(AuditWindow *aw)
{
    wchar_t buf[900];
    wchar_t part[320];
    wchar_t n1[32], n2[32];
    uint32_t total = 0;
    int li;
    if (aw->status == NULL)
    {
        return;
    }
    if (aw->view == AUD_VIEW_BATCHES)
    {
        Aud_UpdateBatchStatus(aw);
        return;
    }
    buf[0] = L'\0'; /* the county, election and date are in the title bar */
    if (aw->have_notes && aw->notes.per_category[EE_AUDIT_N_RESULTS_EARLY] > 0)
    {
        /* first, so it is never cut off */
        StringCchPrintfW(part, ARRAYSIZE(part),
                         L"  \x2022  RED FLAG: %u results output%s before Election Day (Reports \x2192 Notable Events)",
                         aw->notes.per_category[EE_AUDIT_N_RESULTS_EARLY],
                         aw->notes.per_category[EE_AUDIT_N_RESULTS_EARLY] == 1 ? L"" : L"s");
        StringCchCatW(buf, ARRAYSIZE(buf), part);
    }
    for (li = 0; li < EE_AUDIT_NLOGS; li++)
    {
        const EeAuditLog *log = Aud_Log(aw, li);
        if (log->ncopies == 0)
        {
            continue;
        }
        total += log->nevents;
        StringCchPrintfW(part, ARRAYSIZE(part), L"  \x2022  %s log: %u cop%s", Aud_LogName(log->kind),
                         log->ncopies, log->ncopies == 1 ? L"y" : L"ies");
        StringCchCatW(buf, ARRAYSIZE(buf), part);
    }
    if (aw->show_copy != AUD_MERGED)
    {
        const EeAuditLog *log = Aud_Log(aw, aw->show_copy_log);
        fmt_count_w((int64_t)aw->nrows, n1, ARRAYSIZE(n1));
        fmt_count_w((int64_t)aw->nrows_all, n2, ARRAYSIZE(n2));
        StringCchPrintfW(part, ARRAYSIZE(part), L"  \x2022  showing %s log copy %d of %u as printed: %s of %s events",
                         Aud_LogName(log->kind), aw->show_copy + 1, log->ncopies, n1, n2);
    }
    else
    {
        fmt_count_w((int64_t)aw->nrows, n1, ARRAYSIZE(n1));
        fmt_count_w((int64_t)total, n2, ARRAYSIZE(n2));
        StringCchPrintfW(part, ARRAYSIZE(part), L"  \x2022  %s of %s events", n1, n2);
    }
    StringCchCatW(buf, ARRAYSIZE(buf), part);
    if (aw->filt_active)
    {
        StringCchCatW(buf, ARRAYSIZE(buf), L" (filtered)");
    }
    if (aw->have_acct && aw->acct.election_session >= 0)
    {
        fmt_count_w(aw->acct.sessions[aw->acct.election_session].ballots, n1, ARRAYSIZE(n1));
        StringCchPrintfW(part, ARRAYSIZE(part),
                         aw->acct.hart ? L"  \x2022  ballots (election task): %s" : L"  \x2022  EMS ballots (election session): %s",
                         n1);
        StringCchCatW(buf, ARRAYSIZE(buf), part);
    }
    for (li = 0; li < EE_AUDIT_NLOGS; li++)
    {
        if (aw->lacking[li] > 0)
        {
            const EeAuditLog *log = Aud_Log(aw, li);
            uint32_t outside = log->checks[log->ncopies - 1].lacks_outside;
            fmt_count_w((int64_t)aw->lacking[li], n1, ARRAYSIZE(n1));
            StringCchPrintfW(part, ARRAYSIZE(part), L"  \x2022  final %s log copy lacks %s event%s",
                             Aud_LogName(log->kind), n1, aw->lacking[li] == 1 ? L"" : L"s");
            StringCchCatW(buf, ARRAYSIZE(buf), part);
            if (outside > 0)
            {
                fmt_count_w((int64_t)outside, n2, ARRAYSIZE(n2));
                StringCchPrintfW(part, ARRAYSIZE(part), L" (%s outside its date range)", n2);
                StringCchCatW(buf, ARRAYSIZE(buf), part);
            }
        }
    }
    /* every part starts with a separator: drop the first */
    SendMessageW(aw->status, SB_SETTEXTW, 0, (LPARAM)(wcsncmp(buf, L"  \x2022  ", 5) == 0 ? buf + 5 : buf));
}

/* Status bar part 2: where the focused event comes from. */
static void Aud_UpdateSelectionStatus(AuditWindow *aw)
{
    wchar_t buf[400] = L"";
    int item;
    if (aw->status == NULL)
    {
        return;
    }
    item = ListView_GetNextItem(aw->list, -1, LVNI_FOCUSED);
    if (item >= 0 && (uint32_t)item < aw->nrows && aw->view == AUD_VIEW_BATCHES)
    {
        const EeAuditBatch *b = &aw->acct.batches[aw->rows[item].idx];
        const EeAuditCopy *copy = NULL;
        const EeAuditEvent *ev = Aud_BatchEvent(aw, b, &copy);
        const EeAuditLog *log = Aud_Log(aw, aw->acct.log);
        StringCchPrintfW(buf, ARRAYSIZE(buf), aw->acct.hart ? L"Read first in copy %u of %u: %s, page %u"
                                                            : L"Commit first in copy %u of %u: %s, page %u",
                         log->events[b->event].copy + 1, log->ncopies, App_PathBaseName(copy->path), ev->page);
    }
    else if (item >= 0 && (uint32_t)item < aw->nrows)
    {
        const AudRow *row = &aw->rows[item];
        const EeAuditCopy *copy = NULL;
        const EeAuditEvent *ev = Aud_RowEvent(aw, row, &copy);
        if (aw->show_copy != AUD_MERGED)
        {
            StringCchPrintfW(buf, ARRAYSIZE(buf), L"Page %u", ev->page);
        }
        else
        {
            const EeAuditLog *log = Aud_Log(aw, row->log);
            uint32_t ci = log->events[row->idx].copy;
            StringCchPrintfW(buf, ARRAYSIZE(buf), L"First in copy %u of %u: %s, page %u", ci + 1, log->ncopies,
                             App_PathBaseName(copy->path), ev->page);
        }
    }
    SendMessageW(aw->status, SB_SETTEXTW, 1, (LPARAM)buf);
}

static void Aud_Layout(AuditWindow *aw, int width, int height)
{
    int sb_h = 0;
    if (aw->status != NULL)
    {
        RECT sb;
        int parts[2];
        parts[0] = width > Scale(aw->app, 600) ? width - Scale(aw->app, 300) : width / 2;
        parts[1] = -1;
        SendMessageW(aw->status, SB_SETPARTS, 2, (LPARAM)parts);
        SendMessageW(aw->status, WM_SIZE, 0, 0);
        if (GetWindowRect(aw->status, &sb))
        {
            sb_h = sb.bottom - sb.top;
        }
    }
    if (aw->list != NULL)
    {
        MoveWindow(aw->list, 0, 0, width, height > sb_h ? height - sb_h : 0, TRUE);
    }
}

/* Rebuild rows and columns after a view or option change; the sort is kept while its
 * column is still shown. */
static void Aud_Rebuild(AuditWindow *aw)
{
    uint8_t sort_kind = (aw->sort_col >= 0 && aw->sort_col < aw->ncols) ? aw->col_kind[aw->sort_col] : 0xFF;
    int i;
    if (!Aud_BuildRows(aw))
    {
        MessageBoxW(aw->hwnd, L"Out of memory.", k_WindowTitle, MB_ICONERROR | MB_OK);
        aw->nrows = 0;
    }
    SendMessageW(aw->list, WM_SETREDRAW, FALSE, 0);
    ListView_SetItemState(aw->list, -1, 0, LVIS_SELECTED | LVIS_FOCUSED);
    Aud_BuildColumns(aw);
    aw->sort_col = -1;
    for (i = 0; i < aw->ncols && sort_kind != 0xFF; i++)
    {
        if (aw->col_kind[i] == sort_kind)
        {
            aw->sort_col = i;
            break;
        }
    }
    Aud_Sort(aw);
    ListView_SetItemCountEx(aw->list, (int)aw->nrows, 0);
    SendMessageW(aw->list, WM_SETREDRAW, TRUE, 0);
    InvalidateRect(aw->list, NULL, TRUE);
    Aud_UpdateStatus(aw);
    Aud_UpdateSelectionStatus(aw);
}

/* Copy the selected rows as tab-separated UTF-8, every column, CR/LF between rows. */
static void Aud_CopySelected(AuditWindow *aw)
{
    wchar_t *buf = NULL;
    size_t cap = 0, len = 0;
    wchar_t cell[4096];
    int i;
    if (aw == NULL || aw->list == NULL)
    {
        return;
    }
    for (i = ListView_GetNextItem(aw->list, -1, LVNI_SELECTED); i >= 0;
         i = ListView_GetNextItem(aw->list, i, LVNI_SELECTED))
    {
        int c;
        for (c = 0; c < aw->ncols; c++)
        {
            size_t n;
            Aud_CellText(aw, (uint32_t)i, c, cell, ARRAYSIZE(cell));
            n = wcslen(cell);
            if (!cvr_wbuf_reserve(&buf, &cap, len + n + 3))
            {
                free(buf);
                return;
            }
            memcpy(buf + len, cell, n * sizeof(wchar_t));
            len += n;
            if (c + 1 < aw->ncols)
            {
                buf[len++] = L'\t';
            }
            else
            {
                buf[len++] = L'\r';
                buf[len++] = L'\n';
            }
        }
    }
    if (buf != NULL && len > 0)
    {
        int u8len;
        buf[len] = L'\0';
        u8len = WideCharToMultiByte(CP_UTF8, 0, buf, -1, NULL, 0, NULL, NULL);
        if (u8len > 0)
        {
            char *utf8 = (char *)malloc((size_t)u8len);
            if (utf8 != NULL && WideCharToMultiByte(CP_UTF8, 0, buf, -1, utf8, u8len, NULL, NULL) > 0)
            {
                App_SetClipboardUtf8(aw->hwnd, utf8);
            }
            free(utf8);
        }
    }
    free(buf);
}

/* Export the view: the selected rows, or all of them after asking (Events view) for the
 * displayed view or the reloadable export. The Ballot Batches view adds the session. */
static void App_ExportAudit(AuditWindow *aw, BOOL selection_only)
{
    static BOOL s_reload = FALSE; /* the last layout chosen in this session */
    wchar_t suggested[300], path[MAX_PATH];
    char delim = ',';
    uint32_t *idx = NULL;
    uint32_t n = aw->nrows, i;
    BOOL reload = FALSE, batches = aw->view == AUD_VIEW_BATCHES, ok = TRUE;
    char *buf = NULL;
    size_t len = 0;
    HCURSOR old;
    if (selection_only)
    {
        idx = App_CollectSelectedIndices(aw->list, aw->nrows, &n);
        if (n == 0)
        {
            free(idx);
            return;
        }
    }
    else if (!batches)
    {
        ResExportOpt opt;
        ZeroMemory(&opt, sizeof(opt));
        opt.app = aw->app;
        opt.detail = s_reload;
        opt.prompt = L"Export the events as:";
        opt.view_text = L"Displayed view: the rows and columns shown (filter, log, copy, sort)";
        opt.detail_text = L"Reloadable: every column, in printed order (loads back as audit logs)";
        if (App_RunModalDialogOwned(aw->app, aw->hwnd, L"Export Audit Log", ResExportDlgProc, (LPARAM)&opt) != 1)
        {
            return;
        }
        s_reload = reload = opt.detail;
    }
    StringCchPrintfW(suggested, ARRAYSIZE(suggested), L"%s-%s%s", aw->base_name,
                     selection_only ? L"Selected_" : (aw->filt_active ? L"Filtered_" : L""),
                     batches ? L"Ballot_Batches" : (reload ? L"Events_Reloadable" : L"Events"));
    if (!App_PromptExportPath(aw->hwnd, suggested, path, ARRAYSIZE(path), &delim))
    {
        free(idx);
        return;
    }
    old = SetCursor(LoadCursorW(NULL, IDC_WAIT));
    if (reload)
    {
        /* in printed order, whatever the sort */
        AudRow *ord = (AudRow *)malloc(((size_t)n + 1) * sizeof(AudRow));
        EeAuditExportRow *rows = (EeAuditExportRow *)malloc(((size_t)n + 1) * sizeof(EeAuditExportRow));
        ok = ord != NULL && rows != NULL;
        if (ok)
        {
            memcpy(ord, aw->rows, (size_t)n * sizeof(AudRow));
            qsort(ord, n, sizeof(AudRow), Aud_SeqCmp);
            for (i = 0; i < n; i++)
            {
                rows[i].log = ord[i].log;
                rows[i].index = ord[i].idx;
            }
            ok = EeAudit_FormatExportUtf8(aw->el, aw->show_copy_log, aw->show_copy, rows, n, delim, &buf, &len);
        }
        free(ord);
        free(rows);
    }
    else
    {
        Utf8Export ex;
        wchar_t cell[4096], session[300];
        int c;
        ZeroMemory(&ex, sizeof(ex));
        session[0] = L'\0';
        if (batches && aw->session >= 0)
        {
            Aud_SessionLabel(aw, (uint32_t)aw->session, session, ARRAYSIZE(session));
        }
        for (c = 0; c < aw->ncols && ok; c++)
        {
            LVCOLUMNW lc;
            ZeroMemory(&lc, sizeof(lc));
            lc.mask = LVCF_TEXT;
            lc.pszText = cell;
            lc.cchTextMax = ARRAYSIZE(cell);
            cell[0] = L'\0';
            ListView_GetColumn(aw->list, c, &lc);
            ok = (c == 0 || Utf8Export_AppendChar(&ex, delim)) && Utf8Export_AppendFieldW(&ex, cell, delim);
        }
        if (batches && ok)
        {
            ok = Utf8Export_AppendChar(&ex, delim) &&
                 Utf8Export_AppendFieldW(&ex, aw->acct.hart ? L"Task" : L"Session", delim);
        }
        ok = ok && Utf8Export_Append(&ex, "\r\n", 2);
        for (i = 0; i < n && ok; i++)
        {
            uint32_t ri = idx != NULL ? idx[i] : i;
            for (c = 0; c < aw->ncols && ok; c++)
            {
                Aud_CellText(aw, ri, c, cell, ARRAYSIZE(cell));
                ok = (c == 0 || Utf8Export_AppendChar(&ex, delim)) && Utf8Export_AppendFieldW(&ex, cell, delim);
            }
            if (batches && ok)
            {
                ok = Utf8Export_AppendChar(&ex, delim) && Utf8Export_AppendFieldW(&ex, session, delim);
            }
            ok = ok && Utf8Export_Append(&ex, "\r\n", 2);
        }
        buf = ex.data;
        len = ex.len;
    }
    SetCursor(old);
    if (!ok)
    {
        MessageBoxW(aw->hwnd, L"Out of memory building the export.", L"Export", MB_ICONERROR | MB_OK);
    }
    else
    {
        App_WriteExportUtf8(aw->hwnd, path, buf != NULL ? buf : "", len);
    }
    free(buf);
    free(idx);
}

/* Show merged event @p ev of log @p li (index in el->logs) in the merged Events view,
 * selected, and bring the window forward. */
static void Aud_GotoEvent(AuditWindow *aw, int li, uint32_t ev)
{
    uint32_t i;
    aw->view = AUD_VIEW_EVENTS;
    aw->show_copy = AUD_MERGED;
    if (aw->show_log != 0 && aw->show_log != Aud_Log(aw, li)->kind)
    {
        aw->show_log = 0;
    }
    Aud_Rebuild(aw);
    for (i = 0; i < aw->nrows; i++)
    {
        if (aw->rows[i].log == (uint8_t)li && aw->rows[i].idx == ev)
        {
            ListView_SetItemState(aw->list, (int)i, LVIS_SELECTED | LVIS_FOCUSED, LVIS_SELECTED | LVIS_FOCUSED);
            ListView_EnsureVisible(aw->list, (int)i, FALSE);
            break;
        }
    }
    SetForegroundWindow(aw->hwnd);
    SetFocus(aw->list);
    if (i == aw->nrows && aw->filt_active)
    {
        MessageBoxW(aw->hwnd, L"The filter hides this event. Filter \x2192 Reset Filter shows every event.",
                    k_WindowTitle, MB_ICONINFORMATION | MB_OK);
    }
}

/* Show batch @p bi's commit in the merged Events view, selected. */
static void Aud_GotoBatchEvent(AuditWindow *aw, uint32_t bi)
{
    Aud_GotoEvent(aw, aw->acct.log, aw->acct.batches[bi].event);
}

/* Make session @p si the one the Ballot Batches view and the accounting tabs show. */
static void Aud_SetSession(AuditWindow *aw, int si)
{
    if (!aw->have_acct || si < 0 || (uint32_t)si >= aw->acct.nsessions || si == aw->session)
    {
        return;
    }
    aw->session = si;
    if (aw->view == AUD_VIEW_BATCHES)
    {
        Aud_Rebuild(aw);
    }
    if (aw->reps[AUD_REP_ACCT] != NULL)
    {
        AudRep_Refresh(aw->reps[AUD_REP_ACCT]);
    }
}

static void Aud_OnContextMenu(AuditWindow *aw, int item, POINT screen)
{
    HMENU m;
    UINT cmd;
    if (item >= 0 && !(ListView_GetItemState(aw->list, item, LVIS_SELECTED) & LVIS_SELECTED))
    {
        ListView_SetItemState(aw->list, -1, 0, LVIS_SELECTED | LVIS_FOCUSED);
        ListView_SetItemState(aw->list, item, LVIS_SELECTED | LVIS_FOCUSED, LVIS_SELECTED | LVIS_FOCUSED);
    }
    m = CreatePopupMenu();
    if (m == NULL)
    {
        return;
    }
    AppendMenuW(m, MF_STRING, IDM_EDIT_COPY, L"&Copy");
    AppendMenuW(m, MF_SEPARATOR, 0, NULL);
    AppendMenuW(m, MF_STRING, IDM_EXPORT_SELECTED, L"Export &Selected…");
    if (aw->view == AUD_VIEW_BATCHES && item >= 0)
    {
        AppendMenuW(m, MF_SEPARATOR, 0, NULL);
        AppendMenuW(m, MF_STRING, IDM_AUD_GOTO_EVENT, L"&Go to Event");
    }
    cmd = (UINT)TrackPopupMenu(m, TPM_RIGHTBUTTON | TPM_RETURNCMD, screen.x, screen.y, 0, aw->hwnd, NULL);
    DestroyMenu(m);
    if (cmd == IDM_EDIT_COPY)
    {
        Aud_CopySelected(aw);
    }
    else if (cmd == IDM_EXPORT_SELECTED)
    {
        App_ExportAudit(aw, TRUE);
    }
    else if (cmd == IDM_AUD_GOTO_EVENT && item >= 0 && (uint32_t)item < aw->nrows)
    {
        Aud_GotoBatchEvent(aw, aw->rows[item].idx);
    }
}

/* An event the final copy of its log lacks: bold red. A batch that no longer counts: grey. */
static LRESULT Aud_CustomDraw(AuditWindow *aw, NMLVCUSTOMDRAW *cd)
{
    switch (cd->nmcd.dwDrawStage)
    {
    case CDDS_PREPAINT:
        return CDRF_NOTIFYITEMDRAW;
    case CDDS_ITEMPREPAINT:
    {
        uint32_t ri = (uint32_t)cd->nmcd.dwItemSpec;
        if (ri < aw->nrows && aw->view == AUD_VIEW_BATCHES)
        {
            if (aw->acct.batches[aw->rows[ri].idx].status != EE_AUDIT_BATCH_COUNTED)
            {
                cd->clrText = RGB(128, 128, 128);
            }
            return CDRF_DODEFAULT;
        }
        if (ri < aw->nrows && Aud_RowLacking(aw, &aw->rows[ri]))
        {
            cd->clrText = RGB(192, 0, 0);
            if (aw->app->font_header != NULL)
            {
                SelectObject(cd->nmcd.hdc, aw->app->font_header);
                return CDRF_NEWFONT;
            }
        }
        return CDRF_DODEFAULT;
    }
    default:
        return CDRF_DODEFAULT;
    }
}

/* --- Filter: the rule window's host -------------------------------------- */

/* "05/26/2026" for day @p day (days since 1970). */
static void Aud_DayText(int64_t day, wchar_t *out, size_t cch)
{
    wchar_t t[48];
    Aud_TimeText(day * 86400, t, ARRAYSIZE(t));
    t[10] = L'\0'; /* the date part of "MM/DD/YYYY hh:mm:ssAM" */
    StringCchCopyW(out, cch, t);
}

static int __cdecl Aud_I64Cmp(const void *pa, const void *pb)
{
    return Aud_CmpI64(*(const int64_t *)pa, *(const int64_t *)pb);
}

static BOOL Aud_FilterCollect(void *ctx, uint32_t column, wchar_t ***vals, uint32_t *n)
{
    static const wchar_t *const k_vt[EE_AUDIT_VT_COUNT] = {L"Early voting", L"Election Day", L"Mail",
                                                           L"Provisional", L"Other"};
    AuditWindow *aw = (AuditWindow *)ctx;
    size_t cap = 0;
    BOOL ok = TRUE;
    int li;
    *vals = NULL;
    *n = 0;
    switch (column)
    {
    case AF_LOG:
        for (li = 0; li < EE_AUDIT_NLOGS && ok; li++)
        {
            if (Aud_Log(aw, li)->ncopies > 0)
            {
                ok = Res_AddValue(vals, n, &cap, Aud_LogName(Aud_Log(aw, li)->kind));
            }
        }
        return ok;
    case AF_VOTETYPE:
        for (li = 0; li < EE_AUDIT_VT_COUNT && ok; li++)
        {
            ok = Res_AddValue(vals, n, &cap, k_vt[li]);
        }
        return ok;
    case AF_DESC:
        return TRUE; /* typed: "contains" */
    case AF_TIMESTAMP:
    {
        /* the days that have events, newest first: suggestions for a typed date */
        int64_t *days = NULL;
        uint32_t nd = 0, total = 0, i, k;
        for (li = 0; li < EE_AUDIT_NLOGS; li++)
        {
            total += Aud_Log(aw, li)->nevents;
        }
        if (total == 0)
        {
            return TRUE;
        }
        days = (int64_t *)malloc((size_t)total * sizeof(int64_t));
        if (days == NULL)
        {
            return FALSE;
        }
        for (li = 0; li < EE_AUDIT_NLOGS; li++)
        {
            const EeAuditLog *log = Aud_Log(aw, li);
            for (i = 0; i < log->nevents; i++)
            {
                const EeAuditCopy *c = NULL;
                int64_t ts = EeAudit_MergedEvent(log, i, &c)->ts;
                days[nd++] = ts >= 0 ? ts / 86400 : (ts - 86399) / 86400;
            }
        }
        qsort(days, nd, sizeof(int64_t), Aud_I64Cmp);
        for (k = nd; k-- > 0 && ok;)
        {
            if (k + 1 == nd || days[k] != days[k + 1])
            {
                wchar_t d[16];
                Aud_DayText(days[k], d, ARRAYSIZE(d));
                ok = Res_AddValue(vals, n, &cap, d);
            }
        }
        free(days);
        return ok;
    }
    default:
        break;
    }
    /* Event, Module, User Id: the distinct strings of the logs' merged events */
    {
        const char **uniq = NULL;
        uint32_t nu = 0, capu = 0, i, k;
        const char *last = NULL;
        for (li = 0; li < EE_AUDIT_NLOGS && ok; li++)
        {
            const EeAuditLog *log = Aud_Log(aw, li);
            for (i = 0; i < log->nevents && ok; i++)
            {
                const EeAuditCopy *c = NULL;
                const EeAuditEvent *ev = EeAudit_MergedEvent(log, i, &c);
                const char *s = EeAudit_Str(c, column == AF_EVENT ? ev->level : column == AF_MODULE ? ev->module
                                                                                                     : ev->user);
                if (s == last || s[0] == '\0')
                {
                    continue;
                }
                last = s;
                for (k = 0; k < nu && strcmp(uniq[k], s) != 0; k++)
                {
                }
                if (k < nu)
                {
                    continue;
                }
                if (nu == capu)
                {
                    uint32_t nc = capu ? capu * 2 : 64;
                    const char **p = (const char **)realloc((void *)uniq, nc * sizeof(char *));
                    if (p == NULL)
                    {
                        ok = FALSE;
                        break;
                    }
                    uniq = p;
                    capu = nc;
                }
                uniq[nu++] = s;
            }
        }
        for (k = 0; k < nu && ok; k++)
        {
            wchar_t w[EE_FILTER_VALUE_CCH];
            Aud_Widen(uniq[k], w, ARRAYSIZE(w));
            ok = Res_AddValue(vals, n, &cap, w);
        }
        free((void *)uniq);
        if (*n > 1)
        {
            qsort(*vals, *n, sizeof(wchar_t *), Res_ValueCmp);
        }
    }
    return ok;
}

static uint32_t Aud_FilterRelations(void *ctx, uint32_t column)
{
    (void)ctx;
    if (column == AF_DESC)
    {
        return (1u << EeRel_Contains) | (1u << EeRel_Excludes);
    }
    if (column == AF_TIMESTAMP)
    {
        return (1u << EeRel_MoreThan) | (1u << EeRel_LessThan);
    }
    return EE_FILTER_REL_EQUALITY;
}

static const wchar_t *Aud_FilterRelText(void *ctx, uint32_t column, EeFilterRelation rel)
{
    (void)ctx;
    (void)column;
    switch (rel)
    {
    case EeRel_Excludes:
        return L"does not contain";
    case EeRel_MoreThan:
        return L"on or after";
    case EeRel_LessThan:
        return L"before";
    default:
        return EeFilter_RelationText(rel);
    }
}

static BOOL Aud_FilterValidate(void *ctx, const EeFilterRule *r, wchar_t *msg, size_t cch)
{
    int64_t ts;
    (void)ctx;
    if (r->column == AF_TIMESTAMP && !Aud_ParseTime(r->value, &ts))
    {
        StringCchCopyW(msg, cch,
                       L"Enter a date as MM/DD/YYYY, or a date and time such as 11/05/2024 7:00 PM "
                       L"(or 19:00).");
        return FALSE;
    }
    return TRUE;
}

static void Aud_ApplyFilter(AuditWindow *aw)
{
    if (!Aud_CompileFilter(aw))
    {
        MessageBoxW(aw->hwnd, L"Out of memory.", k_WindowTitle, MB_ICONERROR | MB_OK);
    }
    Aud_Rebuild(aw);
}

static void Aud_FilterApplyCb(void *ctx)
{
    Aud_ApplyFilter((AuditWindow *)ctx);
}

static void Aud_ResetFilter(AuditWindow *aw)
{
    EeFilter_Clear(&aw->filters);
    Aud_ApplyFilter(aw);
}

static BOOL App_ShowAuditFilter(AuditWindow *aw)
{
    CvrFilterState *st;
    if (aw->hwnd_filter != NULL)
    {
        SetForegroundWindow(aw->hwnd_filter);
        return TRUE;
    }
    st = Filter_NewState(aw->app, aw->hwnd, &aw->filters, &aw->hwnd_filter);
    if (st == NULL)
    {
        return FALSE;
    }
    st->titles = k_AudFilterTitles;
    st->ncols = AF_COUNT;
    st->base_name = aw->base_name;
    st->file_noun = L"Audit Log";
    st->caption = L"Election Explorer Audit Log Filter";
    st->collect = Aud_FilterCollect;
    st->apply = Aud_FilterApplyCb;
    st->relations = Aud_FilterRelations;
    st->relation_text = Aud_FilterRelText;
    st->validate = Aud_FilterValidate;
    st->relation_mask = EE_FILTER_REL_EQUALITY | (1u << EeRel_Contains) | (1u << EeRel_Excludes) |
                        (1u << EeRel_MoreThan) | (1u << EeRel_LessThan);
    st->ctx = aw;
    return Filter_ShowRuleWindow(st, L"Show events (and ballot batches) matching these conditions:");
}

/* --- Reports: Copies Check and Ballot Accounting -------------------------- */

#define AUD_REP_MAX_COLS 12
#define AUD_CELL         256 /* characters of a report cell, built */

/* Row flags of a report. */
#define AUD_RF_BOLD  1 /* the final copy, the election session */
#define AUD_RF_GREY  2 /* an L&A test session */
#define AUD_RF_RED   4 /* a copy that lacks events inside its own time span */
#define AUD_RF_TOTAL 8 /* a total row: bold, kept last when sorted */

/* A report row's key for an event: its log (index in el->logs) and merged event. */
#define AUD_KEY(log, ev) ((int)(log) + EE_AUDIT_NLOGS * (int)(ev))

/* Ballot Accounting tabs. */
enum
{
    AT_SESSIONS = 0, /* Hart: tasks */
    AT_PLACES,
    AT_VOTETYPES,
    AT_CLEARS, /* Hart: Central transfers */
    AT_COUNT
};

/* Notable Events tabs. */
enum
{
    NT_CATEGORIES = 0,
    NT_EVENTS,
    NT_COUNT
};

/* A row of a Ballot Reconciliation (a snapshot taken when it was opened). */
typedef struct AudReconRow
{
    wchar_t measure[96];
    int64_t ems;   /* EE_RES_UNKNOWN when the audit log has no such figure */
    int64_t other; /* EE_RES_UNKNOWN when the other window does not report it */
    int status;    /* AUD_RS_* */
    wchar_t note[200];
} AudReconRow;

struct AudReportWindow
{
    AuditWindow *aw; /* the audit window it reports on */
    int kind;        /* AUD_REP_* */
    HWND hwnd;
    HWND tab; /* Ballot Accounting's or Notable Events' tabs, else NULL */
    HWND summary;
    HWND list;
    int tab_sel;    /* AT_* / NT_* */
    int cat_filter; /* Notable Events' Events tab: one category (EE_AUDIT_N_*), or -1 for all */
    int ncols;
    const wchar_t *col_title[AUD_REP_MAX_COLS];
    int col_width[AUD_REP_MAX_COLS];
    BOOL col_right[AUD_REP_MAX_COLS];
    wchar_t **cells; /* nrows * AUD_REP_MAX_COLS, each malloc'd (NULL = "") */
    uint8_t *flags;  /* AUD_RF_* per row */
    int *keys;       /* per row: its session (Sessions tab), category (Categories tab), event
                      * (log + 2 * merged event: Events tab, timeline, users), else -1 */
    uint32_t *order; /* display order: row indexes */
    uint32_t nrows;
    uint32_t cap;
    int sort_col; /* -1 = built order */
    BOOL sort_asc;
    wchar_t summary_text[4096];
    AudReconRow *recon; /* Ballot Reconciliation: its snapshot */
    uint32_t nrecon;
    wchar_t recon_summary[2048];
    wchar_t recon_other_title[32]; /* "CVR" or "Results" */
};

static const wchar_t k_AuditReportClassName[] = L"ElectionExplorerAuditReport";
static WNDPROC g_old_audrep_list_proc = NULL;

static void AudRep_FreeRows(AudReportWindow *r)
{
    uint32_t i;
    int c;
    for (i = 0; i < r->nrows; i++)
    {
        for (c = 0; c < AUD_REP_MAX_COLS; c++)
        {
            free(r->cells[(size_t)i * AUD_REP_MAX_COLS + c]);
        }
    }
    free(r->cells);
    free(r->flags);
    free(r->keys);
    free(r->order);
    r->cells = NULL;
    r->flags = NULL;
    r->keys = NULL;
    r->order = NULL;
    r->nrows = r->cap = 0;
}

static void AudRep_Columns(AudReportWindow *r, int n, const wchar_t *const *titles, const int *widths,
                           const BOOL *right)
{
    int c;
    r->ncols = n < AUD_REP_MAX_COLS ? n : AUD_REP_MAX_COLS;
    for (c = 0; c < r->ncols; c++)
    {
        r->col_title[c] = titles[c];
        r->col_width[c] = widths[c];
        r->col_right[c] = right[c];
    }
}

/* Append a row of r->ncols cells (copied). FALSE when out of memory. */
static BOOL AudRep_AddRow(AudReportWindow *r, uint8_t flags, int key, wchar_t cells[][AUD_CELL])
{
    int c;
    if (r->nrows == r->cap)
    {
        uint32_t nc = r->cap ? r->cap * 2 : 64;
        wchar_t **pc = (wchar_t **)realloc(r->cells, (size_t)nc * AUD_REP_MAX_COLS * sizeof(wchar_t *));
        uint8_t *pf;
        int *pk;
        if (pc == NULL)
        {
            return FALSE;
        }
        r->cells = pc;
        if ((pf = (uint8_t *)realloc(r->flags, nc)) == NULL)
        {
            return FALSE;
        }
        r->flags = pf;
        if ((pk = (int *)realloc(r->keys, (size_t)nc * sizeof(int))) == NULL)
        {
            return FALSE;
        }
        r->keys = pk;
        r->cap = nc;
    }
    for (c = 0; c < AUD_REP_MAX_COLS; c++)
    {
        r->cells[(size_t)r->nrows * AUD_REP_MAX_COLS + c] =
            (c < r->ncols && cells[c][0] != L'\0') ? _wcsdup(cells[c]) : NULL;
    }
    r->flags[r->nrows] = flags;
    r->keys[r->nrows] = key;
    r->nrows++;
    return TRUE;
}

static const wchar_t *AudRep_Cell(const AudReportWindow *r, uint32_t row, int col)
{
    const wchar_t *s = r->cells[(size_t)row * AUD_REP_MAX_COLS + col];
    return s != NULL ? s : L"";
}

static void AudRep_Line(AudReportWindow *r, const wchar_t *line)
{
    if (r->summary_text[0] != L'\0')
    {
        StringCchCatW(r->summary_text, ARRAYSIZE(r->summary_text), L"\r\n");
    }
    StringCchCatW(r->summary_text, ARRAYSIZE(r->summary_text), line);
}

/* --- Copies Check ---------------------------------------------------------- */

static BOOL AudRep_BuildCopies(AudReportWindow *r)
{
    static const wchar_t *const k_titles[11] = {L"Log",        L"Copy",        L"File",       L"Printed",
                                                L"Election",   L"First Event", L"Last Event", L"Events",
                                                L"New Events", L"Lacks: Outside Span", L"Lacks: Missing"};
    static const int k_widths[11] = {64, 56, 220, 150, 170, 150, 150, 70, 80, 124, 100};
    static const BOOL k_right[11] = {FALSE, FALSE, FALSE, FALSE, FALSE, FALSE, FALSE, TRUE, TRUE, TRUE, TRUE};
    const AuditWindow *aw = r->aw;
    int li;
    AudRep_Columns(r, 11, k_titles, k_widths, k_right);
    for (li = 0; li < EE_AUDIT_NLOGS; li++)
    {
        const EeAuditLog *log = Aud_Log(aw, li);
        uint32_t ci;
        wchar_t line[600], n1[32], n2[32], n3[32];
        if (log->ncopies == 0)
        {
            continue;
        }
        for (ci = 0; ci < log->ncopies; ci++)
        {
            const EeAuditCopy *c = &log->copies[ci];
            const EeAuditCopyCheck *k = &log->checks[ci];
            wchar_t cells[AUD_REP_MAX_COLS][AUD_CELL];
            uint8_t fl = 0;
            ZeroMemory(cells, sizeof(cells));
            StringCchCopyW(cells[0], 96, Aud_LogName(log->kind));
            StringCchPrintfW(cells[1], 96, L"%u of %u", ci + 1, log->ncopies);
            StringCchCopyW(cells[2], 96, App_PathBaseName(c->path));
            if (c->missing_rows > 0)
            {
                fl |= AUD_RF_RED;
            }
            if (c->printed != 0)
            {
                Aud_TimeText(c->printed, cells[3], AUD_CELL);
            }
            Aud_Widen(c->election, cells[4], AUD_CELL);
            if (k->events > 0)
            {
                Aud_TimeText(k->first_ts, cells[5], AUD_CELL);
                Aud_TimeText(k->last_ts, cells[6], AUD_CELL);
            }
            fmt_count_w((int64_t)k->events, cells[7], AUD_CELL);
            fmt_count_w((int64_t)k->new_events, cells[8], AUD_CELL);
            fmt_count_w((int64_t)k->lacks_outside, cells[9], AUD_CELL);
            fmt_count_w((int64_t)k->lacks_missing, cells[10], AUD_CELL);
            if (ci + 1 == log->ncopies)
            {
                fl |= AUD_RF_BOLD;
            }
            if (k->lacks_missing > 0)
            {
                fl |= AUD_RF_RED;
            }
            if (!AudRep_AddRow(r, fl, -1, cells))
            {
                return FALSE;
            }
        }
        /* the verdict on the final copy */
        {
            const EeAuditCopy *last = &log->copies[log->ncopies - 1];
            const EeAuditCopyCheck *k = &log->checks[log->ncopies - 1];
            fmt_count_w((int64_t)log->nevents, n3, ARRAYSIZE(n3));
            if (log->ncopies == 1)
            {
                StringCchPrintfW(line, ARRAYSIZE(line),
                                 L"%s log: one copy (%s, %s events), so there is no earlier copy to compare it with.",
                                 Aud_LogName(log->kind), App_PathBaseName(last->path), n3);
            }
            else if (k->lacks_outside == 0 && k->lacks_missing == 0)
            {
                StringCchPrintfW(line, ARRAYSIZE(line),
                                 L"%s log: the final copy (%s) contains every event of the %u earlier cop%s "
                                 L"(%s events merged).",
                                 Aud_LogName(log->kind), App_PathBaseName(last->path), log->ncopies - 1,
                                 log->ncopies == 2 ? L"y" : L"ies", n3);
            }
            else if (k->lacks_missing == 0)
            {
                fmt_count_w((int64_t)k->lacks_outside, n1, ARRAYSIZE(n1));
                StringCchPrintfW(line, ARRAYSIZE(line),
                                 L"%s log: the final copy (%s) lacks %s events that earlier copies hold, all outside its "
                                 L"own time span (it was printed for a date range), so none is missing.",
                                 Aud_LogName(log->kind), App_PathBaseName(last->path), n1);
            }
            else
            {
                fmt_count_w((int64_t)k->lacks_outside + k->lacks_missing, n1, ARRAYSIZE(n1));
                fmt_count_w((int64_t)k->lacks_outside, n2, ARRAYSIZE(n2));
                StringCchPrintfW(line, ARRAYSIZE(line),
                                 L"%s log: the final copy (%s) lacks %s events that earlier copies hold: %s outside "
                                 L"its own time span (a copy printed for a date range)",
                                 Aud_LogName(log->kind), App_PathBaseName(last->path), n1, n2);
                fmt_count_w((int64_t)k->lacks_missing, n2, ARRAYSIZE(n2));
                StringCchCatW(line, ARRAYSIZE(line), L" and ");
                StringCchCatW(line, ARRAYSIZE(line), n2);
                StringCchCatW(line, ARRAYSIZE(line), L" inside it (missing).");
            }
            AudRep_Line(r, line);
            for (ci = 0; ci + 1 < log->ncopies; ci++)
            {
                if (log->checks[ci].lacks_missing > 0)
                {
                    StringCchPrintfW(line, ARRAYSIZE(line),
                                     L"   An earlier copy lacks events inside its own time span: see the red rows.");
                    AudRep_Line(r, line);
                    break;
                }
            }
            if (log->names_differ)
            {
                StringCchPrintfW(line, ARRAYSIZE(line),
                                 L"   The %s log copies' headers name more than one election (see the Election column).",
                                 Aud_LogName(log->kind));
                AudRep_Line(r, line);
            }
            for (ci = 0; ci < log->ncopies; ci++)
            {
                const EeAuditCopy *c = &log->copies[ci];
                if (c->component != NULL && c->missing_rows == 0)
                {
                    StringCchPrintfW(line, ARRAYSIZE(line), L"   Copy %u (%S): rows numbered 1 to %u, none missing.", ci + 1,
                                     c->component, c->nevents);
                    AudRep_Line(r, line);
                }
                else if (c->component != NULL)
                {
                    StringCchPrintfW(line, ARRAYSIZE(line),
                                     L"   Copy %u (%S): %u row number%s missing from its numbering (shown in red).", ci + 1,
                                     c->component, c->missing_rows, c->missing_rows == 1 ? L" is" : L"s are");
                    AudRep_Line(r, line);
                }
            }
        }
    }
    if (r->summary_text[0] == L'\0')
    {
        AudRep_Line(r, L"No copies loaded.");
    }
    return TRUE;
}

/* --- Ballot Accounting --------------------------------------------------- */

static BOOL AudRep_BuildSessions(AudReportWindow *r)
{
    static const wchar_t *const k_titles[9] = {L"Session", L"Start",   L"End",         L"Started By", L"Batches",
                                               L"Counted", L"Ballots", L"Media Clears", L"Kind"};
    static const int k_widths[9] = {64, 150, 150, 100, 70, 70, 80, 90, 220};
    static const BOOL k_right[9] = {TRUE, FALSE, FALSE, FALSE, TRUE, TRUE, TRUE, TRUE, FALSE};
    static const wchar_t *const k_hart_titles[8] = {L"Task",    L"Name",    L"First Opened", L"Last Event",
                                                    L"vDrives", L"Counted", L"Ballots",      L"Kind"};
    static const int k_hart_widths[8] = {48, 220, 150, 150, 70, 70, 80, 200};
    static const BOOL k_hart_right[8] = {TRUE, FALSE, FALSE, FALSE, TRUE, TRUE, TRUE, FALSE};
    const AuditWindow *aw = r->aw;
    uint32_t si;
    if (aw->acct.hart)
    {
        AudRep_Columns(r, 8, k_hart_titles, k_hart_widths, k_hart_right);
        for (si = 0; si < aw->acct.nsessions; si++)
        {
            const EeAuditSession *s = &aw->acct.sessions[si];
            wchar_t cells[AUD_REP_MAX_COLS][AUD_CELL];
            uint8_t fl = 0;
            ZeroMemory(cells, sizeof(cells));
            StringCchPrintfW(cells[0], 96, L"%u", si + 1);
            Aud_Widen(s->name, cells[1], AUD_CELL);
            Aud_TimeText(s->start, cells[2], AUD_CELL);
            Aud_TimeText(s->end, cells[3], AUD_CELL);
            fmt_count_w((int64_t)s->nbatches, cells[4], AUD_CELL);
            fmt_count_w((int64_t)s->counted, cells[5], AUD_CELL);
            fmt_count_w(s->ballots, cells[6], AUD_CELL);
            StringCchPrintfW(cells[7], 96, L"%s%s", Aud_SessionKind(aw, si), (int)si == aw->session ? L" \x2014 shown" : L"");
            if ((int)si == aw->acct.election_session)
            {
                fl |= AUD_RF_BOLD;
            }
            else if (s->test || s->nbatches == 0)
            {
                fl |= AUD_RF_GREY;
            }
            if (!AudRep_AddRow(r, fl, (int)si, cells))
            {
                return FALSE;
            }
        }
        return TRUE;
    }
    AudRep_Columns(r, 9, k_titles, k_widths, k_right);
    for (si = 0; si < aw->acct.nsessions; si++)
    {
        const EeAuditSession *s = &aw->acct.sessions[si];
        wchar_t cells[AUD_REP_MAX_COLS][AUD_CELL];
        uint8_t fl = 0;
        ZeroMemory(cells, sizeof(cells));
        StringCchPrintfW(cells[0], 96, L"%u", si + 1);
        Aud_TimeText(s->start, cells[1], AUD_CELL);
        Aud_TimeText(s->end, cells[2], AUD_CELL);
        StringCchCopyW(cells[3], 96, s->after_clear ? L"Full clear" : L"Start of log");
        fmt_count_w((int64_t)s->nbatches, cells[4], AUD_CELL);
        fmt_count_w((int64_t)s->counted, cells[5], AUD_CELL);
        fmt_count_w(s->ballots, cells[6], AUD_CELL);
        fmt_count_w((int64_t)s->nclears, cells[7], AUD_CELL);
        StringCchPrintfW(cells[8], 96, L"%s%s", Aud_SessionKind(aw, si),
                         (int)si == aw->session ? L" \x2014 shown" : L"");
        if ((int)si == aw->acct.election_session)
        {
            fl |= AUD_RF_BOLD;
        }
        else if (s->test || s->nbatches == 0)
        {
            fl |= AUD_RF_GREY;
        }
        if (!AudRep_AddRow(r, fl, (int)si, cells))
        {
            return FALSE;
        }
    }
    return TRUE;
}

typedef struct AudPlaceSum
{
    const char *place;
    int vote_type;
    uint32_t counted;
    int64_t ballots;
    uint32_t removed;
    uint32_t replaced;
} AudPlaceSum;

static BOOL AudRep_BuildPlaces(AudReportWindow *r, const EeAuditSession *s)
{
    static const wchar_t *const k_titles[6] = {L"Poll Place",           L"Vote Type", L"Batches Counted",
                                               L"Ballots",              L"Removed by Media Clears", L"Replaced"};
    static const int k_widths[6] = {260, 110, 110, 90, 160, 80};
    static const BOOL k_right[6] = {FALSE, FALSE, TRUE, TRUE, TRUE, TRUE};
    static const wchar_t *const k_hart_titles[5] = {L"Polling Place", L"Vote Type", L"vDrives Counted", L"Ballots",
                                                    L"Read Again"};
    const AuditWindow *aw = r->aw;
    BOOL hart = aw->acct.hart;
    AudPlaceSum *sums = NULL;
    uint32_t nsums = 0, i, k;
    AudPlaceSum tot;
    BOOL ok = TRUE;
    AudRep_Columns(r, hart ? 5 : 6, hart ? k_hart_titles : k_titles, k_widths, k_right);
    ZeroMemory(&tot, sizeof(tot));
    if (s->nbatches > 0)
    {
        sums = (AudPlaceSum *)calloc(s->nbatches, sizeof(AudPlaceSum));
        if (sums == NULL)
        {
            return FALSE;
        }
    }
    for (i = s->first_batch; i < s->first_batch + s->nbatches; i++)
    {
        const EeAuditBatch *b = &aw->acct.batches[i];
        for (k = 0; k < nsums && strcmp(sums[k].place, b->place) != 0; k++)
        {
        }
        if (k == nsums)
        {
            sums[nsums].place = b->place;
            sums[nsums].vote_type = b->vote_type;
            nsums++;
        }
        if (b->status == EE_AUDIT_BATCH_COUNTED)
        {
            sums[k].counted++;
            sums[k].ballots += b->ballots;
        }
        else if (b->status == EE_AUDIT_BATCH_CLEARED)
        {
            sums[k].removed++;
        }
        else
        {
            sums[k].replaced++;
        }
    }
    for (k = 0; k <= nsums && ok; k++)
    {
        const AudPlaceSum *p = k < nsums ? &sums[k] : &tot;
        wchar_t cells[AUD_REP_MAX_COLS][AUD_CELL];
        ZeroMemory(cells, sizeof(cells));
        if (k < nsums)
        {
            Aud_Widen(p->place, cells[0], AUD_CELL);
            if (cells[0][0] == L'\0')
            {
                StringCchCopyW(cells[0], 96, L"(none named)");
            }
            StringCchCopyW(cells[1], 96, EeAudit_VoteTypeName(p->vote_type));
            tot.counted += p->counted;
            tot.ballots += p->ballots;
            tot.removed += p->removed;
            tot.replaced += p->replaced;
        }
        else
        {
            StringCchPrintfW(cells[0], 96, hart ? L"Total (%u polling places)" : L"Total (%u poll places)", nsums);
        }
        fmt_count_w((int64_t)p->counted, cells[2], AUD_CELL);
        fmt_count_w(p->ballots, cells[3], AUD_CELL);
        fmt_count_w((int64_t)(hart ? p->replaced : p->removed), cells[4], AUD_CELL);
        if (!hart)
        {
            fmt_count_w((int64_t)p->replaced, cells[5], AUD_CELL);
        }
        ok = AudRep_AddRow(r, k < nsums ? 0 : AUD_RF_TOTAL, -1, cells);
    }
    free(sums);
    return ok;
}

static BOOL AudRep_BuildVoteTypes(AudReportWindow *r, const EeAuditSession *s)
{
    static const wchar_t *const k_titles[4] = {L"Vote Type", L"Batches Counted", L"Ballots", L"Share"};
    static const wchar_t *const k_hart_titles[4] = {L"Vote Type", L"vDrives Counted", L"Ballots", L"Share"};
    static const int k_widths[4] = {160, 120, 100, 80};
    static const BOOL k_right[4] = {FALSE, TRUE, TRUE, TRUE};
    static const int k_order[EE_AUDIT_VT_COUNT] = {EE_AUDIT_VT_EARLY, EE_AUDIT_VT_ELECTION_DAY, EE_AUDIT_VT_MAIL,
                                                   EE_AUDIT_VT_PROVISIONAL, EE_AUDIT_VT_OTHER};
    const AuditWindow *aw = r->aw;
    uint32_t counted[EE_AUDIT_VT_COUNT] = {0}, total_counted = 0, i;
    int k;
    AudRep_Columns(r, 4, aw->acct.hart ? k_hart_titles : k_titles, k_widths, k_right);
    for (i = s->first_batch; i < s->first_batch + s->nbatches; i++)
    {
        const EeAuditBatch *b = &aw->acct.batches[i];
        if (b->status == EE_AUDIT_BATCH_COUNTED && b->vote_type >= 0 && b->vote_type < EE_AUDIT_VT_COUNT)
        {
            counted[b->vote_type]++;
            total_counted++;
        }
    }
    for (k = 0; k <= EE_AUDIT_VT_COUNT; k++)
    {
        wchar_t cells[AUD_REP_MAX_COLS][AUD_CELL];
        int64_t ballots = k < EE_AUDIT_VT_COUNT ? s->ballots_vt[k_order[k]] : s->ballots;
        ZeroMemory(cells, sizeof(cells));
        if (k < EE_AUDIT_VT_COUNT && counted[k_order[k]] == 0 && ballots == 0)
        {
            continue;
        }
        StringCchCopyW(cells[0], 96, k < EE_AUDIT_VT_COUNT ? EeAudit_VoteTypeName(k_order[k]) : L"Total");
        fmt_count_w((int64_t)(k < EE_AUDIT_VT_COUNT ? counted[k_order[k]] : total_counted), cells[1], AUD_CELL);
        fmt_count_w(ballots, cells[2], AUD_CELL);
        if (s->ballots > 0)
        {
            StringCchPrintfW(cells[3], 96, L"%.1f%%", 100.0 * (double)ballots / (double)s->ballots);
        }
        if (!AudRep_AddRow(r, k < EE_AUDIT_VT_COUNT ? 0 : AUD_RF_TOTAL, -1, cells))
        {
            return FALSE;
        }
    }
    return TRUE;
}

/* Hart: VerityCentral's vDrives and VerityCount's reads of them. */
static BOOL AudRep_BuildTransfers(AudReportWindow *r)
{
    static const wchar_t *const k_titles[8] = {L"Written",        L"Central Batches", L"Ballots Written", L"vDrive ID",
                                               L"Read Into Count", L"Ballots Read",   L"Difference",      L"Status"};
    static const int k_widths[8] = {150, 200, 110, 190, 150, 100, 80, 260};
    static const BOOL k_right[8] = {FALSE, FALSE, TRUE, FALSE, FALSE, TRUE, TRUE, FALSE};
    const AuditWindow *aw = r->aw;
    uint32_t i;
    AudRep_Columns(r, 8, k_titles, k_widths, k_right);
    for (i = 0; i < aw->acct.ntransfers; i++)
    {
        const EeAuditTransfer *t = &aw->acct.transfers[i];
        wchar_t cells[AUD_REP_MAX_COLS][AUD_CELL];
        uint8_t fl = 0;
        ZeroMemory(cells, sizeof(cells));
        Aud_TimeText(t->ts, cells[0], AUD_CELL);
        Aud_Widen(t->batches, cells[1], AUD_CELL);
        fmt_count_w(t->ballots, cells[2], AUD_CELL);
        if (t->device != NULL)
        {
            Aud_Widen(t->device, cells[3], AUD_CELL);
        }
        else
        {
            StringCchCopyW(cells[3], 96, L"(no vDrive closed logged)");
        }
        if (t->read_batch >= 0)
        {
            Aud_TimeText(aw->acct.batches[t->read_batch].ts, cells[4], AUD_CELL);
            fmt_count_w(t->read_ballots, cells[5], AUD_CELL);
            StringCchPrintfW(cells[6], 96, L"%+lld", (long long)(t->read_ballots - t->ballots));
        }
        switch (t->status)
        {
        case EE_AUDIT_XFER_MATCHED:
            StringCchCopyW(cells[7], 96, t->by_count ? L"Match (by ballots and time: no vDrive ID logged)" : L"Match");
            break;
        case EE_AUDIT_XFER_DIFFERS:
            StringCchCopyW(cells[7], 96, L"Differs");
            fl |= AUD_RF_RED;
            break;
        case EE_AUDIT_XFER_UNREAD:
            StringCchCopyW(cells[7], 96, L"No read of this vDrive in the VerityCount log");
            fl |= AUD_RF_RED;
            break;
        default:
            StringCchCopyW(cells[7], 96, L"No VerityCount log loaded");
            break;
        }
        if (!AudRep_AddRow(r, fl, -1, cells))
        {
            return FALSE;
        }
    }
    return TRUE;
}

static BOOL AudRep_BuildClears(AudReportWindow *r, const EeAuditSession *s)
{
    static const wchar_t *const k_titles[9] = {L"Cleared At",      L"Media",           L"Device",
                                               L"Poll Place",      L"Batches Removed", L"Ballots Removed",
                                               L"Places on Media", L"Total If None Removed",
                                               L"Total If All Places Removed"};
    static const int k_widths[9] = {140, 50, 96, 170, 120, 110, 110, 150, 180};
    static const BOOL k_right[9] = {FALSE, TRUE, FALSE, FALSE, TRUE, TRUE, TRUE, TRUE, TRUE};
    const AuditWindow *aw = r->aw;
    uint32_t i;
    AudRep_Columns(r, 9, k_titles, k_widths, k_right);
    for (i = s->first_clear; i < s->first_clear + s->nclears; i++)
    {
        const EeAuditMediaClear *mc = &aw->acct.clears[i];
        wchar_t cells[AUD_REP_MAX_COLS][AUD_CELL];
        ZeroMemory(cells, sizeof(cells));
        Aud_TimeText(mc->ts, cells[0], AUD_CELL);
        StringCchPrintfW(cells[1], 96, L"%d", mc->media);
        StringCchCopyW(cells[2], 96, mc->device == 1 ? L"Central count" : mc->device == 2 ? L"Poll place" : L"Not named");
        if (mc->place != NULL)
        {
            Aud_Widen(mc->place, cells[3], AUD_CELL);
        }
        else
        {
            StringCchCopyW(cells[3], 96, mc->places == 0 ? L"(no batches on this media)"
                                                         : L"(not known: nothing removed)");
        }
        fmt_count_w((int64_t)mc->removed, cells[4], AUD_CELL);
        fmt_count_w(mc->removed_ballots, cells[5], AUD_CELL);
        fmt_count_w((int64_t)mc->places, cells[6], AUD_CELL);
        fmt_count_w(s->ballots + mc->removed_ballots, cells[7], AUD_CELL);
        fmt_count_w(s->ballots - (mc->all_ballots - mc->removed_ballots), cells[8], AUD_CELL);
        if (!AudRep_AddRow(r, 0, -1, cells))
        {
            return FALSE;
        }
    }
    return TRUE;
}

/* The lines above the accounting tabs: sessions, the election session and the session shown,
 * the media clears' other readings, and the load errors. */
/* Hart: tasks, the election task, the task shown, rejected vDrives and the Central transfers. */
static void AudRep_HartAcctSummary(AudReportWindow *r)
{
    const AuditWindow *aw = r->aw;
    const EeAuditAccounting *a = &aw->acct;
    wchar_t line[700], lab[300], n1[32], n2[32], n3[32], n4[32], n5[32], n6[32];
    uint32_t si, tests = 0, differ = 0;
    for (si = 0; si < a->nsessions; si++)
    {
        tests += a->sessions[si].test ? 1u : 0u;
    }
    fmt_count_w((int64_t)a->nbatches, n1, ARRAYSIZE(n1));
    StringCchPrintfW(line, ARRAYSIZE(line),
                     L"The VerityCount log shows %u task%s (%u test task%s, told by name) and %s vDrive%s read "
                     L"(State Read).",
                     a->nsessions, a->nsessions == 1 ? L"" : L"s", tests, tests == 1 ? L"" : L"s", n1,
                     a->nbatches == 1 ? L"" : L"s");
    AudRep_Line(r, line);
    if (a->election_session >= 0)
    {
        const EeAuditSession *s = &a->sessions[a->election_session];
        Aud_SessionLabel(aw, (uint32_t)a->election_session, lab, ARRAYSIZE(lab));
        fmt_count_w(s->ballots, n1, ARRAYSIZE(n1));
        fmt_count_w((int64_t)s->counted, n2, ARRAYSIZE(n2));
        fmt_count_w(s->ballots_vt[EE_AUDIT_VT_EARLY], n3, ARRAYSIZE(n3));
        fmt_count_w(s->ballots_vt[EE_AUDIT_VT_ELECTION_DAY], n4, ARRAYSIZE(n4));
        fmt_count_w(s->ballots_vt[EE_AUDIT_VT_MAIL], n5, ARRAYSIZE(n5));
        fmt_count_w(s->ballots_vt[EE_AUDIT_VT_PROVISIONAL] + s->ballots_vt[EE_AUDIT_VT_OTHER], n6, ARRAYSIZE(n6));
        StringCchPrintfW(line, ARRAYSIZE(line),
                         L"Election task (the official task read into last): %s. %s ballots on %s vDrives (by "
                         L"polling place name: early voting %s, Election Day %s, mail %s, other %s).",
                         lab, n1, n2, n3, n4, n5, n6);
    }
    else
    {
        StringCchCopyW(line, ARRAYSIZE(line), L"No official task (a name without a test word) had vDrives read.");
    }
    AudRep_Line(r, line);
    if (aw->session >= 0 && aw->session != a->election_session)
    {
        Aud_SessionLabel(aw, (uint32_t)aw->session, lab, ARRAYSIZE(lab));
        StringCchPrintfW(line, ARRAYSIZE(line), L"Shown in vDrive Reads and these tabs: %s.", lab);
        AudRep_Line(r, line);
    }
    if (a->rejected > 0)
    {
        StringCchPrintfW(line, ARRAYSIZE(line), L"%u vDrive%s rejected (not counted).", a->rejected,
                         a->rejected == 1 ? L" was" : L"s were");
        AudRep_Line(r, line);
    }
    for (si = 0; si < a->ntransfers; si++)
    {
        differ += a->transfers[si].status == EE_AUDIT_XFER_DIFFERS || a->transfers[si].status == EE_AUDIT_XFER_UNREAD;
    }
    if (a->ntransfers > 0)
    {
        StringCchPrintfW(line, ARRAYSIZE(line),
                         L"VerityCentral wrote %u vDrive%s of CVRs; %u %s not read into VerityCount with the same ballots "
                         L"(Central Transfers tab).",
                         a->ntransfers, a->ntransfers == 1 ? L"" : L"s", differ, differ == 1 ? L"was" : L"were");
        AudRep_Line(r, line);
    }
    AudRep_Line(r, L"Ballots are each vDrive read's Ballot count. A vDrive read into a task again replaces its "
                   L"earlier read. Test tasks are told by name (Edit \x2192 Options\x2026).");
}

static void AudRep_AcctSummary(AudReportWindow *r)
{
    const AuditWindow *aw = r->aw;
    const EeAuditAccounting *a = &aw->acct;
    wchar_t line[700], n1[32], n2[32], n3[32], n4[32], n5[32], n6[32];
    uint32_t si, tests = 0;
    if (a->hart)
    {
        AudRep_HartAcctSummary(r);
        return;
    }
    for (si = 0; si < a->nsessions; si++)
    {
        tests += a->sessions[si].test && a->sessions[si].nbatches > 0 ? 1u : 0u;
    }
    fmt_count_w((int64_t)a->nbatches, n1, ARRAYSIZE(n1));
    StringCchPrintfW(line, ARRAYSIZE(line),
                     L"The Election log shows %u session%s (each full clear starts one; %u L&A test session%s), %s "
                     L"batch commits and %u media clear%s.",
                     a->nsessions, a->nsessions == 1 ? L"" : L"s", tests, tests == 1 ? L"" : L"s", n1, a->nclears,
                     a->nclears == 1 ? L"" : L"s");
    AudRep_Line(r, line);
    if (a->election_session >= 0)
    {
        const EeAuditSession *s = &a->sessions[a->election_session];
        wchar_t lab[300];
        Aud_SessionLabel(aw, (uint32_t)a->election_session, lab, ARRAYSIZE(lab));
        fmt_count_w(s->ballots, n1, ARRAYSIZE(n1));
        fmt_count_w((int64_t)s->counted, n2, ARRAYSIZE(n2));
        fmt_count_w(s->ballots_vt[EE_AUDIT_VT_EARLY], n3, ARRAYSIZE(n3));
        fmt_count_w(s->ballots_vt[EE_AUDIT_VT_ELECTION_DAY], n4, ARRAYSIZE(n4));
        fmt_count_w(s->ballots_vt[EE_AUDIT_VT_MAIL], n5, ARRAYSIZE(n5));
        fmt_count_w(s->ballots_vt[EE_AUDIT_VT_PROVISIONAL] + s->ballots_vt[EE_AUDIT_VT_OTHER], n6, ARRAYSIZE(n6));
        StringCchPrintfW(line, ARRAYSIZE(line),
                         L"Election session: %s. %s ballots in %s batches still loaded at its end (early voting %s, "
                         L"Election Day %s, mail %s, provisional and other %s).",
                         lab, n1, n2, n3, n4, n5, n6);
    }
    else
    {
        StringCchCopyW(line, ARRAYSIZE(line), L"No session loaded batches other than L&A test decks.");
    }
    AudRep_Line(r, line);
    if (aw->session >= 0 && aw->session != a->election_session)
    {
        wchar_t lab[300];
        Aud_SessionLabel(aw, (uint32_t)aw->session, lab, ARRAYSIZE(lab));
        StringCchPrintfW(line, ARRAYSIZE(line), L"Shown in Ballot Batches and these tabs: %s.", lab);
        AudRep_Line(r, line);
    }
    if (aw->session >= 0 && a->sessions[aw->session].nclears > 0)
    {
        const EeAuditSession *s = &a->sessions[aw->session];
        int64_t none = s->ballots, every = s->ballots;
        for (si = s->first_clear; si < s->first_clear + s->nclears; si++)
        {
            none += a->clears[si].removed_ballots;
            every -= a->clears[si].all_ballots - a->clears[si].removed_ballots;
        }
        fmt_count_w(none, n1, ARRAYSIZE(n1));
        fmt_count_w(every, n2, ARRAYSIZE(n2));
        fmt_count_w(s->ballots, n3, ARRAYSIZE(n3));
        StringCchPrintfW(line, ARRAYSIZE(line),
                         L"Media clears: the session holds %s ballots with each clear removing the poll place loaded "
                         L"next; %s had they removed nothing, %s had each removed every poll place's batches on its "
                         L"media.",
                         n3, n1, n2);
        AudRep_Line(r, line);
    }
    if (a->previously_loaded > 0)
    {
        fmt_count_w((int64_t)a->previously_loaded, n1, ARRAYSIZE(n1));
        StringCchPrintfW(line, ARRAYSIZE(line), L"%s batch%s reported as \"previously loaded\" (not loaded again).",
                         n1, a->previously_loaded == 1 ? L" was" : L"es were");
        AudRep_Line(r, line);
    }
    if (a->sheet_errors > 0)
    {
        fmt_count_w((int64_t)a->sheet_errors, n2, ARRAYSIZE(n2));
        fmt_count_w((int64_t)a->sheet_error_ids, n3, ARRAYSIZE(n3));
        StringCchPrintfW(line, ARRAYSIZE(line), L"%s ballot sheet load errors (%s sheet IDs).", n2, n3);
        AudRep_Line(r, line);
    }
    AudRep_Line(r, L"Ballots are each commit's Number of Ballots: ballots, not ballot sheets.");
}

static BOOL AudRep_BuildAcct(AudReportWindow *r)
{
    const AuditWindow *aw = r->aw;
    const EeAuditSession *s;
    static const EeAuditSession k_empty = {0};
    if (!aw->have_acct || (Aud_Log(aw, aw->acct.log)->ncopies == 0 && aw->acct.ntransfers == 0))
    {
        static const wchar_t *const k_t[1] = {L"Ballot Accounting"};
        static const int k_w[1] = {400};
        static const BOOL k_r[1] = {FALSE};
        AudRep_Columns(r, 1, k_t, k_w, k_r);
        AudRep_Line(r, Aud_Hart(aw) ? L"No VerityCount log is loaded: the ballot accounting comes from its vDrive reads."
                                    : L"No Election log is loaded: the ballot accounting comes from the Election log.");
        return TRUE;
    }
    AudRep_AcctSummary(r);
    s = aw->session >= 0 ? &aw->acct.sessions[aw->session] : &k_empty;
    switch (r->tab_sel)
    {
    case AT_PLACES:
        return AudRep_BuildPlaces(r, s);
    case AT_VOTETYPES:
        return AudRep_BuildVoteTypes(r, s);
    case AT_CLEARS:
        return aw->acct.hart ? AudRep_BuildTransfers(r) : AudRep_BuildClears(r, s);
    default:
        return AudRep_BuildSessions(r);
    }
}

/* --- L&A Testing Timeline ------------------------------------------------- */

/* "12 min", "2 h 45 min", "3 d 4 h". */
static void Aud_Duration(int64_t secs, wchar_t *out, size_t cch)
{
    if (secs < 0)
    {
        secs = -secs;
    }
    if (secs < 60)
    {
        StringCchCopyW(out, cch, L"under a minute");
    }
    else if (secs < 3600)
    {
        StringCchPrintfW(out, cch, L"%lld min", (long long)(secs / 60));
    }
    else if (secs < 86400)
    {
        StringCchPrintfW(out, cch, L"%lld h %lld min", (long long)(secs / 3600), (long long)(secs % 3600 / 60));
    }
    else
    {
        StringCchPrintfW(out, cch, L"%lld d %lld h", (long long)(secs / 86400), (long long)(secs % 86400 / 3600));
    }
}

/* "05/26 01:11PM", or "01:11PM" without the date. */
static void Aud_ShortTime(int64_t ts, BOOL with_date, wchar_t *out, size_t cch)
{
    wchar_t t[48];
    Aud_TimeText(ts, t, ARRAYSIZE(t)); /* MM/DD/YYYY hh:mm:ssAM */
    if (wcslen(t) < 21)
    {
        StringCchCopyW(out, cch, t);
    }
    else if (with_date)
    {
        StringCchPrintfW(out, cch, L"%.5s %.5s%.2s", t, t + 11, t + 19);
    }
    else
    {
        StringCchPrintfW(out, cch, L"%.5s%.2s", t + 11, t + 19);
    }
}

static void Aud_TestLabel(const AuditWindow *aw, const EeAuditTest *t, wchar_t *out, size_t cch)
{
    wchar_t nm[200], n1[32];
    if (t->kind == EE_AUDIT_TEST_SESSION && aw->acct.hart && t->session < aw->acct.nsessions)
    {
        fmt_count_w(t->ballots, n1, ARRAYSIZE(n1));
        Aud_Widen(aw->acct.sessions[t->session].name, nm, ARRAYSIZE(nm));
        StringCchPrintfW(out, cch, L"Test task %s: %u vDrive%s read, %s ballots", nm, t->batches,
                         t->batches == 1 ? L"" : L"s", n1);
        return;
    }
    if (t->kind == EE_AUDIT_TEST_SESSION)
    {
        fmt_count_w(t->ballots, n1, ARRAYSIZE(n1));
        StringCchPrintfW(out, cch, L"Test session %u in the Election log: %u test batch%s, %s ballots", t->session + 1,
                         t->batches, t->batches == 1 ? L"" : L"es", n1);
        return;
    }
    Aud_Widen(t->name, nm, ARRAYSIZE(nm));
    StringCchPrintfW(out, cch, t->kind == EE_AUDIT_TEST_RESTORED ? L"Test copy restored and opened: %s"
                                                                 : L"Test copy opened: %s",
                     nm);
}

/* "05/23 10:49AM, 2 h 44 min before (Election log)", or "No test found in these logs". */
static void Aud_TestRef(const EeAuditTimeline *tl, int ti, int64_t at, BOOL before, wchar_t *out, size_t cch)
{
    wchar_t when[32], dur[48];
    if (ti < 0)
    {
        StringCchCopyW(out, cch, L"No test found in these logs");
        return;
    }
    Aud_ShortTime(tl->tests[ti].start, TRUE, when, ARRAYSIZE(when));
    Aud_Duration(before ? at - tl->tests[ti].start : tl->tests[ti].start - at, dur, ARRAYSIZE(dur));
    StringCchPrintfW(out, cch, L"%s, %s %s (%s log)", when, dur, before ? L"before" : L"after",
                     Aud_LogName(tl->tests[ti].log + 1));
}

static BOOL AudRep_BuildTimeline(AudReportWindow *r)
{
    static const wchar_t *const k_titles[7] = {L"Start", L"End",         L"Kind",       L"Log",
                                               L"What",  L"Test Before", L"Test After"};
    static const int k_widths[7] = {140, 140, 110, 64, 330, 290, 290};
    static const BOOL k_right[7] = {FALSE, FALSE, FALSE, FALSE, FALSE, FALSE, FALSE};
    const AuditWindow *aw = r->aw;
    const EeAuditTimeline *tl = &aw->tl;
    uint32_t ti = 0, pi = 0, missing = 0, el_tests = 0, k;
    wchar_t line[700], n1[32], date[32];
    AudRep_Columns(r, 7, k_titles, k_widths, k_right);
    if (!aw->have_tl)
    {
        AudRep_Line(r, Aud_Log(aw, aw->acct.log)->ncopies == 0
                           ? (Aud_Hart(aw) ? L"No VerityCount log is loaded: the timeline needs its vDrive reads."
                                           : L"No Election log is loaded: the timeline needs its batch loads.")
                           : L"The timeline could not be built.");
        return TRUE;
    }
    /* tests and counting periods in time order */
    while (ti < tl->ntests || pi < tl->nperiods)
    {
        wchar_t cells[AUD_REP_MAX_COLS][AUD_CELL];
        BOOL test = pi >= tl->nperiods || (ti < tl->ntests && tl->tests[ti].start <= tl->periods[pi].first);
        uint8_t fl = 0;
        int key;
        ZeroMemory(cells, sizeof(cells));
        if (test)
        {
            const EeAuditTest *t = &tl->tests[ti++];
            Aud_TimeText(t->start, cells[0], AUD_CELL);
            Aud_TimeText(t->end, cells[1], AUD_CELL);
            StringCchCopyW(cells[2], AUD_CELL, L"L&A test");
            StringCchCopyW(cells[3], AUD_CELL, Aud_LogName(t->log + 1));
            Aud_TestLabel(aw, t, cells[4], AUD_CELL);
            key = AUD_KEY(t->log, t->event);
            el_tests += t->kind == EE_AUDIT_TEST_SESSION ? 1u : 0u;
        }
        else
        {
            const EeAuditPeriod *p = &tl->periods[pi++];
            wchar_t part[96];
            Aud_TimeText(p->first, cells[0], AUD_CELL);
            Aud_TimeText(p->last, cells[1], AUD_CELL);
            StringCchCopyW(cells[2], AUD_CELL, L"Counting period");
            StringCchCopyW(cells[3], AUD_CELL, Aud_AcctLogName(aw));
            fmt_count_w(p->ballots, n1, ARRAYSIZE(n1));
            if (aw->acct.hart)
            {
                StringCchPrintfW(cells[4], AUD_CELL, L"%u vDrive%s read into official tasks, %s ballots", p->batches,
                                 p->batches == 1 ? L"" : L"s", n1);
            }
            else
            {
                StringCchPrintfW(cells[4], AUD_CELL, L"%u batch%s loaded, %s ballots", p->batches,
                                 p->batches == 1 ? L"" : L"es", n1);
            }
            for (k = p->first_pause; k < p->first_pause + p->npauses; k++)
            {
                const EeAuditPause *ps = &tl->pauses[k];
                wchar_t a[32], b[32];
                BOOL days = ps->to / 86400 != ps->from / 86400;
                Aud_ShortTime(ps->from, days, a, ARRAYSIZE(a));
                Aud_ShortTime(ps->to, days, b, ARRAYSIZE(b));
                StringCchPrintfW(part, ARRAYSIZE(part), L"%s %s\x2013%s%s", k == p->first_pause ? L"; pauses" : L",", a,
                                 b, ps->closed ? L" (election closed)" : L"");
                StringCchCatW(cells[4], AUD_CELL, part);
            }
            Aud_TestRef(tl, p->test_before, p->first, TRUE, cells[5], AUD_CELL);
            Aud_TestRef(tl, p->test_after, p->last, FALSE, cells[6], AUD_CELL);
            key = AUD_KEY(aw->acct.log, p->first_event);
            fl = AUD_RF_BOLD;
            if (p->test_before < 0 || p->test_after < 0)
            {
                fl |= AUD_RF_RED;
                missing++;
            }
        }
        if (!AudRep_AddRow(r, fl, key, cells))
        {
            return FALSE;
        }
    }
    /* summary */
    Aud_FormatDate(aw->el->election_date, date, ARRAYSIZE(date));
    if (tl->ntests > 0)
    {
        wchar_t first[48];
        int64_t eday = aw->el->election_date > 0 ? EeAudit_MakeTime(aw->el->election_date / 10000,
                                                                    aw->el->election_date / 100 % 100,
                                                                    aw->el->election_date % 100, 0, 0, 0)
                                                 : 0;
        Aud_TimeText(tl->tests[0].start, first, ARRAYSIZE(first));
        if (eday != 0)
        {
            StringCchPrintfW(line, ARRAYSIZE(line),
                             L"Election Day: %s. The first test found: %s, %lld days before Election Day (%s log).", date,
                             first, (long long)((eday - tl->tests[0].start + 86399) / 86400),
                             Aud_LogName(tl->tests[0].log + 1));
        }
        else
        {
            StringCchPrintfW(line, ARRAYSIZE(line), L"The first test found: %s (%s log).", first,
                             Aud_LogName(tl->tests[0].log + 1));
        }
    }
    else
    {
        StringCchPrintfW(line, ARRAYSIZE(line), L"Election Day: %s. No test was found in these logs.", date);
    }
    AudRep_Line(r, line);
    if (aw->acct.hart)
    {
        StringCchPrintfW(line, ARRAYSIZE(line),
                         L"%u test%s (vDrives read into test tasks, one test per task and day) and %u counting "
                         L"period%s of vDrives read into official tasks, each running from one test to the next.",
                         tl->ntests, tl->ntests == 1 ? L"" : L"s", tl->nperiods, tl->nperiods == 1 ? L"" : L"s");
    }
    else
    {
        StringCchPrintfW(line, ARRAYSIZE(line),
                         L"%u test%s (%u test session%s in the Election log, %u test cop%s opened in the Admin log) and %u "
                         L"counting period%s of ballots (not test decks), each running from one test to the next.",
                         tl->ntests, tl->ntests == 1 ? L"" : L"s", el_tests, el_tests == 1 ? L"" : L"s",
                         tl->ntests - el_tests, tl->ntests - el_tests == 1 ? L"y" : L"ies", tl->nperiods,
                         tl->nperiods == 1 ? L"" : L"s");
    }
    AudRep_Line(r, line);
    if (missing > 0)
    {
        StringCchPrintfW(line, ARRAYSIZE(line),
                         L"%u counting period%s %s no test found before or after it in these logs (red).", missing,
                         missing == 1 ? L"" : L"s", missing == 1 ? L"has" : L"have");
    }
    else if (tl->nperiods > 0)
    {
        StringCchCopyW(line, ARRAYSIZE(line), L"Every counting period has a test found before it and after it.");
    }
    else
    {
        line[0] = L'\0';
    }
    if (line[0] != L'\0')
    {
        AudRep_Line(r, line);
    }
    if (tl->other_copies > 0)
    {
        wchar_t nm[160];
        Aud_Widen(tl->other_copy, nm, ARRAYSIZE(nm));
        StringCchPrintfW(line, ARRAYSIZE(line),
                         L"%u test cop%s of other elections in the Admin log %s left out (for example %s): a test copy "
                         L"counts as this election's when one of its restores follows a backup of this election.",
                         tl->other_copies, tl->other_copies == 1 ? L"y" : L"ies",
                         tl->other_copies == 1 ? L"is" : L"are", nm);
        AudRep_Line(r, line);
    }
    if (tl->other_loads > 0)
    {
        StringCchPrintfW(line, ARRAYSIZE(line),
                         L"%u load%s while the Election log shows another election open (the database's earlier life "
                         L"as a copy of that election) %s left out.",
                         tl->other_loads, tl->other_loads == 1 ? L"" : L"s", tl->other_loads == 1 ? L"is" : L"are");
        AudRep_Line(r, line);
    }
    AudRep_Line(r, aw->acct.hart
                       ? L"Test tasks are told by name (Edit \x2192 Options\x2026). The timeline reports what the logs "
                         L"show; it does not judge compliance."
                       : L"A test copy's own log is not loaded, so its test shows only as the Admin log's restore and "
                         L"open. The timeline reports what the logs show; it does not judge compliance.");
    return TRUE;
}

/* --- Notable Events -------------------------------------------------------- */

static const wchar_t *Aud_SeverityName(int sev)
{
    return sev == EE_AUDIT_SEV_RED ? L"Red flag" : sev == EE_AUDIT_SEV_REVIEW ? L"Review" : L"Information";
}

static uint8_t Aud_SeverityFlags(int sev)
{
    return sev == EE_AUDIT_SEV_RED ? AUD_RF_RED : sev == EE_AUDIT_SEV_INFO ? AUD_RF_GREY : 0;
}

/* The event a note points to, and its copy. */
static const EeAuditEvent *Aud_NoteEvent(const AuditWindow *aw, const EeAuditNote *n, const EeAuditCopy **copy)
{
    return EeAudit_MergedEvent(Aud_Log(aw, n->log), n->event, copy);
}

static void Aud_NoteDetail(const AuditWindow *aw, const EeAuditNote *n, wchar_t *out, size_t cch)
{
    wchar_t n1[32], dur[48];
    out[0] = L'\0';
    switch (n->category)
    {
    case EE_AUDIT_N_RESULTS_EARLY:
        fmt_count_w(n->value, n1, ARRAYSIZE(n1));
        StringCchPrintfW(out, cch, L"%u batch%s, %s ballots loaded", n->batches, n->batches == 1 ? L"" : L"es", n1);
        break;
    case EE_AUDIT_N_RESULTS_TEST:
        if (n->batches > 0)
        {
            StringCchPrintfW(out, cch, L"%u test batch%s loaded", n->batches, n->batches == 1 ? L"" : L"es");
        }
        else
        {
            StringCchCopyW(out, cch, L"no ballots loaded");
        }
        break;
    case EE_AUDIT_N_LOADED_EARLY:
    {
        const EeAuditCopy *c;
        const EeAuditEvent *e = Aud_NoteEvent(aw, n, &c);
        int d = aw->el->election_date;
        int64_t eday = d > 0 ? EeAudit_MakeTime(d / 10000, d / 100 % 100, d % 100, 0, 0, 0) : 0;
        fmt_count_w(n->value, n1, ARRAYSIZE(n1));
        StringCchPrintfW(out, cch, L"%s ballots, %lld days before Election Day", n1,
                         (long long)((eday - e->ts + 86399) / 86400));
        break;
    }
    case EE_AUDIT_N_CLOCK:
        Aud_Duration(n->value, dur, ARRAYSIZE(dur));
        if (EE_AUDIT_IS_HART(Aud_Log(aw, n->log)->kind))
        {
            StringCchPrintfW(out, cch, L"logged %s %s its Time read", dur, n->value < 0 ? L"before" : L"after");
        }
        else
        {
            StringCchPrintfW(out, cch, L"%s later than the event printed above it", dur);
        }
        break;
    case EE_AUDIT_N_MANUAL_VOTES:
        if (n->value > 0)
        {
            fmt_count_w(n->value, n1, ARRAYSIZE(n1));
            StringCchPrintfW(out, cch, L"%s ballot%s", n1, n->value == 1 ? L"" : L"s");
        }
        break;
    case EE_AUDIT_N_TRANSFER:
        if (n->value != 0)
        {
            StringCchPrintfW(out, cch, L"VerityCount read %lld ballot%s %s than written", (long long)(n->value < 0 ? -n->value : n->value),
                             n->value == 1 || n->value == -1 ? L"" : L"s", n->value < 0 ? L"fewer" : L"more");
        }
        else
        {
            StringCchCopyW(out, cch, L"no read of this vDrive in the VerityCount log");
        }
        break;
    default:
        break;
    }
}

static BOOL AudRep_BuildNotable(AudReportWindow *r)
{
    const AuditWindow *aw = r->aw;
    const EeAuditNotes *nt = &aw->notes;
    wchar_t line[1400], n1[32];
    int cat;
    uint32_t i;
    if (!aw->have_notes)
    {
        static const wchar_t *const k_t[1] = {L"Notable Events"};
        static const int k_w[1] = {400};
        static const BOOL k_r[1] = {FALSE};
        AudRep_Columns(r, 1, k_t, k_w, k_r);
        AudRep_Line(r, L"The notable events could not be found (out of memory).");
        return TRUE;
    }
    /* summary */
    if (nt->per_category[EE_AUDIT_N_RESULTS_EARLY] > 0)
    {
        const EeAuditNote *first = NULL;
        const EeAuditCopy *c;
        const EeAuditEvent *e;
        wchar_t when[48], user[64];
        for (i = 0; i < nt->count && first == NULL; i++)
        {
            if (nt->notes[i].category == EE_AUDIT_N_RESULTS_EARLY)
            {
                first = &nt->notes[i];
            }
        }
        e = Aud_NoteEvent(aw, first, &c);
        Aud_TimeText(e->ts, when, ARRAYSIZE(when));
        Aud_Widen(EeAudit_Str(c, e->user), user, ARRAYSIZE(user));
        StringCchPrintfW(line, ARRAYSIZE(line),
                         L"RED FLAG: %u results report%s printed, saved to a file or exported before Election Day while "
                         L"ballots (not test decks%s) were loaded; the first at %s by %s. Ballots may be tabulated early "
                         L"(in a county of 100,000 or more, from %d days before Election Day), but results are not to be "
                         L"printed or released before Election Day.",
                         nt->per_category[EE_AUDIT_N_RESULTS_EARLY],
                         nt->per_category[EE_AUDIT_N_RESULTS_EARLY] == 1 ? L" was" : L"s were",
                         Aud_Hart(aw) ? L"; read into the official task open at the time" : L"", when, user,
                         EE_AUDIT_EARLY_COUNT_DAYS);
    }
    else if (aw->el->election_date > 0)
    {
        StringCchCopyW(line, ARRAYSIZE(line),
                       L"No results report was printed, saved to a file or exported before Election Day while ballots "
                       L"(not test decks) were loaded.");
    }
    else
    {
        StringCchCopyW(line, ARRAYSIZE(line),
                       L"The reports' headers give no election date, so no event is compared with Election Day.");
    }
    AudRep_Line(r, line);
    {
        int sev;
        for (sev = EE_AUDIT_SEV_REVIEW; sev >= EE_AUDIT_SEV_INFO; sev--)
        {
            BOOL any = FALSE;
            StringCchCopyW(line, ARRAYSIZE(line), sev == EE_AUDIT_SEV_REVIEW ? L"For review: " : L"Information: ");
            for (cat = 0; cat < EE_AUDIT_N_COUNT; cat++)
            {
                if (EeAudit_NoteSeverity(cat) != sev || nt->per_category[cat] == 0)
                {
                    continue;
                }
                fmt_count_w((int64_t)nt->per_category[cat], n1, ARRAYSIZE(n1));
                if (any)
                {
                    StringCchCatW(line, ARRAYSIZE(line), L"; ");
                }
                StringCchCatW(line, ARRAYSIZE(line), EeAudit_NoteName(cat));
                StringCchCatW(line, ARRAYSIZE(line), L" ");
                StringCchCatW(line, ARRAYSIZE(line), n1);
                any = TRUE;
            }
            if (any)
            {
                StringCchCatW(line, ARRAYSIZE(line), L".");
                AudRep_Line(r, line);
            }
        }
    }
    if (nt->other_events > 0)
    {
        wchar_t nm[160];
        Aud_Widen(nt->other_election, nm, ARRAYSIZE(nm));
        fmt_count_w((int64_t)nt->other_events, n1, ARRAYSIZE(n1));
        StringCchPrintfW(line, ARRAYSIZE(line),
                         L"%s Election log events while %s was open (the database's earlier life as a copy of that "
                         L"election) are not checked.",
                         n1, nm[0] != L'\0' ? nm : L"another election");
        AudRep_Line(r, line);
    }
    AudRep_Line(r, Aud_Hart(aw)
                       ? L"A task is official unless its name holds a test word (Edit \x2192 Options\x2026). Double-click "
                         L"a category to list its events, and an event to see it in the Events view."
                       : L"The Admin log covers the whole system, so its deletes, restores and user changes may concern "
                         L"other elections. Double-click a category to list its events, and an event to see it in the "
                         L"Events view.");

    if (r->tab_sel == NT_CATEGORIES)
    {
        static const wchar_t *const k_titles[6] = {L"Severity", L"Category", L"Events",
                                                   L"First",    L"Last",     L"What It Means"};
        static const int k_widths[6] = {90, 280, 70, 150, 150, 560};
        static const BOOL k_right[6] = {FALSE, FALSE, TRUE, FALSE, FALSE, FALSE};
        AudRep_Columns(r, 6, k_titles, k_widths, k_right);
        for (cat = 0; cat < EE_AUDIT_N_COUNT; cat++)
        {
            wchar_t cells[AUD_REP_MAX_COLS][AUD_CELL];
            int64_t first = 0, last = 0;
            BOOL any = FALSE;
            if (nt->per_category[cat] == 0)
            {
                continue;
            }
            for (i = 0; i < nt->count; i++)
            {
                if (nt->notes[i].category == cat)
                {
                    const EeAuditCopy *c;
                    int64_t ts = Aud_NoteEvent(aw, &nt->notes[i], &c)->ts;
                    first = !any || ts < first ? ts : first;
                    last = !any || ts > last ? ts : last;
                    any = TRUE;
                }
            }
            ZeroMemory(cells, sizeof(cells));
            StringCchCopyW(cells[0], AUD_CELL, Aud_SeverityName(EeAudit_NoteSeverity(cat)));
            StringCchCopyW(cells[1], AUD_CELL, EeAudit_NoteName(cat));
            fmt_count_w((int64_t)nt->per_category[cat], cells[2], AUD_CELL);
            Aud_TimeText(first, cells[3], AUD_CELL);
            Aud_TimeText(last, cells[4], AUD_CELL);
            StringCchCopyW(cells[5], AUD_CELL, EeAudit_NoteMeaning(cat));
            if (!AudRep_AddRow(r, Aud_SeverityFlags(EeAudit_NoteSeverity(cat)), cat, cells))
            {
                return FALSE;
            }
        }
        if (r->nrows == 0)
        {
            AudRep_Line(r, L"No notable events.");
        }
        return TRUE;
    }
    {
        static const wchar_t *const k_titles[7] = {L"Severity", L"Category",    L"Timestamp", L"Log",
                                                   L"User Id",  L"Description", L"Detail"};
        static const int k_widths[7] = {90, 240, 150, 90, 100, 440, 240};
        static const BOOL k_right[7] = {FALSE, FALSE, FALSE, FALSE, FALSE, FALSE, FALSE};
        uint32_t shown = 0;
        AudRep_Columns(r, 7, k_titles, k_widths, k_right);
        for (cat = 0; cat < EE_AUDIT_N_COUNT; cat++)
        {
            if (r->cat_filter >= 0 && cat != r->cat_filter)
            {
                continue;
            }
            for (i = 0; i < nt->count; i++)
            {
                const EeAuditNote *n = &nt->notes[i];
                const EeAuditCopy *c;
                const EeAuditEvent *e;
                wchar_t cells[AUD_REP_MAX_COLS][AUD_CELL];
                if (n->category != cat)
                {
                    continue;
                }
                e = Aud_NoteEvent(aw, n, &c);
                ZeroMemory(cells, sizeof(cells));
                StringCchCopyW(cells[0], AUD_CELL, Aud_SeverityName(EeAudit_NoteSeverity(cat)));
                StringCchCopyW(cells[1], AUD_CELL, EeAudit_NoteName(cat));
                Aud_TimeText(e->ts, cells[2], AUD_CELL);
                StringCchCopyW(cells[3], AUD_CELL, Aud_LogName(n->log + 1));
                Aud_Widen(EeAudit_Str(c, e->user), cells[4], AUD_CELL);
                Aud_Widen(EeAudit_Desc(c, e), cells[5], AUD_CELL);
                Aud_NoteDetail(aw, n, cells[6], AUD_CELL);
                if (!AudRep_AddRow(r, Aud_SeverityFlags(EeAudit_NoteSeverity(cat)), AUD_KEY(n->log, n->event), cells))
                {
                    return FALSE;
                }
                shown++;
            }
        }
        fmt_count_w((int64_t)shown, n1, ARRAYSIZE(n1));
        if (r->cat_filter >= 0)
        {
            StringCchPrintfW(line, ARRAYSIZE(line), L"Showing %s: %s event%s (the Events tab chosen by hand shows all).",
                             EeAudit_NoteName(r->cat_filter), n1, shown == 1 ? L"" : L"s");
        }
        else
        {
            StringCchPrintfW(line, ARRAYSIZE(line), L"Showing all %s notable events, most serious first.", n1);
        }
        AudRep_Line(r, line);
    }
    return TRUE;
}

/* --- Users and Workstations ----------------------------------------------- */

#define AUD_USER_MAX_ITEMS 24

typedef struct AudUserSum
{
    const char *user;
    uint32_t events[EE_AUDIT_NLOGS];
    int64_t first, last;
    uint32_t first_log, first_event;
    const char *modules[AUD_USER_MAX_ITEMS];
    uint32_t nmodules;
    char stations[AUD_USER_MAX_ITEMS][40];
    uint32_t nstations;
} AudUserSum;

/* The workstation an event names ("... on WORKSTATION-11.", "from machine TCC12"), or FALSE. */
static BOOL Aud_Workstation(const char *d, char *out, size_t cap)
{
    const char *p = strstr(d, " from machine ");
    const char *best = NULL;
    size_t n = 0;
    if (p != NULL)
    {
        best = p + 14;
    }
    else
    {
        for (p = strstr(d, " on "); p != NULL; p = strstr(p + 1, " on "))
        {
            const char *q = p + 4;
            BOOL digit = FALSE;
            size_t k = 0;
            while ((q[k] >= 'A' && q[k] <= 'Z') || (q[k] >= '0' && q[k] <= '9') || q[k] == '-' || q[k] == '_')
            {
                digit = digit || (q[k] >= '0' && q[k] <= '9') || q[k] == '-';
                k++;
            }
            if (k >= 3 && digit && (q[k] == '\0' || q[k] == '.' || q[k] == ' ' || q[k] == ','))
            {
                best = q;
            }
        }
    }
    if (best == NULL)
    {
        return FALSE;
    }
    while (best[n] != '\0' && best[n] != ' ' && best[n] != '.' && best[n] != ',' && n + 1 < cap)
    {
        n++;
    }
    memcpy(out, best, n);
    out[n] = '\0';
    return n > 0;
}

static BOOL AudRep_BuildUsers(AudReportWindow *r)
{
    static const wchar_t *const k_titles[8] = {L"User Id", L"Events", L"Election Log", L"Admin Log",
                                               L"First",   L"Last",   L"Modules",      L"Workstations"};
    static const int k_widths[8] = {120, 80, 90, 80, 150, 150, 330, 230};
    static const BOOL k_right[8] = {FALSE, TRUE, TRUE, TRUE, FALSE, FALSE, FALSE, FALSE};
    /* Hart: one count per component log; the Device ID is the workstation */
    static const wchar_t *const k_hart_titles[8] = {L"User",  L"Events", L"VerityCount", L"VerityCentral",
                                                    L"VerityData", L"First", L"Last",     L"Device IDs"};
    static const int k_hart_widths[8] = {120, 70, 90, 100, 90, 150, 150, 300};
    static const BOOL k_hart_right[8] = {FALSE, TRUE, TRUE, TRUE, TRUE, FALSE, FALSE, FALSE};
    const AuditWindow *aw = r->aw;
    BOOL hart = Aud_Hart(aw);
    AudUserSum *sums = NULL;
    uint32_t nsums = 0, cap = 0, i, k;
    int li;
    char all_stations[AUD_USER_MAX_ITEMS * 2][40];
    uint32_t nall = 0;
    wchar_t line[1400];
    AudRep_Columns(r, 8, hart ? k_hart_titles : k_titles, hart ? k_hart_widths : k_widths,
                   hart ? k_hart_right : k_right);
    for (li = 0; li < EE_AUDIT_NLOGS; li++)
    {
        const EeAuditLog *log = Aud_Log(aw, li);
        for (i = log->nevents; i-- > 0;) /* oldest first */
        {
            const EeAuditCopy *c;
            const EeAuditEvent *e = EeAudit_MergedEvent(log, i, &c);
            const char *user = EeAudit_Str(c, e->user);
            const char *module = EeAudit_Str(c, e->module);
            char ws[40];
            AudUserSum *u = NULL;
            if (user[0] == '\0')
            {
                continue;
            }
            for (k = 0; k < nsums; k++)
            {
                if (strcmp(sums[k].user, user) == 0)
                {
                    u = &sums[k];
                    break;
                }
            }
            if (u == NULL)
            {
                if (nsums == cap)
                {
                    uint32_t nc = cap ? cap * 2 : 32;
                    AudUserSum *p = (AudUserSum *)realloc(sums, nc * sizeof(AudUserSum));
                    if (p == NULL)
                    {
                        free(sums);
                        return FALSE;
                    }
                    sums = p;
                    cap = nc;
                }
                u = &sums[nsums++];
                ZeroMemory(u, sizeof(*u));
                u->user = user;
                u->first = e->ts;
                u->first_log = (uint32_t)li;
                u->first_event = i;
                u->last = e->ts;
            }
            u->events[li]++;
            if (e->ts < u->first)
            {
                u->first = e->ts;
                u->first_log = (uint32_t)li;
                u->first_event = i;
            }
            u->last = e->ts > u->last ? e->ts : u->last;
            for (k = 0; k < u->nmodules && strcmp(u->modules[k], module) != 0; k++)
            {
            }
            if (k == u->nmodules && u->nmodules < AUD_USER_MAX_ITEMS && module[0] != '\0')
            {
                u->modules[u->nmodules++] = module;
            }
            if (Aud_Workstation(EeAudit_Desc(c, e), ws, sizeof(ws)))
            {
                for (k = 0; k < u->nstations && strcmp(u->stations[k], ws) != 0; k++)
                {
                }
                if (k == u->nstations && u->nstations < AUD_USER_MAX_ITEMS)
                {
                    StringCchCopyA(u->stations[u->nstations++], 40, ws);
                }
                for (k = 0; k < nall && strcmp(all_stations[k], ws) != 0; k++)
                {
                }
                if (k == nall && nall < ARRAYSIZE(all_stations))
                {
                    StringCchCopyA(all_stations[nall++], 40, ws);
                }
            }
        }
    }
    for (i = 0; i < nsums; i++)
    {
        const AudUserSum *u = &sums[i];
        wchar_t cells[AUD_REP_MAX_COLS][AUD_CELL];
        wchar_t w[64];
        ZeroMemory(cells, sizeof(cells));
        Aud_Widen(u->user, cells[0], AUD_CELL);
        if (hart)
        {
            fmt_count_w((int64_t)u->events[2] + u->events[3] + u->events[4], cells[1], AUD_CELL);
            fmt_count_w((int64_t)u->events[2], cells[2], AUD_CELL);
            fmt_count_w((int64_t)u->events[3], cells[3], AUD_CELL);
            fmt_count_w((int64_t)u->events[4], cells[4], AUD_CELL);
            Aud_TimeText(u->first, cells[5], AUD_CELL);
            Aud_TimeText(u->last, cells[6], AUD_CELL);
            for (k = 0; k < u->nmodules; k++)
            {
                Aud_Widen(u->modules[k], w, ARRAYSIZE(w));
                if (k > 0)
                {
                    StringCchCatW(cells[7], AUD_CELL, L", ");
                }
                StringCchCatW(cells[7], AUD_CELL, w);
            }
            if (!AudRep_AddRow(r, 0, AUD_KEY(u->first_log, u->first_event), cells))
            {
                free(sums);
                return FALSE;
            }
            continue;
        }
        fmt_count_w((int64_t)u->events[0] + u->events[1], cells[1], AUD_CELL);
        fmt_count_w((int64_t)u->events[0], cells[2], AUD_CELL);
        fmt_count_w((int64_t)u->events[1], cells[3], AUD_CELL);
        Aud_TimeText(u->first, cells[4], AUD_CELL);
        Aud_TimeText(u->last, cells[5], AUD_CELL);
        for (k = 0; k < u->nmodules; k++)
        {
            Aud_Widen(u->modules[k], w, ARRAYSIZE(w));
            if (k > 0)
            {
                StringCchCatW(cells[6], AUD_CELL, L", ");
            }
            StringCchCatW(cells[6], AUD_CELL, w);
        }
        for (k = 0; k < u->nstations; k++)
        {
            Aud_Widen(u->stations[k], w, ARRAYSIZE(w));
            if (k > 0)
            {
                StringCchCatW(cells[7], AUD_CELL, L", ");
            }
            StringCchCatW(cells[7], AUD_CELL, w);
        }
        if (!AudRep_AddRow(r, 0, AUD_KEY(u->first_log, u->first_event), cells))
        {
            free(sums);
            return FALSE;
        }
    }
    StringCchPrintfW(line, ARRAYSIZE(line), L"%u user%s in these logs.", nsums, nsums == 1 ? L"" : L"s");
    if (nall > 0)
    {
        StringCchCatW(line, ARRAYSIZE(line), L" Workstations the events name: ");
        for (k = 0; k < nall; k++)
        {
            wchar_t w[64];
            Aud_Widen(all_stations[k], w, ARRAYSIZE(w));
            if (k > 0)
            {
                StringCchCatW(line, ARRAYSIZE(line), L", ");
            }
            StringCchCatW(line, ARRAYSIZE(line), w);
        }
        StringCchCatW(line, ARRAYSIZE(line), L".");
    }
    AudRep_Line(r, line);
    AudRep_Line(r, hart ? L"Device IDs are the workstations the events were logged on. Double-click a user to see their first "
                          L"event in the Events view."
                        : L"A workstation is listed when an event names it (\"Load Results activated by ... on "
                          L"WORKSTATION-11\", \"from machine ...\"). Double-click a user to see their first event in the "
                          L"Events view.");
    free(sums);
    return TRUE;
}

/* --- Ballot Reconciliation ------------------------------------------------- */

/* Status of a reconciliation row. */
#define AUD_RS_MATCH         0
#define AUD_RS_DIFFERS       1
#define AUD_RS_BOUND_MET     2
#define AUD_RS_BOUND_NOT_MET 3
#define AUD_RS_NOT_REPORTED  4
#define AUD_RS_INFO          5 /* shown, not compared */

static const wchar_t *Aud_ReconStatusText(int st)
{
    switch (st)
    {
    case AUD_RS_MATCH:
        return L"Match";
    case AUD_RS_DIFFERS:
        return L"Differs";
    case AUD_RS_BOUND_MET:
        return L"Lower bound met";
    case AUD_RS_BOUND_NOT_MET:
        return L"Lower bound not met";
    case AUD_RS_NOT_REPORTED:
        return L"Not reported";
    default:
        return L"";
    }
}

static BOOL AudRep_BuildRecon(AudReportWindow *r)
{
    static const wchar_t *k_titles[6] = {L"Measure", L"Audit Log (EMS)", L"Other Window", L"Difference",
                                         L"Status",  L"Note"};
    static const int k_widths[6] = {300, 130, 130, 100, 140, 520};
    static const BOOL k_right[6] = {FALSE, TRUE, TRUE, TRUE, FALSE, FALSE};
    uint32_t i;
    k_titles[2] = r->recon_other_title;
    AudRep_Columns(r, 6, k_titles, k_widths, k_right);
    for (i = 0; i < r->nrecon; i++)
    {
        const AudReconRow *x = &r->recon[i];
        wchar_t cells[AUD_REP_MAX_COLS][AUD_CELL];
        uint8_t fl = 0;
        ZeroMemory(cells, sizeof(cells));
        StringCchCopyW(cells[0], AUD_CELL, x->measure);
        if (x->ems != EE_RES_UNKNOWN)
        {
            fmt_count_w(x->ems, cells[1], AUD_CELL);
        }
        if (x->other != EE_RES_UNKNOWN)
        {
            fmt_count_w(x->other, cells[2], AUD_CELL);
        }
        else if (x->status == AUD_RS_NOT_REPORTED)
        {
            StringCchCopyW(cells[2], AUD_CELL, L"not reported");
        }
        if (x->ems != EE_RES_UNKNOWN && x->other != EE_RES_UNKNOWN)
        {
            int64_t d = x->ems - x->other;
            fmt_count_w(d < 0 ? -d : d, cells[3], AUD_CELL);
            if (d != 0)
            {
                wchar_t tmp[AUD_CELL];
                StringCchPrintfW(tmp, AUD_CELL, L"%s%s", d > 0 ? L"+" : L"\x2212", cells[3]);
                StringCchCopyW(cells[3], AUD_CELL, tmp);
            }
        }
        StringCchCopyW(cells[4], AUD_CELL, Aud_ReconStatusText(x->status));
        StringCchCopyW(cells[5], AUD_CELL, x->note);
        if (x->status == AUD_RS_DIFFERS || x->status == AUD_RS_BOUND_NOT_MET)
        {
            fl = AUD_RF_RED;
        }
        else if (x->status == AUD_RS_INFO || x->status == AUD_RS_NOT_REPORTED)
        {
            fl = AUD_RF_GREY;
        }
        else if (i == 0)
        {
            fl = AUD_RF_BOLD;
        }
        if (!AudRep_AddRow(r, fl, -1, cells))
        {
            return FALSE;
        }
    }
    StringCchCopyW(r->summary_text, ARRAYSIZE(r->summary_text), r->recon_summary);
    return TRUE;
}

/* Add a row to a reconciliation snapshot. */
static BOOL Aud_ReconAdd(AudReconRow **rows, uint32_t *n, const wchar_t *measure, int64_t ems, int64_t other,
                         int status, const wchar_t *note)
{
    AudReconRow *p = (AudReconRow *)realloc(*rows, ((size_t)*n + 1) * sizeof(AudReconRow));
    if (p == NULL)
    {
        return FALSE;
    }
    *rows = p;
    p = &p[(*n)++];
    StringCchCopyW(p->measure, ARRAYSIZE(p->measure), measure);
    p->ems = ems;
    p->other = other;
    p->status = status;
    StringCchCopyW(p->note, ARRAYSIZE(p->note), note != NULL ? note : L"");
    return TRUE;
}

/* The audit vote type (EE_AUDIT_VT_*) a results vote type's name points to. */
static int Aud_ResultsVoteType(const wchar_t *name)
{
    char u8[160];
    if (name == NULL || WideCharToMultiByte(CP_UTF8, 0, name, -1, u8, (int)sizeof(u8), NULL, NULL) <= 0)
    {
        return EE_AUDIT_VT_OTHER;
    }
    return EeAudit_VoteTypeOf(u8);
}

/* For audit vote type @p a: the most votes any Vote-For-1 contest got in the results' vote types
 * that map to it (every ballot of that type that held the contest cast at most one), and that
 * contest's title; -1 when no contest has vote-type detail. */
static int64_t Aud_ResultsVtBound(const EeResults *res, const int *vt_map, int a, const wchar_t **title)
{
    int64_t best = -1;
    uint32_t ci;
    uint32_t nvt = EE_RESULTS_NVT(res);
    *title = NULL;
    if (res->nvt == 0)
    {
        return -1;
    }
    for (ci = 0; ci < res->ncontests; ci++)
    {
        const EeResultsContest *ct = &res->contests[ci];
        int64_t sum = 0;
        uint32_t k, p, v;
        if (ct->vote_for != 1 || ct->counts == NULL || ct->npre == 0)
        {
            continue;
        }
        for (k = 0; k < ct->nchoices; k++)
        {
            for (p = 0; p < ct->npre; p++)
            {
                for (v = 0; v < res->nvt; v++)
                {
                    if (vt_map[v] == a)
                    {
                        sum += ct->counts[((size_t)k * ct->npre + p) * nvt + v];
                    }
                }
            }
        }
        if (sum > best)
        {
            best = sum;
            *title = ct->title;
        }
    }
    return best;
}

/* Fill a reconciliation snapshot of audit window @p aw against CVR window @p cw or results
 * window @p rw. */
static BOOL Aud_BuildReconSnapshot(const AuditWindow *aw, CvrWindow *cw, ResultsWindow *rw, AudReconRow **rows,
                                   uint32_t *n, wchar_t *summary, size_t scch)
{
    const EeAuditSession *s = &aw->acct.sessions[aw->session];
    wchar_t line[900], lab[300], n1[32], n2[32], n3[32], note[200];
    const EeAuditLog *alog = Aud_Log(aw, aw->acct.log);
    summary[0] = L'\0';
    fmt_count_w(s->ballots, n1, ARRAYSIZE(n1));
    fmt_count_w((int64_t)s->counted, n2, ARRAYSIZE(n2));
    if (aw->acct.hart)
    {
        wchar_t name[128];
        Aud_Widen(s->name, name, ARRAYSIZE(name));
        StringCchPrintfW(line, ARRAYSIZE(line),
                         L"Audit log: task %d of %u, %s (%s): %s ballots on %s vDrives read into VerityCount (each "
                         L"read's Ballot count). Reports \x2192 Select Task\x2026 picks another task.",
                         aw->session + 1, aw->acct.nsessions, name, Aud_SessionKind(aw, (uint32_t)aw->session), n1, n2);
    }
    else
    {
        StringCchPrintfW(lab, ARRAYSIZE(lab), L"Session %d of %u (%s)", aw->session + 1, aw->acct.nsessions,
                         Aud_SessionKind(aw, (uint32_t)aw->session));
        StringCchPrintfW(line, ARRAYSIZE(line),
                         L"Audit log: %s: %s ballots in %s batches still loaded into the EMS at its end (each commit's "
                         L"Number of Ballots: ballots, not sheets). Reports \x2192 Select Session\x2026 picks another "
                         L"session.",
                         lab, n1, n2);
    }
    StringCchCatW(summary, scch, line);
    if (alog->ncopies > 0 && alog->nevents > 0)
    {
        /* a log printed before the last ballots were loaded (a final canvass) lacks them */
        const EeAuditCopy *last = &alog->copies[alog->ncopies - 1];
        const EeAuditCopy *c = NULL;
        wchar_t printed[48] = L"(print time unknown)", newest[48];
        if (last->printed != 0)
        {
            Aud_TimeText(last->printed, printed, ARRAYSIZE(printed));
        }
        Aud_TimeText(EeAudit_MergedEvent(alog, 0, &c)->ts, newest, ARRAYSIZE(newest));
        StringCchPrintfW(line, ARRAYSIZE(line),
                         L"\r\nThe %s log's last copy was printed %s; its newest event is at %s. A log printed before the "
                         L"last ballots were loaded (a final canvass) does not show them.",
                         Aud_LogName(alog->kind), printed, newest);
        StringCchCatW(summary, scch, line);
        if (rw != NULL && rw->res.timestamp != NULL && rw->res.timestamp[0] != L'\0')
        {
            StringCchPrintfW(line, ARRAYSIZE(line), L" The results file's timestamp: %.80s.", rw->res.timestamp);
            StringCchCatW(summary, scch, line);
        }
    }
    if (cw != NULL)
    {
        const EeCvrCardCounts *cc = &cw->cards;
        const wchar_t *how = cc->method == EE_CVR_CARDS_SHEET_COLUMN
                                 ? L"the rows its Sheet Number / Card column numbers 1"
                             : cc->method == EE_CVR_CARDS_PAGE_ONE
                                 ? L"its first cards: rows with a top-of-ballot contest (President, U.S. Senator, "
                                   L"Governor), or with most of their ballot style's common contests"
                                 : L"one row per ballot";
        fmt_count_w((int64_t)cc->rows, n1, ARRAYSIZE(n1));
        fmt_count_w((int64_t)cc->ballots, n2, ARRAYSIZE(n2));
        StringCchPrintfW(line, ARRAYSIZE(line), L"\r\nCast vote records: %s rows; %s ballots, counted as %s.", n1, n2,
                         how);
        StringCchCatW(summary, scch, line);
        if (cw->filt_active)
        {
            StringCchCatW(summary, scch, L" The CVR window's filter is not applied: every row is counted.");
        }
        StringCchCopyW(note, ARRAYSIZE(note),
                       cc->continuation > 0 ? L"the CVR's first cards (ballots), not its rows" : L"one CVR row per ballot");
        if (!Aud_ReconAdd(rows, n, L"Ballots", s->ballots, (int64_t)cc->ballots,
                          s->ballots == (int64_t)cc->ballots ? AUD_RS_MATCH : AUD_RS_DIFFERS, note))
        {
            return FALSE;
        }
        fmt_count_w((int64_t)cc->continuation, n3, ARRAYSIZE(n3));
        StringCchPrintfW(note, ARRAYSIZE(note),
                         cc->continuation > 0 ? L"rows = ballots + %s continuation sheets (second and later cards)"
                                              : L"each row is a ballot",
                         n3);
        if (!Aud_ReconAdd(rows, n, L"CVR rows (ballot sheets)", EE_RES_UNKNOWN, (int64_t)cc->rows, AUD_RS_INFO,
                          note) ||
            !Aud_ReconAdd(rows, n, L"Continuation sheets", EE_RES_UNKNOWN, (int64_t)cc->continuation, AUD_RS_INFO,
                          L"second and later cards of multi-card ballots"))
        {
            return FALSE;
        }
        if (s->ballots == (int64_t)cc->ballots)
        {
            StringCchPrintfW(line, ARRAYSIZE(line), L"\r\nThe EMS ballots and the CVR ballots match.");
        }
        else
        {
            int64_t d = s->ballots - (int64_t)cc->ballots;
            fmt_count_w(d < 0 ? -d : d, n1, ARRAYSIZE(n1));
            StringCchPrintfW(line, ARRAYSIZE(line), L"\r\nThe EMS has %s %s ballot%s than the CVR.", n1,
                             d > 0 ? L"more" : L"fewer", (d == 1 || d == -1) ? L"" : L"s");
        }
        StringCchCatW(summary, scch, line);
        return TRUE;
    }
    {
        const EeResults *res = &rw->res;
        int vt_map[64];
        uint32_t v;
        int a;
        BOOL bounds = FALSE, exact_vt = res->ballots_vt != NULL && res->nvt > 0;
        for (v = 0; v < res->nvt && v < ARRAYSIZE(vt_map); v++)
        {
            vt_map[v] = Aud_ResultsVoteType(res->vote_types[v]);
        }
        if (res->ballots_total != EE_RES_UNKNOWN)
        {
            fmt_count_w(res->ballots_total, n1, ARRAYSIZE(n1));
            StringCchPrintfW(line, ARRAYSIZE(line), L"\r\nElection results: ballots cast %s.", n1);
        }
        else
        {
            StringCchCopyW(line, ARRAYSIZE(line), L"\r\nElection results: the files do not report ballots cast.");
        }
        StringCchCatW(summary, scch, line);
        if (rw->filt_active)
        {
            StringCchCatW(summary, scch, L" The results window's filter is not applied: the county totals are used.");
        }
        if (!Aud_ReconAdd(rows, n, L"Ballots cast", s->ballots, res->ballots_total,
                          res->ballots_total == EE_RES_UNKNOWN ? AUD_RS_NOT_REPORTED
                          : res->ballots_total == s->ballots   ? AUD_RS_MATCH
                                                               : AUD_RS_DIFFERS,
                          L"the results' ballots cast"))
        {
            return FALSE;
        }
        if (aw->acct.hart)
        {
            /* a central count vDrive names no polling place and holds mail, provisional and other ballots */
            StringCchCatW(summary, scch,
                          L"\r\nHart vDrive reads give no reliable vote type (a central count vDrive names no polling "
                          L"place), so only the totals are compared.");
        }
        for (a = 1; !aw->acct.hart && a <= EE_AUDIT_VT_COUNT && res->nvt > 0 && res->nvt <= ARRAYSIZE(vt_map); a++)
        {
            int vt = a % EE_AUDIT_VT_COUNT; /* early, Election Day, mail, provisional, then other */
            int64_t ems = s->ballots_vt[vt];
            wchar_t measure[96];
            BOOL mapped = FALSE;
            for (v = 0; v < res->nvt; v++)
            {
                mapped = mapped || vt_map[v] == vt;
            }
            if (!mapped && ems == 0)
            {
                continue;
            }
            if (exact_vt)
            {
                int64_t sum = 0;
                BOOL unknown = !mapped;
                for (v = 0; v < res->nvt; v++)
                {
                    if (vt_map[v] == vt)
                    {
                        unknown = unknown || res->ballots_vt[v] == EE_RES_UNKNOWN;
                        sum += res->ballots_vt[v] == EE_RES_UNKNOWN ? 0 : res->ballots_vt[v];
                    }
                }
                StringCchPrintfW(measure, ARRAYSIZE(measure), L"Ballots cast: %s", EeAudit_VoteTypeName(vt));
                if (!Aud_ReconAdd(rows, n, measure, ems, unknown ? EE_RES_UNKNOWN : sum,
                                  unknown ? AUD_RS_NOT_REPORTED : sum == ems ? AUD_RS_MATCH : AUD_RS_DIFFERS,
                                  L"EMS vote type from the batches' poll place names"))
                {
                    return FALSE;
                }
            }
            else
            {
                const wchar_t *title = NULL;
                int64_t bound = mapped ? Aud_ResultsVtBound(res, vt_map, vt, &title) : -1;
                StringCchPrintfW(measure, ARRAYSIZE(measure), L"Ballots: %s (lower bound)", EeAudit_VoteTypeName(vt));
                if (bound < 0)
                {
                    if (!Aud_ReconAdd(rows, n, measure, ems, EE_RES_UNKNOWN, AUD_RS_NOT_REPORTED,
                                      L"no results vote type points to it"))
                    {
                        return FALSE;
                    }
                    continue;
                }
                StringCchPrintfW(note, ARRAYSIZE(note), L"votes in \"%.120s\" (Vote For 1) of this vote type",
                                 title != NULL ? title : L"");
                bounds = TRUE;
                if (!Aud_ReconAdd(rows, n, measure, ems, bound, ems >= bound ? AUD_RS_BOUND_MET : AUD_RS_BOUND_NOT_MET,
                                  note))
                {
                    return FALSE;
                }
            }
        }
        if (bounds)
        {
            StringCchCatW(summary, scch,
                          L"\r\nThe results give votes by vote type but not ballots, so each vote type is a lower "
                          L"bound: the EMS ballots of that type must be at least the votes of the Vote-For-1 contest "
                          L"with the most votes of that type. EMS vote types come from the batches' poll place "
                          L"names, results vote types from their names.");
        }
        if (res->ballots_total != EE_RES_UNKNOWN)
        {
            if (res->ballots_total == s->ballots)
            {
                StringCchCopyW(line, ARRAYSIZE(line), L"\r\nThe EMS ballots and the results' ballots cast match.");
            }
            else
            {
                int64_t d = s->ballots - res->ballots_total;
                fmt_count_w(d < 0 ? -d : d, n1, ARRAYSIZE(n1));
                StringCchPrintfW(line, ARRAYSIZE(line), L"\r\nThe EMS has %s %s ballot%s than the results report.",
                                 n1, d > 0 ? L"more" : L"fewer", (d == 1 || d == -1) ? L"" : L"s");
            }
            StringCchCatW(summary, scch, line);
        }
    }
    return TRUE;
}

/* --- Report window --------------------------------------------------------- */

/* A number ("1,234", "12.5%") or a report time ("11/05/2024 09:25:36PM") as a sort key. */
static BOOL AudRep_Key(const wchar_t *s, double *out)
{
    wchar_t digits[64];
    size_t n = 0;
    const wchar_t *p;
    unsigned mo, d, y, h, mi, sec;
    wchar_t ap[3] = L"";
    if (swscanf_s(s, L"%2u/%2u/%4u %2u:%2u:%2u%2ls", &mo, &d, &y, &h, &mi, &sec, ap, (unsigned)ARRAYSIZE(ap)) == 7)
    {
        h %= 12;
        if (ap[0] == L'P')
        {
            h += 12;
        }
        *out = (double)EeAudit_MakeTime((int)y, (int)mo, (int)d, (int)h, (int)mi, (int)sec);
        return TRUE;
    }
    for (p = s; *p != L'\0' && n + 1 < ARRAYSIZE(digits); p++)
    {
        if (*p == L',')
        {
            continue;
        }
        if (!(iswdigit(*p) || *p == L'.' || (*p == L'-' && n == 0) || (*p == L'%' && p[1] == L'\0')))
        {
            return FALSE;
        }
        if (*p != L'%')
        {
            digits[n++] = *p;
        }
    }
    digits[n] = L'\0';
    if (n == 0)
    {
        return FALSE;
    }
    *out = wcstod(digits, NULL);
    return TRUE;
}

typedef struct AudRepSortCtx
{
    const AudReportWindow *r;
    int col;
    BOOL asc;
} AudRepSortCtx;

static int __cdecl AudRep_SortCmp(void *ctx, const void *pa, const void *pb)
{
    const AudRepSortCtx *sc = (const AudRepSortCtx *)ctx;
    uint32_t a = *(const uint32_t *)pa, b = *(const uint32_t *)pb;
    BOOL ta = (sc->r->flags[a] & AUD_RF_TOTAL) != 0, tb = (sc->r->flags[b] & AUD_RF_TOTAL) != 0;
    const wchar_t *x = AudRep_Cell(sc->r, a, sc->col), *y = AudRep_Cell(sc->r, b, sc->col);
    double kx, ky;
    int r;
    if (ta != tb)
    {
        return ta ? 1 : -1; /* totals stay last */
    }
    if (AudRep_Key(x, &kx) && AudRep_Key(y, &ky))
    {
        r = kx < ky ? -1 : (kx > ky ? 1 : 0);
    }
    else
    {
        r = _wcsicmp(x, y);
    }
    if (!sc->asc)
    {
        r = -r;
    }
    return r != 0 ? r : (a < b ? -1 : (a > b ? 1 : 0));
}

static void AudRep_Sort(AudReportWindow *r)
{
    uint32_t i;
    for (i = 0; i < r->nrows; i++)
    {
        r->order[i] = i;
    }
    if (r->sort_col >= 0 && r->sort_col < r->ncols && r->nrows > 1)
    {
        AudRepSortCtx sc;
        sc.r = r;
        sc.col = r->sort_col;
        sc.asc = r->sort_asc;
        qsort_s(r->order, r->nrows, sizeof(uint32_t), AudRep_SortCmp, &sc);
    }
}

static void AudRep_Layout(AudReportWindow *r)
{
    RECT rc;
    int y = 0, lines = 1, h;
    const wchar_t *p;
    HDC dc;
    TEXTMETRICW tm;
    int line_h = Scale(r->aw->app, 16);
    if (r->hwnd == NULL)
    {
        return;
    }
    GetClientRect(r->hwnd, &rc);
    for (p = r->summary_text; *p != L'\0'; p++)
    {
        lines += *p == L'\n' ? 1 : 0;
    }
    dc = GetDC(r->summary);
    if (dc != NULL)
    {
        HFONT old = (HFONT)SelectObject(dc, r->aw->app->font_ui);
        if (GetTextMetricsW(dc, &tm))
        {
            line_h = tm.tmHeight + tm.tmExternalLeading;
        }
        SelectObject(dc, old);
        ReleaseDC(r->summary, dc);
    }
    /* long lines wrap: allow one extra line for every two */
    h = (lines + (lines + 1) / 2) * line_h + Scale(r->aw->app, 10);
    if (h > rc.bottom / 2)
    {
        h = rc.bottom / 2;
    }
    MoveWindow(r->summary, Scale(r->aw->app, 6), Scale(r->aw->app, 4), rc.right - Scale(r->aw->app, 12), h, TRUE);
    y = h + Scale(r->aw->app, 6);
    if (r->tab != NULL)
    {
        int tab_h = Scale(r->aw->app, 26);
        MoveWindow(r->tab, 0, y, rc.right, tab_h, TRUE);
        y += tab_h;
    }
    MoveWindow(r->list, 0, y, rc.right, rc.bottom > y ? rc.bottom - y : 0, TRUE);
}

/* Rebuild the report's rows, columns and summary from the audit window's state. */
static void AudRep_Refresh(AudReportWindow *r)
{
    HWND header;
    int c;
    BOOL ok;
    if (r == NULL || r->list == NULL)
    {
        return;
    }
    AudRep_FreeRows(r);
    r->summary_text[0] = L'\0';
    switch (r->kind)
    {
    case AUD_REP_COPIES:
        ok = AudRep_BuildCopies(r);
        break;
    case AUD_REP_TIMELINE:
        ok = AudRep_BuildTimeline(r);
        break;
    case AUD_REP_NOTABLE:
        ok = AudRep_BuildNotable(r);
        break;
    case AUD_REP_USERS:
        ok = AudRep_BuildUsers(r);
        break;
    case AUD_REP_RECON:
        ok = AudRep_BuildRecon(r);
        break;
    default:
        ok = AudRep_BuildAcct(r);
        break;
    }
    r->order = (uint32_t *)malloc(((size_t)r->nrows + 1) * sizeof(uint32_t));
    if (!ok || r->order == NULL)
    {
        AudRep_FreeRows(r);
        StringCchCopyW(r->summary_text, ARRAYSIZE(r->summary_text), L"Out of memory.");
    }
    SendMessageW(r->list, WM_SETREDRAW, FALSE, 0);
    header = ListView_GetHeader(r->list);
    while (header != NULL && Header_GetItemCount(header) > 0)
    {
        ListView_DeleteColumn(r->list, 0);
    }
    for (c = 0; c < r->ncols; c++)
    {
        LVCOLUMNW col;
        ZeroMemory(&col, sizeof(col));
        col.mask = LVCF_TEXT | LVCF_WIDTH | LVCF_FMT;
        col.fmt = (c > 0 && r->col_right[c]) ? LVCFMT_RIGHT : LVCFMT_LEFT;
        col.pszText = (wchar_t *)r->col_title[c];
        col.cx = ScaleDisplay(r->aw->app, r->col_width[c]);
        ListView_InsertColumn(r->list, c, &col);
    }
    r->sort_col = -1;
    if (r->order != NULL)
    {
        AudRep_Sort(r);
    }
    ListView_SetItemState(r->list, -1, 0, LVIS_SELECTED | LVIS_FOCUSED);
    ListView_SetItemCountEx(r->list, (int)r->nrows, 0);
    SendMessageW(r->list, WM_SETREDRAW, TRUE, 0);
    InvalidateRect(r->list, NULL, TRUE);
    SetWindowTextW(r->summary, r->summary_text);
    AudRep_Layout(r);
}

static void AudRep_CopySelected(AudReportWindow *r)
{
    wchar_t *buf = NULL;
    size_t cap = 0, len = 0;
    int i;
    for (i = ListView_GetNextItem(r->list, -1, LVNI_SELECTED); i >= 0; i = ListView_GetNextItem(r->list, i, LVNI_SELECTED))
    {
        int c;
        for (c = 0; c < r->ncols && (uint32_t)i < r->nrows; c++)
        {
            const wchar_t *cell = AudRep_Cell(r, r->order[i], c);
            size_t n = wcslen(cell);
            if (!cvr_wbuf_reserve(&buf, &cap, len + n + 3))
            {
                free(buf);
                return;
            }
            memcpy(buf + len, cell, n * sizeof(wchar_t));
            len += n;
            buf[len++] = c + 1 < r->ncols ? L'\t' : L'\r';
            if (c + 1 == r->ncols)
            {
                buf[len++] = L'\n';
            }
        }
    }
    if (buf != NULL && len > 0)
    {
        int u8len;
        buf[len] = L'\0';
        u8len = WideCharToMultiByte(CP_UTF8, 0, buf, -1, NULL, 0, NULL, NULL);
        if (u8len > 0)
        {
            char *utf8 = (char *)malloc((size_t)u8len);
            if (utf8 != NULL && WideCharToMultiByte(CP_UTF8, 0, buf, -1, utf8, u8len, NULL, NULL) > 0)
            {
                App_SetClipboardUtf8(r->hwnd, utf8);
            }
            free(utf8);
        }
    }
    free(buf);
}

typedef struct AudRepExportCtx
{
    const AudReportWindow *r;
    const uint32_t *idx; /* selected display rows, or NULL for all */
} AudRepExportCtx;

static void AudRep_ExportCell(void *user, uint32_t row, uint32_t col, wchar_t *buf, size_t cch)
{
    const AudRepExportCtx *x = (const AudRepExportCtx *)user;
    uint32_t item = x->idx != NULL ? x->idx[row] : row;
    StringCchCopyW(buf, cch, AudRep_Cell(x->r, x->r->order[item], (int)col));
}

/* Export a report's rows (the tab shown), in display order: the selected ones or all. */
static void AudRep_Export(AudReportWindow *r, BOOL selection_only)
{
    wchar_t suggested[400], tabname[64];
    AudRepExportCtx ctx;
    uint32_t *idx = NULL;
    uint32_t n = r->nrows;
    wchar_t *p;
    if (selection_only)
    {
        idx = App_CollectSelectedIndices(r->list, r->nrows, &n);
        if (n == 0)
        {
            free(idx);
            return;
        }
    }
    tabname[0] = L'\0';
    if (r->tab != NULL)
    {
        TCITEMW ti;
        ZeroMemory(&ti, sizeof(ti));
        ti.mask = TCIF_TEXT;
        ti.pszText = tabname;
        ti.cchTextMax = ARRAYSIZE(tabname);
        TabCtrl_GetItem(r->tab, r->tab_sel, &ti);
    }
    StringCchPrintfW(suggested, ARRAYSIZE(suggested), L"%s-%s%s%s%s", r->aw->base_name,
                     selection_only ? L"Selected_" : L"", k_AudRepTitles[r->kind], tabname[0] != L'\0' ? L"-" : L"",
                     tabname);
    for (p = suggested + wcslen(r->aw->base_name); *p != L'\0'; p++)
    {
        if (*p == L' ' || *p == L'&')
        {
            *p = L'_';
        }
    }
    ctx.r = r;
    ctx.idx = idx;
    App_ExportReportModel(r->hwnd, suggested, (uint32_t)r->ncols, r->col_title, n, AudRep_ExportCell, &ctx);
    free(idx);
}

/* The session of display row @p item on the Sessions tab, or -1. */
static int AudRep_RowSession(const AudReportWindow *r, int item)
{
    if (r->kind != AUD_REP_ACCT || r->tab_sel != AT_SESSIONS || item < 0 || (uint32_t)item >= r->nrows)
    {
        return -1;
    }
    return r->keys[r->order[item]];
}

/* The event of display row @p item (log * 1 + event * 2 in keys) in the reports whose rows are
 * events, or -1. */
static int AudRep_RowEvent(const AudReportWindow *r, int item)
{
    BOOL events = r->kind == AUD_REP_TIMELINE || r->kind == AUD_REP_USERS ||
                  (r->kind == AUD_REP_NOTABLE && r->tab_sel == NT_EVENTS);
    if (!events || item < 0 || (uint32_t)item >= r->nrows)
    {
        return -1;
    }
    return r->keys[r->order[item]];
}

/* The category of display row @p item on Notable Events' Categories tab, or -1. */
static int AudRep_RowCategory(const AudReportWindow *r, int item)
{
    if (r->kind != AUD_REP_NOTABLE || r->tab_sel != NT_CATEGORIES || item < 0 || (uint32_t)item >= r->nrows)
    {
        return -1;
    }
    return r->keys[r->order[item]];
}

/* Notable Events: show the Events tab, of one category (or all, -1). */
static void AudRep_ShowNotableEvents(AudReportWindow *r, int category)
{
    r->cat_filter = category;
    r->tab_sel = NT_EVENTS;
    if (r->tab != NULL)
    {
        TabCtrl_SetCurSel(r->tab, NT_EVENTS);
    }
    AudRep_Refresh(r);
}

/* Act on a double-click (or Enter) on display row @p item. */
static void AudRep_Activate(AudReportWindow *r, int item)
{
    int si = AudRep_RowSession(r, item);
    int ev = AudRep_RowEvent(r, item);
    int cat = AudRep_RowCategory(r, item);
    if (si >= 0)
    {
        Aud_SetSession(r->aw, si);
    }
    else if (ev >= 0)
    {
        Aud_GotoEvent(r->aw, ev % EE_AUDIT_NLOGS, (uint32_t)ev / EE_AUDIT_NLOGS);
    }
    else if (cat >= 0)
    {
        AudRep_ShowNotableEvents(r, cat);
    }
}

static void AudRep_OnContextMenu(AudReportWindow *r, int item, POINT screen)
{
    HMENU m;
    UINT cmd;
    int si = AudRep_RowSession(r, item);
    int ev = AudRep_RowEvent(r, item);
    int cat = AudRep_RowCategory(r, item);
    if (item >= 0 && !(ListView_GetItemState(r->list, item, LVIS_SELECTED) & LVIS_SELECTED))
    {
        ListView_SetItemState(r->list, -1, 0, LVIS_SELECTED | LVIS_FOCUSED);
        ListView_SetItemState(r->list, item, LVIS_SELECTED | LVIS_FOCUSED, LVIS_SELECTED | LVIS_FOCUSED);
    }
    m = CreatePopupMenu();
    if (m == NULL)
    {
        return;
    }
    AppendMenuW(m, MF_STRING, IDM_EDIT_COPY, L"&Copy");
    AppendMenuW(m, MF_SEPARATOR, 0, NULL);
    AppendMenuW(m, MF_STRING, IDM_EXPORT_SELECTED, L"Export &Selected…");
    AppendMenuW(m, MF_STRING, IDM_EXPORT_ALL, L"Export &All…");
    if (si >= 0)
    {
        AppendMenuW(m, MF_SEPARATOR, 0, NULL);
        AppendMenuW(m, MF_STRING | (si == r->aw->session ? MF_GRAYED : 0), IDM_AUD_USE_SESSION,
                    r->aw->acct.hart ? L"&Use This Task" : L"&Use This Session");
    }
    if (ev >= 0)
    {
        AppendMenuW(m, MF_SEPARATOR, 0, NULL);
        AppendMenuW(m, MF_STRING, IDM_AUD_GOTO_EVENT, L"&Go to Event");
    }
    if (cat >= 0)
    {
        AppendMenuW(m, MF_SEPARATOR, 0, NULL);
        AppendMenuW(m, MF_STRING, IDM_AUD_SHOW_EVENTS, L"&Show Its Events");
    }
    cmd = (UINT)TrackPopupMenu(m, TPM_RIGHTBUTTON | TPM_RETURNCMD, screen.x, screen.y, 0, r->hwnd, NULL);
    DestroyMenu(m);
    if (cmd == IDM_EDIT_COPY)
    {
        AudRep_CopySelected(r);
    }
    else if (cmd == IDM_EXPORT_SELECTED || cmd == IDM_EXPORT_ALL)
    {
        AudRep_Export(r, cmd == IDM_EXPORT_SELECTED);
    }
    else if (cmd == IDM_AUD_USE_SESSION || cmd == IDM_AUD_GOTO_EVENT || cmd == IDM_AUD_SHOW_EVENTS)
    {
        AudRep_Activate(r, item);
    }
}

static LRESULT AudRep_CustomDraw(AudReportWindow *r, NMLVCUSTOMDRAW *cd)
{
    switch (cd->nmcd.dwDrawStage)
    {
    case CDDS_PREPAINT:
        return CDRF_NOTIFYITEMDRAW;
    case CDDS_ITEMPREPAINT:
    {
        uint32_t ri = (uint32_t)cd->nmcd.dwItemSpec;
        uint8_t fl;
        if (ri >= r->nrows)
        {
            return CDRF_DODEFAULT;
        }
        fl = r->flags[r->order[ri]];
        if (fl & AUD_RF_RED)
        {
            cd->clrText = RGB(192, 0, 0);
        }
        else if (fl & AUD_RF_GREY)
        {
            cd->clrText = RGB(128, 128, 128);
        }
        if ((fl & (AUD_RF_BOLD | AUD_RF_TOTAL | AUD_RF_RED)) && r->aw->app->font_header != NULL)
        {
            SelectObject(cd->nmcd.hdc, r->aw->app->font_header);
            return CDRF_NEWFONT;
        }
        return CDRF_DODEFAULT;
    }
    default:
        return CDRF_DODEFAULT;
    }
}

static LRESULT CALLBACK AudRepListSubclass(HWND hwnd, UINT msg, WPARAM wParam, LPARAM lParam)
{
    if (msg == WM_NOTIFY)
    {
        NMHDR *nm = (NMHDR *)lParam;
        HWND header = ListView_GetHeader(hwnd);
        AudReportWindow *r = (AudReportWindow *)GetWindowLongPtrW(GetParent(hwnd), GWLP_USERDATA);
        if (r != NULL && nm != NULL && header != NULL && nm->hwndFrom == header && nm->code == NM_CUSTOMDRAW)
        {
            return App_HeaderCustomDraw(r->aw->app, (NMCUSTOMDRAW *)lParam, FALSE);
        }
    }
    return CallWindowProcW(g_old_audrep_list_proc, hwnd, msg, wParam, lParam);
}

/* After a zoom change in the audit window. */
static void AudRep_ApplyFont(AudReportWindow *r)
{
    if (r != NULL && r->aw->app->font_grid != NULL)
    {
        SendMessageW(r->list, WM_SETFONT, (WPARAM)r->aw->app->font_grid, TRUE);
        AudRep_Refresh(r); /* column widths follow the zoom */
    }
}

static LRESULT CALLBACK AudReportWndProc(HWND hwnd, UINT msg, WPARAM wParam, LPARAM lParam)
{
    AudReportWindow *r = (AudReportWindow *)GetWindowLongPtrW(hwnd, GWLP_USERDATA);
    switch (msg)
    {
    case WM_CREATE:
    {
        CREATESTRUCTW *cs = (CREATESTRUCTW *)lParam;
        AppState *app;
        r = (AudReportWindow *)cs->lpCreateParams;
        SetWindowLongPtrW(hwnd, GWLP_USERDATA, (LONG_PTR)r);
        r->hwnd = hwnd;
        app = r->aw->app;
        r->summary = CreateWindowExW(0, L"EDIT", L"", WS_CHILD | WS_VISIBLE | ES_MULTILINE | ES_READONLY | WS_VSCROLL,
                                     0, 0, 0, 0, hwnd, NULL, app->instance, NULL);
        if (r->kind == AUD_REP_ACCT || r->kind == AUD_REP_NOTABLE)
        {
            static const wchar_t *const k_acct_tabs[AT_COUNT] = {L"Sessions", L"Poll Places", L"Vote Types",
                                                                 L"Media Clears"};
            static const wchar_t *const k_hart_tabs[AT_COUNT] = {L"Tasks", L"Polling Places", L"Vote Types",
                                                                 L"Central Transfers"};
            static const wchar_t *const k_note_tabs[NT_COUNT] = {L"Categories", L"Events"};
            const wchar_t *const *tabs = r->kind != AUD_REP_ACCT ? k_note_tabs
                                         : Aud_Hart(r->aw)        ? k_hart_tabs
                                                                  : k_acct_tabs;
            int ntabs = r->kind == AUD_REP_ACCT ? AT_COUNT : NT_COUNT;
            int t;
            r->tab = CreateWindowExW(0, WC_TABCONTROLW, L"", WS_CHILD | WS_VISIBLE | WS_CLIPSIBLINGS, 0, 0, 0, 0, hwnd,
                                     NULL, app->instance, NULL);
            for (t = 0; t < ntabs && r->tab != NULL; t++)
            {
                TCITEMW ti;
                ZeroMemory(&ti, sizeof(ti));
                ti.mask = TCIF_TEXT;
                ti.pszText = (wchar_t *)tabs[t];
                TabCtrl_InsertItem(r->tab, t, &ti);
            }
            if (r->tab != NULL)
            {
                TabCtrl_SetCurSel(r->tab, r->tab_sel);
            }
        }
        r->list = CreateWindowExW(0, WC_LISTVIEWW, L"",
                                  WS_CHILD | WS_VISIBLE | LVS_REPORT | LVS_OWNERDATA | LVS_SHOWSELALWAYS, 0, 0, 0, 0,
                                  hwnd, NULL, app->instance, NULL);
        if (r->list == NULL || r->summary == NULL)
        {
            return -1;
        }
        {
            WNDPROC old = (WNDPROC)SetWindowLongPtrW(r->list, GWLP_WNDPROC, (LONG_PTR)AudRepListSubclass);
            if (g_old_audrep_list_proc == NULL)
            {
                g_old_audrep_list_proc = old;
            }
        }
        ListView_SetExtendedListViewStyle(r->list, LVS_EX_FULLROWSELECT | LVS_EX_DOUBLEBUFFER | LVS_EX_GRIDLINES);
        if (app->font_grid != NULL)
        {
            SendMessageW(r->list, WM_SETFONT, (WPARAM)app->font_grid, TRUE);
        }
        if (app->font_ui != NULL)
        {
            SendMessageW(r->summary, WM_SETFONT, (WPARAM)app->font_ui, TRUE);
            if (r->tab != NULL)
            {
                SendMessageW(r->tab, WM_SETFONT, (WPARAM)app->font_ui, TRUE);
            }
        }
        AudRep_Refresh(r);
        return 0;
    }

    case WM_SIZE:
        if (r != NULL)
        {
            AudRep_Layout(r);
        }
        return 0;

    case WM_CTLCOLORSTATIC:
        /* the read-only summary on the window's face, not grey-on-grey */
        if (r != NULL && (HWND)lParam == r->summary)
        {
            SetBkColor((HDC)wParam, GetSysColor(COLOR_WINDOW));
            return (LRESULT)GetSysColorBrush(COLOR_WINDOW);
        }
        break;

    case WM_SETFOCUS:
        if (r != NULL)
        {
            SetFocus(r->list);
        }
        return 0;

    case WM_COMMAND:
        if (r != NULL && LOWORD(wParam) == IDM_EDIT_COPY) /* Ctrl+C */
        {
            AudRep_CopySelected(r);
            return 0;
        }
        break;

    case WM_NOTIFY:
    {
        NMHDR *hdr = (NMHDR *)lParam;
        if (r == NULL)
        {
            break;
        }
        if (hdr->hwndFrom == r->tab && hdr->code == TCN_SELCHANGE)
        {
            r->tab_sel = TabCtrl_GetCurSel(r->tab);
            r->cat_filter = -1; /* a tab chosen by hand shows every event */
            AudRep_Refresh(r);
            return 0;
        }
        if (hdr->hwndFrom != r->list)
        {
            break;
        }
        if (hdr->code == LVN_GETDISPINFOW)
        {
            NMLVDISPINFOW *di = (NMLVDISPINFOW *)lParam;
            if ((di->item.mask & LVIF_TEXT) && di->item.iItem >= 0 && (uint32_t)di->item.iItem < r->nrows &&
                di->item.iSubItem < r->ncols && di->item.cchTextMax > 0)
            {
                StringCchCopyW(di->item.pszText, (size_t)di->item.cchTextMax,
                               AudRep_Cell(r, r->order[di->item.iItem], di->item.iSubItem));
            }
            return 0;
        }
        if (hdr->code == NM_CUSTOMDRAW)
        {
            return AudRep_CustomDraw(r, (NMLVCUSTOMDRAW *)lParam);
        }
        if (hdr->code == LVN_COLUMNCLICK)
        {
            NMLISTVIEW *nlv = (NMLISTVIEW *)lParam;
            if (nlv->iSubItem == r->sort_col)
            {
                r->sort_asc = !r->sort_asc;
            }
            else
            {
                r->sort_col = nlv->iSubItem;
                r->sort_asc = !r->col_right[nlv->iSubItem]; /* numbers largest first */
            }
            AudRep_Sort(r);
            InvalidateRect(r->list, NULL, FALSE);
            return 0;
        }
        if (hdr->code == NM_DBLCLK)
        {
            AudRep_Activate(r, ((LPNMITEMACTIVATE)lParam)->iItem);
            return 0;
        }
        if (hdr->code == NM_RCLICK)
        {
            LPNMITEMACTIVATE ia = (LPNMITEMACTIVATE)lParam;
            POINT screen = ia->ptAction;
            ClientToScreen(r->list, &screen);
            AudRep_OnContextMenu(r, ia->iItem, screen);
            return 0;
        }
        break;
    }

    case WM_CLOSE:
        DestroyWindow(hwnd);
        return 0;

    case WM_DESTROY:
        if (r != NULL)
        {
            if (r->aw->reps[r->kind] == r)
            {
                r->aw->reps[r->kind] = NULL;
            }
            AudRep_FreeRows(r);
            free(r->recon);
            free(r);
            SetWindowLongPtrW(hwnd, GWLP_USERDATA, 0);
        }
        return 0;

    default:
        break;
    }
    return DefWindowProcW(hwnd, msg, wParam, lParam);
}

/* Open (or bring forward) report @p kind of @p aw; a report with tabs opens on tab @p tab. */
static void App_ShowAuditReport(AuditWindow *aw, int kind, int tab)
{
    const wchar_t *const *k_titles = k_AudRepTitles;
    static const int k_widths[AUD_REP_COUNT] = {1360, 1160, 1420, 1320, 1180, 1400};
    AudReportWindow **slot = &aw->reps[kind];
    AudReportWindow *r;
    wchar_t label[320], title[400];
    RECT pr;
    int x = CW_USEDEFAULT, y = CW_USEDEFAULT;
    if (*slot != NULL)
    {
        if ((kind == AUD_REP_ACCT || kind == AUD_REP_NOTABLE) && (*slot)->tab_sel != tab && (*slot)->tab != NULL)
        {
            (*slot)->tab_sel = tab;
            (*slot)->cat_filter = -1;
            TabCtrl_SetCurSel((*slot)->tab, tab);
            AudRep_Refresh(*slot);
        }
        ShowWindow((*slot)->hwnd, SW_SHOWNORMAL);
        SetForegroundWindow((*slot)->hwnd);
        return;
    }
    r = (AudReportWindow *)calloc(1, sizeof(AudReportWindow));
    if (r == NULL)
    {
        return;
    }
    r->aw = aw;
    r->kind = kind;
    r->tab_sel = tab;
    r->cat_filter = -1;
    r->sort_col = -1;
    r->sort_asc = TRUE;
    Aud_ElectionLabel(aw, label, ARRAYSIZE(label));
    StringCchPrintfW(title, ARRAYSIZE(title), L"%s - %s", k_titles[kind], label);
    if (GetWindowRect(aw->hwnd, &pr))
    {
        x = pr.left + Scale(aw->app, 40);
        y = pr.top + Scale(aw->app, 40);
    }
    if (CreateWindowExW(0, k_AuditReportClassName, title, WS_OVERLAPPEDWINDOW | WS_CLIPCHILDREN, x, y,
                        Scale(aw->app, k_widths[kind]), Scale(aw->app, 600), NULL, NULL, aw->app->instance,
                        r) == NULL)
    {
        free(r);
        return;
    }
    *slot = r;
    ShowWindow(r->hwnd, SW_SHOWNORMAL);
    UpdateWindow(r->hwnd);
}


/* Compare → Check Ballot Counts: reconcile audit window @p audit's EMS ballots with CVR or results
 * window @p other, in its Ballot Reconciliation report. */
static void App_RunBallotRecon(HWND audit, HWND other)
{
    AuditWindow *aw = Aud_IsAuditWindow(audit) ? (AuditWindow *)GetWindowLongPtrW(audit, GWLP_USERDATA) : NULL;
    CvrWindow *cw = Res_IsCvrWindow(other) ? (CvrWindow *)GetWindowLongPtrW(other, GWLP_USERDATA) : NULL;
    ResultsWindow *rw = Res_IsResultsWindow(other) ? (ResultsWindow *)GetWindowLongPtrW(other, GWLP_USERDATA) : NULL;
    AudReconRow *rows = NULL;
    uint32_t nrows = 0;
    wchar_t summary[2048], title[300];
    AudReportWindow *r;
    if (aw == NULL || (cw == NULL && rw == NULL))
    {
        return;
    }
    if (!aw->have_acct || aw->session < 0)
    {
        MessageBoxW(other, L"The audit log window has no ballots to compare (no ballot batches or vDrive reads).",
                    L"Check Ballot Counts", MB_ICONINFORMATION | MB_OK);
        return;
    }
    if (!Aud_BuildReconSnapshot(aw, cw, rw, &rows, &nrows, summary, ARRAYSIZE(summary)))
    {
        free(rows);
        MessageBoxW(other, L"Out of memory.", L"Check Ballot Counts", MB_ICONERROR | MB_OK);
        return;
    }
    title[0] = L'\0';
    GetWindowTextW(other, title, ARRAYSIZE(title));
    if (aw->reps[AUD_REP_RECON] == NULL)
    {
        App_ShowAuditReport(aw, AUD_REP_RECON, -1); /* opens empty, then filled below */
    }
    r = aw->reps[AUD_REP_RECON];
    if (r == NULL)
    {
        free(rows);
        return;
    }
    free(r->recon);
    r->recon = rows;
    r->nrecon = nrows;
    StringCchCopyW(r->recon_summary, ARRAYSIZE(r->recon_summary), summary);
    StringCchCopyW(r->recon_other_title, ARRAYSIZE(r->recon_other_title), cw != NULL ? L"CVR" : L"Results");
    {
        wchar_t label[320], wt[700];
        Aud_ElectionLabel(aw, label, ARRAYSIZE(label));
        StringCchPrintfW(wt, ARRAYSIZE(wt), L"Ballot Reconciliation - %s with %.240s", label, title);
        SetWindowTextW(r->hwnd, wt);
    }
    AudRep_Refresh(r);
    ShowWindow(r->hwnd, SW_SHOWNORMAL);
    SetForegroundWindow(r->hwnd);
}

/* --- Options (zoom) --------------------------------------------------------- */

typedef struct AudOptData
{
    AppState *app;
    int zoom;
    wchar_t words[128]; /* the Hart test task words ("" = the built-in list) */
} AudOptData;

static INT_PTR CALLBACK AudOptionsDlgProc(HWND dlg, UINT msg, WPARAM wParam, LPARAM lParam)
{
    AudOptData *d = (AudOptData *)GetWindowLongPtrW(dlg, GWLP_USERDATA);
    switch (msg)
    {
    case WM_INITDIALOG:
    {
        AppState *app;
        RECT rc;
        int margin, btn_w, btn_h, gap, y;
        HWND ctl[8];
        int nctl = 0, i;
        wchar_t ztxt[16];
        d = (AudOptData *)lParam;
        SetWindowLongPtrW(dlg, GWLP_USERDATA, (LONG_PTR)d);
        app = d->app;
        App_CenterModalClient(dlg, app, Scale(app, 420), Scale(app, 196));
        GetClientRect(dlg, &rc);
        margin = Scale(app, 14);
        btn_w = Scale(app, 84);
        btn_h = Scale(app, 26);
        gap = Scale(app, 8);
        y = margin;
        ctl[nctl++] = CreateWindowExW(0, L"STATIC", L"Zoom (%):", WS_CHILD | WS_VISIBLE, margin, y + Scale(app, 3),
                                      Scale(app, 80), Scale(app, 18), dlg, NULL, app->instance, NULL);
        StringCchPrintfW(ztxt, ARRAYSIZE(ztxt), L"%d", d->zoom);
        ctl[nctl++] = CreateWindowExW(WS_EX_CLIENTEDGE, L"EDIT", ztxt, WS_CHILD | WS_VISIBLE | WS_TABSTOP | ES_NUMBER,
                                      margin + Scale(app, 84), y, Scale(app, 64), Scale(app, 22), dlg,
                                      (HMENU)(INT_PTR)IDC_AUD_ZOOM, app->instance, NULL);
        ctl[nctl++] = CreateWindowExW(0, UPDOWN_CLASSW, NULL,
                                      WS_CHILD | WS_VISIBLE | UDS_SETBUDDYINT | UDS_ALIGNRIGHT | UDS_ARROWKEYS |
                                          UDS_NOTHOUSANDS,
                                      0, 0, 0, 0, dlg, (HMENU)(INT_PTR)IDC_AUD_ZOOM_SPIN, app->instance, NULL);
        SendMessageW(ctl[nctl - 1], UDM_SETBUDDY, (WPARAM)ctl[nctl - 2], 0);
        SendMessageW(ctl[nctl - 1], UDM_SETRANGE32, (WPARAM)k_ZoomMin, (LPARAM)k_ZoomMax);
        SendMessageW(ctl[nctl - 1], UDM_SETPOS32, 0, (LPARAM)d->zoom);
        y += Scale(app, 34);
        ctl[nctl++] = CreateWindowExW(0, L"STATIC",
                                      L"Hart Verity logs: a task is a test task when its name holds one of these "
                                      L"words (comma separated; empty for the built-in list):",
                                      WS_CHILD | WS_VISIBLE, margin, y, rc.right - 2 * margin, Scale(app, 34), dlg, NULL,
                                      app->instance, NULL);
        y += Scale(app, 38);
        ctl[nctl++] = CreateWindowExW(WS_EX_CLIENTEDGE, L"EDIT", d->words[0] != L'\0' ? d->words : EeAudit_DefaultTestTaskWords(),
                                      WS_CHILD | WS_VISIBLE | WS_TABSTOP | ES_AUTOHSCROLL, margin, y,
                                      rc.right - 2 * margin, Scale(app, 22), dlg, (HMENU)(INT_PTR)IDC_AUD_TEST_WORDS,
                                      app->instance, NULL);
        SendMessageW(ctl[nctl - 1], EM_LIMITTEXT, ARRAYSIZE(d->words) - 1, 0);
        ctl[nctl++] = CreateWindowExW(0, L"BUTTON", L"OK",
                                      WS_CHILD | WS_VISIBLE | WS_TABSTOP | WS_GROUP | BS_DEFPUSHBUTTON,
                                      rc.right - margin - 2 * btn_w - gap, rc.bottom - margin - btn_h, btn_w, btn_h,
                                      dlg, (HMENU)(INT_PTR)IDOK, app->instance, NULL);
        ctl[nctl++] = CreateWindowExW(0, L"BUTTON", L"Cancel", WS_CHILD | WS_VISIBLE | WS_TABSTOP,
                                      rc.right - margin - btn_w, rc.bottom - margin - btn_h, btn_w, btn_h, dlg,
                                      (HMENU)(INT_PTR)IDCANCEL, app->instance, NULL);
        for (i = 0; i < nctl; i++)
        {
            if (ctl[i] != NULL && app->font_ui != NULL)
            {
                SendMessageW(ctl[i], WM_SETFONT, (WPARAM)app->font_ui, TRUE);
            }
        }
        SetFocus(GetDlgItem(dlg, IDC_AUD_ZOOM));
        SendMessageW(GetDlgItem(dlg, IDC_AUD_ZOOM), EM_SETSEL, 0, -1);
        return (INT_PTR)FALSE;
    }

    case WM_COMMAND:
        if (LOWORD(wParam) == IDOK && d != NULL)
        {
            BOOL parsed = FALSE;
            UINT z = GetDlgItemInt(dlg, IDC_AUD_ZOOM, &parsed, FALSE);
            if (!parsed || (int)z < k_ZoomMin || (int)z > k_ZoomMax)
            {
                wchar_t msg_text[96];
                StringCchPrintfW(msg_text, ARRAYSIZE(msg_text), L"Zoom must be a number from %d to %d.", k_ZoomMin,
                                 k_ZoomMax);
                MessageBoxW(dlg, msg_text, k_WindowTitle, MB_ICONWARNING | MB_OK);
                SetFocus(GetDlgItem(dlg, IDC_AUD_ZOOM));
                return (INT_PTR)TRUE;
            }
            d->zoom = (int)z;
            GetDlgItemTextW(dlg, IDC_AUD_TEST_WORDS, d->words, ARRAYSIZE(d->words));
            if (wcscmp(d->words, EeAudit_DefaultTestTaskWords()) == 0)
            {
                d->words[0] = L'\0'; /* the built-in list, kept as it changes */
            }
            EndDialog(dlg, 1);
            return (INT_PTR)TRUE;
        }
        if (LOWORD(wParam) == IDCANCEL)
        {
            EndDialog(dlg, 0);
            return (INT_PTR)TRUE;
        }
        break;

    case WM_CLOSE:
        EndDialog(dlg, 0);
        return (INT_PTR)TRUE;

    default:
        break;
    }
    return (INT_PTR)FALSE;
}

/* Count the ballots again (the test task words changed): accounting, notable events, timeline. */
static void Aud_Recount(AuditWindow *aw)
{
    int k;
    EeAudit_FreeNotes(&aw->notes);
    EeAudit_FreeTimeline(&aw->tl);
    if (aw->have_acct)
    {
        EeAudit_FreeAccounting(&aw->acct);
    }
    aw->have_acct = (aw->el->logs[0].ncopies > 0 || EeAudit_IsHart(aw->el)) &&
                    EeAudit_ElectionAccounting(aw->el, &aw->acct);
    aw->session = Aud_DefaultSession(aw);
    aw->have_notes = EeAudit_Notable(aw->el, &aw->notes);
    aw->have_tl = aw->have_acct && EeAudit_Timeline(aw->el, &aw->acct, &aw->tl);
    Aud_Rebuild(aw);
    for (k = 0; k < AUD_REP_COUNT; k++)
    {
        if (aw->reps[k] != NULL)
        {
            AudRep_Refresh(aw->reps[k]);
        }
    }
}

static void App_ShowAuditOptions(AuditWindow *aw)
{
    AudOptData d;
    d.app = aw->app;
    d.zoom = aw->app->zoom_percent > 0 ? aw->app->zoom_percent : k_ZoomDefault;
    StringCchCopyW(d.words, ARRAYSIZE(d.words), g_settings.audit_test_words);
    if (App_RunModalDialogOwned(aw->app, aw->hwnd, L"Audit Log Options", AudOptionsDlgProc, (LPARAM)&d) != 1)
    {
        return;
    }
    if (wcscmp(d.words, g_settings.audit_test_words) != 0)
    {
        StringCchCopyW(g_settings.audit_test_words, ARRAYSIZE(g_settings.audit_test_words), d.words);
        EeSettings_Save(&g_settings);
        EeAudit_SetTestTaskWords(d.words);
        if (EeAudit_IsHart(aw->el))
        {
            Aud_Recount(aw); /* other open windows count again when reopened */
        }
    }
    if (d.zoom == aw->app->zoom_percent)
    {
        return;
    }
    aw->app->zoom_percent = App_ClampZoom(d.zoom);
    App_UpdateDpiMetrics(aw->app, aw->app->dpi); /* rebuilds the zoomed grid font */
    if (aw->app->font_grid != NULL)
    {
        SendMessageW(aw->list, WM_SETFONT, (WPARAM)aw->app->font_grid, TRUE);
    }
    g_settings.zoom_percent = aw->app->zoom_percent;
    EeSettings_Save(&g_settings);
    Aud_Rebuild(aw); /* column widths follow the zoom */
    {
        int k;
        for (k = 0; k < AUD_REP_COUNT; k++)
        {
            AudRep_ApplyFont(aw->reps[k]);
        }
    }
}

/* --- Window ---------------------------------------------------------------- */

/* Menu bar of an audit window; View -> Copy lists the election's copies. */
static HMENU App_CreateAuditMenu(const AuditWindow *aw)
{
    HMENU menu = CreateMenu();
    HMENU file_menu = CreatePopupMenu();
    HMENU edit_menu = CreatePopupMenu();
    HMENU view_menu = CreatePopupMenu();
    HMENU log_menu = CreatePopupMenu();
    HMENU copy_menu = CreatePopupMenu();
    HMENU filter_menu = CreatePopupMenu();
    HMENU reports_menu = CreatePopupMenu();
    HMENU compare_menu = CreatePopupMenu();
    HMENU help_menu = CreatePopupMenu();
    int li;
    if (menu == NULL || file_menu == NULL || edit_menu == NULL || view_menu == NULL || log_menu == NULL ||
        copy_menu == NULL || filter_menu == NULL || reports_menu == NULL || compare_menu == NULL ||
        help_menu == NULL)
    {
        HMENU all[10];
        int i;
        all[0] = file_menu;
        all[1] = edit_menu;
        all[2] = view_menu;
        all[3] = log_menu;
        all[4] = copy_menu;
        all[5] = filter_menu;
        all[6] = reports_menu;
        all[7] = compare_menu;
        all[8] = help_menu;
        all[9] = menu;
        for (i = 0; i < 10; i++)
        {
            if (all[i] != NULL)
            {
                DestroyMenu(all[i]);
            }
        }
        return NULL;
    }
    AppendMenuW(file_menu, MF_STRING, IDM_FILE_OPEN_VOTER_LIST, L"&Load Voter List…\tCtrl+O");
    AppendMenuW(file_menu, MF_STRING, IDM_FILE_OPEN_CVR, L"Load Cast &Vote Records…");
    AppendMenuW(file_menu, MF_STRING, IDM_FILE_OPEN_ROSTER, L"Load Voter &Roster…");
    AppendMenuW(file_menu, MF_STRING, IDM_FILE_OPEN_RESULTS, L"Load Election Re&sults…");
    AppendMenuW(file_menu, MF_STRING, IDM_FILE_OPEN_AUDIT, L"Load &Audit Logs…");
    AppendMenuW(file_menu, MF_STRING, IDM_FILE_CLOSE_AUDIT, L"&Close Audit Logs");
    AppendMenuW(file_menu, MF_SEPARATOR, 0, NULL);
    AppendMenuW(file_menu, MF_STRING, IDM_FILE_EXPORT_AUDIT, L"&Export…");
    AppendMenuW(file_menu, MF_SEPARATOR, 0, NULL);
    AppendMenuW(file_menu, MF_STRING, IDM_FILE_EXIT, L"E&xit");
    AppendMenuW(edit_menu, MF_STRING, IDM_EDIT_COPY, L"&Copy\tCtrl+C");
    AppendMenuW(edit_menu, MF_SEPARATOR, 0, NULL);
    AppendMenuW(edit_menu, MF_STRING, IDM_AUD_OPTIONS, L"&Options…");
    AppendMenuW(log_menu, MF_STRING, IDM_AUD_LOG_ALL, L"&All");
    if (Aud_Hart(aw))
    {
        AppendMenuW(log_menu, MF_STRING, IDM_AUD_LOG_HART_COUNT, L"Verity&Count");
        AppendMenuW(log_menu, MF_STRING, IDM_AUD_LOG_HART_CENTRAL, L"VerityC&entral");
        AppendMenuW(log_menu, MF_STRING, IDM_AUD_LOG_HART_DATA, L"Verity&Data");
    }
    if (!Aud_Hart(aw) || Aud_Log(aw, 0)->ncopies > 0 || Aud_Log(aw, 1)->ncopies > 0)
    {
        AppendMenuW(log_menu, MF_STRING, IDM_AUD_LOG_ELECTION, L"&Election");
        AppendMenuW(log_menu, MF_STRING, IDM_AUD_LOG_ADMIN, L"A&dmin");
    }
    AppendMenuW(copy_menu, MF_STRING, IDM_AUD_COPY_MERGED, L"All Copies &Merged");
    for (li = 0; li < EE_AUDIT_NLOGS; li++)
    {
        const EeAuditLog *log = Aud_Log(aw, li);
        uint32_t ci;
        if (log->ncopies > 0)
        {
            AppendMenuW(copy_menu, MF_SEPARATOR, 0, NULL);
        }
        for (ci = 0; ci < log->ncopies; ci++)
        {
            wchar_t label[400];
            Aud_CopyLabel(aw, li, ci, label, ARRAYSIZE(label));
            AppendMenuW(copy_menu, MF_STRING, IDM_AUD_COPY_FIRST + li * EE_AUDIT_MAX_COPIES + ci, label);
        }
    }
    AppendMenuW(view_menu, MF_STRING, IDM_AUD_VIEW_EVENTS, L"&Events");
    AppendMenuW(view_menu, MF_STRING, IDM_AUD_VIEW_BATCHES, Aud_Hart(aw) ? L"vDrive &Reads" : L"Ballot &Batches");
    AppendMenuW(view_menu, MF_SEPARATOR, 0, NULL);
    AppendMenuW(view_menu, MF_POPUP, (UINT_PTR)log_menu, L"&Log");
    AppendMenuW(view_menu, MF_POPUP, (UINT_PTR)copy_menu, L"&Copy");
    AppendMenuW(view_menu, MF_SEPARATOR, 0, NULL);
    AppendMenuW(view_menu, MF_STRING, IDM_AUD_VIEW_RESET, L"&Reset View");
    AppendMenuW(filter_menu, MF_STRING, IDM_FILTER_EDIT, L"&Filter…\tCtrl+L");
    AppendMenuW(filter_menu, MF_STRING, IDM_FILTER_RESET, L"&Reset Filter");
    AppendMenuW(reports_menu, MF_STRING, IDM_AUD_REP_COPIES, L"&Copies Check…");
    AppendMenuW(reports_menu, MF_STRING, IDM_AUD_REP_ACCT, L"Ballot &Accounting…");
    AppendMenuW(reports_menu, MF_STRING, IDM_AUD_REP_TIMELINE, L"L&&A &Testing Timeline…");
    AppendMenuW(reports_menu, MF_STRING, IDM_AUD_REP_NOTABLE, L"&Notable Events…");
    AppendMenuW(reports_menu, MF_STRING, IDM_AUD_REP_USERS, L"&Users and Workstations…");
    AppendMenuW(reports_menu, MF_SEPARATOR, 0, NULL);
    AppendMenuW(reports_menu, MF_STRING, IDM_AUD_SELECT_SESSION, Aud_Hart(aw) ? L"&Select Task…" : L"&Select Session…");
    AppendMenuW(help_menu, MF_STRING, IDM_HELP_LOADING, L"&Loading Audit Logs");
    AppendMenuW(help_menu, MF_STRING, IDM_HELP_OPTIONS, L"&Views and Options");
    AppendMenuW(help_menu, MF_STRING, IDM_HELP_FILTERS, L"&Filters");
    AppendMenuW(help_menu, MF_STRING, IDM_HELP_REPORTS, L"&Reports");
    AppendMenuW(help_menu, MF_STRING, IDM_HELP_COMPARE, L"&Compare");
    AppendMenuW(help_menu, MF_STRING, IDM_HELP_EXPORT, L"&Export");
    AppendMenuW(compare_menu, MF_STRING | MF_GRAYED, 0, L"(open a CVR or results window to check ballot counts)");
    AppendMenuW(help_menu, MF_SEPARATOR, 0, NULL);
    AppendMenuW(help_menu, MF_STRING, IDM_HELP_ABOUT, L"&About Election Explorer…");
    AppendMenuW(menu, MF_POPUP, (UINT_PTR)file_menu, L"&File");
    AppendMenuW(menu, MF_POPUP, (UINT_PTR)edit_menu, L"&Edit");
    AppendMenuW(menu, MF_POPUP, (UINT_PTR)view_menu, L"&View");
    AppendMenuW(menu, MF_POPUP, (UINT_PTR)filter_menu, L"F&ilter");
    AppendMenuW(menu, MF_POPUP, (UINT_PTR)reports_menu, L"&Reports");
    AppendMenuW(menu, MF_POPUP, (UINT_PTR)compare_menu, L"&Compare");
    AppendMenuW(menu, MF_POPUP, (UINT_PTR)help_menu, L"&Help");
    return menu;
}

/* Subclass so the list draws its header bold on the grey band, like the voter list. */
static LRESULT CALLBACK AuditListSubclass(HWND hwnd, UINT msg, WPARAM wParam, LPARAM lParam)
{
    if (msg == WM_NOTIFY)
    {
        NMHDR *nm = (NMHDR *)lParam;
        HWND header = ListView_GetHeader(hwnd);
        AuditWindow *aw = (AuditWindow *)GetWindowLongPtrW(GetParent(hwnd), GWLP_USERDATA);
        if (aw != NULL && nm != NULL && header != NULL && nm->hwndFrom == header && nm->code == NM_CUSTOMDRAW)
        {
            return App_HeaderCustomDraw(aw->app, (NMCUSTOMDRAW *)lParam, FALSE);
        }
    }
    return CallWindowProcW(g_old_audit_list_proc, hwnd, msg, wParam, lParam);
}

static void Aud_FreeWindowData(AuditWindow *aw)
{
    free(aw->rows);
    free(aw->rules);
    EeFilter_Clear(&aw->filters);
    EeAudit_FreeNotes(&aw->notes);
    EeAudit_FreeTimeline(&aw->tl);
    if (aw->have_acct)
    {
        EeAudit_FreeAccounting(&aw->acct);
    }
    EeAudit_FreeElections(aw->el, 1);
    App_FreeCvrUi(aw->app);
    free(aw);
}

static LRESULT CALLBACK AuditWndProc(HWND hwnd, UINT msg, WPARAM wParam, LPARAM lParam)
{
    AuditWindow *aw = (AuditWindow *)GetWindowLongPtrW(hwnd, GWLP_USERDATA);

    switch (msg)
    {
    case WM_CREATE:
    {
        CREATESTRUCTW *cs = (CREATESTRUCTW *)lParam;
        RECT rc;
        aw = (AuditWindow *)cs->lpCreateParams;
        SetWindowLongPtrW(hwnd, GWLP_USERDATA, (LONG_PTR)aw);
        aw->hwnd = hwnd;
        GetClientRect(hwnd, &rc);
        aw->list = CreateWindowExW(0, WC_LISTVIEWW, L"",
                                   WS_CHILD | WS_VISIBLE | LVS_REPORT | LVS_OWNERDATA | LVS_SHOWSELALWAYS, 0, 0,
                                   rc.right, rc.bottom, hwnd, NULL, aw->app->instance, NULL);
        if (aw->list == NULL)
        {
            return -1;
        }
        {
            WNDPROC old = (WNDPROC)SetWindowLongPtrW(aw->list, GWLP_WNDPROC, (LONG_PTR)AuditListSubclass);
            if (g_old_audit_list_proc == NULL)
            {
                g_old_audit_list_proc = old;
            }
        }
        ListView_SetExtendedListViewStyle(aw->list, LVS_EX_FULLROWSELECT | LVS_EX_DOUBLEBUFFER | LVS_EX_GRIDLINES);
        if (aw->app->font_grid != NULL)
        {
            SendMessageW(aw->list, WM_SETFONT, (WPARAM)aw->app->font_grid, TRUE);
        }
        aw->status = CreateWindowExW(0, STATUSCLASSNAMEW, NULL, WS_CHILD | WS_VISIBLE | SBARS_SIZEGRIP, 0, 0, 0, 0,
                                     hwnd, NULL, aw->app->instance, NULL);
        if (aw->status != NULL && aw->app->font_ui != NULL)
        {
            SendMessageW(aw->status, WM_SETFONT, (WPARAM)aw->app->font_ui, TRUE);
        }
        Aud_Layout(aw, rc.right, rc.bottom);
        Aud_Rebuild(aw);
        return 0;
    }

    case WM_SIZE:
        if (aw != NULL)
        {
            Aud_Layout(aw, LOWORD(lParam), HIWORD(lParam));
        }
        return 0;

    case WM_SETFOCUS:
        if (aw != NULL && aw->list != NULL)
        {
            SetFocus(aw->list);
        }
        return 0;

    case WM_INITMENUPOPUP:
        if (aw != NULL)
        {
            HMENU m = (HMENU)wParam;
            int li;
            BOOL events = aw->view == AUD_VIEW_EVENTS;
            BOOL merged = events && aw->show_copy == AUD_MERGED;
            CheckMenuItem(m, IDM_AUD_VIEW_EVENTS, MF_BYCOMMAND | (events ? MF_CHECKED : MF_UNCHECKED));
            CheckMenuItem(m, IDM_AUD_VIEW_BATCHES, MF_BYCOMMAND | (events ? MF_UNCHECKED : MF_CHECKED));
            EnableMenuItem(m, IDM_AUD_VIEW_BATCHES, MF_BYCOMMAND | (aw->session >= 0 ? MF_ENABLED : MF_GRAYED));
            EnableMenuItem(m, IDM_AUD_SELECT_SESSION, MF_BYCOMMAND | (aw->session >= 0 ? MF_ENABLED : MF_GRAYED));
            CheckMenuItem(m, IDM_AUD_LOG_ALL, MF_BYCOMMAND | (merged && aw->show_log == 0 ? MF_CHECKED : MF_UNCHECKED));
            for (li = 0; li < EE_AUDIT_NLOGS; li++)
            {
                UINT id = Aud_LogMenuId(li + 1);
                CheckMenuItem(m, id, MF_BYCOMMAND | (merged && aw->show_log == li + 1 ? MF_CHECKED : MF_UNCHECKED));
                EnableMenuItem(m, id, MF_BYCOMMAND | (Aud_Log(aw, li)->ncopies > 0 ? MF_ENABLED : MF_GRAYED));
            }
            CheckMenuItem(m, IDM_AUD_COPY_MERGED, MF_BYCOMMAND | (merged ? MF_CHECKED : MF_UNCHECKED));
            for (li = 0; li < EE_AUDIT_NLOGS; li++)
            {
                uint32_t ci;
                for (ci = 0; ci < Aud_Log(aw, li)->ncopies; ci++)
                {
                    BOOL on = events && !merged && aw->show_copy_log == li && aw->show_copy == (int)ci;
                    CheckMenuItem(m, IDM_AUD_COPY_FIRST + li * EE_AUDIT_MAX_COPIES + ci,
                                  MF_BYCOMMAND | (on ? MF_CHECKED : MF_UNCHECKED));
                }
            }
            EnableMenuItem(m, IDM_AUD_VIEW_RESET, MF_BYCOMMAND | (aw->sort_col >= 0 ? MF_ENABLED : MF_GRAYED));
            EnableMenuItem(m, IDM_FILTER_RESET, MF_BYCOMMAND | (aw->filters.count > 0 ? MF_ENABLED : MF_GRAYED));
            /* Compare (File, Edit, View, Filter, Reports, Compare, Help) */
            if (m == GetSubMenu(GetMenu(hwnd), 5))
            {
                int i, n = 0;
                while (GetMenuItemCount(m) > 0)
                {
                    DeleteMenu(m, 0, MF_BYPOSITION);
                }
                for (i = 0; i < g_cvr_window_count && n < (int)ARRAYSIZE(aw->cmp_targets); i++)
                {
                    HWND h = g_cvr_windows[i];
                    wchar_t t[300], item[340];
                    if (!Res_IsCvrWindow(h) && !Res_IsResultsWindow(h))
                    {
                        continue;
                    }
                    t[0] = L'\0';
                    GetWindowTextW(h, t, ARRAYSIZE(t));
                    StringCchPrintfW(item, ARRAYSIZE(item), L"Check Ballot Counts with %.200s", t);
                    aw->cmp_targets[n] = h;
                    AppendMenuW(m, MF_STRING, (UINT_PTR)(IDM_COMPARE_WITH_FIRST + n), item);
                    n++;
                }
                if (n == 0)
                {
                    AppendMenuW(m, MF_STRING | MF_GRAYED, 0, L"(open a CVR or results window to check ballot counts)");
                }
            }
        }
        break;

    case WM_COMMAND:
        if (aw == NULL)
        {
            break;
        }
        if (LOWORD(wParam) >= IDM_COMPARE_WITH_FIRST &&
            LOWORD(wParam) < IDM_COMPARE_WITH_FIRST + ARRAYSIZE(aw->cmp_targets))
        {
            HWND target = aw->cmp_targets[LOWORD(wParam) - IDM_COMPARE_WITH_FIRST];
            if (target != NULL && IsWindow(target))
            {
                App_RunBallotRecon(hwnd, target);
            }
            return 0;
        }
        if (LOWORD(wParam) >= IDM_AUD_COPY_FIRST &&
            LOWORD(wParam) < IDM_AUD_COPY_FIRST + EE_AUDIT_NLOGS * EE_AUDIT_MAX_COPIES)
        {
            int k = LOWORD(wParam) - IDM_AUD_COPY_FIRST;
            int li = k / EE_AUDIT_MAX_COPIES;
            int ci = k % EE_AUDIT_MAX_COPIES;
            if ((uint32_t)ci < Aud_Log(aw, li)->ncopies)
            {
                aw->view = AUD_VIEW_EVENTS;
                aw->show_copy_log = li;
                aw->show_copy = ci;
                Aud_Rebuild(aw);
            }
            return 0;
        }
        switch (LOWORD(wParam))
        {
        case IDM_EDIT_COPY: /* also Ctrl+C */
            Aud_CopySelected(aw);
            return 0;
        case IDM_FILE_EXPORT_AUDIT:
            App_ExportAudit(aw, FALSE);
            return 0;
        case IDM_HELP_EXPORT:
            App_ShowHelpTopicOn(aw->app, hwnd, L"Help — Export", k_AudHelpExport);
            return 0;
        case IDM_AUD_OPTIONS:
            App_ShowAuditOptions(aw);
            return 0;
        case IDM_AUD_VIEW_EVENTS:
        case IDM_AUD_VIEW_BATCHES:
        {
            int want = LOWORD(wParam) == IDM_AUD_VIEW_BATCHES ? AUD_VIEW_BATCHES : AUD_VIEW_EVENTS;
            if (want != aw->view && (want == AUD_VIEW_EVENTS || aw->session >= 0))
            {
                aw->view = want;
                Aud_Rebuild(aw);
            }
            return 0;
        }
        case IDM_AUD_LOG_ALL:
        case IDM_AUD_LOG_ELECTION:
        case IDM_AUD_LOG_ADMIN:
        case IDM_AUD_LOG_HART_COUNT:
        case IDM_AUD_LOG_HART_CENTRAL:
        case IDM_AUD_LOG_HART_DATA:
        {
            int kind;
            aw->show_log = 0;
            for (kind = 1; kind <= EE_AUDIT_NLOGS; kind++)
            {
                if (LOWORD(wParam) == Aud_LogMenuId(kind))
                {
                    aw->show_log = kind;
                }
            }
            aw->view = AUD_VIEW_EVENTS;
            aw->show_copy = AUD_MERGED;
            Aud_Rebuild(aw);
            return 0;
        }
        case IDM_AUD_COPY_MERGED:
            if (aw->show_copy != AUD_MERGED || aw->view != AUD_VIEW_EVENTS)
            {
                aw->view = AUD_VIEW_EVENTS;
                aw->show_copy = AUD_MERGED;
                Aud_Rebuild(aw);
            }
            return 0;
        case IDM_AUD_REP_COPIES:
            App_ShowAuditReport(aw, AUD_REP_COPIES, 0);
            return 0;
        case IDM_AUD_REP_ACCT:
            App_ShowAuditReport(aw, AUD_REP_ACCT, aw->reps[AUD_REP_ACCT] != NULL ? aw->reps[AUD_REP_ACCT]->tab_sel : AT_SESSIONS);
            return 0;
        case IDM_AUD_SELECT_SESSION:
            App_ShowAuditReport(aw, AUD_REP_ACCT, AT_SESSIONS);
            return 0;
        case IDM_AUD_REP_TIMELINE:
            App_ShowAuditReport(aw, AUD_REP_TIMELINE, 0);
            return 0;
        case IDM_AUD_REP_NOTABLE:
            App_ShowAuditReport(aw, AUD_REP_NOTABLE, NT_CATEGORIES);
            return 0;
        case IDM_AUD_REP_USERS:
            App_ShowAuditReport(aw, AUD_REP_USERS, 0);
            return 0;
        case IDM_FILTER_EDIT: /* also Ctrl+L */
            App_ShowAuditFilter(aw);
            return 0;
        case IDM_FILTER_RESET:
            Aud_ResetFilter(aw);
            return 0;
        case IDM_HELP_FILTERS:
            App_ShowHelpTopicOn(aw->app, hwnd, L"Help — Filters", k_AudHelpFilters);
            return 0;
        case IDM_HELP_COMPARE:
            App_ShowHelpTopicOn(aw->app, hwnd, L"Help — Compare", k_AudHelpCompare);
            return 0;
        case IDM_HELP_REPORTS:
            App_ShowHelpTopicOn(aw->app, hwnd, L"Help — Reports", k_AudHelpReports);
            return 0;
        case IDM_AUD_VIEW_RESET:
            if (aw->sort_col >= 0)
            {
                aw->sort_col = -1;
                Aud_Sort(aw);
                InvalidateRect(aw->list, NULL, FALSE);
                Aud_UpdateSelectionStatus(aw);
            }
            return 0;
        case IDM_FILE_OPEN_VOTER_LIST:
            App_BeginOpenVoterList(aw->app);
            return 0;
        case IDM_FILE_OPEN_CVR:
            App_BeginOpenCvr(aw->app);
            return 0;
        case IDM_FILE_OPEN_ROSTER:
            App_BeginOpenRoster(aw->app);
            return 0;
        case IDM_FILE_OPEN_RESULTS:
            App_BeginOpenResults(aw->app);
            return 0;
        case IDM_FILE_OPEN_AUDIT:
            App_BeginOpenAudit(aw->app);
            return 0;
        case IDM_FILE_CLOSE_AUDIT:
            DestroyWindow(hwnd);
            return 0;
        case IDM_FILE_EXIT:
            App_ExitAll();
            return 0;
        case IDM_HELP_LOADING:
            App_ShowHelpTopicOn(aw->app, hwnd, L"Help — Loading Audit Logs", k_AudHelpLoading);
            return 0;
        case IDM_HELP_OPTIONS:
            App_ShowHelpTopicOn(aw->app, hwnd, L"Help — Views and Options", k_AudHelpOptions);
            return 0;
        case IDM_HELP_ABOUT:
            App_ShowAboutOn(aw->app, hwnd);
            return 0;
        default:
            break;
        }
        break;

    case WM_NOTIFY:
    {
        NMHDR *hdr = (NMHDR *)lParam;
        if (aw == NULL || hdr->hwndFrom != aw->list)
        {
            break;
        }
        if (hdr->code == LVN_GETDISPINFOW)
        {
            NMLVDISPINFOW *di = (NMLVDISPINFOW *)lParam;
            if ((di->item.mask & LVIF_TEXT) && di->item.iItem >= 0 && di->item.cchTextMax > 0)
            {
                Aud_CellText(aw, (uint32_t)di->item.iItem, di->item.iSubItem, di->item.pszText,
                             (size_t)di->item.cchTextMax);
            }
            return 0;
        }
        if (hdr->code == NM_CUSTOMDRAW)
        {
            return Aud_CustomDraw(aw, (NMLVCUSTOMDRAW *)lParam);
        }
        if (hdr->code == LVN_ITEMCHANGED)
        {
            NMLISTVIEW *nlv = (NMLISTVIEW *)lParam;
            if ((nlv->uChanged & LVIF_STATE) && ((nlv->uNewState ^ nlv->uOldState) & LVIS_FOCUSED))
            {
                Aud_UpdateSelectionStatus(aw);
            }
            return 0;
        }
        if (hdr->code == LVN_COLUMNCLICK)
        {
            NMLISTVIEW *nlv = (NMLISTVIEW *)lParam;
            if (nlv->iSubItem == aw->sort_col)
            {
                aw->sort_asc = !aw->sort_asc;
            }
            else
            {
                aw->sort_col = nlv->iSubItem;
                /* copy and ballot counts sort most first; times oldest first (printed order
                 * is newest first, and Reset View returns to it) */
                aw->sort_asc = aw->col_kind[aw->sort_col] != AC_COPIES && aw->col_kind[aw->sort_col] != BC_BALLOTS;
            }
            Aud_Sort(aw);
            InvalidateRect(aw->list, NULL, FALSE);
            Aud_UpdateSelectionStatus(aw);
            return 0;
        }
        if (hdr->code == NM_DBLCLK && aw->view == AUD_VIEW_BATCHES)
        {
            int item = ((LPNMITEMACTIVATE)lParam)->iItem;
            if (item >= 0 && (uint32_t)item < aw->nrows)
            {
                Aud_GotoBatchEvent(aw, aw->rows[item].idx);
            }
            return 0;
        }
        if (hdr->code == NM_RCLICK)
        {
            LPNMITEMACTIVATE ia = (LPNMITEMACTIVATE)lParam;
            POINT screen = ia->ptAction;
            ClientToScreen(aw->list, &screen);
            Aud_OnContextMenu(aw, ia->iItem, screen);
            return 0;
        }
        break;
    }

    case WM_CLOSE:
        DestroyWindow(hwnd);
        return 0;

    case WM_DESTROY:
        if (aw != NULL)
        {
            if (aw->hwnd_filter != NULL)
            {
                DestroyWindow(aw->hwnd_filter); /* clears aw->hwnd_filter */
            }
            {
                int k;
                for (k = 0; k < AUD_REP_COUNT; k++)
                {
                    if (aw->reps[k] != NULL)
                    {
                        DestroyWindow(aw->reps[k]->hwnd); /* clears aw->reps[k] */
                    }
                }
            }
            Aud_FreeWindowData(aw);
            CvrWindow_Unregister(hwnd);
            SetWindowLongPtrW(hwnd, GWLP_USERDATA, 0);
            App_MaybeQuit();
        }
        return 0;

    default:
        break;
    }
    return DefWindowProcW(hwnd, msg, wParam, lParam);
}

/* Create an audit window for one election, taking ownership of @p el (moved; the source is
 * zeroed). Like a results window it is a first-class top-level window with its own
 * resource-only AppState, so it outlives the window that loaded it. */
static void App_CreateAuditWindow(AppState *app, EeAuditElection *el)
{
    AuditWindow *aw = (AuditWindow *)calloc(1, sizeof(AuditWindow));
    EeAuditElection *mine = (EeAuditElection *)malloc(sizeof(EeAuditElection));
    AppState *ui = NULL;
    HWND h = NULL;
    wchar_t label[320], title[360];
    int li;
    if (aw == NULL || mine == NULL ||
        (ui = (AppState *)HeapAlloc(GetProcessHeap(), HEAP_ZERO_MEMORY, sizeof(AppState))) == NULL)
    {
        free(aw);
        free(mine); /* @p el stays with the caller, who frees it */
        MessageBoxW(app->hwnd_main, L"Out of memory.", k_WindowTitle, MB_ICONERROR | MB_OK);
        return;
    }
    *mine = *el; /* move */
    ZeroMemory(el, sizeof(*el));
    App_InitViewerState(ui, app->instance, app); /* seed zoom from the launcher */
    ui->is_cvr_ui = TRUE;                        /* resource-only, like a CVR window's */
    App_UpdateDpiMetrics(ui, app->dpi ? app->dpi : 96u);
    aw->app = ui;
    aw->el = mine;
    aw->show_copy = AUD_MERGED;
    aw->sort_col = -1;
    aw->sort_asc = TRUE;
    EeFilter_Init(&aw->filters);
    for (li = 0; li < EE_AUDIT_NLOGS; li++)
    {
        if (mine->logs[li].ncopies > 0 && aw->base_name[0] == L'\0')
        {
            App_BaseNameNoExt(mine->logs[li].copies[0].path, aw->base_name, ARRAYSIZE(aw->base_name));
        }
    }
    for (li = 0; li < EE_AUDIT_NLOGS; li++)
    {
        const EeAuditLog *log = &mine->logs[li];
        uint32_t i;
        if (log->ncopies < 2)
        {
            continue;
        }
        for (i = 0; i < log->nevents; i++)
        {
            if ((log->events[i].copies & (1ull << (log->ncopies - 1))) == 0)
            {
                aw->lacking[li]++;
            }
        }
    }
    if (mine->logs[0].ncopies > 0 || EeAudit_IsHart(mine))
    {
        aw->have_acct = EeAudit_ElectionAccounting(mine, &aw->acct);
    }
    aw->session = Aud_DefaultSession(aw);
    aw->have_notes = EeAudit_Notable(mine, &aw->notes);
    aw->have_tl = aw->have_acct && EeAudit_Timeline(mine, &aw->acct, &aw->tl);
    Aud_ElectionLabel(aw, label, ARRAYSIZE(label));
    StringCchPrintfW(title, ARRAYSIZE(title), L"%s \x2014 Audit Logs", label);
    {
        HMENU menu = App_CreateAuditMenu(aw);
        h = CreateWindowExW(0, k_AuditClassName, title, WS_OVERLAPPEDWINDOW | WS_CLIPCHILDREN, CW_USEDEFAULT,
                            CW_USEDEFAULT, Scale(ui, 1240), Scale(ui, 660), NULL, menu, ui->instance, aw);
        if (h == NULL && menu != NULL)
        {
            DestroyMenu(menu);
        }
    }
    if (h == NULL)
    {
        Aud_FreeWindowData(aw);
        MessageBoxW(app->hwnd_main, L"Could not create the Audit Log window.", k_WindowTitle, MB_ICONERROR | MB_OK);
        return;
    }
    ui->hwnd_main = h;
    CvrWindow_Register(h); /* counts toward App_MaybeQuit and closes on Exit */
    ShowWindow(h, SW_SHOWNORMAL);
    UpdateWindow(h);
}

/* --- Loading (worker thread behind a modal progress dialog) --------------- */

#define EEM_AUD_LOAD_DONE     (WM_APP + 17)
#define EEM_AUD_LOAD_PROGRESS (WM_APP + 18)

typedef struct AudLoadJob
{
    AppState *app;
    const wchar_t *const *paths;
    int count;
    EeAuditElection *elections; /* out: one per election */
    uint32_t nelections;
    volatile LONG cancel;
    EeLoadStatus status;
    wchar_t err[512];
    HWND dlg;
    HWND bar;
    HANDLE thread;
} AudLoadJob;

static BOOL AudLoadProgressCb(const EeLoadProgress *pr, void *user)
{
    AudLoadJob *j = (AudLoadJob *)user;
    if (j != NULL && j->dlg != NULL)
    {
        PostMessageW(j->dlg, EEM_AUD_LOAD_PROGRESS, (WPARAM)(pr->percent > 100u ? 100u : pr->percent), 0);
    }
    return j == NULL || j->cancel == 0;
}

static DWORD WINAPI AudLoadThreadProc(void *param)
{
    AudLoadJob *j = (AudLoadJob *)param;
    j->status = EeAudit_LoadFiles(j->paths, (uint32_t)j->count, &j->elections, &j->nelections, &j->cancel,
                                  AudLoadProgressCb, j, j->err, ARRAYSIZE(j->err));
    PostMessageW(j->dlg, EEM_AUD_LOAD_DONE, 0, 0);
    return 0;
}

static INT_PTR CALLBACK AudLoadDlgProc(HWND dlg, UINT msg, WPARAM wParam, LPARAM lParam)
{
    AudLoadJob *j = (AudLoadJob *)GetWindowLongPtrW(dlg, GWLP_USERDATA);
    switch (msg)
    {
    case WM_INITDIALOG:
    {
        AppState *app;
        RECT rc;
        int margin;
        HWND label, cancel;
        j = (AudLoadJob *)lParam;
        SetWindowLongPtrW(dlg, GWLP_USERDATA, (LONG_PTR)j);
        j->dlg = dlg;
        app = j->app;
        App_CenterModalClient(dlg, app, Scale(app, 340), Scale(app, 120));
        GetClientRect(dlg, &rc);
        margin = Scale(app, 14);
        label = CreateWindowExW(0, L"STATIC", L"Loading audit logs…", WS_CHILD | WS_VISIBLE, margin, margin,
                                rc.right - 2 * margin, Scale(app, 20), dlg, NULL, app->instance, NULL);
        j->bar = CreateWindowExW(0, PROGRESS_CLASSW, NULL, WS_CHILD | WS_VISIBLE, margin, margin + Scale(app, 26),
                                 rc.right - 2 * margin, Scale(app, 16), dlg, NULL, app->instance, NULL);
        cancel = CreateWindowExW(0, L"BUTTON", L"Cancel", WS_CHILD | WS_VISIBLE | WS_TABSTOP,
                                 rc.right - margin - Scale(app, 84), rc.bottom - margin - Scale(app, 26),
                                 Scale(app, 84), Scale(app, 26), dlg, (HMENU)(INT_PTR)IDCANCEL, app->instance, NULL);
        if (app->font_ui != NULL)
        {
            SendMessageW(label, WM_SETFONT, (WPARAM)app->font_ui, TRUE);
            SendMessageW(cancel, WM_SETFONT, (WPARAM)app->font_ui, TRUE);
        }
        if (j->bar != NULL)
        {
            SendMessageW(j->bar, PBM_SETRANGE32, 0, 100);
        }
        j->thread = CreateThread(NULL, 0, AudLoadThreadProc, j, 0, NULL);
        if (j->thread == NULL)
        {
            j->status = EeLoadStatus_Error;
            StringCchCopyW(j->err, ARRAYSIZE(j->err), L"Could not start loading.");
            EndDialog(dlg, 0);
        }
        return (INT_PTR)TRUE;
    }

    case EEM_AUD_LOAD_PROGRESS:
        if (j != NULL && j->bar != NULL)
        {
            SendMessageW(j->bar, PBM_SETPOS, wParam, 0);
        }
        return (INT_PTR)TRUE;

    case EEM_AUD_LOAD_DONE:
        if (j != NULL && j->thread != NULL)
        {
            WaitForSingleObject(j->thread, INFINITE);
            CloseHandle(j->thread);
            j->thread = NULL;
        }
        EndDialog(dlg, 1);
        return (INT_PTR)TRUE;

    case WM_COMMAND:
        if (j != NULL && LOWORD(wParam) == IDCANCEL)
        {
            InterlockedExchange(&j->cancel, 1);
            EnableWindow(GetDlgItem(dlg, IDCANCEL), FALSE);
            return (INT_PTR)TRUE;
        }
        break;

    case WM_CLOSE:
        if (j != NULL)
        {
            InterlockedExchange(&j->cancel, 1);
        }
        return (INT_PTR)TRUE;

    default:
        break;
    }
    return (INT_PTR)FALSE;
}

static void App_BeginOpenAudit(AppState *app)
{
    OPENFILENAMEW ofn;
    wchar_t *files;
    wchar_t *paths[256];
    int count = 0;
    int i;
    AudLoadJob job;
    const DWORD files_cch = 65536;

    if (app == NULL)
    {
        return;
    }
    files = (wchar_t *)malloc(files_cch * sizeof(wchar_t));
    if (files == NULL)
    {
        return;
    }
    files[0] = L'\0';
    ZeroMemory(&ofn, sizeof(ofn));
    ofn.lStructSize = sizeof(ofn);
    ofn.hwndOwner = app->hwnd_main;
    ofn.lpstrFilter = L"Audit logs and exports (*.pdf;*.zip;*.csv;*.tsv)\0*.pdf;*.zip;*.csv;*.tsv\0"
                      L"Audit log PDFs (*.pdf;*.zip)\0*.pdf;*.zip\0"
                      L"Audit log exports (*.csv;*.tsv)\0*.csv;*.tsv\0All files (*.*)\0*.*\0";
    ofn.lpstrFile = files;
    ofn.nMaxFile = files_cch;
    ofn.Flags = OFN_FILEMUSTEXIST | OFN_PATHMUSTEXIST | OFN_EXPLORER | OFN_ALLOWMULTISELECT;
    ofn.lpstrTitle = L"Load Audit Logs";
    if (!GetOpenFileNameW(&ofn))
    {
        free(files);
        return;
    }
    /* A single full path, or a directory followed by NUL-separated names. */
    {
        const wchar_t *dir = files;
        size_t dlen = wcslen(dir);
        const wchar_t *p = files + dlen + 1;
        if (*p == L'\0')
        {
            paths[count] = _wcsdup(files);
            if (paths[count] != NULL)
            {
                count++;
            }
        }
        else
        {
            BOOL need_sep = (dlen > 0 && dir[dlen - 1] != L'\\');
            while (*p != L'\0' && count < (int)ARRAYSIZE(paths))
            {
                size_t need = dlen + 1 + wcslen(p) + 1;
                wchar_t *full = (wchar_t *)malloc(need * sizeof(wchar_t));
                if (full == NULL)
                {
                    break;
                }
                StringCchCopyW(full, need, dir);
                if (need_sep)
                {
                    StringCchCatW(full, need, L"\\");
                }
                StringCchCatW(full, need, p);
                paths[count++] = full;
                p += wcslen(p) + 1;
            }
        }
    }
    free(files);
    if (count == 0)
    {
        return;
    }

    ZeroMemory(&job, sizeof(job));
    job.app = app;
    job.paths = (const wchar_t *const *)paths;
    job.count = count;
    job.status = EeLoadStatus_Error;
    App_RunModalDialog(app, L"Loading Audit Logs", AudLoadDlgProc, (LPARAM)&job);
    if (job.status == EeLoadStatus_Ok)
    {
        uint32_t g;
        for (g = 0; g < job.nelections; g++)
        {
            App_CreateAuditWindow(app, &job.elections[g]);
        }
        EeAudit_FreeElections(job.elections, job.nelections);
    }
    else if (job.status == EeLoadStatus_Error)
    {
        MessageBoxW(app->hwnd_main, job.err[0] != L'\0' ? job.err : L"Could not load the audit logs.", k_WindowTitle,
                    MB_ICONERROR | MB_OK);
    }
    for (i = 0; i < count; i++)
    {
        free(paths[i]);
    }
}

/* -------------------------------------------------------------------------- */
/* Voter Roster loading (worker thread behind a modal progress dialog)        */
/* -------------------------------------------------------------------------- */

#define EEM_ROSTER_LOAD_DONE     (WM_APP + 13)
#define EEM_ROSTER_LOAD_PROGRESS (WM_APP + 14)

typedef struct RosterLoadJob
{
    AppState *app;
    const wchar_t *const *paths;
    int count;
    EeVoterTable *table;
    EeRosterLoadInfo *info;
    volatile LONG cancel;
    EeLoadStatus status;
    wchar_t err[512];
    HWND dlg;
    HWND count_label; /* "N voter records" */
    HWND bar;         /* determinate (0..100), subclassed to draw eta[] over the fill */
    WNDPROC bar_proc; /* the progress bar's own window procedure */
    wchar_t eta[96];  /* text centered on the bar: time remaining */
    ULONGLONG start_tick;
    ULONGLONG eta_tick; /* when eta[] last changed (updated at most twice a second) */
    HANDLE thread;
} RosterLoadJob;

static BOOL RosterLoadProgressCb(const EeLoadProgress *pr, void *user)
{
    RosterLoadJob *j = (RosterLoadJob *)user;
    if (j != NULL && j->dlg != NULL)
    {
        uint32_t pct = (pr->percent > 100u) ? 100u : pr->percent;
        PostMessageW(j->dlg, EEM_ROSTER_LOAD_PROGRESS, (WPARAM)pr->rows_loaded, (LPARAM)pct);
    }
    return TRUE; /* cancellation is driven by j->cancel, checked by the loader */
}

static DWORD WINAPI RosterLoadThreadProc(void *param)
{
    RosterLoadJob *j = (RosterLoadJob *)param;
    j->status = EeRoster_LoadFiles(j->paths,
                                   j->count,
                                   j->table,
                                   j->info,
                                   &j->cancel,
                                   RosterLoadProgressCb,
                                   j,
                                   j->err,
                                   ARRAYSIZE(j->err));
    PostMessageW(j->dlg, EEM_ROSTER_LOAD_DONE, 0, 0);
    return 0;
}

/* The progress bar paints itself into a memory DC (WM_PRINTCLIENT), then the time
 * remaining is drawn centered over it, so the text never flickers as the bar moves. */
static LRESULT CALLBACK RosterBarProc(HWND hwnd, UINT msg, WPARAM wParam, LPARAM lParam)
{
    RosterLoadJob *j = (RosterLoadJob *)GetPropW(hwnd, L"EeRosterJob");
    WNDPROC base = (j != NULL) ? j->bar_proc : NULL;
    if (base == NULL)
    {
        return DefWindowProcW(hwnd, msg, wParam, lParam);
    }
    if (msg == WM_PAINT)
    {
        PAINTSTRUCT ps;
        RECT rc;
        HDC dc = BeginPaint(hwnd, &ps);
        HDC mem;
        HBITMAP bmp;
        HGDIOBJ old_bmp;
        HGDIOBJ old_font = NULL;
        GetClientRect(hwnd, &rc);
        mem = CreateCompatibleDC(dc);
        bmp = CreateCompatibleBitmap(dc, rc.right, rc.bottom);
        if (mem != NULL && bmp != NULL)
        {
            old_bmp = SelectObject(mem, bmp);
            FillRect(mem, &rc, GetSysColorBrush(COLOR_BTNFACE));
            CallWindowProcW(base, hwnd, WM_PRINTCLIENT, (WPARAM)mem, PRF_CLIENT | PRF_ERASEBKGND);
            if (j->eta[0] != L'\0')
            {
                if (j->app != NULL && j->app->font_ui != NULL)
                {
                    old_font = SelectObject(mem, j->app->font_ui);
                }
                SetBkMode(mem, TRANSPARENT);
                SetTextColor(mem, GetSysColor(COLOR_WINDOWTEXT));
                DrawTextW(mem, j->eta, -1, &rc, DT_CENTER | DT_VCENTER | DT_SINGLELINE | DT_NOPREFIX);
                if (old_font != NULL)
                {
                    SelectObject(mem, old_font);
                }
            }
            BitBlt(dc, 0, 0, rc.right, rc.bottom, mem, 0, 0, SRCCOPY);
            SelectObject(mem, old_bmp);
        }
        if (bmp != NULL)
        {
            DeleteObject(bmp);
        }
        if (mem != NULL)
        {
            DeleteDC(mem);
        }
        EndPaint(hwnd, &ps);
        return 0;
    }
    if (msg == WM_ERASEBKGND)
    {
        return 1; /* painted in WM_PAINT */
    }
    if (msg == WM_NCDESTROY)
    {
        SetWindowLongPtrW(hwnd, GWLP_WNDPROC, (LONG_PTR)base);
        RemovePropW(hwnd, L"EeRosterJob");
        return CallWindowProcW(base, hwnd, msg, wParam, lParam);
    }
    return CallWindowProcW(base, hwnd, msg, wParam, lParam);
}

/* "About N minutes remaining" from the elapsed time and percent done. */
static void Roster_FormatEta(RosterLoadJob *j, int pct)
{
    ULONGLONG now = GetTickCount64();
    ULONGLONG elapsed = now - j->start_tick;
    if (pct >= 100)
    {
        StringCchCopyW(j->eta, ARRAYSIZE(j->eta), L"Finishing…");
        return;
    }
    if (pct < 3 || elapsed < 1500)
    {
        StringCchCopyW(j->eta, ARRAYSIZE(j->eta), L"Estimating time remaining…");
        return;
    }
    if (j->eta_tick != 0 && now - j->eta_tick < 500)
    {
        return; /* keep the text steady */
    }
    j->eta_tick = now;
    {
        ULONGLONG rem_ms = elapsed * (ULONGLONG)(100 - pct) / (ULONGLONG)pct;
        unsigned long secs = (unsigned long)((rem_ms + 999) / 1000);
        if (secs < 5)
        {
            StringCchCopyW(j->eta, ARRAYSIZE(j->eta), L"A few seconds remaining");
        }
        else if (secs < 60)
        {
            secs = (secs + 4) / 5 * 5; /* 5-second steps */
            StringCchPrintfW(j->eta, ARRAYSIZE(j->eta), L"About %lu seconds remaining", secs);
        }
        else
        {
            unsigned long mins = (secs + 30) / 60;
            StringCchPrintfW(j->eta,
                             ARRAYSIZE(j->eta),
                             L"About %lu minute%s remaining",
                             mins,
                             mins == 1ul ? L"" : L"s");
        }
    }
}

static INT_PTR CALLBACK RosterLoadDlgProc(HWND dlg, UINT msg, WPARAM wParam, LPARAM lParam)
{
    RosterLoadJob *j = (RosterLoadJob *)GetWindowLongPtrW(dlg, GWLP_USERDATA);

    switch (msg)
    {
        case WM_INITDIALOG:
        {
            AppState *app;
            RECT rc;
            int margin;
            HWND label;
            HWND count;
            HWND bar;
            HWND cancel;
            j = (RosterLoadJob *)lParam;
            SetWindowLongPtrW(dlg, GWLP_USERDATA, (LONG_PTR)j);
            j->dlg = dlg;
            app = j->app;

            App_CenterModalClient(dlg, app, Scale(app, 380), Scale(app, 146));
            GetClientRect(dlg, &rc);
            margin = Scale(app, 14);
            label = CreateWindowExW(0,
                                    L"STATIC",
                                    L"Loading Voter Roster…",
                                    WS_CHILD | WS_VISIBLE | SS_LEFT,
                                    margin,
                                    margin,
                                    rc.right - 2 * margin,
                                    Scale(app, 20),
                                    dlg,
                                    NULL,
                                    app->instance,
                                    NULL);
            count = CreateWindowExW(0,
                                    L"STATIC",
                                    L"0 voter records",
                                    WS_CHILD | WS_VISIBLE | SS_LEFT,
                                    margin,
                                    margin + Scale(app, 22),
                                    rc.right - 2 * margin,
                                    Scale(app, 18),
                                    dlg,
                                    NULL,
                                    app->instance,
                                    NULL);
            j->count_label = count;
            bar = CreateWindowExW(0,
                                  PROGRESS_CLASSW,
                                  NULL,
                                  WS_CHILD | WS_VISIBLE,
                                  margin,
                                  margin + Scale(app, 46),
                                  rc.right - 2 * margin,
                                  Scale(app, 22),
                                  dlg,
                                  NULL,
                                  app->instance,
                                  NULL);
            j->bar = bar;
            cancel = CreateWindowExW(0,
                                     L"BUTTON",
                                     L"Cancel",
                                     WS_CHILD | WS_VISIBLE | WS_TABSTOP,
                                     rc.right - margin - Scale(app, 84),
                                     rc.bottom - margin - Scale(app, 26),
                                     Scale(app, 84),
                                     Scale(app, 26),
                                     dlg,
                                     (HMENU)(INT_PTR)IDCANCEL,
                                     app->instance,
                                     NULL);
            if (app->font_ui)
            {
                SendMessageW(label, WM_SETFONT, (WPARAM)app->font_ui, TRUE);
                SendMessageW(count, WM_SETFONT, (WPARAM)app->font_ui, TRUE);
                SendMessageW(cancel, WM_SETFONT, (WPARAM)app->font_ui, TRUE);
            }
            j->start_tick = GetTickCount64();
            StringCchCopyW(j->eta, ARRAYSIZE(j->eta), L"Estimating time remaining…");
            if (bar != NULL)
            {
                SendMessageW(bar, PBM_SETRANGE32, 0, 100);
                SendMessageW(bar, PBM_SETPOS, 0, 0);
                SetPropW(bar, L"EeRosterJob", (HANDLE)j);
                j->bar_proc =
                    (WNDPROC)SetWindowLongPtrW(bar, GWLP_WNDPROC, (LONG_PTR)RosterBarProc);
            }
            j->thread = CreateThread(NULL, 0, RosterLoadThreadProc, j, 0, NULL);
            if (j->thread == NULL)
            {
                j->status = EeLoadStatus_Error;
                StringCchCopyW(j->err, ARRAYSIZE(j->err), L"Could not start the load thread.");
                EndDialog(dlg, 0);
            }
            return (INT_PTR)FALSE;
        }

        case EEM_ROSTER_LOAD_PROGRESS:
            if (j != NULL)
            {
                unsigned long n = (unsigned long)wParam;
                int pct = (int)lParam;
                Roster_FormatEta(j, pct);
                if (j->bar != NULL)
                {
                    SendMessageW(j->bar, PBM_SETPOS, (WPARAM)pct, 0);
                    InvalidateRect(j->bar, NULL, FALSE);
                }
                if (j->count_label != NULL)
                {
                    wchar_t txt[96];
                    StringCchPrintfW(txt, ARRAYSIZE(txt), L"%lu voter record%s", n,
                                     n == 1ul ? L"" : L"s");
                    SetWindowTextW(j->count_label, txt);
                }
            }
            return (INT_PTR)TRUE;

        case EEM_ROSTER_LOAD_DONE:
            if (j != NULL && j->thread != NULL)
            {
                WaitForSingleObject(j->thread, INFINITE);
                CloseHandle(j->thread);
                j->thread = NULL;
            }
            EndDialog(dlg, 1);
            return (INT_PTR)TRUE;

        case WM_COMMAND:
            if (j != NULL && LOWORD(wParam) == IDCANCEL)
            {
                HWND b = GetDlgItem(dlg, IDCANCEL);
                InterlockedExchange(&j->cancel, 1);
                if (b != NULL)
                {
                    EnableWindow(b, FALSE);
                }
                return (INT_PTR)TRUE;
            }
            break;

        case WM_CLOSE:
            if (j != NULL)
            {
                InterlockedExchange(&j->cancel, 1);
            }
            return (INT_PTR)TRUE;

        default:
            break;
    }
    return (INT_PTR)FALSE;
}

/* Menu bar of a Voter Roster window. Compare stays at index k_CompareMenuPos. */
static HMENU App_CreateRosterMenu(void)
{
    HMENU menu = CreateMenu();
    HMENU file_menu = CreatePopupMenu();
    HMENU edit_menu = CreatePopupMenu();
    HMENU filter_menu = CreatePopupMenu();
    HMENU reports_menu = CreatePopupMenu();
    HMENU compare_menu = CreatePopupMenu();
    HMENU help_menu = CreatePopupMenu();
    AppendMenuW(file_menu, MF_STRING, IDM_FILE_OPEN_VOTER_LIST, L"&Load Voter List…	Ctrl+O");
    AppendMenuW(file_menu, MF_STRING, IDM_FILE_OPEN_CVR, L"Load Cast &Vote Records…");
    AppendMenuW(file_menu, MF_STRING, IDM_FILE_OPEN_ROSTER, L"Load Voter &Roster…");
    AppendMenuW(file_menu, MF_STRING, IDM_FILE_OPEN_RESULTS, L"Load Election Re&sults…");
    AppendMenuW(file_menu, MF_STRING, IDM_FILE_OPEN_AUDIT, L"Load &Audit Logs…");
    AppendMenuW(file_menu, MF_STRING, IDM_FILE_CLOSE_VOTER_LIST, L"&Close Voter Roster");
    AppendMenuW(file_menu, MF_STRING, IDM_FILE_EXPORT_VOTERS, L"&Export Voter Roster…");
    AppendMenuW(file_menu, MF_SEPARATOR, 0, NULL);
    AppendMenuW(file_menu, MF_STRING, IDM_FILE_EXIT, L"E&xit");
    AppendMenuW(edit_menu, MF_STRING, IDM_EDIT_COPY, L"&Copy	Ctrl+C");
    AppendMenuW(edit_menu, MF_SEPARATOR, 0, NULL);
    AppendMenuW(edit_menu, MF_STRING, IDM_EDIT_OPTIONS, L"&Options…");
    AppendMenuW(filter_menu, MF_STRING, IDM_FILTER_EDIT, L"&Filter…	Ctrl+L");
    AppendMenuW(filter_menu, MF_STRING, IDM_FILTER_RESET, L"&Reset Filter");
    AppendMenuW(filter_menu, MF_STRING, IDM_FILTER_DUP_VOTER_IDS, L"Show &Duplicate Voter IDs…");
    AppendMenuW(filter_menu, MF_SEPARATOR, 0, NULL);
    AppendMenuW(filter_menu, MF_STRING, IDM_FILTER_RESET_VIEW, L"Reset Vie&w…");
    AppendMenuW(reports_menu, MF_STRING, IDM_REPORT_PRECINCT, L"Display &Precinct Report…");
    AppendMenuW(reports_menu, MF_STRING, IDM_REPORT_VOTING_TOTALS, L"Display &Voting Totals…");
    AppendMenuW(reports_menu, MF_SEPARATOR, 0, NULL);
    AppendMenuW(reports_menu, MF_STRING, IDM_REPORT_LOAD_SUMMARY, L"Display &Load Summary…");
    AppendMenuW(help_menu, MF_STRING, IDM_HELP_LOADING, L"&Loading Voter Rosters");
    AppendMenuW(help_menu, MF_STRING, IDM_HELP_OPTIONS, L"&Options");
    AppendMenuW(help_menu, MF_STRING, IDM_HELP_FILTERS, L"&Filters");
    AppendMenuW(help_menu, MF_STRING, IDM_HELP_REPORTS, L"&Reports");
    AppendMenuW(help_menu, MF_STRING, IDM_HELP_COMPARE, L"&Compare");
    AppendMenuW(help_menu, MF_STRING, IDM_HELP_EXPORT, L"&Export");
    AppendMenuW(help_menu, MF_SEPARATOR, 0, NULL);
    AppendMenuW(help_menu, MF_STRING, IDM_HELP_ABOUT, L"&About Election Explorer…");
    AppendMenuW(menu, MF_POPUP, (UINT_PTR)file_menu, L"&File");
    AppendMenuW(menu, MF_POPUP, (UINT_PTR)edit_menu, L"&Edit");
    AppendMenuW(menu, MF_POPUP, (UINT_PTR)filter_menu, L"F&ilter");
    AppendMenuW(menu, MF_POPUP, (UINT_PTR)reports_menu, L"&Reports");
    AppendMenuW(menu, MF_POPUP, (UINT_PTR)compare_menu, L"&Compare");
    AppendMenuW(menu, MF_POPUP, (UINT_PTR)help_menu, L"&Help");
    return menu;
}

/* The roster window's columns: Voter ID, Precinct, Name (normalized), then the source
 * columns other than the VUID / name parts behind them (Voting Method, Date Voted,
 * Party, extras, Source File). */
static void App_BuildRosterColumns(AppState *app)
{
    static const wchar_t *const k_hidden[] = {L"VUID",        L"Precinct",    L"Last Name",
                                              L"First Name",  L"Middle Name", L"Name Suffix"};
    uint32_t c;
    app->nroster_cols = 0;
    app->roster_cols[app->nroster_cols++] = EE_COL_VOTER_ID;
    app->roster_cols[app->nroster_cols++] = EE_COL_PRECINCT;
    app->roster_cols[app->nroster_cols++] = EE_COL_NAME;
    for (c = EE_FROZEN_COLUMN_COUNT;
         c < app->table.column_count && app->nroster_cols < ARRAYSIZE(app->roster_cols);
         c++)
    {
        const wchar_t *t = app->table.column_titles[c] ? app->table.column_titles[c] : L"";
        BOOL hide = FALSE;
        size_t k;
        for (k = 0; k < ARRAYSIZE(k_hidden); k++)
        {
            if (_wcsicmp(t, k_hidden[k]) == 0)
            {
                hide = TRUE;
                break;
            }
        }
        if (!hide)
        {
            app->roster_cols[app->nroster_cols++] = c;
        }
    }
}

/* TRUE when the load summary says something beyond the counts (skipped or unread
 * files, recovered sheets, unusual or duplicate Voter IDs), so it opens after loading. */
static BOOL Roster_InfoIsNotable(const EeRosterLoadInfo *in)
{
    return in->repeated_files > 0 || in->copy_files > 0 || in->unsupported_files > 0 ||
           in->recovered_sheets > 0 || in->skipped_sheets > 0 || in->vuids_malformed > 0 ||
           in->vuids_missing > 0 || in->vuids_duplicated > 0;
}

/* File → Load Voter Roster… (any window): pick roster files, load them behind the
 * progress dialog, and open the result in a new Voter Roster window. */
static void App_BeginOpenRoster(AppState *app)
{
    OPENFILENAMEW ofn;
    wchar_t *files;
    wchar_t *paths[256];
    int count = 0;
    int i;
    EeVoterTable table;
    EeRosterLoadInfo info;
    RosterLoadJob job;
    wchar_t err[512];

    if (app == NULL)
    {
        return;
    }
    files = (wchar_t *)malloc(65536 * sizeof(wchar_t));
    if (files == NULL)
    {
        return;
    }
    files[0] = L'\0';

    ZeroMemory(&ofn, sizeof(ofn));
    ofn.lStructSize = sizeof(ofn);
    ofn.hwndOwner = app->hwnd_main;
    ofn.lpstrFilter = L"Voter rosters (*.zip;*.xlsx;*.xls;*.csv;*.tsv;*.txt;*.pdf)\0"
                      L"*.zip;*.xlsx;*.xls;*.csv;*.tsv;*.txt;*.pdf\0"
                      L"All files (*.*)\0*.*\0";
    ofn.lpstrFile = files;
    ofn.nMaxFile = 65536;
    ofn.Flags = OFN_FILEMUSTEXIST | OFN_PATHMUSTEXIST | OFN_EXPLORER | OFN_ALLOWMULTISELECT;
    ofn.lpstrTitle = L"Load Voter Roster";

    if (!GetOpenFileNameW(&ofn))
    {
        free(files);
        return;
    }

    /* Multi-select result: a single full path, or a directory followed by NUL-separated
     * file names, terminated by a double NUL. */
    {
        const wchar_t *dir = files;
        size_t dlen = wcslen(dir);
        const wchar_t *p = files + dlen + 1;
        if (*p == L'\0')
        {
            paths[count] = _wcsdup(files);
            if (paths[count] != NULL)
            {
                count++;
            }
        }
        else
        {
            BOOL need_sep = (dlen > 0 && dir[dlen - 1] != L'\\');
            while (*p != L'\0' && count < (int)ARRAYSIZE(paths))
            {
                size_t need = dlen + 1 + wcslen(p) + 1;
                wchar_t *full = (wchar_t *)malloc(need * sizeof(wchar_t));
                if (full == NULL)
                {
                    break;
                }
                StringCchCopyW(full, need, dir);
                if (need_sep)
                {
                    StringCchCatW(full, need, L"\\");
                }
                StringCchCatW(full, need, p);
                paths[count++] = full;
                p += wcslen(p) + 1;
            }
        }
    }
    free(files);
    if (count == 0)
    {
        return;
    }

    EeVoterTable_Init(&table);
    table.name_surname_first = app->name_surname_first;
    ZeroMemory(&info, sizeof(info));
    ZeroMemory(&job, sizeof(job));
    job.app = app;
    job.paths = (const wchar_t *const *)paths;
    job.count = count;
    job.table = &table;
    job.info = &info;
    job.status = EeLoadStatus_Error;
    App_RunModalDialog(app, L"Loading Voter Roster", RosterLoadDlgProc, (LPARAM)&job);
    StringCchCopyW(err, ARRAYSIZE(err), job.err);

    if (job.status == EeLoadStatus_Ok)
    {
        AppState *r = App_CreateViewerEx(app->instance, SW_SHOWNORMAL, app->hwnd_main, app, TRUE);
        if (r == NULL)
        {
            EeVoterTable_Clear(&table);
            EeRoster_FreeInfo(&info);
            MessageBoxW(app->hwnd_main, L"Could not open the Voter Roster window.", k_WindowTitle,
                        MB_ICONERROR | MB_OK);
        }
        else
        {
            const wchar_t *leaf = paths[0];
            const wchar_t *q;
            wchar_t title[MAX_PATH + 64];
            for (q = paths[0]; *q != L'\0'; q++)
            {
                if (*q == L'\\' || *q == L'/')
                {
                    leaf = q + 1;
                }
            }
            EeVoterTable_Clear(&r->table);
            r->table = table; /* move */
            r->table.name_surname_first = r->name_surname_first;
            EeVoterTable_Init(&table);
            r->roster_info = info; /* move */
            ZeroMemory(&info, sizeof(info));
            StringCchCopyW(r->load_path, ARRAYSIZE(r->load_path), paths[0]);
            if (count > 1)
            {
                StringCchPrintfW(title, ARRAYSIZE(title), L"%s (+%d more) — Voter Roster", leaf,
                                 count - 1);
            }
            else
            {
                StringCchPrintfW(title, ARRAYSIZE(title), L"%s — Voter Roster", leaf);
            }
            SetWindowTextW(r->hwnd_main, title);
            App_BuildRosterColumns(r);
            App_RebuildColumns(r);
            App_UpdateRowStatus(r);
            SetForegroundWindow(r->hwnd_main);
            SetFocus(r->hwnd_scroll);
            if (r->roster_info.note != NULL && Roster_InfoIsNotable(&r->roster_info))
            {
                App_ShowHelpTopicOn(r, r->hwnd_main, L"Voter Roster Load Summary",
                                    r->roster_info.note);
            }
        }
    }
    else
    {
        EeVoterTable_Clear(&table);
        EeRoster_FreeInfo(&info);
        if (job.status == EeLoadStatus_Error)
        {
            MessageBoxW(app->hwnd_main,
                        (err[0] != L'\0') ? err : L"Could not load the voter roster.",
                        k_WindowTitle,
                        MB_ICONERROR | MB_OK);
        }
    }

    for (i = 0; i < count; i++)
    {
        free(paths[i]);
    }
}

/* -------------------------------------------------------------------------- */
/* Voter Roster: Voting Totals report                                         */
/* -------------------------------------------------------------------------- */

#define RTOT_TOTAL_ROW UINT32_MAX /* row date index of a Total row */
#define RTOT_ALL_PARTIES (-1)     /* row party of an all-parties row */

enum
{
    RTOT_AVAIL = 0,      /* counts shown */
    RTOT_NOT_AVAILABLE,  /* no file or record of this method */
    RTOT_NOT_LOADED      /* its files were found but not read (PDF) */
};

struct RosterTotalsWindow
{
    AppState *app;
    HWND hwnd;
    HWND list;
    HWND status;
    HFONT font_bold;     /* bold font_ui for Total rows */
    EeRosterTotals tot;
    uint32_t *row_date;  /* display row -> date index or RTOT_TOTAL_ROW */
    int32_t *row_party;  /* display row -> party index or RTOT_ALL_PARTIES */
    uint32_t nrows;
    int method_col[EE_VM_COUNT]; /* list column of each method, or -1 */
    int avail[EE_VM_COUNT];
    int col_party;       /* -1 when the roster has no parties */
    int col_total;
    int ncols;
};

static WNDPROC g_old_rtot_list_proc = NULL;

/* Voters for a (date, party) cell group and method; date RTOT_TOTAL_ROW and party
 * RTOT_ALL_PARTIES sum over all dates / parties. */
static uint32_t RosterTotals_Count(const RosterTotalsWindow *rw, uint32_t date, int32_t party, int m)
{
    const EeRosterTotals *t = &rw->tot;
    uint32_t d0 = (date == RTOT_TOTAL_ROW) ? 0 : date;
    uint32_t d1 = (date == RTOT_TOTAL_ROW) ? t->ndates : date + 1;
    uint32_t p0 = (party == RTOT_ALL_PARTIES) ? 0 : (uint32_t)party;
    uint32_t p1 = (party == RTOT_ALL_PARTIES) ? t->nparties : (uint32_t)party + 1;
    uint32_t sum = 0;
    uint32_t d;
    uint32_t p;
    for (d = d0; d < d1; d++)
    {
        for (p = p0; p < p1; p++)
        {
            const uint32_t *c = &t->counts[((size_t)d * t->nparties + p) * EE_VM_COUNT];
            if (m >= 0)
            {
                sum += c[m];
            }
            else
            {
                int k;
                for (k = 0; k < EE_VM_COUNT; k++)
                {
                    sum += c[k];
                }
            }
        }
    }
    return sum;
}

/* "1234567" -> "1,234,567". */
static void RosterTotals_Group(uint32_t v, wchar_t *buf, size_t cch)
{
    wchar_t raw[16];
    size_t n;
    size_t i;
    size_t o = 0;
    StringCchPrintfW(raw, ARRAYSIZE(raw), L"%u", v);
    n = wcslen(raw);
    for (i = 0; i < n && o + 1 < cch; i++)
    {
        if (i > 0 && (n - i) % 3 == 0 && o + 2 < cch)
        {
            buf[o++] = L',';
        }
        buf[o++] = raw[i];
    }
    buf[(o < cch) ? o : cch - 1] = L'\0';
}

/* Cell text; @p display groups digits (copy / export use plain numbers). */
static void RosterTotals_CellText(const RosterTotalsWindow *rw, uint32_t row, int col, BOOL display,
                                  wchar_t *buf, size_t cch)
{
    uint32_t date;
    int32_t party;
    int m;
    buf[0] = L'\0';
    if (row >= rw->nrows || cch == 0)
    {
        return;
    }
    date = rw->row_date[row];
    party = rw->row_party[row];
    if (col == 0)
    {
        if (date == RTOT_TOTAL_ROW)
        {
            StringCchCopyW(buf, cch, L"Total");
        }
        else if (rw->tot.dates[date] == 0)
        {
            StringCchCopyW(buf, cch, L"(no date)");
        }
        else
        {
            uint32_t ymd = rw->tot.dates[date];
            StringCchPrintfW(buf, cch, L"%02u/%02u/%04u", (ymd / 100u) % 100u, ymd % 100u, ymd / 10000u);
        }
        return;
    }
    if (col == rw->col_party)
    {
        if (party == RTOT_ALL_PARTIES)
        {
            StringCchCopyW(buf, cch, L"All parties");
        }
        else if (rw->tot.parties[party][0] == '\0')
        {
            StringCchCopyW(buf, cch, L"(blank)");
        }
        else
        {
            MultiByteToWideChar(CP_UTF8, 0, rw->tot.parties[party], -1, buf, (int)cch);
        }
        return;
    }
    if (col == rw->col_total)
    {
        uint32_t v = RosterTotals_Count(rw, date, party, -1);
        if (display)
            RosterTotals_Group(v, buf, cch);
        else
            StringCchPrintfW(buf, cch, L"%u", v);
        return;
    }
    for (m = 0; m < EE_VM_COUNT; m++)
    {
        if (rw->method_col[m] == col)
        {
            if (rw->avail[m] == RTOT_NOT_AVAILABLE)
            {
                StringCchCopyW(buf, cch, L"not available");
            }
            else if (rw->avail[m] == RTOT_NOT_LOADED)
            {
                StringCchCopyW(buf, cch, L"not loaded");
            }
            else
            {
                uint32_t v = RosterTotals_Count(rw, date, party, m);
                if (display)
                    RosterTotals_Group(v, buf, cch);
                else
                    StringCchPrintfW(buf, cch, L"%u", v);
            }
            return;
        }
    }
}

static const wchar_t *RosterTotals_ColumnTitle(const RosterTotalsWindow *rw, int col)
{
    static const wchar_t *const k_titles[EE_VM_COUNT] = {L"Other",
                                                         L"Mail Ballot",
                                                         L"Early Vote In-Person",
                                                         L"Election Day In-Person",
                                                         L"Provisional",
                                                         L"Limited"};
    int m;
    if (col == 0)
        return L"Date Voted";
    if (col == rw->col_party)
        return L"Party";
    if (col == rw->col_total)
        return L"Total";
    for (m = 0; m < EE_VM_COUNT; m++)
    {
        if (rw->method_col[m] == col)
        {
            return k_titles[m];
        }
    }
    return L"";
}

/* Lay out columns and rows from rw->tot: one row per date (per party, plus an
 * all-parties row when several parties voted that day), then the Total rows. */
static BOOL RosterTotals_Build(RosterTotalsWindow *rw)
{
    const EeRosterTotals *t = &rw->tot;
    const EeRosterLoadInfo *in = &rw->app->roster_info;
    static const int k_order[] = {EE_VM_MAIL, EE_VM_EARLY, EE_VM_ELECTION_DAY, EE_VM_PROVISIONAL,
                                  EE_VM_LIMITED, EE_VM_NONE};
    size_t cap;
    uint32_t d;
    uint32_t p;
    size_t k;
    int col = 1;

    rw->col_party = t->has_party ? col++ : -1;
    for (k = 0; k < ARRAYSIZE(k_order); k++)
    {
        int m = k_order[k];
        rw->method_col[m] = -1;
        if (m == EE_VM_NONE && !t->method_present[EE_VM_NONE])
        {
            continue; /* "Other" only for unrecognized Voting Method values */
        }
        rw->method_col[m] = col++;
        if (t->method_present[m] || in->method_read[m])
            rw->avail[m] = RTOT_AVAIL;
        else if (in->method_source[m])
            rw->avail[m] = RTOT_NOT_LOADED;
        else
            rw->avail[m] = RTOT_NOT_AVAILABLE;
    }
    rw->col_total = col++;
    rw->ncols = col;

    cap = (size_t)t->ndates * (t->nparties + 1u) + t->nparties + 2u;
    rw->row_date = (uint32_t *)malloc(cap * sizeof(uint32_t));
    rw->row_party = (int32_t *)malloc(cap * sizeof(int32_t));
    if (rw->row_date == NULL || rw->row_party == NULL)
    {
        return FALSE;
    }
    rw->nrows = 0;
    for (d = 0; d < t->ndates; d++)
    {
        uint32_t shown = 0;
        if (!t->has_party)
        {
            rw->row_date[rw->nrows] = d;
            rw->row_party[rw->nrows++] = RTOT_ALL_PARTIES;
            continue;
        }
        for (p = 0; p < t->nparties; p++)
        {
            if (RosterTotals_Count(rw, d, (int32_t)p, -1) > 0)
            {
                rw->row_date[rw->nrows] = d;
                rw->row_party[rw->nrows++] = (int32_t)p;
                shown++;
            }
        }
        if (shown > 1)
        {
            rw->row_date[rw->nrows] = d;
            rw->row_party[rw->nrows++] = RTOT_ALL_PARTIES;
        }
    }
    if (t->has_party)
    {
        for (p = 0; p < t->nparties; p++)
        {
            rw->row_date[rw->nrows] = RTOT_TOTAL_ROW;
            rw->row_party[rw->nrows++] = (int32_t)p;
        }
    }
    rw->row_date[rw->nrows] = RTOT_TOTAL_ROW;
    rw->row_party[rw->nrows++] = RTOT_ALL_PARTIES;
    return TRUE;
}

static void RosterTotals_UpdateStatus(RosterTotalsWindow *rw)
{
    wchar_t a[32];
    wchar_t b[32];
    wchar_t c[32];
    wchar_t st[320];
    int m;
    if (rw->status == NULL)
    {
        return;
    }
    RosterTotals_Group(rw->tot.counted, a, ARRAYSIZE(a));
    if (rw->tot.repeats > 0)
    {
        RosterTotals_Group(rw->tot.repeats, b, ARRAYSIZE(b));
        RosterTotals_Group(rw->tot.repeated_ids, c, ARRAYSIZE(c));
        StringCchPrintfW(st, ARRAYSIZE(st),
                         L"%s voters, each counted once (%s later record%s of %s Voter ID%s not "
                         L"counted)",
                         a, b, rw->tot.repeats == 1 ? L"" : L"s", c,
                         rw->tot.repeated_ids == 1 ? L"" : L"s");
    }
    else
    {
        StringCchPrintfW(st, ARRAYSIZE(st), L"%s voters", a);
    }
    for (m = 0; m < EE_VM_COUNT; m++)
    {
        if (rw->method_col[m] >= 0 && rw->avail[m] == RTOT_NOT_LOADED)
        {
            StringCchCatW(st, ARRAYSIZE(st), L" — not loaded = PDF rosters that cannot be read");
            break;
        }
    }
    StringCchCatW(st, ARRAYSIZE(st), L" — right-click to copy or export");
    SendMessageW(rw->status, SB_SETTEXTW, 0, (LPARAM)st);
}

static void RosterTotals_Layout(RosterTotalsWindow *rw, int width, int height)
{
    int sb_h = 0;
    if (rw->status != NULL)
    {
        RECT sb;
        SendMessageW(rw->status, WM_SIZE, 0, 0);
        if (GetWindowRect(rw->status, &sb))
        {
            sb_h = sb.bottom - sb.top;
        }
    }
    if (rw->list != NULL)
    {
        MoveWindow(rw->list, 0, 0, width, (height > sb_h) ? height - sb_h : 0, TRUE);
    }
}

static LRESULT CALLBACK RosterTotalsListSubclass(HWND hwnd, UINT msg, WPARAM wParam, LPARAM lParam)
{
    if (msg == WM_NOTIFY)
    {
        NMHDR *nm = (NMHDR *)lParam;
        HWND header = ListView_GetHeader(hwnd);
        RosterTotalsWindow *rw =
            (RosterTotalsWindow *)GetWindowLongPtrW(GetParent(hwnd), GWLP_USERDATA);
        if (rw != NULL && nm != NULL && header != NULL && nm->hwndFrom == header &&
            nm->code == NM_CUSTOMDRAW)
        {
            return App_HeaderCustomDraw(rw->app, (NMCUSTOMDRAW *)lParam, FALSE);
        }
    }
    return CallWindowProcW(g_old_rtot_list_proc, hwnd, msg, wParam, lParam);
}

/* Copy the selected rows (with a header row) as tab-separated UTF-8. */
static void RosterTotals_CopySelected(RosterTotalsWindow *rw)
{
    Utf8Export ex;
    int i;
    int c;
    BOOL ok = TRUE;
    if (rw == NULL || rw->list == NULL || ListView_GetNextItem(rw->list, -1, LVNI_SELECTED) < 0)
    {
        return;
    }
    ZeroMemory(&ex, sizeof(ex));
    for (c = 0; c < rw->ncols && ok; c++)
    {
        if (c > 0)
            ok = Utf8Export_AppendChar(&ex, '\t');
        ok = ok && Utf8Export_AppendFieldW(&ex, RosterTotals_ColumnTitle(rw, c), '\t');
    }
    ok = ok && Utf8Export_Append(&ex, "\r\n", 2);
    i = ListView_GetNextItem(rw->list, -1, LVNI_SELECTED);
    while (i >= 0 && ok)
    {
        for (c = 0; c < rw->ncols && ok; c++)
        {
            wchar_t cell[128];
            RosterTotals_CellText(rw, (uint32_t)i, c, FALSE, cell, ARRAYSIZE(cell));
            if (c > 0)
                ok = Utf8Export_AppendChar(&ex, '\t');
            ok = ok && Utf8Export_AppendFieldW(&ex, cell, '\t');
        }
        ok = ok && Utf8Export_Append(&ex, "\r\n", 2);
        i = ListView_GetNextItem(rw->list, i, LVNI_SELECTED);
    }
    if (ok && ex.data != NULL && Utf8Export_AppendChar(&ex, '\0'))
    {
        App_SetClipboardUtf8(rw->hwnd, ex.data);
    }
    free(ex.data);
}

typedef struct RosterTotalsExportCtx
{
    RosterTotalsWindow *rw;
    const uint32_t *idx;
} RosterTotalsExportCtx;

static void RosterTotals_ExportCell(void *user, uint32_t row, uint32_t col, wchar_t *buf, size_t cch)
{
    RosterTotalsExportCtx *c = (RosterTotalsExportCtx *)user;
    RosterTotals_CellText(c->rw, (c->idx != NULL) ? c->idx[row] : row, (int)col, FALSE, buf, cch);
}

static void RosterTotals_Export(RosterTotalsWindow *rw, BOOL selection_only)
{
    const wchar_t *headers[EE_VM_COUNT + 3];
    wchar_t base[192];
    wchar_t suggested[256];
    uint32_t *idx = NULL;
    uint32_t n;
    int c;
    RosterTotalsExportCtx ctx;

    if (rw == NULL || rw->nrows == 0)
    {
        return;
    }
    if (selection_only)
    {
        idx = App_CollectSelectedIndices(rw->list, rw->nrows, &n);
        if (n == 0)
        {
            free(idx);
            return;
        }
    }
    else
    {
        n = rw->nrows;
    }
    for (c = 0; c < rw->ncols && c < (int)ARRAYSIZE(headers); c++)
    {
        headers[c] = RosterTotals_ColumnTitle(rw, c);
    }
    App_BaseNameNoExt(rw->app->load_path, base, ARRAYSIZE(base));
    StringCchPrintfW(suggested, ARRAYSIZE(suggested), L"%s-%sVoting_Totals", base,
                     selection_only ? L"Selected_" : L"");
    ctx.rw = rw;
    ctx.idx = idx;
    App_ExportReportModel(rw->hwnd, suggested, (uint32_t)rw->ncols, headers, n, RosterTotals_ExportCell,
                          &ctx);
    free(idx);
}

static void RosterTotals_OnContextMenu(RosterTotalsWindow *rw, int item, POINT screen)
{
    HMENU m;
    UINT cmd;
    if (item >= 0 && !(ListView_GetItemState(rw->list, item, LVIS_SELECTED) & LVIS_SELECTED))
    {
        ListView_SetItemState(rw->list, -1, 0, LVIS_SELECTED | LVIS_FOCUSED);
        ListView_SetItemState(rw->list, item, LVIS_SELECTED | LVIS_FOCUSED,
                              LVIS_SELECTED | LVIS_FOCUSED);
    }
    m = CreatePopupMenu();
    if (m == NULL)
    {
        return;
    }
    AppendMenuW(m, MF_STRING, IDM_EDIT_COPY, L"&Copy");
    AppendMenuW(m, MF_SEPARATOR, 0, NULL);
    AppendMenuW(m, MF_STRING, IDM_EXPORT_SELECTED, L"Export &Selected…");
    AppendMenuW(m, MF_STRING, IDM_EXPORT_ALL, L"Export &All…");
    cmd = (UINT)TrackPopupMenu(m, TPM_RIGHTBUTTON | TPM_RETURNCMD, screen.x, screen.y, 0, rw->hwnd, NULL);
    DestroyMenu(m);
    if (cmd == IDM_EDIT_COPY)
        RosterTotals_CopySelected(rw);
    else if (cmd == IDM_EXPORT_SELECTED)
        RosterTotals_Export(rw, TRUE);
    else if (cmd == IDM_EXPORT_ALL)
        RosterTotals_Export(rw, FALSE);
}

static void RosterTotals_BuildColumns(RosterTotalsWindow *rw)
{
    int c;
    for (c = 0; c < rw->ncols; c++)
    {
        LVCOLUMNW col;
        int cx = 140;
        ZeroMemory(&col, sizeof(col));
        col.mask = LVCF_TEXT | LVCF_WIDTH | LVCF_FMT | LVCF_SUBITEM;
        col.fmt = (c == 0 || c == rw->col_party) ? LVCFMT_LEFT : LVCFMT_RIGHT;
        if (c == 0)
            cx = 110;
        else if (c == rw->col_party)
            cx = 100;
        else if (c == rw->col_total)
            cx = 100;
        else if (c == rw->method_col[EE_VM_EARLY] || c == rw->method_col[EE_VM_ELECTION_DAY])
            cx = 165;
        else
            cx = 120;
        col.cx = Scale(rw->app, cx);
        col.pszText = (LPWSTR)RosterTotals_ColumnTitle(rw, c);
        col.iSubItem = c;
        ListView_InsertColumn(rw->list, c, &col);
    }
}

static LRESULT CALLBACK RosterTotalsWndProc(HWND hwnd, UINT msg, WPARAM wParam, LPARAM lParam)
{
    RosterTotalsWindow *rw = (RosterTotalsWindow *)GetWindowLongPtrW(hwnd, GWLP_USERDATA);

    switch (msg)
    {
        case WM_CREATE:
        {
            CREATESTRUCTW *cs = (CREATESTRUCTW *)lParam;
            RECT rc;
            rw = (RosterTotalsWindow *)cs->lpCreateParams;
            SetWindowLongPtrW(hwnd, GWLP_USERDATA, (LONG_PTR)rw);
            rw->hwnd = hwnd;
            GetClientRect(hwnd, &rc);
            rw->list = CreateWindowExW(0, WC_LISTVIEWW, L"",
                                       WS_CHILD | WS_VISIBLE | LVS_REPORT | LVS_OWNERDATA |
                                           LVS_SHOWSELALWAYS,
                                       0, 0, rc.right, rc.bottom, hwnd, NULL, rw->app->instance, NULL);
            if (rw->list == NULL)
            {
                return -1;
            }
            {
                WNDPROC old = (WNDPROC)SetWindowLongPtrW(rw->list, GWLP_WNDPROC,
                                                         (LONG_PTR)RosterTotalsListSubclass);
                if (g_old_rtot_list_proc == NULL)
                {
                    g_old_rtot_list_proc = old;
                }
            }
            ListView_SetExtendedListViewStyle(rw->list,
                                              LVS_EX_FULLROWSELECT | LVS_EX_DOUBLEBUFFER | LVS_EX_GRIDLINES);
            if (rw->app->font_ui)
            {
                LOGFONTW lf;
                SendMessageW(rw->list, WM_SETFONT, (WPARAM)rw->app->font_ui, TRUE);
                if (GetObjectW(rw->app->font_ui, sizeof(lf), &lf) == sizeof(lf))
                {
                    lf.lfWeight = FW_BOLD;
                    rw->font_bold = CreateFontIndirectW(&lf);
                }
            }
            RosterTotals_BuildColumns(rw);
            rw->status = CreateWindowExW(0, STATUSCLASSNAMEW, NULL, WS_CHILD | WS_VISIBLE | SBARS_SIZEGRIP,
                                         0, 0, 0, 0, hwnd, NULL, rw->app->instance, NULL);
            if (rw->status != NULL && rw->app->font_ui)
            {
                SendMessageW(rw->status, WM_SETFONT, (WPARAM)rw->app->font_ui, TRUE);
            }
            RosterTotals_UpdateStatus(rw);
            ListView_SetItemCountEx(rw->list, (int)rw->nrows, LVSICF_NOINVALIDATEALL);
            RosterTotals_Layout(rw, rc.right, rc.bottom);
            return 0;
        }

        case WM_SIZE:
            if (rw != NULL)
            {
                RosterTotals_Layout(rw, LOWORD(lParam), HIWORD(lParam));
            }
            return 0;

        case WM_SETFOCUS:
            if (rw != NULL && rw->list != NULL)
            {
                SetFocus(rw->list);
            }
            return 0;

        case WM_COMMAND:
            /* Ctrl+C is routed here by the shared accelerator table. */
            if (rw != NULL && LOWORD(wParam) == IDM_EDIT_COPY)
            {
                RosterTotals_CopySelected(rw);
                return 0;
            }
            break;

        case WM_NOTIFY:
        {
            NMHDR *hdr = (NMHDR *)lParam;
            if (rw == NULL || hdr->hwndFrom != rw->list)
            {
                break;
            }
            if (hdr->code == LVN_GETDISPINFOW)
            {
                NMLVDISPINFOW *di = (NMLVDISPINFOW *)lParam;
                if ((di->item.mask & LVIF_TEXT) && di->item.iItem >= 0 && di->item.iSubItem >= 0)
                {
                    RosterTotals_CellText(rw, (uint32_t)di->item.iItem, di->item.iSubItem, TRUE,
                                          di->item.pszText, (size_t)di->item.cchTextMax);
                }
                return 0;
            }
            if (hdr->code == NM_CUSTOMDRAW)
            {
                /* Total and all-parties rows in bold. */
                NMLVCUSTOMDRAW *cd = (NMLVCUSTOMDRAW *)lParam;
                if (cd->nmcd.dwDrawStage == CDDS_PREPAINT)
                {
                    return CDRF_NOTIFYITEMDRAW;
                }
                if (cd->nmcd.dwDrawStage == CDDS_ITEMPREPAINT)
                {
                    uint32_t row = (uint32_t)cd->nmcd.dwItemSpec;
                    if (rw->font_bold != NULL && row < rw->nrows &&
                        (rw->row_date[row] == RTOT_TOTAL_ROW ||
                         (rw->tot.has_party && rw->row_party[row] == RTOT_ALL_PARTIES)))
                    {
                        SelectObject(cd->nmcd.hdc, rw->font_bold);
                        return CDRF_NEWFONT;
                    }
                    return CDRF_DODEFAULT;
                }
                return CDRF_DODEFAULT;
            }
            if (hdr->code == NM_RCLICK)
            {
                LPNMITEMACTIVATE ia = (LPNMITEMACTIVATE)lParam;
                POINT screen = ia->ptAction;
                ClientToScreen(rw->list, &screen);
                RosterTotals_OnContextMenu(rw, ia->iItem, screen);
                return 0;
            }
            break;
        }

        case WM_CLOSE:
            DestroyWindow(hwnd);
            return 0;

        case WM_DESTROY:
            if (rw != NULL)
            {
                if (rw->app != NULL && rw->app->report_totals == rw)
                {
                    rw->app->report_totals = NULL;
                }
                if (rw->font_bold != NULL)
                {
                    DeleteObject(rw->font_bold);
                }
                EeRoster_FreeTotals(&rw->tot);
                free(rw->row_date);
                free(rw->row_party);
                free(rw);
                SetWindowLongPtrW(hwnd, GWLP_USERDATA, 0);
            }
            return 0;

        default:
            break;
    }
    return DefWindowProcW(hwnd, msg, wParam, lParam);
}

static void App_CloseRosterTotals(AppState *app)
{
    /* DestroyWindow -> WM_DESTROY frees the window and clears the slot. */
    if (app != NULL && app->report_totals != NULL)
    {
        DestroyWindow(app->report_totals->hwnd);
    }
}

/* Reports → Display Voting Totals… in a Voter Roster window. */
static void App_ShowRosterTotals(AppState *app)
{
    RosterTotalsWindow *rw;
    wchar_t title[MAX_PATH + 64];
    HCURSOR old_cursor;
    RECT pr;
    int x = CW_USEDEFAULT;
    int y = CW_USEDEFAULT;
    int w;
    BOOL ok;
    const wchar_t *base;

    if (app == NULL || !app->is_roster || app->table.row_count == 0)
    {
        return;
    }
    if (app->report_totals != NULL)
    {
        SetForegroundWindow(app->report_totals->hwnd);
        return;
    }
    rw = (RosterTotalsWindow *)calloc(1, sizeof(RosterTotalsWindow));
    if (rw == NULL)
    {
        return;
    }
    rw->app = app;
    old_cursor = SetCursor(LoadCursorW(NULL, IDC_WAIT));
    ok = EeRoster_ComputeTotals(&app->table, &rw->tot) && RosterTotals_Build(rw);
    SetCursor(old_cursor);
    if (!ok)
    {
        EeRoster_FreeTotals(&rw->tot);
        free(rw->row_date);
        free(rw->row_party);
        free(rw);
        MessageBoxW(app->hwnd_main,
                    L"Could not compute the voting totals (out of memory, or the roster has "
                    L"no Voting Method column).",
                    L"Voting Totals", MB_ICONERROR | MB_OK);
        return;
    }
    base = App_PathBaseName(app->load_path);
    StringCchPrintfW(title, ARRAYSIZE(title), L"Voting Totals - %s", base[0] ? base : L"(voter roster)");
    if (GetWindowRect(app->hwnd_main, &pr))
    {
        x = pr.left + Scale(app, 48);
        y = pr.top + Scale(app, 48);
    }
    w = 110 + (rw->col_party >= 0 ? 100 : 0) + 100 + 40;
    {
        int m;
        for (m = 0; m < EE_VM_COUNT; m++)
        {
            if (rw->method_col[m] >= 0)
                w += (m == EE_VM_EARLY || m == EE_VM_ELECTION_DAY) ? 165 : 120;
        }
    }
    /* Unowned top-level window like the other reports; closed with the roster window. */
    rw->hwnd = CreateWindowExW(0, k_RosterTotalsClassName, title, WS_OVERLAPPEDWINDOW | WS_CLIPCHILDREN, x,
                               y, Scale(app, w), Scale(app, 600), NULL, NULL, app->instance, rw);
    if (rw->hwnd == NULL)
    {
        EeRoster_FreeTotals(&rw->tot);
        free(rw->row_date);
        free(rw->row_party);
        free(rw);
        return;
    }
    app->report_totals = rw;
    ShowWindow(rw->hwnd, SW_SHOW);
    SetForegroundWindow(rw->hwnd);
}

/* -------------------------------------------------------------------------- */
/* Entry                                                                      */
/* -------------------------------------------------------------------------- */

int APIENTRY wWinMain(_In_ HINSTANCE hInstance,
                      _In_opt_ HINSTANCE hPrevInstance,
                      _In_ LPWSTR lpCmdLine,
                      _In_ int nCmdShow)
{
    WNDCLASSEXW wc;
    WNDCLASSEXW pc;
    WNDCLASSEXW oc;
    MSG msg;
    INITCOMMONCONTROLSEX icc;
    ACCEL accels[3];
    HACCEL haccel;
    BOOL getResult;

    (void)hPrevInstance;
    (void)lpCmdLine;

    /* Per-monitor v2 (manifest also declares this). */
    SetProcessDpiAwarenessContext(DPI_AWARENESS_CONTEXT_PER_MONITOR_AWARE_V2);

    /* Restore user options saved on a previous run (defaults if none). */
    EeSettings_Load(&g_settings);
    EeAudit_SetTestTaskWords(g_settings.audit_test_words);

    icc.dwSize = sizeof(icc);
    icc.dwICC = ICC_LISTVIEW_CLASSES | ICC_PROGRESS_CLASS | ICC_BAR_CLASSES | ICC_STANDARD_CLASSES |
                ICC_UPDOWN_CLASS | ICC_LINK_CLASS;
    InitCommonControlsEx(&icc);

    ZeroMemory(&wc, sizeof(wc));
    wc.cbSize = sizeof(wc);
    wc.style = CS_HREDRAW | CS_VREDRAW;
    wc.lpfnWndProc = MainWndProc;
    wc.hInstance = hInstance;
    wc.hCursor = LoadCursorW(NULL, IDC_ARROW);
    wc.hbrBackground = (HBRUSH)(COLOR_WINDOW + 1);
    wc.lpszClassName = k_WindowClassName;
    wc.hIcon = LoadIconW(hInstance, MAKEINTRESOURCEW(IDI_APPICON));
    wc.hIconSm = LoadIconW(hInstance, MAKEINTRESOURCEW(IDI_APPICON));
    if (wc.hIcon == NULL)
    {
        wc.hIcon = LoadIconW(NULL, IDI_APPLICATION);
        wc.hIconSm = wc.hIcon;
    }
    if (RegisterClassExW(&wc) == 0)
    {
        return 1;
    }

    ZeroMemory(&pc, sizeof(pc));
    pc.cbSize = sizeof(pc);
    pc.lpfnWndProc = ProgressWndProc;
    pc.hInstance = hInstance;
    pc.hCursor = LoadCursorW(NULL, IDC_ARROW);
    pc.hbrBackground = (HBRUSH)(COLOR_3DFACE + 1);
    pc.lpszClassName = k_ProgressClassName;
    pc.hIcon = wc.hIcon;
    pc.hIconSm = wc.hIconSm;
    if (RegisterClassExW(&pc) == 0)
    {
        return 1;
    }

    ZeroMemory(&oc, sizeof(oc));
    oc.cbSize = sizeof(oc);
    oc.lpfnWndProc = OptionsWndProc;
    oc.hInstance = hInstance;
    oc.hCursor = LoadCursorW(NULL, IDC_ARROW);
    oc.hbrBackground = (HBRUSH)(COLOR_3DFACE + 1);
    oc.lpszClassName = k_OptionsClassName;
    oc.hIcon = wc.hIcon;
    oc.hIconSm = wc.hIconSm;
    if (RegisterClassExW(&oc) == 0)
    {
        return 1;
    }

    {
        WNDCLASSEXW fc;
        ZeroMemory(&fc, sizeof(fc));
        fc.cbSize = sizeof(fc);
        fc.lpfnWndProc = FilterWndProc;
        fc.hInstance = hInstance;
        fc.hCursor = LoadCursorW(NULL, IDC_ARROW);
        fc.hbrBackground = (HBRUSH)(COLOR_3DFACE + 1);
        fc.lpszClassName = k_FilterClassName;
        fc.hIcon = wc.hIcon;
        fc.hIconSm = wc.hIconSm;
        if (RegisterClassExW(&fc) == 0)
        {
            return 1;
        }
    }

    {
        WNDCLASSEXW rc2;
        ZeroMemory(&rc2, sizeof(rc2));
        rc2.cbSize = sizeof(rc2);
        rc2.lpfnWndProc = ReportWndProc;
        rc2.hInstance = hInstance;
        rc2.hCursor = LoadCursorW(NULL, IDC_ARROW);
        rc2.hbrBackground = (HBRUSH)(COLOR_3DFACE + 1);
        rc2.lpszClassName = k_ReportClassName;
        rc2.hIcon = wc.hIcon;
        rc2.hIconSm = wc.hIconSm;
        if (RegisterClassExW(&rc2) == 0)
        {
            return 1;
        }
    }

    {
        WNDCLASSEXW cc;
        ZeroMemory(&cc, sizeof(cc));
        cc.cbSize = sizeof(cc);
        cc.lpfnWndProc = CompareWndProc;
        cc.hInstance = hInstance;
        cc.hCursor = LoadCursorW(NULL, IDC_ARROW);
        cc.hbrBackground = (HBRUSH)(COLOR_3DFACE + 1);
        cc.lpszClassName = k_CompareClassName;
        cc.hIcon = wc.hIcon;
        cc.hIconSm = wc.hIconSm;
        if (RegisterClassExW(&cc) == 0)
        {
            return 1;
        }
    }

    {
        WNDCLASSEXW dc;
        ZeroMemory(&dc, sizeof(dc));
        dc.cbSize = sizeof(dc);
        dc.lpfnWndProc = DiffWndProc;
        dc.hInstance = hInstance;
        dc.hCursor = LoadCursorW(NULL, IDC_ARROW);
        dc.hbrBackground = (HBRUSH)(COLOR_3DFACE + 1);
        dc.lpszClassName = k_DiffClassName;
        dc.hIcon = wc.hIcon;
        dc.hIconSm = wc.hIconSm;
        if (RegisterClassExW(&dc) == 0)
        {
            return 1;
        }
    }

    {
        WNDCLASSEXW vc;
        ZeroMemory(&vc, sizeof(vc));
        vc.cbSize = sizeof(vc);
        vc.lpfnWndProc = CvrWndProc;
        vc.hInstance = hInstance;
        vc.hCursor = LoadCursorW(NULL, IDC_ARROW);
        vc.hbrBackground = (HBRUSH)(COLOR_3DFACE + 1);
        vc.lpszClassName = k_CvrClassName;
        vc.hIcon = wc.hIcon;
        vc.hIconSm = wc.hIconSm;
        if (RegisterClassExW(&vc) == 0)
        {
            return 1;
        }
    }

    {
        WNDCLASSEXW rcw;
        ZeroMemory(&rcw, sizeof(rcw));
        rcw.cbSize = sizeof(rcw);
        rcw.lpfnWndProc = CvrReportWndProc;
        rcw.hInstance = hInstance;
        rcw.hCursor = LoadCursorW(NULL, IDC_ARROW);
        rcw.hbrBackground = (HBRUSH)(COLOR_3DFACE + 1);
        rcw.lpszClassName = k_CvrReportClassName;
        rcw.hIcon = wc.hIcon;
        rcw.hIconSm = wc.hIconSm;
        if (RegisterClassExW(&rcw) == 0)
        {
            return 1;
        }
    }

    {
        WNDCLASSEXW rrc;
        ZeroMemory(&rrc, sizeof(rrc));
        rrc.cbSize = sizeof(rrc);
        rrc.lpfnWndProc = CvrRcvWndProc;
        rrc.hInstance = hInstance;
        rrc.hCursor = LoadCursorW(NULL, IDC_ARROW);
        rrc.hbrBackground = (HBRUSH)(COLOR_3DFACE + 1);
        rrc.lpszClassName = k_CvrRcvClassName;
        rrc.hIcon = wc.hIcon;
        rrc.hIconSm = wc.hIconSm;
        if (RegisterClassExW(&rrc) == 0)
        {
            return 1;
        }
    }

    {
        WNDCLASSEXW tc;
        ZeroMemory(&tc, sizeof(tc));
        tc.cbSize = sizeof(tc);
        tc.lpfnWndProc = RosterTotalsWndProc;
        tc.hInstance = hInstance;
        tc.hCursor = LoadCursorW(NULL, IDC_ARROW);
        tc.hbrBackground = (HBRUSH)(COLOR_3DFACE + 1);
        tc.lpszClassName = k_RosterTotalsClassName;
        tc.hIcon = wc.hIcon;
        tc.hIconSm = wc.hIconSm;
        if (RegisterClassExW(&tc) == 0)
        {
            return 1;
        }
    }

    {
        WNDCLASSEXW vrc;
        ZeroMemory(&vrc, sizeof(vrc));
        vrc.cbSize = sizeof(vrc);
        vrc.lpfnWndProc = CvrValueReportWndProc;
        vrc.hInstance = hInstance;
        vrc.hCursor = LoadCursorW(NULL, IDC_ARROW);
        vrc.hbrBackground = (HBRUSH)(COLOR_3DFACE + 1);
        vrc.lpszClassName = k_CvrValueReportClassName;
        vrc.hIcon = wc.hIcon;
        vrc.hIconSm = wc.hIconSm;
        if (RegisterClassExW(&vrc) == 0)
        {
            return 1;
        }
    }

    {
        WNDCLASSEXW cfc;
        ZeroMemory(&cfc, sizeof(cfc));
        cfc.cbSize = sizeof(cfc);
        cfc.lpfnWndProc = CvrFilterWndProc;
        cfc.hInstance = hInstance;
        cfc.hCursor = LoadCursorW(NULL, IDC_ARROW);
        cfc.hbrBackground = (HBRUSH)(COLOR_3DFACE + 1);
        cfc.lpszClassName = k_CvrFilterClassName;
        cfc.hIcon = wc.hIcon;
        cfc.hIconSm = wc.hIconSm;
        if (RegisterClassExW(&cfc) == 0)
        {
            return 1;
        }
    }

    {
        WNDCLASSEXW rcc;
        ZeroMemory(&rcc, sizeof(rcc));
        rcc.cbSize = sizeof(rcc);
        rcc.lpfnWndProc = ResCompareWndProc;
        rcc.hInstance = hInstance;
        rcc.hCursor = LoadCursorW(NULL, IDC_ARROW);
        rcc.hbrBackground = (HBRUSH)(COLOR_3DFACE + 1);
        rcc.lpszClassName = k_ResCompareClassName;
        rcc.hIcon = wc.hIcon;
        rcc.hIconSm = wc.hIconSm;
        if (RegisterClassExW(&rcc) == 0)
        {
            return 1;
        }
    }

    {
        WNDCLASSEXW rtc;
        ZeroMemory(&rtc, sizeof(rtc));
        rtc.cbSize = sizeof(rtc);
        rtc.lpfnWndProc = ResTurnoutWndProc;
        rtc.hInstance = hInstance;
        rtc.hCursor = LoadCursorW(NULL, IDC_ARROW);
        rtc.hbrBackground = (HBRUSH)(COLOR_3DFACE + 1);
        rtc.lpszClassName = k_ResTurnoutClassName;
        rtc.hIcon = wc.hIcon;
        rtc.hIconSm = wc.hIconSm;
        if (RegisterClassExW(&rtc) == 0)
        {
            return 1;
        }
    }

    {
        WNDCLASSEXW rsc;
        ZeroMemory(&rsc, sizeof(rsc));
        rsc.cbSize = sizeof(rsc);
        rsc.lpfnWndProc = ResultsWndProc;
        rsc.hInstance = hInstance;
        rsc.hCursor = LoadCursorW(NULL, IDC_ARROW);
        rsc.hbrBackground = (HBRUSH)(COLOR_3DFACE + 1);
        rsc.lpszClassName = k_ResultsClassName;
        rsc.hIcon = wc.hIcon;
        rsc.hIconSm = wc.hIconSm;
        if (RegisterClassExW(&rsc) == 0)
        {
            return 1;
        }
    }

    {
        WNDCLASSEXW asc;
        ZeroMemory(&asc, sizeof(asc));
        asc.cbSize = sizeof(asc);
        asc.lpfnWndProc = AuditWndProc;
        asc.hInstance = hInstance;
        asc.hCursor = LoadCursorW(NULL, IDC_ARROW);
        asc.hbrBackground = (HBRUSH)(COLOR_3DFACE + 1);
        asc.lpszClassName = k_AuditClassName;
        asc.hIcon = wc.hIcon;
        asc.hIconSm = wc.hIconSm;
        if (RegisterClassExW(&asc) == 0)
        {
            return 1;
        }
    }

    {
        WNDCLASSEXW arc;
        ZeroMemory(&arc, sizeof(arc));
        arc.cbSize = sizeof(arc);
        arc.lpfnWndProc = AudReportWndProc;
        arc.hInstance = hInstance;
        arc.hCursor = LoadCursorW(NULL, IDC_ARROW);
        arc.hbrBackground = (HBRUSH)(COLOR_3DFACE + 1);
        arc.lpszClassName = k_AuditReportClassName;
        arc.hIcon = wc.hIcon;
        arc.hIconSm = wc.hIconSm;
        if (RegisterClassExW(&arc) == 0)
        {
            return 1;
        }
    }

    if (App_CreateViewer(hInstance, nCmdShow, NULL, NULL) == NULL)
    {
        return 1;
    }

    accels[0].fVirt = FCONTROL | FVIRTKEY;
    accels[0].key = 'O';
    accels[0].cmd = IDM_FILE_OPEN_VOTER_LIST;
    accels[1].fVirt = FCONTROL | FVIRTKEY;
    accels[1].key = 'C';
    accels[1].cmd = IDM_EDIT_COPY;
    accels[2].fVirt = FCONTROL | FVIRTKEY;
    accels[2].key = 'L';
    accels[2].cmd = IDM_FILTER_EDIT;
    haccel = CreateAcceleratorTableW(accels, 3);

    while ((getResult = GetMessageW(&msg, NULL, 0, 0)) != 0)
    {
        HWND accel_hwnd;

        if (getResult == -1)
        {
            break;
        }
        if (App_RouteDialogMessage(&msg))
        {
            continue;
        }
        accel_hwnd = GetForegroundWindow();
        if (haccel == NULL || accel_hwnd == NULL ||
            !TranslateAcceleratorW(accel_hwnd, haccel, &msg))
        {
            TranslateMessage(&msg);
            DispatchMessageW(&msg);
        }
    }

    if (haccel)
    {
        DestroyAcceleratorTable(haccel);
    }
    return (int)msg.wParam;
}
