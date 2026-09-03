/**
 * @file main.c
 * @brief ElectionExplorer Win32 GUI: menu, DPI-aware layout, voter grid.
 */

#include "main.h"
#include "resource.h"
#include "voter_table.h"
#include "filter.h"

#include <commctrl.h>
#include <commdlg.h>
#include <intsafe.h>
#include <shellapi.h>
#include <strsafe.h>
#include <stdlib.h>
#include <uxtheme.h>
#include <windowsx.h>

#pragma comment(lib, "comctl32.lib")
#pragma comment(lib, "comdlg32.lib")
#pragma comment(lib, "uxtheme.lib")
#pragma comment(lib, "shell32.lib")

/* -------------------------------------------------------------------------- */
/* Constants                                                                  */
/* -------------------------------------------------------------------------- */

static const wchar_t k_WindowClassName[] = L"ElectionExplorerMainWindow";
static const wchar_t k_WindowTitle[] = L"Election Explorer";
static const wchar_t k_ProgressClassName[] = L"ElectionExplorerLoadProgress";
static const wchar_t k_OptionsClassName[] = L"ElectionExplorerOptions";
static const wchar_t k_FilterClassName[] = L"ElectionExplorerFilter";
static const wchar_t k_ReportClassName[] = L"ElectionExplorerReport";

static const int k_DefaultWidth = 1100;
static const int k_DefaultHeight = 720;
static const int k_DefaultFrozenWidth = 640;
static const uint32_t k_NameUpdateProgressMinRows = 25000;
/* Below this row count a duplicate scan runs synchronously (sub-100 ms); at or
 * above it we run on a worker thread behind the cancelable progress modal. */
static const uint32_t k_ScanModalMinRows = 250000;

/* Which duplicate scan produced the current row-mark layer. */
enum
{
    EE_SCAN_NONE = 0,
    EE_SCAN_DUP_VOTER_IDS = 1,
    EE_SCAN_DUP_NAME_DOB = 2
};

/* Report kinds (Precinct / Address summary windows). */
enum
{
    EE_REPORT_PRECINCT = 1,
    EE_REPORT_ADDRESS = 2
};

typedef struct ReportWindow ReportWindow;

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
    BOOL loading;
    BOOL close_pending;
    volatile LONG load_cancel;
    HANDLE load_thread;
    wchar_t load_path[MAX_PATH];
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
    int mark_kind; /* EE_SCAN_* that produced mark_rows */

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

    struct AppState *next;
} AppState;

static AppState *g_viewers = NULL;
static int g_viewer_count = 0;
static WNDPROC g_old_frozen_proc = NULL;
static WNDPROC g_old_scroll_proc = NULL;
static WNDPROC g_old_title_proc = NULL;

static int App_ClampZoom(int zoom_percent);
static EeMapEngine App_ClampMapEngine(int engine);
static LRESULT CALLBACK FrozenSubclass(HWND hwnd, UINT msg, WPARAM wParam, LPARAM lParam);
static LRESULT CALLBACK ScrollSubclass(HWND hwnd, UINT msg, WPARAM wParam, LPARAM lParam);
static LRESULT CALLBACK PaneTitleSubclass(HWND hwnd, UINT msg, WPARAM wParam, LPARAM lParam);

static AppState *App_FromMain(HWND hwnd);
static AppState *App_FromChild(HWND hwnd);
static AppState *App_CreateViewer(HINSTANCE instance,
                                  int nCmdShow,
                                  HWND offset_from,
                                  const AppState *prefs);
static void App_StartLoad(AppState *app, const wchar_t *path);
static void App_RequestClose(AppState *app);
static void App_ExitAll(void);
static void App_ApplyFilter(AppState *app);
static BOOL App_ShowFilter(AppState *app);
static void App_ResetFilter(AppState *app);
static void App_ClearMarks(AppState *app);
static void App_ShowReport(AppState *app, int kind);
static void App_CloseReports(AppState *app);
static LRESULT CALLBACK ReportWndProc(HWND hwnd, UINT msg, WPARAM wParam, LPARAM lParam);
static void App_StartDuplicateScan(AppState *app, int kind);
static void App_OnScanFinished(AppState *app);
static void App_ApplyDuplicateMarks(AppState *app, uint8_t *marks, uint32_t count, int kind);
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
        if (App_PathsEqual(p->load_path, path))
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

static BOOL App_RouteDialogMessage(MSG *msg)
{
    AppState *p;

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
        app->copy_prepend_normalized = TRUE;
        app->name_surname_first = TRUE;
        app->zoom_percent = k_ZoomDefault;
        app->map_engine = EeMap_Google;
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
static LRESULT App_HeaderCustomDraw(AppState *app, NMCUSTOMDRAW *cd)
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
                BOOL force_center = (app->hwnd_frozen != NULL &&
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
    if (app->table.row_count == 0)
    {
        App_SetStatus(app, L"No voter list loaded.");
        return;
    }
    if (app->mark_active)
    {
        const wchar_t *what = (app->mark_kind == EE_SCAN_DUP_NAME_DOB)
                                  ? L"duplicate voters (name + DOB)"
                                  : L"duplicate Voter IDs";
        if (EeFilter_HasEnabled(&app->filters))
        {
            StringCchPrintfW(buf,
                             ARRAYSIZE(buf),
                             L"%s within filter: %u of %u voters",
                             what,
                             app->filter_count,
                             app->table.row_count);
        }
        else
        {
            StringCchPrintfW(buf,
                             ARRAYSIZE(buf),
                             L"Showing %u %s of %u voters",
                             app->filter_count,
                             what,
                             app->table.row_count);
        }
    }
    else if (EeFilter_HasEnabled(&app->filters))
    {
        StringCchPrintfW(buf,
                         ARRAYSIZE(buf),
                         L"Filtered: %u of %u voters",
                         app->filter_count,
                         app->table.row_count);
    }
    else
    {
        StringCchPrintfW(buf, ARRAYSIZE(buf), L"%u voters loaded", app->table.row_count);
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
                if (phys < app->table.row_count && app->mark_rows[phys])
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

    /* Scrollable: remaining source columns */
    for (i = EE_FROZEN_COLUMN_COUNT; i < app->table.column_count; i++)
    {
        int idx = (int)(i - EE_FROZEN_COLUMN_COUNT);
        ZeroMemory(&col, sizeof(col));
        col.mask = LVCF_TEXT | LVCF_WIDTH | LVCF_SUBITEM | LVCF_FMT;
        col.fmt = LVCFMT_LEFT;
        col.cx = col_width;
        col.pszText = app->table.column_titles[i];
        col.iSubItem = idx;
        ListView_InsertColumn(app->hwnd_scroll, idx, &col);
    }

    App_ApplyFilter(app);

    if (app->table.sort_column >= 0)
    {
        if (app->table.sort_column < EE_FROZEN_COLUMN_COUNT)
        {
            App_SetHeaderSortArrow(app,
                                   app->hwnd_frozen,
                                   app->table.sort_column,
                                   app->table.sort_column,
                                   app->table.sort_ascending);
            App_SetHeaderSortArrow(app, app->hwnd_scroll, -1, -1, TRUE);
        }
        else
        {
            App_SetHeaderSortArrow(app, app->hwnd_frozen, -1, -1, TRUE);
            App_SetHeaderSortArrow(app,
                                   app->hwnd_scroll,
                                   app->table.sort_column - EE_FROZEN_COLUMN_COUNT,
                                   app->table.sort_column - EE_FROZEN_COLUMN_COUNT,
                                   app->table.sort_ascending);
        }
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

    if (app->hwnd_frozen == NULL || app->hwnd_scroll == NULL)
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

    if (table_column < EE_FROZEN_COLUMN_COUNT)
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
        int local = (int)table_column - EE_FROZEN_COLUMN_COUNT;
        App_SetHeaderSortArrow(app, app->hwnd_frozen, -1, -1, TRUE);
        App_SetHeaderSortArrow(app, app->hwnd_scroll, local, local, app->table.sort_ascending);
    }
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
        table_column = (uint32_t)local_column + EE_FROZEN_COLUMN_COUNT;
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
    app->load_status = EeVoterTable_LoadFromFile(app->load_path,
                                                 &app->table,
                                                 &app->load_cancel,
                                                 LoadProgressThunk,
                                                 app,
                                                 app->load_error,
                                                 ARRAYSIZE(app->load_error));

    PostMessageW(app->hwnd_main, EEM_LOAD_FINISHED, 0, 0);
    return 0;
}

static void App_StartLoad(AppState *app, const wchar_t *path)
{
    if (app == NULL || path == NULL || path[0] == L'\0' || app->loading)
    {
        return;
    }

    StringCchCopyW(app->load_path, ARRAYSIZE(app->load_path), path);
    InterlockedExchange(&app->load_cancel, 0);
    App_ClearMarks(app);   /* prior duplicates view indexed the old table */
    App_CloseReports(app); /* reports summarize the outgoing table */
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
    ofn.lpstrFilter = L"Voter lists (*.csv;*.txt)\0*.csv;*.txt\0"
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

    if (app->load_path[0] != L'\0' || app->table.row_count > 0)
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
        App_StartLoad(created, canon);
        return;
    }

    App_StartLoad(app, canon);
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
    if (app->hwnd_main == NULL)
    {
        return;
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

    if (app->hwnd_frozen == NULL || app->hwnd_scroll == NULL)
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
    AppendMenuW(file_menu, MF_STRING, IDM_FILE_CLOSE_VOTER_LIST, L"&Close Voter List");
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
    AppendMenuW(filter_menu, MF_SEPARATOR, 0, NULL);
    AppendMenuW(filter_menu, MF_STRING, IDM_FILTER_RESET_VIEW, L"Reset Vie&w…");
    HMENU reports_menu = CreatePopupMenu();
    AppendMenuW(reports_menu, MF_STRING, IDM_REPORT_PRECINCT, L"Display &Precinct Report…");
    AppendMenuW(reports_menu, MF_STRING, IDM_REPORT_ADDRESS, L"Display &Address Report…");
    AppendMenuW(menu, MF_POPUP, (UINT_PTR)file_menu, L"&File");
    AppendMenuW(menu, MF_POPUP, (UINT_PTR)edit_menu, L"&Edit");
    AppendMenuW(menu, MF_POPUP, (UINT_PTR)filter_menu, L"F&ilter");
    AppendMenuW(menu, MF_POPUP, (UINT_PTR)reports_menu, L"&Reports");
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
                                     app->copy_prepend_normalized,
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
                hit_col = (uint32_t)sub.iSubItem + EE_FROZEN_COLUMN_COUNT;
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
                        if (map_sel >= 0)
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
    client_h = Scale(app, 250);
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
                        WS_CHILD | WS_VISIBLE | WS_TABSTOP | BS_AUTOCHECKBOX,
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
                                  margin + Scale(app, 28),
                                  client_w - margin * 2,
                                  Scale(app, 24),
                                  app->hwnd_options,
                                  (HMENU)(INT_PTR)IDC_OPT_SURNAME_FIRST,
                                  app->instance,
                                  NULL);
    {
        int zy = margin + Scale(app, 64);
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
                                  WS_CHILD | WS_VISIBLE | SS_LEFT | SS_CENTERIMAGE,
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
                                  WS_CHILD | WS_VISIBLE | WS_TABSTOP | WS_VSCROLL |
                                      CBS_DROPDOWNLIST | CBS_HASSTRINGS,
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
        SendMessageW(cmb_map, WM_SETFONT, (WPARAM)app->font_ui, TRUE);
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
    hdwp = BeginDeferWindowPos(12);
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

    FilterDlg_PopulateColumns(GetDlgItem(hwnd, IDC_FLT_COLUMN), &app->table);
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
}

/* Takes ownership of @p marks (row_count bytes) and shows it as the active
 * duplicates view, ANDed with any existing filter. */
static void App_ApplyDuplicateMarks(AppState *app, uint8_t *marks, uint32_t count, int kind)
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
    App_ClearSelection(app);

    /* Group the duplicates by their key so shared values sit next to each other:
     * Voter ID for the ID scan, Name for the name+DOB scan. The sort refreshes
     * the (mark-intersected) view; fall back to a plain apply if it cannot run. */
    {
        uint32_t sort_col = (kind == EE_SCAN_DUP_NAME_DOB) ? EE_COL_NAME : EE_COL_VOTER_ID;
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
    int kind;            /* EE_REPORT_*                                  */
    uint32_t column;     /* source display column (Precinct / Address)  */
    EeValueCount *items; /* aggregated value + count, current sort order */
    uint32_t count;
    int sort_col;        /* 0 = value, 1 = count                        */
    BOOL sort_asc;
};

/* Empty values are shown (and copied) as this label so incomplete records are
 * visible; the underlying value stays "" so filters match blank cells. */
static const wchar_t *report_display_value(const wchar_t *v)
{
    return (v == NULL || v[0] == L'\0') ? L"(blank)" : v;
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

    cmd = (UINT)
        TrackPopupMenu(m, TPM_RIGHTBUTTON | TPM_RETURNCMD, screen.x, screen.y, 0, rw->hwnd, NULL);
    DestroyMenu(m);

    switch (cmd)
    {
        case IDM_EDIT_COPY:
            Report_CopySelected(rw);
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

    if (rw->list == NULL)
    {
        return;
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
            col.pszText = (rw->kind == EE_REPORT_ADDRESS) ? L"Address" : L"Precinct";
            col.cx = Scale(rw->app, (rw->kind == EE_REPORT_ADDRESS) ? 320 : 160);
            ListView_InsertColumn(rw->list, 0, &col);
            col.fmt = LVCFMT_RIGHT;
            col.pszText = L"Number of Voters";
            col.cx = Scale(rw->app, 130);
            ListView_InsertColumn(rw->list, 1, &col);

            ListView_SetItemCountEx(rw->list, (int)rw->count, LVSICF_NOINVALIDATEALL);
            Report_Sort(rw); /* initial ascending by value */
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
        if (grown != NULL)
        {
            wchar_t *empty = (wchar_t *)malloc(sizeof(wchar_t));
            items = grown;
            if (empty != NULL)
            {
                empty[0] = L'\0';
                items[count].value = empty;
                items[count].count = blank;
                count++;
            }
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
            column = (uint32_t)di->item.iSubItem + EE_FROZEN_COLUMN_COUNT;
        }

        if (di->item.mask & LVIF_TEXT)
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
            menu = App_CreateMenu();
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
            return 0;

        case WM_COMMAND:
            switch (LOWORD(wParam))
            {
                case IDM_FILE_OPEN_VOTER_LIST:
                    App_BeginOpenVoterList(app);
                    return 0;
                case IDM_FILE_CLOSE_VOTER_LIST:
                    App_RequestClose(app);
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
                case IDM_FILTER_RESET_VIEW:
                    /* Leave the duplicates view and any filter: show all records. */
                    App_ResetFilter(app);
                    return 0;
                case IDM_REPORT_PRECINCT:
                    App_ShowReport(app, EE_REPORT_PRECINCT);
                    return 0;
                case IDM_REPORT_ADDRESS:
                    App_ShowReport(app, EE_REPORT_ADDRESS);
                    return 0;
                default:
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
            free(app->scan_marks);
            app->scan_marks = NULL;
            App_ClearMarks(app);
            EeFilter_Clear(&app->filters);
            free(app->filter_map);
            app->filter_map = NULL;
            app->filter_count = 0;
            EeVoterTable_Clear(&app->table);
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
            if (g_viewer_count <= 0)
            {
                PostQuitMessage(0);
            }
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
            return App_HeaderCustomDraw(app, (NMCUSTOMDRAW *)lParam);
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
}

static AppState *App_CreateViewer(HINSTANCE instance,
                                  int nCmdShow,
                                  HWND offset_from,
                                  const AppState *prefs)
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

    icc.dwSize = sizeof(icc);
    icc.dwICC = ICC_LISTVIEW_CLASSES | ICC_PROGRESS_CLASS | ICC_BAR_CLASSES | ICC_STANDARD_CLASSES |
                ICC_UPDOWN_CLASS;
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
