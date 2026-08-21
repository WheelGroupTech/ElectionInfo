/**
 * @file main.c
 * @brief ElectionExplorer Win32 GUI: menu, DPI-aware layout, voter grid.
 */

#include "main.h"
#include "resource.h"
#include "voter_table.h"

#include <commctrl.h>
#include <commdlg.h>
#include <intsafe.h>
#include <strsafe.h>
#include <stdlib.h>
#include <uxtheme.h>
#include <windowsx.h>

#pragma comment(lib, "comctl32.lib")
#pragma comment(lib, "comdlg32.lib")
#pragma comment(lib, "uxtheme.lib")

/* -------------------------------------------------------------------------- */
/* Constants                                                                  */
/* -------------------------------------------------------------------------- */

static const wchar_t k_WindowClassName[] = L"ElectionExplorerMainWindow";
static const wchar_t k_WindowTitle[] = L"Election Explorer";
static const wchar_t k_ProgressClassName[] = L"ElectionExplorerLoadProgress";
static const wchar_t k_OptionsClassName[] = L"ElectionExplorerOptions";

static const int k_DefaultWidth = 1100;
static const int k_DefaultHeight = 720;
static const int k_DefaultFrozenWidth = 320;
static const uint32_t k_NameUpdateProgressMinRows = 25000;
static const int k_ZoomMin = 50;
static const int k_ZoomMax = 250;
static const int k_ZoomDefault = 100;

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
    BOOL copy_prepend_normalized;
    BOOL name_surname_first;
    int zoom_percent;  /* 50..250; 100 is actual size */
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
    volatile LONG load_cancel;
    HANDLE load_thread;
    wchar_t load_path[MAX_PATH];
    wchar_t load_error[512];
    EeLoadStatus load_status;
    CRITICAL_SECTION progress_lock;
    EeLoadProgress last_progress;
    BOOL progress_dirty;
} AppState;

static AppState g_app;

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
    wchar_t buf[128];
    if (app->table.row_count == 0)
    {
        App_SetStatus(app, L"No voter list loaded.");
        return;
    }
    StringCchPrintfW(buf, ARRAYSIZE(buf), L"%u voters loaded", app->table.row_count);
    App_SetStatus(app, buf);
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

static void App_SetHeaderSortArrow(HWND hwnd_list, int column, int sort_column, BOOL ascending)
{
    HWND header = ListView_GetHeader(hwnd_list);
    int count = Header_GetItemCount(header);
    int i;
    HDITEMW item;
    BOOL frozen = (hwnd_list == g_app.hwnd_frozen);

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
    int name_w;

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

    id_w = ScaleDisplay(app, 110);
    name_w = inner_w - id_w - ScaleDisplay(app, 2);
    if (name_w < ScaleDisplay(app, 200))
    {
        name_w = ScaleDisplay(app, 200);
    }

    ListView_SetColumnWidth(app->hwnd_frozen, 0, id_w);
    ListView_SetColumnWidth(app->hwnd_frozen, 1, name_w);
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
        ListView_SetItemCountEx(app->hwnd_frozen, 0, LVSICF_NOINVALIDATEALL);
        ListView_SetItemCountEx(app->hwnd_scroll, 0, LVSICF_NOINVALIDATEALL);
        return;
    }

    col_width = ScaleDisplay(app, 120);

    /* Frozen: Voter ID (center) + Name (left) */
    for (i = 0; i < EE_FROZEN_COLUMN_COUNT && i < app->table.column_count; i++)
    {
        ZeroMemory(&col, sizeof(col));
        col.mask = LVCF_TEXT | LVCF_WIDTH | LVCF_SUBITEM | LVCF_FMT;
        /* Column 0 is forced left by ListView for item text; we center-draw it. */
        col.fmt = (i == 0) ? LVCFMT_CENTER : LVCFMT_LEFT;
        col.cx = (i == 0) ? ScaleDisplay(app, 110) : ScaleDisplay(app, 200);
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

    ListView_SetItemCountEx(app->hwnd_frozen,
                            (int)app->table.row_count,
                            LVSICF_NOINVALIDATEALL | LVSICF_NOSCROLL);
    ListView_SetItemCountEx(app->hwnd_scroll,
                            (int)app->table.row_count,
                            LVSICF_NOINVALIDATEALL | LVSICF_NOSCROLL);

    if (app->table.sort_column >= 0)
    {
        if (app->table.sort_column < EE_FROZEN_COLUMN_COUNT)
        {
            App_SetHeaderSortArrow(app->hwnd_frozen,
                                   app->table.sort_column,
                                   app->table.sort_column,
                                   app->table.sort_ascending);
            App_SetHeaderSortArrow(app->hwnd_scroll, -1, -1, TRUE);
        }
        else
        {
            App_SetHeaderSortArrow(app->hwnd_frozen, -1, -1, TRUE);
            App_SetHeaderSortArrow(app->hwnd_scroll,
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
        ListView_SetItemCountEx(app->hwnd_frozen,
                                (int)app->table.row_count,
                                LVSICF_NOINVALIDATEALL);
        ListView_SetItemCountEx(app->hwnd_scroll,
                                (int)app->table.row_count,
                                LVSICF_NOINVALIDATEALL);

        if (table_column < EE_FROZEN_COLUMN_COUNT)
        {
            App_SetHeaderSortArrow(app->hwnd_frozen,
                                   (int)table_column,
                                   (int)table_column,
                                   app->table.sort_ascending);
            App_SetHeaderSortArrow(app->hwnd_scroll, -1, -1, TRUE);
        }
        else
        {
            int local = (int)table_column - EE_FROZEN_COLUMN_COUNT;
            App_SetHeaderSortArrow(app->hwnd_frozen, -1, -1, TRUE);
            App_SetHeaderSortArrow(app->hwnd_scroll, local, local, app->table.sort_ascending);
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
    App_UpdateRowStatus(app);
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
                if (app->hwnd_progress_status)
                {
                    SetWindowTextW(app->hwnd_progress_status, L"Cancelling…");
                }
                EnableWindow(GetDlgItem(hwnd, IDC_PROGRESS_CANCEL), FALSE);
            }
            return 0;

        case WM_CLOSE:
            /* Force cancel rather than destroy mid-load. */
            if (app != NULL && app->loading)
            {
                InterlockedExchange(&app->load_cancel, 1);
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

static void App_BeginOpenVoterList(AppState *app)
{
    OPENFILENAMEW ofn;
    wchar_t path[MAX_PATH];

    if (app->loading)
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
    ofn.lpstrTitle = L"Open Voter List";

    if (!GetOpenFileNameW(&ofn))
    {
        return;
    }

    StringCchCopyW(app->load_path, ARRAYSIZE(app->load_path), path);
    InterlockedExchange(&app->load_cancel, 0);
    app->loading = TRUE;
    App_SetStatus(app, L"Loading voter list…");

    if (!App_ShowProgress(app))
    {
        app->loading = FALSE;
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
        EnableWindow(app->hwnd_main, TRUE);
        App_DestroyProgress(app);
        MessageBoxW(app->hwnd_main,
                    L"Could not start the load thread.",
                    k_WindowTitle,
                    MB_ICONERROR | MB_OK);
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
        App_RebuildColumns(app);
        App_SetStatus(app, L"Load cancelled.");
        SetWindowTextW(app->hwnd_main, k_WindowTitle);
    }
    else
    {
        EeVoterTable_Clear(&app->table);
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
    AppendMenuW(file_menu, MF_STRING, IDM_FILE_OPEN_VOTER_LIST, L"&Open Voter List…\tCtrl+O");
    AppendMenuW(file_menu, MF_SEPARATOR, 0, NULL);
    AppendMenuW(file_menu, MF_STRING, IDM_FILE_EXIT, L"E&xit");
    AppendMenuW(edit_menu, MF_STRING, IDM_EDIT_COPY, L"&Copy\tCtrl+C");
    AppendMenuW(edit_menu, MF_SEPARATOR, 0, NULL);
    AppendMenuW(edit_menu, MF_STRING, IDM_EDIT_OPTIONS, L"&Options…");
    AppendMenuW(menu, MF_POPUP, (UINT_PTR)file_menu, L"&File");
    AppendMenuW(menu, MF_POPUP, (UINT_PTR)edit_menu, L"&Edit");
    return menu;
}

/* -------------------------------------------------------------------------- */
/* Selection, copy, options                                                   */
/* -------------------------------------------------------------------------- */

static LONG g_syncing_selection = 0;
static LONG g_sel_sync_posted = 0;
static HWND g_sel_sync_source = NULL;

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
    if (app == NULL || source == NULL ||
        InterlockedCompareExchange(&g_syncing_selection, 0, 0) != 0)
    {
        return;
    }
    g_sel_sync_source = source;
    if (InterlockedCompareExchange(&g_sel_sync_posted, 1, 0) == 0)
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

    InterlockedExchange(&g_syncing_selection, 1);
    ListView_SetItemState(dest, -1, 0, LVIS_SELECTED | LVIS_FOCUSED);
    i = -1;
    while ((i = ListView_GetNextItem(source, i, LVNI_SELECTED)) >= 0)
    {
        ListView_SetItemState(dest, i, LVIS_SELECTED, LVIS_SELECTED);
    }
    InterlockedExchange(&g_syncing_selection, 0);
    InvalidateRect(dest, NULL, TRUE);
}

static void App_ClearSelection(AppState *app)
{
    if (app == NULL)
    {
        return;
    }
    InterlockedExchange(&g_syncing_selection, 1);
    if (app->hwnd_frozen != NULL)
    {
        ListView_SetItemState(app->hwnd_frozen, -1, 0, LVIS_SELECTED | LVIS_FOCUSED);
    }
    if (app->hwnd_scroll != NULL)
    {
        ListView_SetItemState(app->hwnd_scroll, -1, 0, LVIS_SELECTED | LVIS_FOCUSED);
    }
    InterlockedExchange(&g_syncing_selection, 0);
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
        rows[n++] = (uint32_t)i;
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
    cmd = (UINT)
        TrackPopupMenu(menu, TPM_RIGHTBUTTON | TPM_RETURNCMD, pt.x, pt.y, 0, app->hwnd_main, NULL);
    DestroyMenu(menu);
    if (cmd == IDM_EDIT_COPY)
    {
        App_CopySelection(app);
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
        if (g_app.hwnd_options != NULL && IsDialogMessageW(g_app.hwnd_options, &msg))
        {
            continue;
        }
        if (g_app.hwnd_progress != NULL && IsDialogMessageW(g_app.hwnd_progress, &msg))
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
                        if (!parsed)
                        {
                            zoom =
                                (UINT)(app->zoom_percent > 0 ? app->zoom_percent : k_ZoomDefault);
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
    client_h = Scale(app, 210);
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

    chk_prepend = CreateWindowExW(0,
                                  L"BUTTON",
                                  L"Pre-pend normalized data for copies",
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
    btn_cancel = CreateWindowExW(0,
                                 L"BUTTON",
                                 L"Cancel",
                                 WS_CHILD | WS_VISIBLE | WS_TABSTOP | BS_PUSHBUTTON,
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
                uint32_t view_row = (uint32_t)item;
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
        uint32_t view_row = (uint32_t)di->item.iItem;
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
    AppState *app = &g_app;

    switch (msg)
    {
        case WM_CREATE:
        {
            HMENU menu;
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
            return 0;

        case WM_COMMAND:
            switch (LOWORD(wParam))
            {
                case IDM_FILE_OPEN_VOTER_LIST:
                    App_BeginOpenVoterList(app);
                    return 0;
                case IDM_FILE_EXIT:
                    PostMessageW(hwnd, WM_CLOSE, 0, 0);
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

        case EEM_SYNC_PANE_SCROLLUI:
            App_SyncPaneScrollChrome(app);
            return 0;

        case EEM_SYNC_SELECTION:
            InterlockedExchange(&g_sel_sync_posted, 0);
            App_CopySelectionToOtherPane(app, g_sel_sync_source);
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
            if (app->loading)
            {
                InterlockedExchange(&app->load_cancel, 1);
                App_SetStatus(app, L"Cancelling load…");
                return 0;
            }
            DestroyWindow(hwnd);
            return 0;

        case WM_DESTROY:
            App_DestroyOptions(app);
            App_DestroyProgress(app);
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
            PostQuitMessage(0);
            return 0;

        default:
            break;
    }
    return DefWindowProcW(hwnd, msg, wParam, lParam);
}

/* Subclass list views to sync vertical scrolling on wheel / scroll messages. */
static WNDPROC g_old_frozen_proc;
static WNDPROC g_old_scroll_proc;
static WNDPROC g_old_title_proc;

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
            RECT rc;
            wchar_t text[64];
            HFONT font;
            HFONT old_font = NULL;
            HPEN pen;
            HPEN old_pen;
            int pen_w;
            HBRUSH brush =
                g_app.brush_header != NULL ? g_app.brush_header : GetSysColorBrush(COLOR_BTNFACE);

            GetClientRect(hwnd, &rc);
            FillRect(hdc, &rc, brush);

            text[0] = L'\0';
            GetWindowTextW(hwnd, text, ARRAYSIZE(text));
            font = g_app.font_header != NULL ? g_app.font_header : g_app.font_ui;
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

            pen_w = Scale(&g_app, 1);
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
    if (msg == WM_CONTEXTMENU)
    {
        App_ShowCopyContextMenu(&g_app, hwnd, GET_X_LPARAM(lParam), GET_Y_LPARAM(lParam));
        return 0;
    }

    /* Header NM_CUSTOMDRAW is delivered to the ListView (header parent). */
    if (msg == WM_NOTIFY)
    {
        NMHDR *nm = (NMHDR *)lParam;
        HWND header = ListView_GetHeader(hwnd);
        if (nm != NULL && header != NULL && nm->hwndFrom == header && nm->code == NM_CUSTOMDRAW)
        {
            return App_HeaderCustomDraw(&g_app, (NMCUSTOMDRAW *)lParam);
        }
    }

    {
        LRESULT r = CallWindowProcW(old_proc, hwnd, msg, wParam, lParam);
        if (msg == WM_VSCROLL || msg == WM_MOUSEWHEEL || msg == WM_MOUSEHWHEEL || msg == WM_KEYDOWN)
        {
            App_SyncVerticalScroll(&g_app, hwnd);
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
    HWND hwnd;
    MSG msg;
    INITCOMMONCONTROLSEX icc;
    ACCEL accels[2];
    HACCEL haccel;
    BOOL getResult;
    UINT dpi;

    (void)hPrevInstance;
    (void)lpCmdLine;

    /* Per-monitor v2 (manifest also declares this). */
    SetProcessDpiAwarenessContext(DPI_AWARENESS_CONTEXT_PER_MONITOR_AWARE_V2);

    ZeroMemory(&g_app, sizeof(g_app));
    g_app.instance = hInstance;
    g_app.dpi = 96;
    g_app.copy_prepend_normalized = TRUE;
    g_app.name_surname_first = TRUE;
    g_app.zoom_percent = k_ZoomDefault;
    App_UpdateDpiMetrics(&g_app, GetDpiForSystem());
    InitializeCriticalSection(&g_app.progress_lock);
    EeVoterTable_Init(&g_app.table);

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

    hwnd = CreateWindowExW(0,
                           k_WindowClassName,
                           k_WindowTitle,
                           WS_OVERLAPPEDWINDOW,
                           CW_USEDEFAULT,
                           CW_USEDEFAULT,
                           k_DefaultWidth,
                           k_DefaultHeight,
                           NULL,
                           NULL,
                           hInstance,
                           NULL);
    if (hwnd == NULL)
    {
        return 1;
    }

    dpi = GetDpiForWindow(hwnd);
    App_UpdateDpiMetrics(&g_app, dpi);
    App_ApplyFont(&g_app);

    /* Scale initial size for DPI. */
    {
        RECT wr;
        GetWindowRect(hwnd, &wr);
        SetWindowPos(hwnd,
                     NULL,
                     wr.left,
                     wr.top,
                     Scale(&g_app, k_DefaultWidth),
                     Scale(&g_app, k_DefaultHeight),
                     SWP_NOZORDER | SWP_NOMOVE);
    }
    App_Layout(&g_app);

    g_old_frozen_proc =
        (WNDPROC)SetWindowLongPtrW(g_app.hwnd_frozen, GWLP_WNDPROC, (LONG_PTR)FrozenSubclass);
    g_old_scroll_proc =
        (WNDPROC)SetWindowLongPtrW(g_app.hwnd_scroll, GWLP_WNDPROC, (LONG_PTR)ScrollSubclass);
    g_old_title_proc = (WNDPROC)SetWindowLongPtrW(g_app.hwnd_frozen_title,
                                                  GWLP_WNDPROC,
                                                  (LONG_PTR)PaneTitleSubclass);
    SetWindowLongPtrW(g_app.hwnd_scroll_title, GWLP_WNDPROC, (LONG_PTR)PaneTitleSubclass);

    accels[0].fVirt = FCONTROL | FVIRTKEY;
    accels[0].key = 'O';
    accels[0].cmd = IDM_FILE_OPEN_VOTER_LIST;
    accels[1].fVirt = FCONTROL | FVIRTKEY;
    accels[1].key = 'C';
    accels[1].cmd = IDM_EDIT_COPY;
    haccel = CreateAcceleratorTableW(accels, 2);

    ShowWindow(hwnd, nCmdShow);
    UpdateWindow(hwnd);

    while ((getResult = GetMessageW(&msg, NULL, 0, 0)) != 0)
    {
        if (getResult == -1)
        {
            break;
        }
        if (g_app.hwnd_options != NULL && IsDialogMessageW(g_app.hwnd_options, &msg))
        {
            continue;
        }
        if (haccel == NULL || !TranslateAcceleratorW(hwnd, haccel, &msg))
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
