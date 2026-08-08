/**
 * @file main.c
 * @brief ElectionExplorer Win32 GUI entry point (no MFC).
 *
 * Links the static CRT (/MT, /MTd) so the binary runs on Windows 10/11
 * x64 and ARM64 without the Visual C++ redistributable.
 */

#include "main.h"

static const wchar_t k_WindowClassName[] = L"ElectionExplorerMainWindow";
static const wchar_t k_WindowTitle[] = L"Election Explorer";

static const int k_DefaultWidth = 960;
static const int k_DefaultHeight = 640;

LRESULT CALLBACK ElectionExplorer_WindowProc(HWND hwnd,
                                             UINT msg,
                                             WPARAM wParam,
                                             LPARAM lParam)
{
    switch (msg)
    {
    case WM_DESTROY:
        PostQuitMessage(0);
        return 0;

    case WM_PAINT:
    {
        PAINTSTRUCT ps;
        HDC hdc = BeginPaint(hwnd, &ps);
        if (hdc != NULL)
        {
            FillRect(hdc, &ps.rcPaint, (HBRUSH)(COLOR_WINDOW + 1));
            EndPaint(hwnd, &ps);
        }
        return 0;
    }

    default:
        return DefWindowProcW(hwnd, msg, wParam, lParam);
    }
}

int APIENTRY wWinMain(_In_ HINSTANCE hInstance,
                      _In_opt_ HINSTANCE hPrevInstance,
                      _In_ LPWSTR lpCmdLine,
                      _In_ int nCmdShow)
{
    WNDCLASSEXW wc;
    HWND hwnd;
    MSG msg;
    BOOL getResult;

    (void)hPrevInstance;
    (void)lpCmdLine;

    ZeroMemory(&wc, sizeof(wc));
    wc.cbSize = sizeof(wc);
    wc.style = CS_HREDRAW | CS_VREDRAW;
    wc.lpfnWndProc = ElectionExplorer_WindowProc;
    wc.hInstance = hInstance;
    wc.hCursor = LoadCursorW(NULL, IDC_ARROW);
    wc.hbrBackground = (HBRUSH)(COLOR_WINDOW + 1);
    wc.lpszClassName = k_WindowClassName;
    wc.hIcon = LoadIconW(NULL, IDI_APPLICATION);
    wc.hIconSm = LoadIconW(NULL, IDI_APPLICATION);

    if (RegisterClassExW(&wc) == 0)
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

    ShowWindow(hwnd, nCmdShow);
    UpdateWindow(hwnd);

    while ((getResult = GetMessageW(&msg, NULL, 0, 0)) != 0)
    {
        if (getResult == -1)
        {
            return 1;
        }

        TranslateMessage(&msg);
        DispatchMessageW(&msg);
    }

    return (int)msg.wParam;
}
