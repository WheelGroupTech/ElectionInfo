/**
 * @file main.h
 * @brief ElectionExplorer Win32 application entry and window procedure.
 */
#pragma once

#include <windows.h>

/**
 * @brief Standard Win32 window procedure for the main application window.
 *
 * @param hwnd    Window handle.
 * @param msg     Message identifier.
 * @param wParam  Message WPARAM.
 * @param lParam  Message LPARAM.
 * @return Message result.
 */
LRESULT CALLBACK ElectionExplorer_WindowProc(HWND hwnd,
                                             UINT msg,
                                             WPARAM wParam,
                                             LPARAM lParam);
