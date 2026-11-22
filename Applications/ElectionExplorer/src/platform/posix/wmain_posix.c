/**
 * @file wmain_posix.c
 * @brief macOS / Linux entry point for console programs written with Windows' wmain: converts
 *        the UTF-8 arguments to wide strings (UTF-16 units, see include/windows.h) and calls
 *        wmain. Linked only into programs that define wmain (the tests).
 */

#include <windows.h>

/* The locale stays "C", as a Windows program's CRT starts: number formatting and parsing,
 * and towupper / towlower, then behave as on Windows. */
int wmain(int argc, wchar_t **argv);

int main(int argc, char **argv)
{
    wchar_t **wargv;
    int i, rc;

    wargv = (wchar_t **)calloc((size_t)argc + 1, sizeof(wchar_t *));
    if (wargv == NULL)
    {
        return 1;
    }
    for (i = 0; i < argc; i++)
    {
        int n = MultiByteToWideChar(CP_UTF8, 0, argv[i], -1, NULL, 0);
        wargv[i] = (wchar_t *)malloc((size_t)(n > 0 ? n : 1) * sizeof(wchar_t));
        if (wargv[i] == NULL)
        {
            return 1;
        }
        wargv[i][0] = L'\0';
        if (n > 0)
        {
            MultiByteToWideChar(CP_UTF8, 0, argv[i], -1, wargv[i], n);
        }
    }
    rc = wmain(argc, wargv);
    for (i = 0; i < argc; i++)
    {
        free(wargv[i]);
    }
    free(wargv);
    return rc;
}
