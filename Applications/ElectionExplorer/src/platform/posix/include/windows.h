/**
 * @file windows.h
 * @brief macOS / Linux stand-in for the Windows headers the Election Explorer engine uses.
 *
 * The engine (everything in src/ except main.c, ocr_win.c and settings.c) was written against
 * Win32. On macOS and Linux the CMake build puts this directory first on the include path,
 * so the engine's `#include <windows.h>`, `<strsafe.h>` and `<intsafe.h>` land here and the
 * engine sources build unchanged. The Windows build never sees this directory.
 *
 * What it supplies: the Win32 types and constants the engine names, and the subset of the
 * Win32 / MSVC CRT functions it calls (strsafe, code-page conversion, file I/O, MSVC string
 * extensions, qsort_s, interlocked operations, overflow-checked arithmetic). They are
 * implemented in src/platform/posix/ee_port.c.
 *
 * Text model: a wchar_t string holds UTF-16 code units on every platform. wchar_t is 4 bytes
 * on macOS and Linux, so each unit takes 4 bytes there, but the values (including surrogate
 * pairs above U+FFFF) and lengths are the ones Windows sees. MultiByteToWideChar produces
 * surrogate pairs and WideCharToMultiByte joins them, so the engine's UTF-16 logic works
 * unchanged. Platform wide-string APIs that expect UTF-32 (wprintf, mbstowcs) are not used
 * on engine strings; the printf family below follows the MSVC rules instead.
 *
 * Paths: file functions take wide paths, convert them to UTF-8, and turn '\\' into '/', so
 * paths built with Windows separators (as the tests do) work.
 */
#pragma once

#ifdef _WIN32
#error "src/platform/posix/include must not be on the include path of a Windows build"
#endif

#include <limits.h>
#include <stdarg.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <wchar.h>
#include <wctype.h>

#ifdef __cplusplus
extern "C"
{
#endif

    /* ---- Calling conventions and MSVC keywords ------------------------------------------- */

#define WINAPI
#define CALLBACK
#define __cdecl
#define __stdcall
#define __forceinline             inline
#define __declspec(x)             EE_PORT_DECLSPEC_##x
#define EE_PORT_DECLSPEC_thread   _Thread_local
#define EE_PORT_DECLSPEC_noinline __attribute__((noinline))
#define _Printf_format_string_
#define _Analysis_assume_(e) ((void)0)
#define _In_
#define _In_opt_
#define _Out_
#define _Out_opt_
#define _Inout_
#define UNREFERENCED_PARAMETER(p) ((void)(p))

    /* ---- Types ---------------------------------------------------------------------------- */

    typedef int BOOL;
    typedef unsigned char BYTE;
    typedef unsigned char UCHAR;
    typedef uint16_t WORD;
    typedef uint16_t USHORT;
    typedef uint32_t DWORD;
    typedef uint32_t UINT;
    typedef uint32_t ULONG;
    typedef int32_t LONG;
    typedef int INT;
    typedef short SHORT;
    typedef int64_t LONGLONG;
    typedef uint64_t ULONGLONG;
    typedef int64_t INT64;
    typedef uint64_t UINT64;
    typedef uint64_t DWORD64;
    typedef uintptr_t ULONG_PTR;
    typedef uintptr_t DWORD_PTR;
    typedef uintptr_t UINT_PTR;
    typedef intptr_t LONG_PTR;
    typedef intptr_t INT_PTR;
    typedef size_t SIZE_T;
    typedef float FLOAT;
    typedef char CHAR;
    typedef wchar_t WCHAR;
    typedef WCHAR *LPWSTR;
    typedef WCHAR *PWSTR;
    typedef const WCHAR *LPCWSTR;
    typedef const WCHAR *PCWSTR;
    typedef CHAR *LPSTR;
    typedef const CHAR *LPCSTR;
    typedef void *LPVOID;
    typedef const void *LPCVOID;
    typedef DWORD *LPDWORD;
    typedef BOOL *LPBOOL;
    typedef void *HANDLE;
    typedef HANDLE *PHANDLE;
    typedef int32_t HRESULT;

    typedef union _LARGE_INTEGER
    {
        struct
        {
            DWORD LowPart;
            LONG HighPart;
        };
        LONGLONG QuadPart;
    } LARGE_INTEGER, *PLARGE_INTEGER;

    typedef struct _FILETIME
    {
        DWORD dwLowDateTime;
        DWORD dwHighDateTime;
    } FILETIME, *PFILETIME, *LPFILETIME;

    typedef struct _SYSTEMTIME
    {
        WORD wYear;
        WORD wMonth;
        WORD wDayOfWeek;
        WORD wDay;
        WORD wHour;
        WORD wMinute;
        WORD wSecond;
        WORD wMilliseconds;
    } SYSTEMTIME, *PSYSTEMTIME, *LPSYSTEMTIME;

    typedef struct _WIN32_FILE_ATTRIBUTE_DATA
    {
        DWORD dwFileAttributes;
        FILETIME ftCreationTime;
        FILETIME ftLastAccessTime;
        FILETIME ftLastWriteTime;
        DWORD nFileSizeHigh;
        DWORD nFileSizeLow;
    } WIN32_FILE_ATTRIBUTE_DATA;

    typedef enum _GET_FILEEX_INFO_LEVELS
    {
        GetFileExInfoStandard,
        GetFileExMaxInfoLevel
    } GET_FILEEX_INFO_LEVELS;

    typedef struct _SECURITY_ATTRIBUTES SECURITY_ATTRIBUTES, *LPSECURITY_ATTRIBUTES;

    /* Only the read/write offset is used (ReadFile / WriteFile at a position, as pread / pwrite). */
    typedef struct _OVERLAPPED
    {
        ULONG_PTR Internal;
        ULONG_PTR InternalHigh;
        DWORD Offset;
        DWORD OffsetHigh;
        HANDLE hEvent;
    } OVERLAPPED, *LPOVERLAPPED;

    typedef int64_t __int64;
    typedef int errno_t;
#define _TRUNCATE ((size_t)-1)
#define STRUNCATE 80

    /* ---- Constants ------------------------------------------------------------------------ */

#ifndef TRUE
#define TRUE 1
#endif
#ifndef FALSE
#define FALSE 0
#endif
#define MAX_PATH 260

#define INVALID_HANDLE_VALUE    ((HANDLE)(intptr_t)-1)
#define INVALID_FILE_SIZE       ((DWORD)0xFFFFFFFF)
#define INVALID_FILE_ATTRIBUTES ((DWORD) - 1)

#define GENERIC_READ              0x80000000u
#define GENERIC_WRITE             0x40000000u
#define FILE_READ_ATTRIBUTES      0x00000080u
#define FILE_WRITE_ATTRIBUTES     0x00000100u
#define FILE_SHARE_READ           0x00000001u
#define FILE_SHARE_WRITE          0x00000002u
#define FILE_SHARE_DELETE         0x00000004u
#define CREATE_NEW                1
#define CREATE_ALWAYS             2
#define OPEN_EXISTING             3
#define OPEN_ALWAYS               4
#define TRUNCATE_EXISTING         5
#define FILE_ATTRIBUTE_READONLY   0x00000001u
#define FILE_ATTRIBUTE_DIRECTORY  0x00000010u
#define FILE_ATTRIBUTE_NORMAL     0x00000080u
#define FILE_FLAG_SEQUENTIAL_SCAN 0x08000000u
#define FILE_BEGIN                0
#define FILE_CURRENT              1
#define FILE_END                  2

#define CP_ACP               0
#define CP_OEMCP             1
#define CP_UTF8              65001
#define MB_PRECOMPOSED       0x00000001
#define MB_ERR_INVALID_CHARS 0x00000008
#define WC_ERR_INVALID_CHARS 0x00000080
#define WC_NO_BEST_FIT_CHARS 0x00000400

#define CSTR_LESS_THAN    1
#define CSTR_EQUAL        2
#define CSTR_GREATER_THAN 3

#define ERROR_SUCCESS                0L
#define ERROR_FILE_NOT_FOUND         2L
#define ERROR_PATH_NOT_FOUND         3L
#define ERROR_ACCESS_DENIED          5L
#define ERROR_INVALID_HANDLE         6L
#define ERROR_NOT_ENOUGH_MEMORY      8L
#define ERROR_INVALID_DATA           13L
#define ERROR_HANDLE_EOF             38L
#define ERROR_INVALID_PARAMETER      87L
#define ERROR_INSUFFICIENT_BUFFER    122L
#define ERROR_NO_UNICODE_TRANSLATION 1113L

#define S_OK                          ((HRESULT)0)
#define S_FALSE                       ((HRESULT)1)
#define E_FAIL                        ((HRESULT)0x80004005)
#define E_OUTOFMEMORY                 ((HRESULT)0x8007000E)
#define E_INVALIDARG                  ((HRESULT)0x80070057)
#define E_NOTIMPL                     ((HRESULT)0x80004001)
#define SUCCEEDED(hr)                 (((HRESULT)(hr)) >= 0)
#define FAILED(hr)                    (((HRESULT)(hr)) < 0)
#define STRSAFE_E_INSUFFICIENT_BUFFER ((HRESULT)0x8007007A)
#define STRSAFE_E_INVALID_PARAMETER   ((HRESULT)0x80070057)
#define STRSAFE_E_END_OF_FILE         ((HRESULT)0x80070026)
#define STRSAFE_MAX_CCH               2147483647
#define INTSAFE_E_ARITHMETIC_OVERFLOW ((HRESULT)0x80070216)

    /* ---- Macros --------------------------------------------------------------------------- */

#define ARRAYSIZE(a)           (sizeof(a) / sizeof((a)[0]))
#define _countof(a)            (sizeof(a) / sizeof((a)[0]))
#define ZeroMemory(p, n)       memset((p), 0, (n))
#define SecureZeroMemory(p, n) memset((p), 0, (n))
#define CopyMemory(d, s, n)    memcpy((d), (s), (n))
#define MoveMemory(d, s, n)    memmove((d), (s), (n))
#define FillMemory(d, n, v)    memset((d), (v), (n))
#ifndef min
#define min(a, b) (((a) < (b)) ? (a) : (b))
#endif
#ifndef max
#define max(a, b) (((a) > (b)) ? (a) : (b))
#endif
#define IS_HIGH_SURROGATE(w) ((((uint32_t)(w)) & 0xFFFFFC00u) == 0xD800u)
#define IS_LOW_SURROGATE(w)  ((((uint32_t)(w)) & 0xFFFFFC00u) == 0xDC00u)
#define MAKEWORD(a, b)       ((WORD)(((BYTE)(a)) | ((WORD)((BYTE)(b))) << 8))
#define MAKELONG(a, b)       ((LONG)(((WORD)(a)) | ((DWORD)((WORD)(b))) << 16))
#define LOWORD(l)            ((WORD)((DWORD_PTR)(l) & 0xffff))
#define HIWORD(l)            ((WORD)(((DWORD_PTR)(l) >> 16) & 0xffff))
#define LOBYTE(w)            ((BYTE)((DWORD_PTR)(w) & 0xff))
#define HIBYTE(w)            ((BYTE)(((DWORD_PTR)(w) >> 8) & 0xff))

    /* ---- Errors --------------------------------------------------------------------------- */

    DWORD GetLastError(void);
    void SetLastError(DWORD err);

    /* ---- strsafe (STRSAFE_*: truncates, always NUL-terminates when cch > 0) ---------------- */

    HRESULT StringCchCopyW(wchar_t *dst, size_t cch, const wchar_t *src);
    HRESULT StringCchCopyNW(wchar_t *dst, size_t cch, const wchar_t *src, size_t max_src);
    HRESULT StringCchCatW(wchar_t *dst, size_t cch, const wchar_t *src);
    HRESULT StringCchCatNW(wchar_t *dst, size_t cch, const wchar_t *src, size_t max_src);
    HRESULT StringCchLengthW(const wchar_t *s, size_t max_cch, size_t *len);
    HRESULT StringCchPrintfW(wchar_t *dst, size_t cch, const wchar_t *fmt, ...);
    HRESULT StringCchVPrintfW(wchar_t *dst, size_t cch, const wchar_t *fmt, va_list ap);
    HRESULT StringCchCopyA(char *dst, size_t cch, const char *src);
    HRESULT StringCchCopyNA(char *dst, size_t cch, const char *src, size_t max_src);
    HRESULT StringCchCatA(char *dst, size_t cch, const char *src);
    HRESULT StringCchCatNA(char *dst, size_t cch, const char *src, size_t max_src);
    HRESULT StringCchLengthA(const char *s, size_t max_cch, size_t *len);
    HRESULT StringCchPrintfA(char *dst, size_t cch, const char *fmt, ...);
    HRESULT StringCchVPrintfA(char *dst, size_t cch, const char *fmt, va_list ap);

    /* ---- printf with MSVC rules: in a wide format %s / %c take wide arguments and %S / %hs
 * narrow ones; in a narrow format %S / %ls / %ws take wide ones. Wide text goes out as UTF-8.
 * The macros replace the platform functions, whose %s rules differ. ---------------------- */

    int EePort_wprintf(const wchar_t *fmt, ...);
    int EePort_fwprintf(FILE *fp, const wchar_t *fmt, ...);
    int EePort_vfwprintf(FILE *fp, const wchar_t *fmt, va_list ap);
    int EePort_swprintf(wchar_t *dst, size_t cch, const wchar_t *fmt, ...);
    int EePort_vswprintf(wchar_t *dst, size_t cch, const wchar_t *fmt, va_list ap);
    int EePort_fputws(const wchar_t *s, FILE *fp);
#define wprintf                        EePort_wprintf
#define fwprintf                       EePort_fwprintf
#define vfwprintf                      EePort_vfwprintf
#define swprintf_s                     EePort_swprintf
#define _snwprintf_s(d, cch, cnt, ...) EePort_swprintf((d), (cch), __VA_ARGS__)
#define fputws                         EePort_fputws

    /* ---- Code pages: CP_UTF8, CP_ACP / 1252 (Windows-1252), 28591 (ISO-8859-1) ------------- */

    int MultiByteToWideChar(UINT cp, DWORD flags, const char *src, int cb, wchar_t *dst, int cch);
    int WideCharToMultiByte(UINT cp,
                            DWORD flags,
                            const wchar_t *src,
                            int cch,
                            char *dst,
                            int cb,
                            const char *def,
                            BOOL *used_def);

    /* ---- MSVC CRT string extensions ------------------------------------------------------- */

    int _wcsicmp(const wchar_t *a, const wchar_t *b);
    int _wcsnicmp(const wchar_t *a, const wchar_t *b, size_t n);
    int _stricmp(const char *a, const char *b);
    int _strnicmp(const char *a, const char *b, size_t n);
    wchar_t *_wcsdup(const wchar_t *s);
    char *_strdup(const char *s);
    int _wtoi(const wchar_t *s);
    long _wtol(const wchar_t *s);
    long long _wtoi64(const wchar_t *s);
    double _wtof(const wchar_t *s);
    int _wcsupr_s(wchar_t *s, size_t cch);
    int _wcslwr_s(wchar_t *s, size_t cch);
    int wcsncpy_s(wchar_t *dst, size_t cch, const wchar_t *src, size_t count);
    int wcscpy_s(wchar_t *dst, size_t cch, const wchar_t *src);
    int strcpy_s(char *dst, size_t cch, const char *src);
    int memcpy_s(void *dst, size_t dst_size, const void *src, size_t count);
#define wcstok_s(s, delim, ctx) wcstok((s), (delim), (ctx))
#define strtok_s(s, delim, ctx) strtok_r((s), (delim), (ctx))
#define _strtoui64              strtoull
#define _strtoi64               strtoll
#define _wcstoui64              wcstoull
#define _wcstoi64               wcstoll
#define _atoi64                 atoll

    /* scanf with MSVC's _s rule: %s, %c and %[ take a buffer and then its size (unsigned). At
 * most 16 conversions. swscanf_s takes numeric conversions only (no %s / %c / %[). */
    int sscanf_s(const char *s, const char *fmt, ...);
    int swscanf_s(const wchar_t *s, const wchar_t *fmt, ...);

    /* Sort with a context argument, MSVC argument order: compare(context, a, b). */
    void qsort_s(void *base,
                 size_t num,
                 size_t width,
                 int (*compare)(void *context, const void *a, const void *b),
                 void *context);

/* Large-file positioning. */
#define _fseeki64 fseeko
#define _ftelli64 ftello

    /* _wfopen_s: mode letters as fopen ("rb", "wb", ...); returns 0 or an errno value. */
    int _wfopen_s(FILE **fp, const wchar_t *path, const wchar_t *mode);
    FILE *_wfopen(const wchar_t *path, const wchar_t *mode);

    /* ---- Case mapping (simple, one-to-one: Latin-1, Latin Extended-A, Greek, Cyrillic) ----- */

    int CompareStringOrdinal(const wchar_t *a, int na, const wchar_t *b, int nb, BOOL ignore_case);
    DWORD CharLowerBuffW(wchar_t *s, DWORD n);
    DWORD CharUpperBuffW(wchar_t *s, DWORD n);
    DWORD CharLowerBuffA(char *s, DWORD n); /* Windows-1252 text */
    DWORD CharUpperBuffA(char *s, DWORD n);

    /* Character classes as the MSVC CRT gives them: iswalpha and friends know non-ASCII letters
 * (glibc's and macOS's "C" locale know only ASCII), while towupper / towlower, like MSVC's in
 * the "C" locale, change only ASCII letters, so they are left to the platform. */
    int EePort_iswalpha(wint_t c);
    int EePort_iswalnum(wint_t c);
    int EePort_iswupper(wint_t c);
    int EePort_iswlower(wint_t c);
#define iswalpha EePort_iswalpha
#define iswalnum EePort_iswalnum
#define iswupper EePort_iswupper
#define iswlower EePort_iswlower

    /* ---- Files ---------------------------------------------------------------------------- */

    HANDLE CreateFileW(const wchar_t *path,
                       DWORD access,
                       DWORD share,
                       LPSECURITY_ATTRIBUTES sa,
                       DWORD disposition,
                       DWORD flags,
                       HANDLE tmpl);
    BOOL ReadFile(HANDLE h, void *buf, DWORD n, DWORD *got, LPOVERLAPPED ov);
    BOOL WriteFile(HANDLE h, const void *buf, DWORD n, DWORD *wrote, LPOVERLAPPED ov);
    BOOL GetFileSizeEx(HANDLE h, LARGE_INTEGER *size);
    DWORD GetFileSize(HANDLE h, DWORD *high);
    BOOL SetFilePointerEx(HANDLE h, LARGE_INTEGER dist, LARGE_INTEGER *pos, DWORD method);
    BOOL GetFileTime(HANDLE h, FILETIME *created, FILETIME *accessed, FILETIME *written);
    BOOL SetFileTime(HANDLE h,
                     const FILETIME *created,
                     const FILETIME *accessed,
                     const FILETIME *written);
    BOOL CloseHandle(HANDLE h);
    BOOL DeleteFileW(const wchar_t *path);
    BOOL CreateDirectoryW(const wchar_t *path, LPSECURITY_ATTRIBUTES sa);
    BOOL RemoveDirectoryW(const wchar_t *path);
    DWORD GetFileAttributesW(const wchar_t *path);
    BOOL GetFileAttributesExW(const wchar_t *path, GET_FILEEX_INFO_LEVELS level, void *info);
    DWORD GetTempPathW(DWORD cch, wchar_t *buf);
    DWORD GetEnvironmentVariableW(const wchar_t *name, wchar_t *buf, DWORD cch);
    DWORD GetCurrentDirectoryW(DWORD cch, wchar_t *buf);

    /* ---- Time and process ----------------------------------------------------------------- */

    void GetLocalTime(SYSTEMTIME *st);
    void GetSystemTime(SYSTEMTIME *st);
    BOOL FileTimeToSystemTime(const FILETIME *ft, SYSTEMTIME *st);
    BOOL FileTimeToLocalFileTime(const FILETIME *ft, FILETIME *local);
    BOOL SystemTimeToFileTime(const SYSTEMTIME *st, FILETIME *ft);
    DWORD GetTickCount(void);
    ULONGLONG GetTickCount64(void);
    DWORD GetCurrentProcessId(void);
    DWORD GetCurrentThreadId(void);
    void Sleep(DWORD ms);

    /* ---- Interlocked (sequentially consistent) -------------------------------------------- */

#define InterlockedExchange(p, v)                  __atomic_exchange_n((p), (v), __ATOMIC_SEQ_CST)
#define InterlockedIncrement(p)                    __atomic_add_fetch((p), 1, __ATOMIC_SEQ_CST)
#define InterlockedDecrement(p)                    __atomic_sub_fetch((p), 1, __ATOMIC_SEQ_CST)
#define InterlockedExchangeAdd(p, v)               __atomic_fetch_add((p), (v), __ATOMIC_SEQ_CST)
#define InterlockedCompareExchange(p, x, c)        ee_port_cas32((volatile LONG *)(p), (x), (c))
#define InterlockedCompareExchangePointer(p, x, c) ee_port_casptr((void *volatile *)(p), (x), (c))
    static inline LONG ee_port_cas32(volatile LONG *p, LONG xchg, LONG cmp)
    {
        __atomic_compare_exchange_n(p, &cmp, xchg, 0, __ATOMIC_SEQ_CST, __ATOMIC_SEQ_CST);
        return cmp;
    }
    static inline void *ee_port_casptr(void *volatile *p, void *xchg, void *cmp)
    {
        __atomic_compare_exchange_n(p, &cmp, xchg, 0, __ATOMIC_SEQ_CST, __ATOMIC_SEQ_CST);
        return cmp;
    }
#define MemoryBarrier() __atomic_thread_fence(__ATOMIC_SEQ_CST)

    /* ---- intsafe --------------------------------------------------------------------------- */

    static inline HRESULT SizeTAdd(size_t a, size_t b, size_t *r)
    {
        return __builtin_add_overflow(a, b, r) ? (*r = SIZE_MAX, INTSAFE_E_ARITHMETIC_OVERFLOW)
                                               : S_OK;
    }
    static inline HRESULT SizeTMult(size_t a, size_t b, size_t *r)
    {
        return __builtin_mul_overflow(a, b, r) ? (*r = SIZE_MAX, INTSAFE_E_ARITHMETIC_OVERFLOW)
                                               : S_OK;
    }
    static inline HRESULT SizeTSub(size_t a, size_t b, size_t *r)
    {
        return __builtin_sub_overflow(a, b, r) ? (*r = SIZE_MAX, INTSAFE_E_ARITHMETIC_OVERFLOW)
                                               : S_OK;
    }
    static inline HRESULT ULongAdd(ULONG a, ULONG b, ULONG *r)
    {
        return __builtin_add_overflow(a, b, r) ? (*r = UINT32_MAX, INTSAFE_E_ARITHMETIC_OVERFLOW)
                                               : S_OK;
    }
    static inline HRESULT ULongMult(ULONG a, ULONG b, ULONG *r)
    {
        return __builtin_mul_overflow(a, b, r) ? (*r = UINT32_MAX, INTSAFE_E_ARITHMETIC_OVERFLOW)
                                               : S_OK;
    }
    static inline HRESULT ULongLongAdd(ULONGLONG a, ULONGLONG b, ULONGLONG *r)
    {
        return __builtin_add_overflow(a, b, r) ? (*r = UINT64_MAX, INTSAFE_E_ARITHMETIC_OVERFLOW)
                                               : S_OK;
    }
    static inline HRESULT ULongLongMult(ULONGLONG a, ULONGLONG b, ULONGLONG *r)
    {
        return __builtin_mul_overflow(a, b, r) ? (*r = UINT64_MAX, INTSAFE_E_ARITHMETIC_OVERFLOW)
                                               : S_OK;
    }
    static inline HRESULT DWordAdd(DWORD a, DWORD b, DWORD *r)
    {
        return __builtin_add_overflow(a, b, r) ? (*r = UINT32_MAX, INTSAFE_E_ARITHMETIC_OVERFLOW)
                                               : S_OK;
    }
    static inline HRESULT DWordMult(DWORD a, DWORD b, DWORD *r)
    {
        return __builtin_mul_overflow(a, b, r) ? (*r = UINT32_MAX, INTSAFE_E_ARITHMETIC_OVERFLOW)
                                               : S_OK;
    }

    /* ---- Settings store (src/platform/posix/settings_posix.c) ------------------------------ */

    typedef struct EePortHkey *HKEY;
#define HKEY_CURRENT_USER ((HKEY)(uintptr_t)0x80000001u)
    /* Removes the settings file that @p subkey maps to (see settings_posix.c). */
    LONG RegDeleteKeyW(HKEY root, const wchar_t *subkey);

#ifdef __cplusplus
}
#endif
