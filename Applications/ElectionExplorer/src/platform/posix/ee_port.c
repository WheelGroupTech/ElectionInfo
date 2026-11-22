/**
 * @file ee_port.c
 * @brief macOS / Linux implementations of the Win32 and MSVC CRT functions the engine calls.
 *        See include/windows.h for the text and path model.
 */

#define _GNU_SOURCE /* qsort_r (glibc), strtok_r */
#include <windows.h>

#include <errno.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <time.h>
#include <unistd.h>

/* ---- Errors --------------------------------------------------------------------------- */

static _Thread_local DWORD t_last_error;

DWORD GetLastError(void)
{
    return t_last_error;
}

void SetLastError(DWORD err)
{
    t_last_error = err;
}

static void set_errno_error(int e)
{
    switch (e)
    {
        case ENOENT:
            t_last_error = ERROR_FILE_NOT_FOUND;
            break;
        case ENOTDIR:
            t_last_error = ERROR_PATH_NOT_FOUND;
            break;
        case EACCES:
        case EPERM:
        case EISDIR:
            t_last_error = ERROR_ACCESS_DENIED;
            break;
        case ENOMEM:
            t_last_error = ERROR_NOT_ENOUGH_MEMORY;
            break;
        case EBADF:
            t_last_error = ERROR_INVALID_HANDLE;
            break;
        default:
            t_last_error = ERROR_INVALID_PARAMETER;
            break;
    }
}

/* ---- UTF-8 / UTF-16 / Windows-1252 ----------------------------------------------------- */

/* Windows-1252 0x80..0x9F; 0 = undefined (Windows maps those five to the same C1 value). */
static const uint16_t k_cp1252_hi[32] = {
    0x20AC, 0,      0x201A, 0x0192, 0x201E, 0x2026, 0x2020, 0x2021, 0x02C6, 0x2030, 0x0160,
    0x2039, 0x0152, 0,      0x017D, 0,      0,      0x2018, 0x2019, 0x201C, 0x201D, 0x2022,
    0x2013, 0x2014, 0x02DC, 0x2122, 0x0161, 0x203A, 0x0153, 0,      0x017E, 0x0178};

static uint32_t cp1252_to_unicode(unsigned char b)
{
    if (b >= 0x80 && b <= 0x9F && k_cp1252_hi[b - 0x80] != 0)
    {
        return k_cp1252_hi[b - 0x80];
    }
    return b;
}

/* Byte for code point @p cp in Windows-1252, or -1. */
static int unicode_to_cp1252(uint32_t cp)
{
    int i;
    if (cp < 0x80 || (cp >= 0xA0 && cp <= 0xFF))
    {
        return (int)cp;
    }
    for (i = 0; i < 32; i++)
    {
        if (k_cp1252_hi[i] == cp || (k_cp1252_hi[i] == 0 && cp == 0x80u + (uint32_t)i))
        {
            return 0x80 + i;
        }
    }
    return -1;
}

/* Decode one UTF-8 sequence at s[0..n); returns its length (>= 1) and sets *cp, or returns
 * -1 for an invalid sequence (one byte is then consumed by the caller). */
static int utf8_decode(const unsigned char *s, size_t n, uint32_t *cp)
{
    uint32_t c = s[0];
    int len, i;
    uint32_t min;
    if (c < 0x80)
    {
        *cp = c;
        return 1;
    }
    if (c >= 0xC2 && c <= 0xDF)
    {
        len = 2;
        c &= 0x1F;
        min = 0x80;
    }
    else if (c >= 0xE0 && c <= 0xEF)
    {
        len = 3;
        c &= 0x0F;
        min = 0x800;
    }
    else if (c >= 0xF0 && c <= 0xF4)
    {
        len = 4;
        c &= 0x07;
        min = 0x10000;
    }
    else
    {
        return -1;
    }
    if ((size_t)len > n)
    {
        return -1;
    }
    for (i = 1; i < len; i++)
    {
        if ((s[i] & 0xC0) != 0x80)
        {
            return -1;
        }
        c = (c << 6) | (s[i] & 0x3F);
    }
    if (c < min || c > 0x10FFFF || (c >= 0xD800 && c <= 0xDFFF))
    {
        return -1;
    }
    *cp = c;
    return len;
}

int MultiByteToWideChar(UINT cp, DWORD flags, const char *src, int cb, wchar_t *dst, int cch)
{
    const unsigned char *s = (const unsigned char *)src;
    size_t n, i = 0;
    int out = 0;

    if (src == NULL || cb == 0 || cch < 0 || (dst == NULL && cch != 0))
    {
        t_last_error = ERROR_INVALID_PARAMETER;
        return 0;
    }
    n = cb < 0 ? strlen(src) + 1 : (size_t)cb;
    if (cp != CP_UTF8 && cp != CP_ACP && cp != CP_OEMCP && cp != 1252 && cp != 28591)
    {
        t_last_error = ERROR_INVALID_PARAMETER;
        return 0;
    }
    while (i < n)
    {
        uint32_t c;
        if (cp == CP_UTF8)
        {
            int len = utf8_decode(s + i, n - i, &c);
            if (len < 0)
            {
                if (flags & MB_ERR_INVALID_CHARS)
                {
                    t_last_error = ERROR_NO_UNICODE_TRANSLATION;
                    return 0;
                }
                c = 0xFFFD;
                len = 1;
            }
            i += (size_t)len;
        }
        else if (cp == 28591)
        {
            c = s[i++];
        }
        else
        {
            c = cp1252_to_unicode(s[i++]);
        }
        if (c >= 0x10000)
        {
            if (cch != 0)
            {
                if (out + 2 > cch)
                {
                    t_last_error = ERROR_INSUFFICIENT_BUFFER;
                    return 0;
                }
                dst[out] = (wchar_t)(0xD800 + ((c - 0x10000) >> 10));
                dst[out + 1] = (wchar_t)(0xDC00 + ((c - 0x10000) & 0x3FF));
            }
            out += 2;
        }
        else
        {
            if (cch != 0)
            {
                if (out + 1 > cch)
                {
                    t_last_error = ERROR_INSUFFICIENT_BUFFER;
                    return 0;
                }
                dst[out] = (wchar_t)c;
            }
            out++;
        }
    }
    return out;
}

int WideCharToMultiByte(UINT cp,
                        DWORD flags,
                        const wchar_t *src,
                        int cch,
                        char *dst,
                        int cb,
                        const char *def,
                        BOOL *used_def)
{
    size_t n, i = 0;
    int out = 0;
    char dflt = (def != NULL && def[0] != '\0') ? def[0] : '?';

    if (used_def != NULL)
    {
        *used_def = FALSE;
    }
    if (src == NULL || cch == 0 || cb < 0 || (dst == NULL && cb != 0))
    {
        t_last_error = ERROR_INVALID_PARAMETER;
        return 0;
    }
    if (cp != CP_UTF8 && cp != CP_ACP && cp != CP_OEMCP && cp != 1252 && cp != 28591)
    {
        t_last_error = ERROR_INVALID_PARAMETER;
        return 0;
    }
    n = cch < 0 ? wcslen(src) + 1 : (size_t)cch;
    while (i < n)
    {
        uint32_t c = (uint32_t)src[i++] & 0xFFFFu;
        unsigned char buf[4];
        int len;
        if (c >= 0xD800 && c <= 0xDBFF && i < n && ((uint32_t)src[i] & 0xFC00u) == 0xDC00u)
        {
            c = 0x10000 + ((c - 0xD800) << 10) + (((uint32_t)src[i] & 0xFFFFu) - 0xDC00);
            i++;
        }
        else if (c >= 0xD800 && c <= 0xDFFF)
        {
            if (cp == CP_UTF8 && (flags & WC_ERR_INVALID_CHARS))
            {
                t_last_error = ERROR_NO_UNICODE_TRANSLATION;
                return 0;
            }
            c = 0xFFFD;
        }
        if (cp == CP_UTF8)
        {
            if (c < 0x80)
            {
                buf[0] = (unsigned char)c;
                len = 1;
            }
            else if (c < 0x800)
            {
                buf[0] = (unsigned char)(0xC0 | (c >> 6));
                buf[1] = (unsigned char)(0x80 | (c & 0x3F));
                len = 2;
            }
            else if (c < 0x10000)
            {
                buf[0] = (unsigned char)(0xE0 | (c >> 12));
                buf[1] = (unsigned char)(0x80 | ((c >> 6) & 0x3F));
                buf[2] = (unsigned char)(0x80 | (c & 0x3F));
                len = 3;
            }
            else
            {
                buf[0] = (unsigned char)(0xF0 | (c >> 18));
                buf[1] = (unsigned char)(0x80 | ((c >> 12) & 0x3F));
                buf[2] = (unsigned char)(0x80 | ((c >> 6) & 0x3F));
                buf[3] = (unsigned char)(0x80 | (c & 0x3F));
                len = 4;
            }
        }
        else
        {
            int b = cp == 28591 ? (c <= 0xFF ? (int)c : -1) : unicode_to_cp1252(c);
            if (b < 0)
            {
                b = (unsigned char)dflt;
                if (used_def != NULL)
                {
                    *used_def = TRUE;
                }
            }
            buf[0] = (unsigned char)b;
            len = 1;
        }
        if (cb != 0)
        {
            if (out + len > cb)
            {
                t_last_error = ERROR_INSUFFICIENT_BUFFER;
                return 0;
            }
            memcpy(dst + out, buf, (size_t)len);
        }
        out += len;
    }
    return out;
}

/* Wide string -> newly allocated UTF-8 (NULL on failure). */
static char *wide_to_utf8(const wchar_t *w)
{
    int n = WideCharToMultiByte(CP_UTF8, 0, w, -1, NULL, 0, NULL, NULL);
    char *s;
    if (n <= 0 || (s = (char *)malloc((size_t)n)) == NULL)
    {
        return NULL;
    }
    WideCharToMultiByte(CP_UTF8, 0, w, -1, s, n, NULL, NULL);
    return s;
}

/* Wide path -> UTF-8 with '\\' as '/' and any "\\?\" prefix dropped. */
static char *path_to_utf8(const wchar_t *w)
{
    char *s, *p;
    if (w == NULL)
    {
        return NULL;
    }
    if (w[0] == L'\\' && w[1] == L'\\' && w[2] == L'?' && w[3] == L'\\')
    {
        w += 4;
    }
    s = wide_to_utf8(w);
    for (p = s; p != NULL && *p != '\0'; p++)
    {
        if (*p == '\\')
        {
            *p = '/';
        }
    }
    return s;
}

/* UTF-8 -> wide into @p buf (cch units); returns the length, or the size needed (with NUL)
 * when it does not fit, as GetTempPathW and GetEnvironmentVariableW do. */
static DWORD utf8_to_wide_buf(const char *s, wchar_t *buf, DWORD cch)
{
    int n = MultiByteToWideChar(CP_UTF8, 0, s, -1, NULL, 0); /* with NUL */
    if (n <= 0)
    {
        return 0;
    }
    if (buf == NULL || (DWORD)n > cch)
    {
        return (DWORD)n;
    }
    MultiByteToWideChar(CP_UTF8, 0, s, -1, buf, (int)cch);
    return (DWORD)(n - 1);
}

/* ---- printf with MSVC conventions ------------------------------------------------------ */

typedef struct Sink
{
    int wide;     /* output units are UTF-16 (wchar_t) or bytes */
    void *buf;    /* wchar_t* or char* */
    size_t cap;   /* units, including room for the NUL */
    size_t len;   /* units written */
    int grow;     /* realloc when full (for FILE output) */
    int overflow; /* a unit did not fit */
} Sink;

static void sink_put(Sink *s, uint32_t u)
{
    if (s->len + 1 >= s->cap)
    {
        if (!s->grow)
        {
            s->overflow = 1;
            return;
        }
        {
            size_t ncap = s->cap < 256 ? 256 : s->cap * 2;
            void *nb = realloc(s->buf, ncap * (s->wide ? sizeof(wchar_t) : 1));
            if (nb == NULL)
            {
                s->overflow = 1;
                return;
            }
            s->buf = nb;
            s->cap = ncap;
        }
    }
    if (s->wide)
    {
        ((wchar_t *)s->buf)[s->len++] = (wchar_t)u;
    }
    else
    {
        ((char *)s->buf)[s->len++] = (char)u;
    }
}

/* Put code point @p cp in the sink's encoding (UTF-16 or UTF-8). */
static void sink_put_cp(Sink *s, uint32_t cp)
{
    if (s->wide)
    {
        if (cp >= 0x10000)
        {
            sink_put(s, 0xD800 + ((cp - 0x10000) >> 10));
            sink_put(s, 0xDC00 + ((cp - 0x10000) & 0x3FF));
        }
        else
        {
            sink_put(s, cp);
        }
        return;
    }
    if (cp < 0x80)
    {
        sink_put(s, cp);
    }
    else if (cp < 0x800)
    {
        sink_put(s, 0xC0 | (cp >> 6));
        sink_put(s, 0x80 | (cp & 0x3F));
    }
    else if (cp < 0x10000)
    {
        sink_put(s, 0xE0 | (cp >> 12));
        sink_put(s, 0x80 | ((cp >> 6) & 0x3F));
        sink_put(s, 0x80 | (cp & 0x3F));
    }
    else
    {
        sink_put(s, 0xF0 | (cp >> 18));
        sink_put(s, 0x80 | ((cp >> 12) & 0x3F));
        sink_put(s, 0x80 | ((cp >> 6) & 0x3F));
        sink_put(s, 0x80 | (cp & 0x3F));
    }
}

static void sink_pad(Sink *s, int n)
{
    while (n-- > 0)
    {
        sink_put(s, ' ');
    }
}

/* Emit a wide string (UTF-16 units), at most @p prec units (-1: all), padded to @p width. */
static void emit_wide_str(Sink *s, const wchar_t *w, int prec, int width, int left)
{
    size_t n = 0, i;
    if (w == NULL)
    {
        w = L"(null)";
    }
    while (w[n] != 0 && (prec < 0 || n < (size_t)prec))
    {
        n++;
    }
    if (!left && width > (int)n)
    {
        sink_pad(s, width - (int)n);
    }
    for (i = 0; i < n; i++)
    {
        uint32_t c = (uint32_t)w[i] & 0xFFFFu;
        if (s->wide)
        {
            sink_put(s, c);
            continue;
        }
        if (c >= 0xD800 && c <= 0xDBFF && i + 1 < n && ((uint32_t)w[i + 1] & 0xFC00u) == 0xDC00u)
        {
            c = 0x10000 + ((c - 0xD800) << 10) + (((uint32_t)w[i + 1] & 0xFFFFu) - 0xDC00);
            i++;
        }
        else if (c >= 0xD800 && c <= 0xDFFF)
        {
            c = 0xFFFD;
        }
        sink_put_cp(s, c);
    }
    if (left && width > (int)n)
    {
        sink_pad(s, width - (int)n);
    }
}

/* Emit a narrow string, at most @p prec bytes (-1: all). Into a wide sink the bytes are read
 * as UTF-8, a byte that is not valid UTF-8 as Windows-1252. */
static void emit_narrow_str(Sink *s, const char *a, int prec, int width, int left)
{
    size_t n = 0, i = 0;
    if (a == NULL)
    {
        a = "(null)";
    }
    while (a[n] != '\0' && (prec < 0 || n < (size_t)prec))
    {
        n++;
    }
    if (!left && width > (int)n)
    {
        sink_pad(s, width - (int)n);
    }
    if (!s->wide)
    {
        for (i = 0; i < n; i++)
        {
            sink_put(s, (unsigned char)a[i]);
        }
    }
    else
    {
        while (i < n)
        {
            uint32_t cp;
            int len = utf8_decode((const unsigned char *)a + i, n - i, &cp);
            if (len < 0)
            {
                cp = cp1252_to_unicode((unsigned char)a[i]);
                len = 1;
            }
            sink_put_cp(s, cp);
            i += (size_t)len;
        }
    }
    if (left && width > (int)n)
    {
        sink_pad(s, width - (int)n);
    }
}

/* Format @p fmt (units of the sink's width: wchar_t when s->wide, else char). */
static void format_core(Sink *s, const void *fmtv, va_list ap)
{
    const wchar_t *fw = (const wchar_t *)fmtv;
    const char *fa = (const char *)fmtv;
    size_t i = 0;
#define FCH(k) (s->wide ? (uint32_t)fw[(k)] : (uint32_t)(unsigned char)fa[(k)])

    while (FCH(i) != 0)
    {
        char spec[48];
        size_t sp = 0;
        int left = 0, width = -1, prec = -1;
        enum
        {
            L_NONE,
            L_HH,
            L_H,
            L_LL,
            L_Z,
            L_J,
            L_T,
            L_BIGL,
            L_WIDE
        } len = L_NONE;
        uint32_t c = FCH(i);
        if (c != '%')
        {
            if (s->wide)
            {
                sink_put(s, c);
            }
            else
            {
                sink_put(s, c);
            }
            i++;
            continue;
        }
        i++;
        if (FCH(i) == '%')
        {
            sink_put(s, '%');
            i++;
            continue;
        }
        spec[sp++] = '%';
        while (FCH(i) == '-' || FCH(i) == '+' || FCH(i) == ' ' || FCH(i) == '#' || FCH(i) == '0')
        {
            if (FCH(i) == '-')
            {
                left = 1;
            }
            if (sp < 8)
            {
                spec[sp++] = (char)FCH(i);
            }
            i++;
        }
        if (FCH(i) == '*')
        {
            width = va_arg(ap, int);
            if (width < 0)
            {
                left = 1;
                spec[sp++] = '-';
                width = -width;
            }
            i++;
        }
        else
        {
            while (FCH(i) >= '0' && FCH(i) <= '9')
            {
                width = (width < 0 ? 0 : width) * 10 + (int)(FCH(i) - '0');
                i++;
            }
        }
        if (FCH(i) == '.')
        {
            i++;
            prec = 0;
            if (FCH(i) == '*')
            {
                prec = va_arg(ap, int);
                i++;
            }
            else
            {
                while (FCH(i) >= '0' && FCH(i) <= '9')
                {
                    prec = prec * 10 + (int)(FCH(i) - '0');
                    i++;
                }
            }
        }
        /* Length. 'l' alone is 32-bit, as on Windows (LLP64): the code passes LONG / DWORD. */
        for (;;)
        {
            c = FCH(i);
            if (c == 'h')
            {
                len = (len == L_H) ? L_HH : L_H;
                i++;
            }
            else if (c == 'l')
            {
                if (FCH(i + 1) == 'l')
                {
                    len = L_LL;
                    i += 2;
                }
                else
                {
                    len = L_WIDE; /* %ls / %lc; a 32-bit integer otherwise */
                    i++;
                }
            }
            else if (c == 'w')
            {
                len = L_WIDE;
                i++;
            }
            else if (c == 'z')
            {
                len = L_Z;
                i++;
            }
            else if (c == 'j')
            {
                len = L_J;
                i++;
            }
            else if (c == 't')
            {
                len = L_T;
                i++;
            }
            else if (c == 'L')
            {
                len = L_BIGL;
                i++;
            }
            else if (c == 'I')
            {
                if (FCH(i + 1) == '6' && FCH(i + 2) == '4')
                {
                    len = L_LL;
                    i += 3;
                }
                else if (FCH(i + 1) == '3' && FCH(i + 2) == '2')
                {
                    len = L_NONE;
                    i += 3;
                }
                else
                {
                    len = L_Z;
                    i++;
                }
            }
            else
            {
                break;
            }
        }
        c = FCH(i);
        if (c == 0)
        {
            break;
        }
        i++;
        if (c == 's' || c == 'S' || c == 'Z')
        {
            /* In a wide format %s is wide; in a narrow one %S is. %hs narrow, %ls / %ws wide. */
            int wide_arg;
            if (len == L_H)
            {
                wide_arg = 0;
            }
            else if (len == L_WIDE)
            {
                wide_arg = 1;
            }
            else
            {
                wide_arg = (c == 's') ? s->wide : !s->wide;
            }
            if (wide_arg)
            {
                emit_wide_str(s, va_arg(ap, const wchar_t *), prec, width, left);
            }
            else
            {
                emit_narrow_str(s, va_arg(ap, const char *), prec, width, left);
            }
            continue;
        }
        if (c == 'c' || c == 'C')
        {
            int wide_arg = (len == L_H)      ? 0
                           : (len == L_WIDE) ? 1
                                             : ((c == 'c') ? s->wide : !s->wide);
            uint32_t ch = (uint32_t)va_arg(ap, int);
            if (!left && width > 1)
            {
                sink_pad(s, width - 1);
            }
            if (wide_arg)
            {
                if (s->wide)
                {
                    sink_put(s, ch & 0xFFFFu);
                }
                else
                {
                    sink_put_cp(s, ch & 0xFFFFu);
                }
            }
            else
            {
                if (s->wide)
                {
                    sink_put_cp(s, cp1252_to_unicode((unsigned char)ch));
                }
                else
                {
                    sink_put(s, ch & 0xFFu);
                }
            }
            if (left && width > 1)
            {
                sink_pad(s, width - 1);
            }
            continue;
        }
        if (c == 'n')
        {
            (void)va_arg(ap, int *); /* not supported (MSVC disables %n too) */
            continue;
        }
        /* Numbers: format with the C library, then copy the ASCII result. */
        {
            char tmp[512];
            char *out = tmp;
            int n;
            if (width >= 0)
            {
                sp += (size_t)snprintf(spec + sp, sizeof(spec) - sp, "%d", width);
            }
            if (prec >= 0)
            {
                sp += (size_t)snprintf(spec + sp, sizeof(spec) - sp, ".%d", prec);
            }
            if (c == 'd' || c == 'i' || c == 'u' || c == 'o' || c == 'x' || c == 'X')
            {
                const char *lm = (len == L_HH)   ? "hh"
                                 : (len == L_H)  ? "h"
                                 : (len == L_LL) ? "ll"
                                 : (len == L_Z)  ? "z"
                                 : (len == L_J)  ? "j"
                                 : (len == L_T)  ? "t"
                                                 : "";
                int is_signed = (c == 'd' || c == 'i');
                sp += (size_t)snprintf(spec + sp, sizeof(spec) - sp, "%s%c", lm, (char)c);
                if (len == L_LL)
                {
                    n = is_signed
                            ? snprintf(tmp, sizeof(tmp), spec, va_arg(ap, long long))
                            : snprintf(tmp, sizeof(tmp), spec, va_arg(ap, unsigned long long));
                }
                else if (len == L_Z)
                {
                    n = snprintf(tmp, sizeof(tmp), spec, va_arg(ap, size_t));
                }
                else if (len == L_J)
                {
                    n = snprintf(tmp, sizeof(tmp), spec, va_arg(ap, intmax_t));
                }
                else if (len == L_T)
                {
                    n = snprintf(tmp, sizeof(tmp), spec, va_arg(ap, ptrdiff_t));
                }
                else
                {
                    n = is_signed ? snprintf(tmp, sizeof(tmp), spec, va_arg(ap, int))
                                  : snprintf(tmp, sizeof(tmp), spec, va_arg(ap, unsigned int));
                }
            }
            else if (c == 'f' || c == 'F' || c == 'e' || c == 'E' || c == 'g' || c == 'G' ||
                     c == 'a' || c == 'A')
            {
                if (len == L_BIGL)
                {
                    long double v = va_arg(ap, long double);
                    sp += (size_t)snprintf(spec + sp, sizeof(spec) - sp, "L%c", (char)c);
                    n = snprintf(NULL, 0, spec, v);
                    if (n >= (int)sizeof(tmp) && (out = (char *)malloc((size_t)n + 1)) == NULL)
                    {
                        continue;
                    }
                    n = snprintf(out, (size_t)n + 1, spec, v);
                }
                else
                {
                    double v = va_arg(ap, double);
                    sp += (size_t)snprintf(spec + sp, sizeof(spec) - sp, "%c", (char)c);
                    n = snprintf(NULL, 0, spec, v);
                    if (n >= (int)sizeof(tmp) && (out = (char *)malloc((size_t)n + 1)) == NULL)
                    {
                        continue;
                    }
                    n = snprintf(out, (size_t)n + 1, spec, v);
                }
            }
            else if (c == 'p')
            {
                /* MSVC prints pointers as zero-padded upper-case hex without "0x". */
                n = snprintf(tmp,
                             sizeof(tmp),
                             "%0*llX",
                             (int)(sizeof(void *) * 2),
                             (unsigned long long)(uintptr_t)va_arg(ap, void *));
            }
            else
            {
                continue; /* unknown conversion: skipped */
            }
            if (n > 0)
            {
                int k;
                for (k = 0; k < n && out[k] != '\0'; k++)
                {
                    sink_put(s, (unsigned char)out[k]);
                }
            }
            if (out != tmp)
            {
                free(out);
            }
        }
    }
#undef FCH
}

static HRESULT cch_vprintf(int wide, void *dst, size_t cch, const void *fmt, va_list ap)
{
    Sink s;
    if (dst == NULL || cch == 0 || cch > STRSAFE_MAX_CCH)
    {
        return STRSAFE_E_INVALID_PARAMETER;
    }
    s.wide = wide;
    s.buf = dst;
    s.cap = cch;
    s.len = 0;
    s.grow = 0;
    s.overflow = 0;
    format_core(&s, fmt, ap);
    if (wide)
    {
        ((wchar_t *)dst)[s.len] = L'\0';
    }
    else
    {
        ((char *)dst)[s.len] = '\0';
    }
    return s.overflow ? STRSAFE_E_INSUFFICIENT_BUFFER : S_OK;
}

HRESULT StringCchVPrintfW(wchar_t *dst, size_t cch, const wchar_t *fmt, va_list ap)
{
    return cch_vprintf(1, dst, cch, fmt, ap);
}

HRESULT StringCchPrintfW(wchar_t *dst, size_t cch, const wchar_t *fmt, ...)
{
    va_list ap;
    HRESULT hr;
    va_start(ap, fmt);
    hr = cch_vprintf(1, dst, cch, fmt, ap);
    va_end(ap);
    return hr;
}

HRESULT StringCchVPrintfA(char *dst, size_t cch, const char *fmt, va_list ap)
{
    return cch_vprintf(0, dst, cch, fmt, ap);
}

HRESULT StringCchPrintfA(char *dst, size_t cch, const char *fmt, ...)
{
    va_list ap;
    HRESULT hr;
    va_start(ap, fmt);
    hr = cch_vprintf(0, dst, cch, fmt, ap);
    va_end(ap);
    return hr;
}

int EePort_vswprintf(wchar_t *dst, size_t cch, const wchar_t *fmt, va_list ap)
{
    Sink s;
    if (dst == NULL || cch == 0)
    {
        return -1;
    }
    s.wide = 1;
    s.buf = dst;
    s.cap = cch;
    s.len = 0;
    s.grow = 0;
    s.overflow = 0;
    format_core(&s, fmt, ap);
    dst[s.len] = L'\0';
    return s.overflow ? -1 : (int)s.len;
}

int EePort_swprintf(wchar_t *dst, size_t cch, const wchar_t *fmt, ...)
{
    va_list ap;
    int n;
    va_start(ap, fmt);
    n = EePort_vswprintf(dst, cch, fmt, ap);
    va_end(ap);
    return n;
}

int EePort_vfwprintf(FILE *fp, const wchar_t *fmt, va_list ap)
{
    Sink s;
    char *u8;
    int n;
    s.wide = 1;
    s.buf = NULL;
    s.cap = 0;
    s.len = 0;
    s.grow = 1;
    s.overflow = 0;
    format_core(&s, fmt, ap);
    if (s.buf == NULL)
    {
        return 0;
    }
    ((wchar_t *)s.buf)[s.len] = L'\0';
    u8 = wide_to_utf8((const wchar_t *)s.buf);
    n = (int)s.len;
    free(s.buf);
    if (u8 == NULL)
    {
        return -1;
    }
    if (fputs(u8, fp) < 0)
    {
        n = -1;
    }
    free(u8);
    return n;
}

int EePort_fwprintf(FILE *fp, const wchar_t *fmt, ...)
{
    va_list ap;
    int n;
    va_start(ap, fmt);
    n = EePort_vfwprintf(fp, fmt, ap);
    va_end(ap);
    return n;
}

int EePort_wprintf(const wchar_t *fmt, ...)
{
    va_list ap;
    int n;
    va_start(ap, fmt);
    n = EePort_vfwprintf(stdout, fmt, ap);
    va_end(ap);
    return n;
}

int EePort_fputws(const wchar_t *s, FILE *fp)
{
    char *u8 = wide_to_utf8(s);
    int rc;
    if (u8 == NULL)
    {
        return -1;
    }
    rc = fputs(u8, fp) < 0 ? -1 : 0;
    free(u8);
    return rc;
}

/* ---- strsafe copy / cat / length -------------------------------------------------------- */

#define DEFINE_CCH_FUNCS(SUF, T)                                                                   \
    HRESULT StringCchCopyN##SUF(T *dst, size_t cch, const T *src, size_t max_src)                  \
    {                                                                                              \
        size_t i = 0;                                                                              \
        if (dst == NULL || cch == 0 || cch > STRSAFE_MAX_CCH)                                      \
        {                                                                                          \
            return STRSAFE_E_INVALID_PARAMETER;                                                    \
        }                                                                                          \
        if (src == NULL)                                                                           \
        {                                                                                          \
            dst[0] = 0;                                                                            \
            return S_OK;                                                                           \
        }                                                                                          \
        while (i < max_src && src[i] != 0)                                                         \
        {                                                                                          \
            if (i + 1 >= cch)                                                                      \
            {                                                                                      \
                dst[i] = 0;                                                                        \
                return STRSAFE_E_INSUFFICIENT_BUFFER;                                              \
            }                                                                                      \
            dst[i] = src[i];                                                                       \
            i++;                                                                                   \
        }                                                                                          \
        dst[i] = 0;                                                                                \
        return S_OK;                                                                               \
    }                                                                                              \
    HRESULT StringCchCopy##SUF(T *dst, size_t cch, const T *src)                                   \
    {                                                                                              \
        return StringCchCopyN##SUF(dst, cch, src, (size_t)STRSAFE_MAX_CCH);                        \
    }                                                                                              \
    HRESULT StringCchLength##SUF(const T *s, size_t max_cch, size_t *len)                          \
    {                                                                                              \
        size_t n = 0;                                                                              \
        if (s == NULL || max_cch > STRSAFE_MAX_CCH)                                                \
        {                                                                                          \
            if (len != NULL)                                                                       \
            {                                                                                      \
                *len = 0;                                                                          \
            }                                                                                      \
            return STRSAFE_E_INVALID_PARAMETER;                                                    \
        }                                                                                          \
        while (n < max_cch && s[n] != 0)                                                           \
        {                                                                                          \
            n++;                                                                                   \
        }                                                                                          \
        if (len != NULL)                                                                           \
        {                                                                                          \
            *len = n < max_cch ? n : 0;                                                            \
        }                                                                                          \
        return n < max_cch ? S_OK : STRSAFE_E_INVALID_PARAMETER;                                   \
    }                                                                                              \
    HRESULT StringCchCatN##SUF(T *dst, size_t cch, const T *src, size_t max_src)                   \
    {                                                                                              \
        size_t n = 0;                                                                              \
        if (dst == NULL || cch == 0 || cch > STRSAFE_MAX_CCH)                                      \
        {                                                                                          \
            return STRSAFE_E_INVALID_PARAMETER;                                                    \
        }                                                                                          \
        while (n < cch && dst[n] != 0)                                                             \
        {                                                                                          \
            n++;                                                                                   \
        }                                                                                          \
        if (n == cch)                                                                              \
        {                                                                                          \
            return STRSAFE_E_INVALID_PARAMETER;                                                    \
        }                                                                                          \
        return StringCchCopyN##SUF(dst + n, cch - n, src, max_src);                                \
    }                                                                                              \
    HRESULT StringCchCat##SUF(T *dst, size_t cch, const T *src)                                    \
    {                                                                                              \
        return StringCchCatN##SUF(dst, cch, src, (size_t)STRSAFE_MAX_CCH);                         \
    }

DEFINE_CCH_FUNCS(W, wchar_t)
DEFINE_CCH_FUNCS(A, char)

/* ---- Case mapping ---------------------------------------------------------------------- */

/* Simple upper-case mapping: ASCII, Latin-1, Latin Extended-A, Greek, Cyrillic, full-width
 * Latin. Enough for the names and places in election files. */
static uint32_t upper1(uint32_t c)
{
    if (c >= 'a' && c <= 'z')
    {
        return c - 32;
    }
    if (c < 0xB5)
    {
        return c;
    }
    if (c == 0xB5)
    {
        return 0x39C;
    }
    if (c >= 0xE0 && c <= 0xFE && c != 0xF7)
    {
        return c - 32;
    }
    if (c == 0xFF)
    {
        return 0x178;
    }
    if (c >= 0x100 && c <= 0x17F)
    {
        if (c == 0x131)
        {
            return 'I';
        }
        if (c == 0x17F)
        {
            return 'S';
        }
        if (c <= 0x137 || (c >= 0x14A && c <= 0x177))
        {
            return (c & 1) ? c - 1 : c;
        }
        if ((c >= 0x139 && c <= 0x148) || (c >= 0x179 && c <= 0x17E))
        {
            return (c & 1) ? c : c - 1;
        }
        return c;
    }
    if (c >= 0x3B1 && c <= 0x3CB && c != 0x3C2)
    {
        return c - 0x20;
    }
    if (c == 0x3C2)
    {
        return 0x3A3;
    }
    if (c == 0x3AC)
    {
        return 0x386;
    }
    if (c >= 0x3AD && c <= 0x3AF)
    {
        return c - 0x25;
    }
    if (c == 0x3CC)
    {
        return 0x38C;
    }
    if (c >= 0x3CD && c <= 0x3CE)
    {
        return c - 0x3F;
    }
    if (c >= 0x430 && c <= 0x44F)
    {
        return c - 0x20;
    }
    if (c >= 0x450 && c <= 0x45F)
    {
        return c - 0x50;
    }
    if ((c >= 0x460 && c <= 0x481) || (c >= 0x48A && c <= 0x4BF))
    {
        return (c & 1) ? c - 1 : c;
    }
    if (c >= 0xFF41 && c <= 0xFF5A)
    {
        return c - 0x20;
    }
    return c;
}

static uint32_t lower1(uint32_t c)
{
    if (c >= 'A' && c <= 'Z')
    {
        return c + 32;
    }
    if (c < 0xC0)
    {
        return c;
    }
    if (c <= 0xDE && c != 0xD7)
    {
        return c + 32;
    }
    if (c == 0x178)
    {
        return 0xFF;
    }
    if (c >= 0x100 && c <= 0x17F)
    {
        if (c == 0x130)
        {
            return 'i';
        }
        if (c <= 0x137 || (c >= 0x14A && c <= 0x177))
        {
            return (c & 1) ? c : c + 1;
        }
        if ((c >= 0x139 && c <= 0x148) || (c >= 0x179 && c <= 0x17E))
        {
            return (c & 1) ? c + 1 : c;
        }
        return c;
    }
    if ((c >= 0x391 && c <= 0x3A1) || (c >= 0x3A3 && c <= 0x3AB))
    {
        return c + 0x20;
    }
    if (c == 0x386)
    {
        return 0x3AC;
    }
    if (c >= 0x388 && c <= 0x38A)
    {
        return c + 0x25;
    }
    if (c == 0x38C)
    {
        return 0x3CC;
    }
    if (c >= 0x38E && c <= 0x38F)
    {
        return c + 0x3F;
    }
    if (c >= 0x410 && c <= 0x42F)
    {
        return c + 0x20;
    }
    if (c >= 0x400 && c <= 0x40F)
    {
        return c + 0x50;
    }
    if ((c >= 0x460 && c <= 0x481) || (c >= 0x48A && c <= 0x4BF))
    {
        return (c & 1) ? c : c + 1;
    }
    if (c >= 0xFF21 && c <= 0xFF3A)
    {
        return c + 0x20;
    }
    return c;
}

static uint32_t ascii_lower(uint32_t c)
{
    return (c >= 'A' && c <= 'Z') ? c + 32 : c;
}

int CompareStringOrdinal(const wchar_t *a, int na, const wchar_t *b, int nb, BOOL ignore_case)
{
    size_t la, lb, i;
    if (a == NULL || b == NULL)
    {
        t_last_error = ERROR_INVALID_PARAMETER;
        return 0;
    }
    la = na < 0 ? wcslen(a) : (size_t)na;
    lb = nb < 0 ? wcslen(b) : (size_t)nb;
    for (i = 0; i < la && i < lb; i++)
    {
        uint32_t x = (uint32_t)a[i] & 0xFFFFu, y = (uint32_t)b[i] & 0xFFFFu;
        if (ignore_case)
        {
            x = upper1(x);
            y = upper1(y);
        }
        if (x != y)
        {
            return x < y ? CSTR_LESS_THAN : CSTR_GREATER_THAN;
        }
    }
    return la == lb ? CSTR_EQUAL : (la < lb ? CSTR_LESS_THAN : CSTR_GREATER_THAN);
}

DWORD CharLowerBuffW(wchar_t *s, DWORD n)
{
    DWORD i;
    for (i = 0; s != NULL && i < n; i++)
    {
        s[i] = (wchar_t)lower1((uint32_t)s[i] & 0xFFFFu);
    }
    return s != NULL ? n : 0;
}

DWORD CharUpperBuffW(wchar_t *s, DWORD n)
{
    DWORD i;
    for (i = 0; s != NULL && i < n; i++)
    {
        s[i] = (wchar_t)upper1((uint32_t)s[i] & 0xFFFFu);
    }
    return s != NULL ? n : 0;
}

static DWORD char_case_buff_a(char *s, DWORD n, int upper)
{
    DWORD i;
    for (i = 0; s != NULL && i < n; i++)
    {
        uint32_t c = cp1252_to_unicode((unsigned char)s[i]);
        int b = unicode_to_cp1252(upper ? upper1(c) : lower1(c));
        if (b >= 0)
        {
            s[i] = (char)b;
        }
    }
    return s != NULL ? n : 0;
}

DWORD CharLowerBuffA(char *s, DWORD n)
{
    return char_case_buff_a(s, n, 0);
}

DWORD CharUpperBuffA(char *s, DWORD n)
{
    return char_case_buff_a(s, n, 1);
}

/* Letters: ASCII, cased letters (they have another case), and the main uncased scripts. */
int EePort_iswalpha(wint_t wc)
{
    uint32_t c = (uint32_t)wc;
    if (c < 0x80)
    {
        return (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z');
    }
    if (c == 0xAA || c == 0xBA || c == 0xDF || upper1(c) != c || lower1(c) != c)
    {
        return 1;
    }
    return (c >= 0x138 && c <= 0x24F && c != 0x149) || (c >= 0x5D0 && c <= 0x5EA) ||
           (c >= 0x620 && c <= 0x64A) || (c >= 0x3041 && c <= 0x30FF) ||
           (c >= 0x4E00 && c <= 0x9FFF) || (c >= 0xAC00 && c <= 0xD7A3);
}

int EePort_iswalnum(wint_t c)
{
    return EePort_iswalpha(c) || (c >= L'0' && c <= L'9');
}

int EePort_iswupper(wint_t c)
{
    return lower1((uint32_t)c) != (uint32_t)c;
}

int EePort_iswlower(wint_t c)
{
    return upper1((uint32_t)c) != (uint32_t)c || c == 0xDF;
}

/* ---- MSVC CRT string extensions (the "C" locale: ASCII case folding only) ------------- */

int _wcsicmp(const wchar_t *a, const wchar_t *b)
{
    for (;; a++, b++)
    {
        uint32_t x = ascii_lower((uint32_t)*a), y = ascii_lower((uint32_t)*b);
        if (x != y || x == 0)
        {
            return (int)x - (int)y;
        }
    }
}

int _wcsnicmp(const wchar_t *a, const wchar_t *b, size_t n)
{
    for (; n > 0; a++, b++, n--)
    {
        uint32_t x = ascii_lower((uint32_t)*a), y = ascii_lower((uint32_t)*b);
        if (x != y || x == 0)
        {
            return (int)x - (int)y;
        }
    }
    return 0;
}

int _stricmp(const char *a, const char *b)
{
    for (;; a++, b++)
    {
        uint32_t x = ascii_lower((unsigned char)*a), y = ascii_lower((unsigned char)*b);
        if (x != y || x == 0)
        {
            return (int)x - (int)y;
        }
    }
}

int _strnicmp(const char *a, const char *b, size_t n)
{
    for (; n > 0; a++, b++, n--)
    {
        uint32_t x = ascii_lower((unsigned char)*a), y = ascii_lower((unsigned char)*b);
        if (x != y || x == 0)
        {
            return (int)x - (int)y;
        }
    }
    return 0;
}

wchar_t *_wcsdup(const wchar_t *s)
{
    size_t n;
    wchar_t *d;
    if (s == NULL)
    {
        return NULL;
    }
    n = wcslen(s) + 1;
    d = (wchar_t *)malloc(n * sizeof(wchar_t));
    if (d != NULL)
    {
        memcpy(d, s, n * sizeof(wchar_t));
    }
    return d;
}

char *_strdup(const char *s)
{
    return s != NULL ? strdup(s) : NULL;
}

int _wtoi(const wchar_t *s)
{
    return (int)wcstol(s, NULL, 10);
}

long _wtol(const wchar_t *s)
{
    return wcstol(s, NULL, 10);
}

long long _wtoi64(const wchar_t *s)
{
    return wcstoll(s, NULL, 10);
}

double _wtof(const wchar_t *s)
{
    return wcstod(s, NULL);
}

int _wcsupr_s(wchar_t *s, size_t cch)
{
    size_t i;
    if (s == NULL || cch == 0)
    {
        return EINVAL;
    }
    for (i = 0; i < cch && s[i] != 0; i++)
    {
        if (s[i] >= L'a' && s[i] <= L'z')
        {
            s[i] = (wchar_t)(s[i] - 32);
        }
    }
    return i < cch ? 0 : EINVAL;
}

int _wcslwr_s(wchar_t *s, size_t cch)
{
    size_t i;
    if (s == NULL || cch == 0)
    {
        return EINVAL;
    }
    for (i = 0; i < cch && s[i] != 0; i++)
    {
        s[i] = (wchar_t)ascii_lower((uint32_t)s[i]);
    }
    return i < cch ? 0 : EINVAL;
}

/* The *_s copies: on overflow the destination becomes "" and an error is returned (the
 * engine always sizes them to fit, so the MSVC invalid-parameter handler never runs);
 * with count _TRUNCATE what fits is copied and STRUNCATE returned. */
int wcsncpy_s(wchar_t *dst, size_t cch, const wchar_t *src, size_t count)
{
    size_t n = 0;
    if (dst == NULL || cch == 0)
    {
        return EINVAL;
    }
    if (src == NULL)
    {
        dst[0] = 0;
        return EINVAL;
    }
    while (n < count && src[n] != 0)
    {
        n++;
    }
    if (n >= cch && count == _TRUNCATE)
    {
        memcpy(dst, src, (cch - 1) * sizeof(wchar_t));
        dst[cch - 1] = 0;
        return STRUNCATE;
    }
    if (n >= cch)
    {
        dst[0] = 0;
        return ERANGE;
    }
    memcpy(dst, src, n * sizeof(wchar_t));
    dst[n] = 0;
    return 0;
}

int wcscpy_s(wchar_t *dst, size_t cch, const wchar_t *src)
{
    return wcsncpy_s(dst, cch, src, (size_t)-1);
}

int strcpy_s(char *dst, size_t cch, const char *src)
{
    size_t n;
    if (dst == NULL || cch == 0)
    {
        return EINVAL;
    }
    if (src == NULL)
    {
        dst[0] = 0;
        return EINVAL;
    }
    n = strlen(src);
    if (n >= cch)
    {
        dst[0] = 0;
        return ERANGE;
    }
    memcpy(dst, src, n + 1);
    return 0;
}

int memcpy_s(void *dst, size_t dst_size, const void *src, size_t count)
{
    if (count == 0)
    {
        return 0;
    }
    if (dst == NULL || src == NULL || count > dst_size)
    {
        if (dst != NULL)
        {
            memset(dst, 0, dst_size);
        }
        return dst == NULL || src == NULL ? EINVAL : ERANGE;
    }
    memcpy(dst, src, count);
    return 0;
}

/* ---- sscanf_s / swscanf_s --------------------------------------------------------------- */

#define SCANF_MAX_ARGS 16

/* Collect sscanf_s's pointer arguments, give each %s / %c / %[ without a width the width its
 * size allows, and call sscanf with them. */
static int vsscanf_s_impl(const char *s, const char *fmt, va_list ap)
{
    void *a[SCANF_MAX_ARGS] = {0};
    char nf[512];
    size_t i = 0, k = 0;
    int na = 0;

    while (fmt[i] != '\0')
    {
        size_t wstart;
        int has_width = 0, suppress = 0;
        if (k + 24 >= sizeof(nf))
        {
            return -1;
        }
        if (fmt[i] != '%')
        {
            nf[k++] = fmt[i++];
            continue;
        }
        nf[k++] = fmt[i++];
        if (fmt[i] == '%')
        {
            nf[k++] = fmt[i++];
            continue;
        }
        if (fmt[i] == '*')
        {
            suppress = 1;
            nf[k++] = fmt[i++];
        }
        wstart = i;
        while (fmt[i] >= '0' && fmt[i] <= '9')
        {
            nf[k++] = fmt[i++];
        }
        has_width = i > wstart;
        while (fmt[i] == 'h' || fmt[i] == 'l' || fmt[i] == 'L' || fmt[i] == 'z' || fmt[i] == 'j' ||
               fmt[i] == 't')
        {
            nf[k++] = fmt[i++];
        }
        if (fmt[i] == '\0')
        {
            break;
        }
        if (!suppress)
        {
            if (na >= SCANF_MAX_ARGS)
            {
                return -1;
            }
            a[na++] = va_arg(ap, void *);
            if (fmt[i] == 's' || fmt[i] == 'c' || fmt[i] == '[')
            {
                unsigned size = va_arg(ap, unsigned);
                if (!has_width && size > 0)
                {
                    /* insert the width before the length modifier and conversion */
                    char wbuf[16];
                    int wl = snprintf(wbuf, sizeof(wbuf), "%u", fmt[i] == 'c' ? 1u : size - 1u);
                    size_t mods = 0;
                    while (mods < k && strchr("hlLzjt", nf[k - 1 - mods]) != NULL)
                    {
                        mods++;
                    }
                    memmove(nf + k - mods + wl, nf + k - mods, mods);
                    memcpy(nf + k - mods, wbuf, (size_t)wl);
                    k += (size_t)wl;
                }
            }
        }
        if (fmt[i] == '[')
        {
            nf[k++] = fmt[i++];
            if (fmt[i] == '^')
            {
                nf[k++] = fmt[i++];
            }
            if (fmt[i] == ']')
            {
                nf[k++] = fmt[i++];
            }
            while (fmt[i] != '\0' && fmt[i] != ']' && k + 2 < sizeof(nf))
            {
                nf[k++] = fmt[i++];
            }
            if (fmt[i] == ']')
            {
                nf[k++] = fmt[i++];
            }
            continue;
        }
        nf[k++] = fmt[i++];
    }
    nf[k] = '\0';
    return sscanf(s,
                  nf,
                  a[0],
                  a[1],
                  a[2],
                  a[3],
                  a[4],
                  a[5],
                  a[6],
                  a[7],
                  a[8],
                  a[9],
                  a[10],
                  a[11],
                  a[12],
                  a[13],
                  a[14],
                  a[15]);
}

int sscanf_s(const char *s, const char *fmt, ...)
{
    va_list ap;
    int n;
    if (s == NULL || fmt == NULL)
    {
        return -1;
    }
    va_start(ap, fmt);
    n = vsscanf_s_impl(s, fmt, ap);
    va_end(ap);
    return n;
}

int swscanf_s(const wchar_t *s, const wchar_t *fmt, ...)
{
    char *s8, *f8;
    va_list ap;
    int n = -1;
    const wchar_t *p;
    if (s == NULL || fmt == NULL)
    {
        return -1;
    }
    /* %s / %c / %[ would need wide output buffers: not supported */
    for (p = fmt; (p = wcschr(p, L'%')) != NULL; p++)
    {
        const wchar_t *q = p + 1;
        while (*q != 0 && wcschr(L"*0123456789hlLzjtI", *q) != NULL)
        {
            q++;
        }
        if (*q == L's' || *q == L'c' || *q == L'[' || *q == L'S' || *q == L'C')
        {
            return -1;
        }
        if (*q == L'%')
        {
            p = q;
        }
    }
    s8 = wide_to_utf8(s);
    f8 = wide_to_utf8(fmt);
    if (s8 != NULL && f8 != NULL)
    {
        va_start(ap, fmt);
        n = vsscanf_s_impl(s8, f8, ap);
        va_end(ap);
    }
    free(s8);
    free(f8);
    return n;
}

/* ---- qsort_s --------------------------------------------------------------------------- */

typedef struct QsortThunk
{
    int (*compare)(void *, const void *, const void *);
    void *context;
} QsortThunk;

#if defined(__APPLE__)
static int qsort_thunk(void *t, const void *a, const void *b)
{
    const QsortThunk *q = (const QsortThunk *)t;
    return q->compare(q->context, a, b);
}
#else
static int qsort_thunk(const void *a, const void *b, void *t)
{
    const QsortThunk *q = (const QsortThunk *)t;
    return q->compare(q->context, a, b);
}
#endif

void qsort_s(void *base,
             size_t num,
             size_t width,
             int (*compare)(void *context, const void *a, const void *b),
             void *context)
{
    QsortThunk q;
    q.compare = compare;
    q.context = context;
#if defined(__APPLE__)
    qsort_r(base, num, width, &q, qsort_thunk);
#else
    qsort_r(base, num, width, qsort_thunk, &q);
#endif
}

/* ---- Files ----------------------------------------------------------------------------- */

typedef struct EePortFile
{
    int fd;
} EePortFile;

static int fd_of(HANDLE h)
{
    if (h == NULL || h == INVALID_HANDLE_VALUE)
    {
        return -1;
    }
    return ((EePortFile *)h)->fd;
}

HANDLE CreateFileW(const wchar_t *path,
                   DWORD access,
                   DWORD share,
                   LPSECURITY_ATTRIBUTES sa,
                   DWORD disposition,
                   DWORD flags,
                   HANDLE tmpl)
{
    char *p = path_to_utf8(path);
    int oflags = 0, fd;
    struct stat st;
    EePortFile *f;
    (void)share;
    (void)sa;
    (void)flags;
    (void)tmpl;
    if (p == NULL)
    {
        t_last_error = ERROR_INVALID_PARAMETER;
        return INVALID_HANDLE_VALUE;
    }
    if ((access & GENERIC_READ) && (access & GENERIC_WRITE))
    {
        oflags = O_RDWR;
    }
    else if (access & GENERIC_WRITE)
    {
        oflags = O_WRONLY;
    }
    else
    {
        oflags = O_RDONLY;
    }
    switch (disposition)
    {
        case CREATE_NEW:
            oflags |= O_CREAT | O_EXCL;
            break;
        case CREATE_ALWAYS:
            oflags |= O_CREAT | O_TRUNC;
            break;
        case OPEN_ALWAYS:
            oflags |= O_CREAT;
            break;
        case TRUNCATE_EXISTING:
            oflags |= O_TRUNC;
            break;
        default:
            break;
    }
    fd = open(p, oflags | O_CLOEXEC, 0666);
    free(p);
    if (fd < 0)
    {
        set_errno_error(errno);
        return INVALID_HANDLE_VALUE;
    }
    /* Windows does not open a folder as a file. */
    if (fstat(fd, &st) != 0 || S_ISDIR(st.st_mode))
    {
        close(fd);
        t_last_error = ERROR_ACCESS_DENIED;
        return INVALID_HANDLE_VALUE;
    }
    f = (EePortFile *)malloc(sizeof(*f));
    if (f == NULL)
    {
        close(fd);
        t_last_error = ERROR_NOT_ENOUGH_MEMORY;
        return INVALID_HANDLE_VALUE;
    }
    f->fd = fd;
    t_last_error = ERROR_SUCCESS;
    return (HANDLE)f;
}

static off_t ov_offset(const OVERLAPPED *ov)
{
    return (off_t)(((uint64_t)ov->OffsetHigh << 32) | ov->Offset);
}

BOOL ReadFile(HANDLE h, void *buf, DWORD n, DWORD *got, LPOVERLAPPED ov)
{
    int fd = fd_of(h);
    size_t total = 0;
    if (got != NULL)
    {
        *got = 0;
    }
    if (fd < 0)
    {
        t_last_error = ERROR_INVALID_HANDLE;
        return FALSE;
    }
    while (total < n)
    {
        ssize_t r = ov != NULL
                        ? pread(fd, (char *)buf + total, n - total, ov_offset(ov) + (off_t)total)
                        : read(fd, (char *)buf + total, n - total);
        if (r < 0)
        {
            if (errno == EINTR)
            {
                continue;
            }
            set_errno_error(errno);
            return FALSE;
        }
        if (r == 0)
        {
            break;
        }
        total += (size_t)r;
    }
    if (got != NULL)
    {
        *got = (DWORD)total;
    }
    return TRUE;
}

BOOL WriteFile(HANDLE h, const void *buf, DWORD n, DWORD *wrote, LPOVERLAPPED ov)
{
    int fd = fd_of(h);
    size_t total = 0;
    if (wrote != NULL)
    {
        *wrote = 0;
    }
    if (fd < 0)
    {
        t_last_error = ERROR_INVALID_HANDLE;
        return FALSE;
    }
    while (total < n)
    {
        ssize_t r =
            ov != NULL
                ? pwrite(fd, (const char *)buf + total, n - total, ov_offset(ov) + (off_t)total)
                : write(fd, (const char *)buf + total, n - total);
        if (r < 0)
        {
            if (errno == EINTR)
            {
                continue;
            }
            set_errno_error(errno);
            return FALSE;
        }
        total += (size_t)r;
    }
    if (wrote != NULL)
    {
        *wrote = (DWORD)total;
    }
    return TRUE;
}

BOOL GetFileSizeEx(HANDLE h, LARGE_INTEGER *size)
{
    struct stat st;
    int fd = fd_of(h);
    if (fd < 0 || size == NULL || fstat(fd, &st) != 0)
    {
        t_last_error = ERROR_INVALID_HANDLE;
        return FALSE;
    }
    size->QuadPart = (LONGLONG)st.st_size;
    return TRUE;
}

DWORD GetFileSize(HANDLE h, DWORD *high)
{
    LARGE_INTEGER sz;
    if (!GetFileSizeEx(h, &sz))
    {
        return INVALID_FILE_SIZE;
    }
    if (high != NULL)
    {
        *high = (DWORD)((ULONGLONG)sz.QuadPart >> 32);
    }
    return (DWORD)((ULONGLONG)sz.QuadPart & 0xFFFFFFFFu);
}

BOOL SetFilePointerEx(HANDLE h, LARGE_INTEGER dist, LARGE_INTEGER *pos, DWORD method)
{
    int fd = fd_of(h);
    int whence = method == FILE_END ? SEEK_END : (method == FILE_CURRENT ? SEEK_CUR : SEEK_SET);
    off_t r;
    if (fd < 0)
    {
        t_last_error = ERROR_INVALID_HANDLE;
        return FALSE;
    }
    r = lseek(fd, (off_t)dist.QuadPart, whence);
    if (r < 0)
    {
        set_errno_error(errno);
        return FALSE;
    }
    if (pos != NULL)
    {
        pos->QuadPart = (LONGLONG)r;
    }
    return TRUE;
}

/* FILETIME: 100 ns units since 1601-01-01 UTC. */
#define EPOCH_DIFF_100NS 116444736000000000ULL

static void timespec_to_filetime(const struct timespec *ts, FILETIME *ft)
{
    ULONGLONG v =
        (ULONGLONG)ts->tv_sec * 10000000ULL + (ULONGLONG)ts->tv_nsec / 100ULL + EPOCH_DIFF_100NS;
    ft->dwLowDateTime = (DWORD)(v & 0xFFFFFFFFu);
    ft->dwHighDateTime = (DWORD)(v >> 32);
}

static void filetime_to_timespec(const FILETIME *ft, struct timespec *ts)
{
    ULONGLONG v = ((ULONGLONG)ft->dwHighDateTime << 32) | ft->dwLowDateTime;
    v = v > EPOCH_DIFF_100NS ? v - EPOCH_DIFF_100NS : 0;
    ts->tv_sec = (time_t)(v / 10000000ULL);
    ts->tv_nsec = (long)((v % 10000000ULL) * 100ULL);
}

#if defined(__APPLE__)
#define ST_MTIM(st) ((st).st_mtimespec)
#define ST_ATIM(st) ((st).st_atimespec)
#define ST_CTIM(st) ((st).st_ctimespec)
#else
#define ST_MTIM(st) ((st).st_mtim)
#define ST_ATIM(st) ((st).st_atim)
#define ST_CTIM(st) ((st).st_ctim)
#endif

BOOL GetFileTime(HANDLE h, FILETIME *created, FILETIME *accessed, FILETIME *written)
{
    struct stat st;
    int fd = fd_of(h);
    if (fd < 0 || fstat(fd, &st) != 0)
    {
        t_last_error = ERROR_INVALID_HANDLE;
        return FALSE;
    }
    if (created != NULL)
    {
        timespec_to_filetime(&ST_CTIM(st), created);
    }
    if (accessed != NULL)
    {
        timespec_to_filetime(&ST_ATIM(st), accessed);
    }
    if (written != NULL)
    {
        timespec_to_filetime(&ST_MTIM(st), written);
    }
    return TRUE;
}

BOOL SetFileTime(HANDLE h,
                 const FILETIME *created,
                 const FILETIME *accessed,
                 const FILETIME *written)
{
    struct timespec ts[2];
    int fd = fd_of(h);
    (void)created; /* no settable creation time on POSIX */
    if (fd < 0)
    {
        t_last_error = ERROR_INVALID_HANDLE;
        return FALSE;
    }
    ts[0].tv_sec = 0;
    ts[0].tv_nsec = UTIME_OMIT;
    ts[1].tv_sec = 0;
    ts[1].tv_nsec = UTIME_OMIT;
    if (accessed != NULL)
    {
        filetime_to_timespec(accessed, &ts[0]);
    }
    if (written != NULL)
    {
        filetime_to_timespec(written, &ts[1]);
    }
    if (futimens(fd, ts) != 0)
    {
        set_errno_error(errno);
        return FALSE;
    }
    return TRUE;
}

BOOL CloseHandle(HANDLE h)
{
    EePortFile *f = (EePortFile *)h;
    if (h == NULL || h == INVALID_HANDLE_VALUE)
    {
        t_last_error = ERROR_INVALID_HANDLE;
        return FALSE;
    }
    close(f->fd);
    free(f);
    return TRUE;
}

BOOL DeleteFileW(const wchar_t *path)
{
    char *p = path_to_utf8(path);
    int rc;
    if (p == NULL)
    {
        return FALSE;
    }
    rc = unlink(p);
    if (rc != 0)
    {
        set_errno_error(errno);
    }
    free(p);
    return rc == 0;
}

BOOL CreateDirectoryW(const wchar_t *path, LPSECURITY_ATTRIBUTES sa)
{
    char *p = path_to_utf8(path);
    int rc;
    (void)sa;
    if (p == NULL)
    {
        return FALSE;
    }
    rc = mkdir(p, 0777);
    if (rc != 0)
    {
        set_errno_error(errno);
    }
    free(p);
    return rc == 0;
}

BOOL RemoveDirectoryW(const wchar_t *path)
{
    char *p = path_to_utf8(path);
    int rc;
    if (p == NULL)
    {
        return FALSE;
    }
    rc = rmdir(p);
    if (rc != 0)
    {
        set_errno_error(errno);
    }
    free(p);
    return rc == 0;
}

static DWORD attrs_of(const struct stat *st)
{
    DWORD a = S_ISDIR(st->st_mode) ? FILE_ATTRIBUTE_DIRECTORY : 0;
    if ((st->st_mode & 0222) == 0)
    {
        a |= FILE_ATTRIBUTE_READONLY;
    }
    return a != 0 ? a : FILE_ATTRIBUTE_NORMAL;
}

DWORD GetFileAttributesW(const wchar_t *path)
{
    char *p = path_to_utf8(path);
    struct stat st;
    int rc;
    if (p == NULL)
    {
        return INVALID_FILE_ATTRIBUTES;
    }
    rc = stat(p, &st);
    free(p);
    if (rc != 0)
    {
        set_errno_error(errno);
        return INVALID_FILE_ATTRIBUTES;
    }
    return attrs_of(&st);
}

BOOL GetFileAttributesExW(const wchar_t *path, GET_FILEEX_INFO_LEVELS level, void *info)
{
    WIN32_FILE_ATTRIBUTE_DATA *d = (WIN32_FILE_ATTRIBUTE_DATA *)info;
    char *p;
    struct stat st;
    int rc;
    if (level != GetFileExInfoStandard || d == NULL || (p = path_to_utf8(path)) == NULL)
    {
        t_last_error = ERROR_INVALID_PARAMETER;
        return FALSE;
    }
    rc = stat(p, &st);
    free(p);
    if (rc != 0)
    {
        set_errno_error(errno);
        return FALSE;
    }
    d->dwFileAttributes = attrs_of(&st);
    timespec_to_filetime(&ST_CTIM(st), &d->ftCreationTime);
    timespec_to_filetime(&ST_ATIM(st), &d->ftLastAccessTime);
    timespec_to_filetime(&ST_MTIM(st), &d->ftLastWriteTime);
    d->nFileSizeHigh = (DWORD)((uint64_t)st.st_size >> 32);
    d->nFileSizeLow = (DWORD)((uint64_t)st.st_size & 0xFFFFFFFFu);
    return TRUE;
}

DWORD GetTempPathW(DWORD cch, wchar_t *buf)
{
    const char *t = getenv("TMPDIR");
    char tmp[4096];
    size_t n;
    if (t == NULL || t[0] == '\0')
    {
        t = "/tmp";
    }
    n = strlen(t);
    if (n + 2 > sizeof(tmp))
    {
        t = "/tmp";
        n = 4;
    }
    memcpy(tmp, t, n);
    if (tmp[n - 1] != '/')
    {
        tmp[n++] = '/';
    }
    tmp[n] = '\0';
    return utf8_to_wide_buf(tmp, buf, cch);
}

DWORD GetEnvironmentVariableW(const wchar_t *name, wchar_t *buf, DWORD cch)
{
    char *n = wide_to_utf8(name);
    const char *v;
    if (n == NULL)
    {
        return 0;
    }
    v = getenv(n);
    free(n);
    if (v == NULL)
    {
        t_last_error = 203; /* ERROR_ENVVAR_NOT_FOUND */
        return 0;
    }
    if (v[0] == '\0')
    {
        if (buf != NULL && cch > 0)
        {
            buf[0] = L'\0';
        }
        return 0;
    }
    return utf8_to_wide_buf(v, buf, cch);
}

DWORD GetCurrentDirectoryW(DWORD cch, wchar_t *buf)
{
    char tmp[4096];
    if (getcwd(tmp, sizeof(tmp)) == NULL)
    {
        set_errno_error(errno);
        return 0;
    }
    return utf8_to_wide_buf(tmp, buf, cch);
}

int _wfopen_s(FILE **fp, const wchar_t *path, const wchar_t *mode)
{
    char *p, m[16];
    size_t i, k = 0;
    if (fp == NULL)
    {
        return EINVAL;
    }
    *fp = NULL;
    if (path == NULL || mode == NULL)
    {
        return EINVAL;
    }
    /* Keep the C mode letters; drop MSVC extras such as "ccs=UTF-8", 't', 'N', 'S'. */
    for (i = 0; mode[i] != 0 && mode[i] != L',' && k + 1 < sizeof(m); i++)
    {
        if (mode[i] == L'r' || mode[i] == L'w' || mode[i] == L'a' || mode[i] == L'+' ||
            mode[i] == L'b' || mode[i] == L'x')
        {
            m[k++] = (char)mode[i];
        }
    }
    m[k] = '\0';
    p = path_to_utf8(path);
    if (p == NULL)
    {
        return EINVAL;
    }
    *fp = fopen(p, m);
    free(p);
    if (*fp == NULL)
    {
        int e = errno;
        set_errno_error(e);
        return e != 0 ? e : EINVAL;
    }
    /* fopen opens folders for reading; MSVC does not. */
    {
        struct stat st;
        if (fstat(fileno(*fp), &st) == 0 && S_ISDIR(st.st_mode))
        {
            fclose(*fp);
            *fp = NULL;
            return EACCES;
        }
    }
    return 0;
}

FILE *_wfopen(const wchar_t *path, const wchar_t *mode)
{
    FILE *fp = NULL;
    _wfopen_s(&fp, path, mode);
    return fp;
}

/* ---- Time and process ------------------------------------------------------------------ */

static void tm_to_systemtime(const struct tm *tm, long ms, SYSTEMTIME *st)
{
    st->wYear = (WORD)(tm->tm_year + 1900);
    st->wMonth = (WORD)(tm->tm_mon + 1);
    st->wDayOfWeek = (WORD)tm->tm_wday;
    st->wDay = (WORD)tm->tm_mday;
    st->wHour = (WORD)tm->tm_hour;
    st->wMinute = (WORD)tm->tm_min;
    st->wSecond = (WORD)tm->tm_sec;
    st->wMilliseconds = (WORD)ms;
}

void GetLocalTime(SYSTEMTIME *st)
{
    struct timespec ts;
    struct tm tm;
    clock_gettime(CLOCK_REALTIME, &ts);
    localtime_r(&ts.tv_sec, &tm);
    tm_to_systemtime(&tm, ts.tv_nsec / 1000000L, st);
}

void GetSystemTime(SYSTEMTIME *st)
{
    struct timespec ts;
    struct tm tm;
    clock_gettime(CLOCK_REALTIME, &ts);
    gmtime_r(&ts.tv_sec, &tm);
    tm_to_systemtime(&tm, ts.tv_nsec / 1000000L, st);
}

/* Days from 1970-01-01 to y-m-d (proleptic Gregorian). */
static int64_t days_from_civil(int64_t y, unsigned m, unsigned d)
{
    int64_t era;
    unsigned yoe, doy, doe;
    y -= m <= 2;
    era = (y >= 0 ? y : y - 399) / 400;
    yoe = (unsigned)(y - era * 400);
    doy = (153 * (m + (m > 2 ? -3 : 9)) + 2) / 5 + d - 1;
    doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    return era * 146097 + (int64_t)doe - 719468;
}

BOOL FileTimeToSystemTime(const FILETIME *ft, SYSTEMTIME *st)
{
    struct timespec ts;
    struct tm tm;
    if (ft == NULL || st == NULL)
    {
        return FALSE;
    }
    filetime_to_timespec(ft, &ts);
    if (gmtime_r(&ts.tv_sec, &tm) == NULL)
    {
        return FALSE;
    }
    tm_to_systemtime(&tm, ts.tv_nsec / 1000000L, st);
    return TRUE;
}

/* UTC -> local by the zone's offset at that moment (as Windows, which uses the current
 * offset, does for times near now). */
BOOL FileTimeToLocalFileTime(const FILETIME *ft, FILETIME *local)
{
    struct timespec ts;
    struct tm tm;
    ULONGLONG v;
    long off;
    if (ft == NULL || local == NULL)
    {
        return FALSE;
    }
    filetime_to_timespec(ft, &ts);
    if (localtime_r(&ts.tv_sec, &tm) == NULL)
    {
        return FALSE;
    }
    off = tm.tm_gmtoff;
    v = ((ULONGLONG)ft->dwHighDateTime << 32) | ft->dwLowDateTime;
    v = (ULONGLONG)((LONGLONG)v + (LONGLONG)off * 10000000LL);
    local->dwLowDateTime = (DWORD)(v & 0xFFFFFFFFu);
    local->dwHighDateTime = (DWORD)(v >> 32);
    return TRUE;
}

BOOL SystemTimeToFileTime(const SYSTEMTIME *st, FILETIME *ft)
{
    struct timespec ts;
    int64_t days;
    if (st == NULL || ft == NULL || st->wMonth < 1 || st->wMonth > 12 || st->wDay < 1 ||
        st->wDay > 31)
    {
        return FALSE;
    }
    days = days_from_civil(st->wYear, st->wMonth, st->wDay);
    ts.tv_sec = (time_t)(days * 86400 + st->wHour * 3600 + st->wMinute * 60 + st->wSecond);
    ts.tv_nsec = (long)st->wMilliseconds * 1000000L;
    if (ts.tv_sec < -11644473600LL)
    {
        return FALSE;
    }
    {
        ULONGLONG v = (ULONGLONG)((int64_t)ts.tv_sec * 10000000LL + 116444736000000000LL) +
                      (ULONGLONG)ts.tv_nsec / 100ULL;
        ft->dwLowDateTime = (DWORD)(v & 0xFFFFFFFFu);
        ft->dwHighDateTime = (DWORD)(v >> 32);
    }
    return TRUE;
}

ULONGLONG GetTickCount64(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (ULONGLONG)ts.tv_sec * 1000ULL + (ULONGLONG)ts.tv_nsec / 1000000ULL;
}

DWORD GetTickCount(void)
{
    return (DWORD)GetTickCount64();
}

DWORD GetCurrentProcessId(void)
{
    return (DWORD)getpid();
}

/* A small number per thread, handed out in order of first use. */
DWORD GetCurrentThreadId(void)
{
    static DWORD s_next;
    static _Thread_local DWORD t_id;
    if (t_id == 0)
    {
        t_id = __atomic_add_fetch(&s_next, 1, __ATOMIC_SEQ_CST);
    }
    return t_id;
}

void Sleep(DWORD ms)
{
    struct timespec ts;
    ts.tv_sec = (time_t)(ms / 1000);
    ts.tv_nsec = (long)(ms % 1000) * 1000000L;
    while (nanosleep(&ts, &ts) != 0 && errno == EINTR)
    {
    }
}
