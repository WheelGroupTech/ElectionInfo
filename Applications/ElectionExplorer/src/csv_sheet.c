/**
 * @file csv_sheet.c
 * @brief Delimited-text (CSV / TSV) reader. See csv_sheet.h.
 */
#include "csv_sheet.h"

#include <stdlib.h>
#include <string.h>
#include <strsafe.h>
#include <limits.h>

/* -------------------------------------------------------------------------- */
/* Small helpers                                                              */
/* -------------------------------------------------------------------------- */

static void csv_set_err(wchar_t *err, size_t cch, const wchar_t *msg)
{
    if (err != NULL && cch > 0)
    {
        StringCchCopyW(err, cch, msg);
    }
}

/* Read the whole file into a freshly malloc'd buffer. *out_len excludes no NUL
 * (raw bytes). Returns EeLoadStatus_Ok / _Error. */
static EeLoadStatus csv_read_file(const wchar_t *path,
                                  unsigned char **out_buf,
                                  size_t *out_len,
                                  wchar_t *err,
                                  size_t errcch)
{
    HANDLE h;
    LARGE_INTEGER sz;
    unsigned char *buf;
    size_t total = 0;

    *out_buf = NULL;
    *out_len = 0;

    h = CreateFileW(path, GENERIC_READ, FILE_SHARE_READ, NULL, OPEN_EXISTING,
                    FILE_ATTRIBUTE_NORMAL | FILE_FLAG_SEQUENTIAL_SCAN, NULL);
    if (h == INVALID_HANDLE_VALUE)
    {
        csv_set_err(err, errcch, L"Could not open the file.");
        return EeLoadStatus_Error;
    }
    if (!GetFileSizeEx(h, &sz) || sz.QuadPart < 0)
    {
        CloseHandle(h);
        csv_set_err(err, errcch, L"Could not read the file size.");
        return EeLoadStatus_Error;
    }
    if (sz.QuadPart == 0)
    {
        CloseHandle(h);
        csv_set_err(err, errcch, L"The file is empty.");
        return EeLoadStatus_Error;
    }
#if SIZE_MAX < 0xFFFFFFFFFFFFFFFFull
    if ((unsigned long long)sz.QuadPart > (unsigned long long)SIZE_MAX)
    {
        CloseHandle(h);
        csv_set_err(err, errcch, L"The file is too large to load.");
        return EeLoadStatus_Error;
    }
#endif
    buf = (unsigned char *)malloc((size_t)sz.QuadPart);
    if (buf == NULL)
    {
        CloseHandle(h);
        csv_set_err(err, errcch, L"Out of memory reading the file.");
        return EeLoadStatus_Error;
    }
    while (total < (size_t)sz.QuadPart)
    {
        DWORD want = (DWORD)(((size_t)sz.QuadPart - total > 0x400000u)
                                 ? 0x400000u
                                 : ((size_t)sz.QuadPart - total));
        DWORD got = 0;
        if (!ReadFile(h, buf + total, want, &got, NULL) || got == 0)
        {
            free(buf);
            CloseHandle(h);
            csv_set_err(err, errcch, L"Could not read the file.");
            return EeLoadStatus_Error;
        }
        total += got;
    }
    CloseHandle(h);
    *out_buf = buf;
    *out_len = total;
    return EeLoadStatus_Ok;
}

/* Well-formed UTF-8 check (no BOM assumed). Pure ASCII passes. Used to decide
 * whether a BOM-less file is UTF-8 or should be read as the system ANSI code
 * page (Excel's "CSV"/"Text" exports are ANSI/Windows-125x). */
static BOOL csv_is_valid_utf8(const unsigned char *p, size_t len)
{
    size_t i = 0;
    while (i < len)
    {
        unsigned char b = p[i];
        size_t need;
        if (b < 0x80)
        {
            i++;
            continue;
        }
        if (b >= 0xC2 && b <= 0xDF)
        {
            need = 1;
        }
        else if (b >= 0xE0 && b <= 0xEF)
        {
            need = 2;
        }
        else if (b >= 0xF0 && b <= 0xF4)
        {
            need = 3;
        }
        else
        {
            return FALSE; /* invalid lead (incl. 0xC0/0xC1/0xF5-0xFF, stray cont.) */
        }
        if (i + need >= len)
        {
            return FALSE; /* truncated multibyte sequence */
        }
        /* Range checks that reject overlong forms and surrogates. */
        {
            unsigned char c1 = p[i + 1];
            if (b == 0xE0 && c1 < 0xA0)
            {
                return FALSE;
            }
            if (b == 0xED && c1 > 0x9F)
            {
                return FALSE;
            }
            if (b == 0xF0 && c1 < 0x90)
            {
                return FALSE;
            }
            if (b == 0xF4 && c1 > 0x8F)
            {
                return FALSE;
            }
        }
        {
            size_t k;
            for (k = 1; k <= need; k++)
            {
                if (p[i + k] < 0x80 || p[i + k] > 0xBF)
                {
                    return FALSE;
                }
            }
        }
        i += need + 1;
    }
    return TRUE;
}

/* Transcode a code-page byte buffer to UTF-8 (frees @p raw). */
static EeLoadStatus csv_codepage_to_utf8(unsigned char *raw,
                                         size_t rawlen,
                                         UINT codepage,
                                         char **out,
                                         size_t *out_len,
                                         wchar_t *err,
                                         size_t errcch)
{
    int nwch;
    wchar_t *w;
    int u8len;
    char *u8;

    if (rawlen == 0 || rawlen > (size_t)INT_MAX)
    {
        free(raw);
        *out = (char *)malloc(1);
        if (*out == NULL)
        {
            csv_set_err(err, errcch, L"Out of memory decoding the file.");
            return EeLoadStatus_Error;
        }
        *out_len = 0;
        return EeLoadStatus_Ok;
    }
    nwch = MultiByteToWideChar(codepage, 0, (const char *)raw, (int)rawlen, NULL, 0);
    if (nwch <= 0)
    {
        /* Undecodable in this code page: fall back to using the bytes verbatim. */
        *out = (char *)raw;
        *out_len = rawlen;
        return EeLoadStatus_Ok;
    }
    w = (wchar_t *)malloc((size_t)nwch * sizeof(wchar_t));
    if (w == NULL)
    {
        free(raw);
        csv_set_err(err, errcch, L"Out of memory decoding the file.");
        return EeLoadStatus_Error;
    }
    MultiByteToWideChar(codepage, 0, (const char *)raw, (int)rawlen, w, nwch);
    free(raw);
    u8len = WideCharToMultiByte(CP_UTF8, 0, w, nwch, NULL, 0, NULL, NULL);
    if (u8len <= 0)
    {
        free(w);
        *out = (char *)malloc(1);
        if (*out == NULL)
        {
            csv_set_err(err, errcch, L"Out of memory decoding the file.");
            return EeLoadStatus_Error;
        }
        *out_len = 0;
        return EeLoadStatus_Ok;
    }
    u8 = (char *)malloc((size_t)u8len);
    if (u8 == NULL)
    {
        free(w);
        csv_set_err(err, errcch, L"Out of memory decoding the file.");
        return EeLoadStatus_Error;
    }
    WideCharToMultiByte(CP_UTF8, 0, w, nwch, u8, u8len, NULL, NULL);
    free(w);
    *out = u8;
    *out_len = (size_t)u8len;
    return EeLoadStatus_Ok;
}

/* Convert raw file bytes to a UTF-8 buffer, honoring a UTF-8/UTF-16 BOM. On
 * return *out is a malloc'd buffer of *out_len bytes (no trailing NUL needed;
 * the parser is length-bounded). The caller frees *out. The raw buffer is
 * consumed (freed here) unless it is returned directly. */
static EeLoadStatus csv_to_utf8(unsigned char *raw,
                                size_t rawlen,
                                char **out,
                                size_t *out_len,
                                wchar_t *err,
                                size_t errcch)
{
    /* UTF-8 BOM */
    if (rawlen >= 3 && raw[0] == 0xEF && raw[1] == 0xBB && raw[2] == 0xBF)
    {
        memmove(raw, raw + 3, rawlen - 3);
        *out = (char *)raw;
        *out_len = rawlen - 3;
        return EeLoadStatus_Ok;
    }

    /* UTF-16 (LE or BE) BOM -> transcode to UTF-8. */
    if (rawlen >= 2 &&
        ((raw[0] == 0xFF && raw[1] == 0xFE) || (raw[0] == 0xFE && raw[1] == 0xFF)))
    {
        BOOL be = (raw[0] == 0xFE);
        size_t nwch = (rawlen - 2) / 2;
        wchar_t *w;
        int u8len;
        char *u8;
        size_t i;

        w = (wchar_t *)malloc((nwch + 1) * sizeof(wchar_t));
        if (w == NULL)
        {
            free(raw);
            csv_set_err(err, errcch, L"Out of memory decoding the file.");
            return EeLoadStatus_Error;
        }
        for (i = 0; i < nwch; i++)
        {
            unsigned char a = raw[2 + i * 2];
            unsigned char b = raw[2 + i * 2 + 1];
            w[i] = be ? (wchar_t)((a << 8) | b) : (wchar_t)((b << 8) | a);
        }
        w[nwch] = L'\0';
        free(raw);

        u8len = WideCharToMultiByte(CP_UTF8, 0, w, (int)nwch, NULL, 0, NULL, NULL);
        if (u8len <= 0)
        {
            free(w);
            *out = (char *)malloc(1);
            if (*out == NULL)
            {
                csv_set_err(err, errcch, L"Out of memory decoding the file.");
                return EeLoadStatus_Error;
            }
            *out_len = 0;
            return EeLoadStatus_Ok;
        }
        u8 = (char *)malloc((size_t)u8len);
        if (u8 == NULL)
        {
            free(w);
            csv_set_err(err, errcch, L"Out of memory decoding the file.");
            return EeLoadStatus_Error;
        }
        WideCharToMultiByte(CP_UTF8, 0, w, (int)nwch, u8, u8len, NULL, NULL);
        free(w);
        *out = u8;
        *out_len = (size_t)u8len;
        return EeLoadStatus_Ok;
    }

    /* No BOM: use the bytes as UTF-8 when they are well-formed (covers plain ASCII
     * and BOM-less UTF-8); otherwise decode as the system ANSI code page, which is
     * what Excel's "CSV (Comma delimited)" / "Text (Tab delimited)" exports use. */
    if (csv_is_valid_utf8(raw, rawlen))
    {
        *out = (char *)raw;
        *out_len = rawlen;
        return EeLoadStatus_Ok;
    }
    return csv_codepage_to_utf8(raw, rawlen, CP_ACP, out, out_len, err, errcch);
}

static const wchar_t *csv_ext(const wchar_t *path)
{
    const wchar_t *dot = NULL;
    const wchar_t *p;
    for (p = path; *p != L'\0'; p++)
    {
        if (*p == L'.')
        {
            dot = p;
        }
        else if (*p == L'\\' || *p == L'/')
        {
            dot = NULL;
        }
    }
    return dot;
}

/* Choose the field delimiter: extension first, then sniff the first line. */
static char csv_pick_delim(const wchar_t *path, const char *u8, size_t len)
{
    const wchar_t *ext = csv_ext(path);
    size_t i;
    size_t commas = 0, tabs = 0;
    BOOL in_quotes = FALSE;

    if (ext != NULL)
    {
        if (_wcsicmp(ext, L".tsv") == 0)
        {
            return '\t';
        }
        if (_wcsicmp(ext, L".csv") == 0)
        {
            return ',';
        }
    }
    /* Sniff the first physical line (respecting quotes). */
    for (i = 0; i < len; i++)
    {
        char c = u8[i];
        if (c == '"')
        {
            in_quotes = !in_quotes;
        }
        else if (!in_quotes && (c == '\n' || c == '\r'))
        {
            break;
        }
        else if (!in_quotes && c == ',')
        {
            commas++;
        }
        else if (!in_quotes && c == '\t')
        {
            tabs++;
        }
    }
    return (tabs > commas) ? '\t' : ',';
}

/* -------------------------------------------------------------------------- */
/* Record accumulator                                                         */
/* -------------------------------------------------------------------------- */

typedef struct RecAcc
{
    char *bytes;      /* concatenated NUL-terminated fields for current record */
    size_t blen, bcap;
    uint32_t *offs;   /* offset of each field start within bytes */
    uint32_t noffs, ocap;
} RecAcc;

static BOOL rec_reserve_bytes(RecAcc *r, size_t extra)
{
    if (r->blen + extra > r->bcap)
    {
        size_t nc = (r->bcap == 0) ? 256 : r->bcap;
        char *nb;
        while (nc < r->blen + extra)
        {
            nc *= 2;
        }
        nb = (char *)realloc(r->bytes, nc);
        if (nb == NULL)
        {
            return FALSE;
        }
        r->bytes = nb;
        r->bcap = nc;
    }
    return TRUE;
}

/* Append one raw byte to the field currently being built. */
static BOOL rec_put(RecAcc *r, char c)
{
    if (!rec_reserve_bytes(r, 1))
    {
        return FALSE;
    }
    r->bytes[r->blen++] = c;
    return TRUE;
}

/* Finalize the current field (terminate it, record its offset, start a new one). */
static BOOL rec_end_field(RecAcc *r, uint32_t field_start)
{
    if (!rec_reserve_bytes(r, 1))
    {
        return FALSE;
    }
    r->bytes[r->blen++] = '\0';
    if (r->noffs == r->ocap)
    {
        uint32_t nc = (r->ocap == 0) ? 16 : r->ocap * 2;
        uint32_t *no = (uint32_t *)realloc(r->offs, nc * sizeof(uint32_t));
        if (no == NULL)
        {
            return FALSE;
        }
        r->offs = no;
        r->ocap = nc;
    }
    r->offs[r->noffs++] = field_start;
    return TRUE;
}

static void rec_free(RecAcc *r)
{
    free(r->bytes);
    free(r->offs);
}

/* -------------------------------------------------------------------------- */
/* Reader                                                                     */
/* -------------------------------------------------------------------------- */

EeLoadStatus EeCsv_ReadSheet(const wchar_t *path,
                             EeCsvRowSink sink,
                             void *sink_ctx,
                             volatile LONG *cancel_flag,
                             EeLoadProgressFn progress_fn,
                             void *progress_user,
                             wchar_t *error_message,
                             size_t error_cch)
{
    unsigned char *raw = NULL;
    size_t rawlen = 0;
    char *u8 = NULL;
    size_t u8len = 0;
    char delim;
    EeLoadStatus st;
    RecAcc rec;
    const char **cellptrs = NULL;
    uint32_t cellptr_cap = 0;
    size_t i;
    uint32_t field_start; /* offset within rec.bytes of the field being built */
    BOOL rec_dirty;       /* any byte or delimiter seen for the current record */
    uint32_t rows = 0;
    uint32_t last_pct = 101;
    EeLoadStatus result = EeLoadStatus_Ok;

    if (path == NULL || sink == NULL)
    {
        csv_set_err(error_message, error_cch, L"Invalid arguments.");
        return EeLoadStatus_Error;
    }
    ZeroMemory(&rec, sizeof(rec));

    st = csv_read_file(path, &raw, &rawlen, error_message, error_cch);
    if (st != EeLoadStatus_Ok)
    {
        return st;
    }
    st = csv_to_utf8(raw, rawlen, &u8, &u8len, error_message, error_cch);
    if (st != EeLoadStatus_Ok)
    {
        return st; /* csv_to_utf8 freed raw */
    }
    raw = NULL;

    delim = csv_pick_delim(path, u8, u8len);

    field_start = 0;
    rec_dirty = FALSE;

    i = 0;
    while (i <= u8len)
    {
        BOOL at_end = (i == u8len);
        char c = at_end ? '\0' : u8[i];

        if (!at_end && c == '"')
        {
            /* Quoted field: consume through the matching close quote. */
            rec_dirty = TRUE;
            i++; /* opening quote */
            while (i < u8len)
            {
                char q = u8[i];
                if (q == '"')
                {
                    if (i + 1 < u8len && u8[i + 1] == '"')
                    {
                        if (!rec_put(&rec, '"'))
                        {
                            goto oom;
                        }
                        i += 2;
                        continue;
                    }
                    i++; /* closing quote */
                    break;
                }
                if (!rec_put(&rec, q))
                {
                    goto oom;
                }
                i++;
            }
            continue;
        }

        if (at_end || c == delim || c == '\n' || c == '\r')
        {
            BOOL newline = at_end || c == '\n' || c == '\r';

            if (c == delim)
            {
                rec_dirty = TRUE;
            }

            /* At EOF with nothing pending, do not emit a phantom record. */
            if (at_end && !rec_dirty && rec.noffs == 0 && rec.blen == field_start)
            {
                break;
            }

            /* Close the field that just ended. */
            if (!rec_end_field(&rec, field_start))
            {
                goto oom;
            }
            field_start = (uint32_t)rec.blen;

            if (newline)
            {
                /* Skip a blank physical line (single empty field, no delimiter). */
                BOOL blank = (!rec_dirty && rec.noffs == 1 &&
                              rec.bytes[rec.offs[0]] == '\0');
                if (!blank)
                {
                    uint32_t k;
                    if (rec.noffs > cellptr_cap)
                    {
                        const char **np = (const char **)realloc(
                            cellptrs, rec.noffs * sizeof(const char *));
                        if (np == NULL)
                        {
                            goto oom;
                        }
                        cellptrs = np;
                        cellptr_cap = rec.noffs;
                    }
                    for (k = 0; k < rec.noffs; k++)
                    {
                        cellptrs[k] = rec.bytes + rec.offs[k];
                    }
                    if (!sink(sink_ctx, cellptrs, rec.noffs))
                    {
                        result = EeLoadStatus_Error;
                        goto done;
                    }
                    rows++;

                    if ((rows & 0x3FF) == 0)
                    {
                        if (cancel_flag != NULL && *cancel_flag != 0)
                        {
                            result = EeLoadStatus_Cancelled;
                            goto done;
                        }
                        if (progress_fn != NULL && u8len > 0)
                        {
                            uint32_t pct = (uint32_t)(((uint64_t)i * 99ull) / u8len);
                            if (pct > 99u)
                            {
                                pct = 99u;
                            }
                            if (pct != last_pct)
                            {
                                EeLoadProgress pr;
                                last_pct = pct;
                                pr.percent = pct;
                                pr.rows_loaded = rows;
                                pr.bytes_read = (uint64_t)i;
                                pr.bytes_total = (uint64_t)u8len;
                                if (!progress_fn(&pr, progress_user) &&
                                    cancel_flag != NULL)
                                {
                                    InterlockedExchange(cancel_flag, 1);
                                }
                            }
                        }
                    }
                }

                /* Reset for the next record. */
                rec.blen = 0;
                rec.noffs = 0;
                field_start = 0;
                rec_dirty = FALSE;

                /* Swallow a CRLF pair as one line ending. */
                if (!at_end && c == '\r' && i + 1 < u8len && u8[i + 1] == '\n')
                {
                    i++;
                }
            }

            if (at_end)
            {
                break;
            }
            i++;
            continue;
        }

        /* Ordinary character in an unquoted field. */
        rec_dirty = TRUE;
        if (!rec_put(&rec, c))
        {
            goto oom;
        }
        i++;
    }

    goto done;

oom:
    csv_set_err(error_message, error_cch, L"Out of memory loading the file.");
    result = EeLoadStatus_Error;

done:
    free(u8);
    free(cellptrs);
    rec_free(&rec);
    return result;
}
