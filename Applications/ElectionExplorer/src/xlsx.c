/**
 * @file xlsx.c
 * @brief Read tabular data from .xlsx workbooks. See xlsx.h and
 *        docs/xlsx-import-design.md.
 *
 * An .xlsx is a ZIP (OPC) of XML parts. We read the file into memory, open it
 * with vendored miniz, extract the parts we need, and scan them with a small
 * purpose-built XML reader (no COM). Cell text is emitted as UTF-8 to a row sink
 * that matches the CSV tokenizer's shape, so both feed the same table pipeline.
 *
 * Text is emitted as-is; t="b" as TRUE/FALSE; t="e" as the error text. Numbers
 * use the cell's style (styles.xml): a date format yields YYYY-MM-DD and a
 * zero-pad format (e.g. a ZIP "00000") keeps leading zeros; other numbers are the
 * stored decimal.
 */

#include "xlsx.h"

#include "third_party/miniz/miniz.h"

#include <stdlib.h>
#include <string.h>
#include <strsafe.h>

/* Bound allocations against malformed / hostile input. */
#define XLSX_MAX_FILE_BYTES (1024ull * 1024ull * 1024ull) /* 1 GiB compressed  */
#define XLSX_MAX_PART_BYTES (2048ull * 1024ull * 1024ull) /* 2 GiB per part    */
#define XLSX_MAX_COLS       16384                         /* Excel column max  */

/* -------------------------------------------------------------------------- */
/* Small growable byte buffer                                                 */
/* -------------------------------------------------------------------------- */

typedef struct Buf
{
    char *p;
    size_t len;
    size_t cap;
} Buf;

static void buf_free(Buf *b)
{
    free(b->p);
    b->p = NULL;
    b->len = b->cap = 0;
}

static BOOL buf_reserve(Buf *b, size_t add)
{
    if (b->len + add + 1 > b->cap)
    {
        size_t ncap = b->cap ? b->cap * 2 : 256;
        char *np;
        while (ncap < b->len + add + 1)
        {
            ncap *= 2;
        }
        np = (char *)realloc(b->p, ncap);
        if (np == NULL)
        {
            return FALSE;
        }
        b->p = np;
        b->cap = ncap;
    }
    return TRUE;
}

static BOOL buf_append(Buf *b, const char *s, size_t n)
{
    if (!buf_reserve(b, n))
    {
        return FALSE;
    }
    memcpy(b->p + b->len, s, n);
    b->len += n;
    b->p[b->len] = '\0';
    return TRUE;
}

static BOOL buf_append_cp(Buf *b, unsigned long cp)
{
    char u[4];
    int n = 0;
    if (cp <= 0x7F)
    {
        u[n++] = (char)cp;
    }
    else if (cp <= 0x7FF)
    {
        u[n++] = (char)(0xC0 | (cp >> 6));
        u[n++] = (char)(0x80 | (cp & 0x3F));
    }
    else if (cp <= 0xFFFF)
    {
        u[n++] = (char)(0xE0 | (cp >> 12));
        u[n++] = (char)(0x80 | ((cp >> 6) & 0x3F));
        u[n++] = (char)(0x80 | (cp & 0x3F));
    }
    else if (cp <= 0x10FFFF)
    {
        u[n++] = (char)(0xF0 | (cp >> 18));
        u[n++] = (char)(0x80 | ((cp >> 12) & 0x3F));
        u[n++] = (char)(0x80 | ((cp >> 6) & 0x3F));
        u[n++] = (char)(0x80 | (cp & 0x3F));
    }
    else
    {
        return TRUE; /* out of range: drop */
    }
    return buf_append(b, u, (size_t)n);
}

/* Append @p s (length @p n) to @p b, decoding the five predefined XML entities
 * and numeric character references (&#..; / &#x..;). No DTD/custom entities. */
static BOOL xml_append_unescaped(Buf *b, const char *s, size_t n)
{
    size_t i = 0;
    while (i < n)
    {
        char c = s[i];
        if (c != '&')
        {
            size_t start = i;
            while (i < n && s[i] != '&')
            {
                i++;
            }
            if (!buf_append(b, s + start, i - start))
            {
                return FALSE;
            }
            continue;
        }
        /* entity: find ';' within a short window */
        {
            size_t semi = i + 1;
            while (semi < n && semi < i + 12 && s[semi] != ';')
            {
                semi++;
            }
            if (semi >= n || s[semi] != ';')
            {
                /* lone '&' -- emit literally */
                if (!buf_append(b, "&", 1))
                {
                    return FALSE;
                }
                i++;
                continue;
            }
            {
                const char *e = s + i + 1;
                size_t elen = semi - (i + 1);
                BOOL ok = TRUE;
                if (elen == 3 && memcmp(e, "amp", 3) == 0)
                {
                    ok = buf_append(b, "&", 1);
                }
                else if (elen == 2 && memcmp(e, "lt", 2) == 0)
                {
                    ok = buf_append(b, "<", 1);
                }
                else if (elen == 2 && memcmp(e, "gt", 2) == 0)
                {
                    ok = buf_append(b, ">", 1);
                }
                else if (elen == 4 && memcmp(e, "quot", 4) == 0)
                {
                    ok = buf_append(b, "\"", 1);
                }
                else if (elen == 4 && memcmp(e, "apos", 4) == 0)
                {
                    ok = buf_append(b, "'", 1);
                }
                else if (elen >= 2 && e[0] == '#')
                {
                    unsigned long cp = 0;
                    if (e[1] == 'x' || e[1] == 'X')
                    {
                        size_t k;
                        for (k = 2; k < elen; k++)
                        {
                            char h = e[k];
                            cp <<= 4;
                            if (h >= '0' && h <= '9')
                                cp |= (unsigned long)(h - '0');
                            else if (h >= 'a' && h <= 'f')
                                cp |= (unsigned long)(h - 'a' + 10);
                            else if (h >= 'A' && h <= 'F')
                                cp |= (unsigned long)(h - 'A' + 10);
                        }
                    }
                    else
                    {
                        size_t k;
                        for (k = 1; k < elen; k++)
                        {
                            if (e[k] >= '0' && e[k] <= '9')
                            {
                                cp = cp * 10 + (unsigned long)(e[k] - '0');
                            }
                        }
                    }
                    ok = buf_append_cp(b, cp);
                }
                else
                {
                    /* unknown entity: emit verbatim including & and ; */
                    ok = buf_append(b, s + i, semi - i + 1);
                }
                if (!ok)
                {
                    return FALSE;
                }
            }
            i = semi + 1;
        }
    }
    return TRUE;
}

/* -------------------------------------------------------------------------- */
/* Tiny scanning helpers over an in-memory [p, end) region                    */
/* -------------------------------------------------------------------------- */

static const char *mem_find(const char *p, const char *end, const char *needle, size_t nlen)
{
    if (nlen == 0 || (size_t)(end - p) < nlen)
    {
        return NULL;
    }
    {
        const char *last = end - nlen;
        const char *q;
        for (q = p; q <= last; q++)
        {
            if (q[0] == needle[0] && memcmp(q, needle, nlen) == 0)
            {
                return q;
            }
        }
    }
    return NULL;
}

/* Within tag text [tag, tagend) find attribute @p name and copy its value
 * (raw, not unescaped) as a pointer/length. Returns TRUE if found. */
static BOOL tag_attr(const char *tag,
                     const char *tagend,
                     const char *name,
                     const char **val,
                     size_t *vlen)
{
    size_t nlen = strlen(name);
    const char *p = tag;
    while (p < tagend)
    {
        /* attribute must be preceded by whitespace */
        const char *hit = mem_find(p, tagend, name, nlen);
        if (hit == NULL)
        {
            return FALSE;
        }
        if (hit > tag && (hit[-1] == ' ' || hit[-1] == '\t' || hit[-1] == '\n' || hit[-1] == '\r'))
        {
            const char *q = hit + nlen;
            while (q < tagend && (*q == ' ' || *q == '\t'))
            {
                q++;
            }
            if (q < tagend && *q == '=')
            {
                q++;
                while (q < tagend && (*q == ' ' || *q == '\t'))
                {
                    q++;
                }
                if (q < tagend && (*q == '"' || *q == '\''))
                {
                    char quote = *q++;
                    const char *vstart = q;
                    while (q < tagend && *q != quote)
                    {
                        q++;
                    }
                    *val = vstart;
                    *vlen = (size_t)(q - vstart);
                    return TRUE;
                }
            }
        }
        p = hit + nlen;
    }
    return FALSE;
}

/* Bijective base-26 column letters (leading part of an A1 reference) -> 0-based
 * column index. Returns (uint32_t)-1 if there are no letters. */
static uint32_t col_from_ref(const char *r, size_t rlen)
{
    uint32_t c = 0;
    size_t i = 0;
    while (i < rlen)
    {
        char ch = r[i];
        if (ch >= 'A' && ch <= 'Z')
            c = c * 26u + (uint32_t)(ch - 'A' + 1);
        else if (ch >= 'a' && ch <= 'z')
            c = c * 26u + (uint32_t)(ch - 'a' + 1);
        else
            break;
        i++;
    }
    if (i == 0)
    {
        return (uint32_t)-1;
    }
    return c - 1u;
}

/* -------------------------------------------------------------------------- */
/* Errors + file IO                                                           */
/* -------------------------------------------------------------------------- */

static void xlsx_err(wchar_t *dst, size_t cch, const wchar_t *msg)
{
    if (dst != NULL && cch > 0)
    {
        size_t i = 0;
        for (; msg[i] != L'\0' && i + 1 < cch; i++)
        {
            dst[i] = msg[i];
        }
        dst[i] = L'\0';
    }
}

static BOOL read_entire_file(const wchar_t *path,
                             unsigned char **out,
                             size_t *out_size,
                             wchar_t *err,
                             size_t errcch)
{
    HANDLE h;
    LARGE_INTEGER sz;
    unsigned char *buf;
    size_t total = 0;

    *out = NULL;
    *out_size = 0;
    h = CreateFileW(path,
                    GENERIC_READ,
                    FILE_SHARE_READ,
                    NULL,
                    OPEN_EXISTING,
                    FILE_ATTRIBUTE_NORMAL | FILE_FLAG_SEQUENTIAL_SCAN,
                    NULL);
    if (h == INVALID_HANDLE_VALUE)
    {
        xlsx_err(err, errcch, L"Could not open the .xlsx file.");
        return FALSE;
    }
    if (!GetFileSizeEx(h, &sz) || sz.QuadPart <= 0 ||
        (unsigned long long)sz.QuadPart > XLSX_MAX_FILE_BYTES)
    {
        CloseHandle(h);
        xlsx_err(err, errcch, L"The .xlsx file is empty or too large.");
        return FALSE;
    }
    buf = (unsigned char *)malloc((size_t)sz.QuadPart);
    if (buf == NULL)
    {
        CloseHandle(h);
        xlsx_err(err, errcch, L"Out of memory reading the .xlsx file.");
        return FALSE;
    }
    while (total < (size_t)sz.QuadPart)
    {
        DWORD want =
            (DWORD)((sz.QuadPart - (LONGLONG)total > 0x10000000) ? 0x10000000
                                                                 : (sz.QuadPart - (LONGLONG)total));
        DWORD got = 0;
        if (!ReadFile(h, buf + total, want, &got, NULL) || got == 0)
        {
            free(buf);
            CloseHandle(h);
            xlsx_err(err, errcch, L"Error reading the .xlsx file.");
            return FALSE;
        }
        total += got;
    }
    CloseHandle(h);
    *out = buf;
    *out_size = total;
    return TRUE;
}

/* Extract one ZIP part by exact name into a heap buffer (caller mz_free's it).
 * Returns NULL if the part is absent or too large. */
static char *extract_part(mz_zip_archive *zip, const char *name, size_t *out_size)
{
    int idx = mz_zip_reader_locate_file(zip, name, NULL, 0);
    mz_zip_archive_file_stat st;
    void *data;
    size_t sz = 0;

    *out_size = 0;
    if (idx < 0)
    {
        return NULL;
    }
    if (!mz_zip_reader_file_stat(zip, (mz_uint)idx, &st) || st.m_uncomp_size > XLSX_MAX_PART_BYTES)
    {
        return NULL;
    }
    data = mz_zip_reader_extract_to_heap(zip, (mz_uint)idx, &sz, 0);
    if (data == NULL)
    {
        return NULL;
    }
    *out_size = sz;
    return (char *)data;
}

/* -------------------------------------------------------------------------- */
/* Shared strings                                                             */
/* -------------------------------------------------------------------------- */

typedef struct SharedStrings
{
    Buf pool;        /* concatenated NUL-terminated strings */
    size_t *offsets; /* offset of string i within pool.p    */
    uint32_t count;
    uint32_t cap;
} SharedStrings;

static void shared_free(SharedStrings *s)
{
    buf_free(&s->pool);
    free(s->offsets);
    s->offsets = NULL;
    s->count = s->cap = 0;
}

static BOOL shared_push(SharedStrings *s, size_t offset)
{
    if (s->count == s->cap)
    {
        uint32_t ncap = s->cap ? s->cap * 2 : 256;
        size_t *no = (size_t *)realloc(s->offsets, (size_t)ncap * sizeof(size_t));
        if (no == NULL)
        {
            return FALSE;
        }
        s->offsets = no;
        s->cap = ncap;
    }
    s->offsets[s->count++] = offset;
    return TRUE;
}

/* Collect concatenated <t> text within [p,end) into @p out, skipping any text
 * inside <rPh>...</rPh> (phonetic runs). */
static BOOL collect_t_text(const char *p, const char *end, Buf *out)
{
    while (p < end)
    {
        const char *lt = memchr(p, '<', (size_t)(end - p));
        if (lt == NULL)
        {
            break;
        }
        if ((size_t)(end - lt) >= 5 && memcmp(lt, "<rPh", 4) == 0 && (lt[4] == ' ' || lt[4] == '>'))
        {
            const char *close = mem_find(lt, end, "</rPh>", 6);
            p = (close != NULL) ? close + 6 : end;
            continue;
        }
        if ((size_t)(end - lt) >= 3 && memcmp(lt, "<t", 2) == 0 &&
            (lt[2] == ' ' || lt[2] == '>' || lt[2] == '/'))
        {
            const char *gt = memchr(lt, '>', (size_t)(end - lt));
            if (gt == NULL)
            {
                break;
            }
            if (gt[-1] == '/')
            {
                p = gt + 1; /* <t/> empty */
                continue;
            }
            {
                const char *tclose = mem_find(gt + 1, end, "</t>", 4);
                if (tclose == NULL)
                {
                    break;
                }
                if (!xml_append_unescaped(out, gt + 1, (size_t)(tclose - (gt + 1))))
                {
                    return FALSE;
                }
                p = tclose + 4;
                continue;
            }
        }
        p = lt + 1;
    }
    return TRUE;
}

static BOOL parse_shared_strings(const char *xml,
                                 size_t len,
                                 SharedStrings *out,
                                 wchar_t *err,
                                 size_t errcch)
{
    const char *p = xml;
    const char *end = xml + len;
    Buf tmp = {0};

    while (p < end)
    {
        const char *si = mem_find(p, end, "<si", 3);
        const char *siend;
        const char *close;
        if (si == NULL)
        {
            break;
        }
        if (si[3] != ' ' && si[3] != '>' && si[3] != '/')
        {
            p = si + 3;
            continue;
        }
        /* self-closing <si/> -> empty string */
        {
            const char *gt = memchr(si, '>', (size_t)(end - si));
            if (gt == NULL)
            {
                break;
            }
            if (gt[-1] == '/')
            {
                if (!shared_push(out, out->pool.len) || !buf_append(&out->pool, "", 0))
                {
                    goto oom;
                }
                out->pool.p[out->pool.len] = '\0';
                out->pool.len++; /* keep NUL as separator */
                p = gt + 1;
                continue;
            }
            siend = gt + 1;
        }
        close = mem_find(siend, end, "</si>", 5);
        if (close == NULL)
        {
            break;
        }
        tmp.len = 0;
        if (!collect_t_text(siend, close, &tmp))
        {
            goto oom;
        }
        if (!shared_push(out, out->pool.len))
        {
            goto oom;
        }
        if (!buf_append(&out->pool, tmp.len ? tmp.p : "", tmp.len))
        {
            goto oom;
        }
        out->pool.len++; /* advance past the NUL so next string starts fresh */
        p = close + 5;
    }
    buf_free(&tmp);
    return TRUE;

oom:
    buf_free(&tmp);
    xlsx_err(err, errcch, L"Out of memory reading shared strings.");
    return FALSE;
}

static const char *shared_at(const SharedStrings *s, long idx)
{
    if (idx < 0 || (uint32_t)idx >= s->count)
    {
        return "";
    }
    return s->pool.p + s->offsets[idx];
}

/* -------------------------------------------------------------------------- */
/* Styles (number formats) + date-serial conversion (Phase 3)                 */
/* -------------------------------------------------------------------------- */

typedef enum NumFmtKind
{
    NF_GENERAL = 0,
    NF_DATE,
    NF_ZEROPAD
} NumFmtKind;

typedef struct Styles
{
    NumFmtKind *xf_kind; /* per cellXfs index */
    int *xf_pad;         /* zero-pad width when kind == NF_ZEROPAD */
    uint32_t xf_count;
} Styles;

static void styles_free(Styles *s)
{
    free(s->xf_kind);
    free(s->xf_pad);
    s->xf_kind = NULL;
    s->xf_pad = NULL;
    s->xf_count = 0;
}

/* Proleptic-Gregorian civil date from a day count relative to 1970-01-01. */
static void civil_from_days(long z, int *yy, int *mm, int *dd)
{
    long era;
    unsigned doe, yoe, doy, mp, d, m;
    long y;
    z += 719468;
    era = (z >= 0 ? z : z - 146096) / 146097;
    doe = (unsigned)(z - era * 146097);
    yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    y = (long)yoe + era * 400;
    doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    mp = (5 * doy + 2) / 153;
    d = doy - (153 * mp + 2) / 5 + 1;
    m = mp < 10 ? mp + 3 : mp - 9;
    *yy = (int)(y + (m <= 2));
    *mm = (int)m;
    *dd = (int)d;
}

/* Append an Excel date serial as YYYY-MM-DD. Returns 1 on success, 0 if @p s is
 * not a number (caller falls back to raw), -1 on OOM. Accurate for dates from
 * 1900-03-01 on (all realistic voter dates); the 1900 leap-year phantom day is
 * absorbed by the standard 25569 epoch offset. */
static int append_serial_date(Buf *b, const char *s, size_t n, BOOL date1904)
{
    char numbuf[64];
    char out[16];
    char *endp = NULL;
    double serial;
    long days;
    int y, m, d;
    size_t cc = (n < sizeof(numbuf) - 1) ? n : sizeof(numbuf) - 1;

    memcpy(numbuf, s, cc);
    numbuf[cc] = '\0';
    serial = strtod(numbuf, &endp);
    if (endp == numbuf)
    {
        return 0;
    }
    days = (long)(serial + 1e-6);
    days -= date1904 ? 24107L : 25569L; /* -> days since 1970-01-01 */
    civil_from_days(days, &y, &m, &d);
    if (FAILED(StringCchPrintfA(out, ARRAYSIZE(out), "%04d-%02d-%02d", y, m, d)))
    {
        return -1;
    }
    return buf_append(b, out, strlen(out)) ? 1 : -1;
}

/* Left-pad an all-digit value to @p pad width. Returns 1 if padded, 0 if the
 * value is not a plain integer shorter than @p pad (caller uses raw), -1 OOM. */
static int append_zeropad(Buf *b, const char *s, size_t n, int pad)
{
    size_t i;
    int zeros;
    if (n == 0 || pad <= 0 || n >= (size_t)pad)
    {
        return 0;
    }
    for (i = 0; i < n; i++)
    {
        if (s[i] < '0' || s[i] > '9')
        {
            return 0;
        }
    }
    for (zeros = (int)((size_t)pad - n); zeros > 0; zeros--)
    {
        if (!buf_append(b, "0", 1))
        {
            return -1;
        }
    }
    return buf_append(b, s, n) ? 1 : -1;
}

static NumFmtKind builtin_numfmt_kind(int id)
{
    if ((id >= 14 && id <= 22) || (id >= 27 && id <= 36) || (id >= 45 && id <= 47) ||
        (id >= 50 && id <= 58))
    {
        return NF_DATE;
    }
    return NF_GENERAL;
}

/* Classify a custom format code: date if it has a y/d token (outside quotes and
 * [..] blocks); zero-pad if it is only '0' characters (e.g. a ZIP "00000"). */
static NumFmtKind classify_format_code(const char *code, size_t len, int *pad)
{
    size_t i;
    BOOL has_yd = FALSE;
    BOOL all_zero = (len > 0);
    for (i = 0; i < len; i++)
    {
        char c = code[i];
        if (c == '"')
        {
            all_zero = FALSE;
            i++;
            while (i < len && code[i] != '"')
            {
                i++;
            }
            continue;
        }
        if (c == '[')
        {
            all_zero = FALSE;
            while (i < len && code[i] != ']')
            {
                i++;
            }
            continue;
        }
        if (c == '\\')
        {
            all_zero = FALSE;
            i++;
            continue;
        }
        if (c == 'y' || c == 'Y' || c == 'd' || c == 'D')
        {
            has_yd = TRUE;
        }
        if (c != '0')
        {
            all_zero = FALSE;
        }
    }
    if (has_yd)
    {
        return NF_DATE;
    }
    if (all_zero)
    {
        *pad = (int)len;
        return NF_ZEROPAD;
    }
    return NF_GENERAL;
}

/* Find a custom numFmt (id >= 164) in styles.xml and classify its formatCode. */
static NumFmtKind classify_custom_numfmt(const char *xml, size_t len, int id, int *pad)
{
    const char *p = xml;
    const char *end = xml + len;
    while (p < end)
    {
        const char *nf = mem_find(p, end, "<numFmt", 7);
        const char *gt;
        const char *iv;
        size_t ivlen;
        const char *cv;
        size_t cvlen;
        int this_id = -1;
        if (nf == NULL || (nf[7] != ' ' && nf[7] != '\t'))
        {
            if (nf == NULL)
            {
                break;
            }
            p = nf + 7;
            continue;
        }
        gt = memchr(nf, '>', (size_t)(end - nf));
        if (gt == NULL)
        {
            break;
        }
        if (tag_attr(nf, gt, "numFmtId", &iv, &ivlen))
        {
            size_t j;
            this_id = 0;
            for (j = 0; j < ivlen && iv[j] >= '0' && iv[j] <= '9'; j++)
            {
                this_id = this_id * 10 + (iv[j] - '0');
            }
        }
        if (this_id == id && tag_attr(nf, gt, "formatCode", &cv, &cvlen))
        {
            return classify_format_code(cv, cvlen, pad);
        }
        p = gt + 1;
    }
    return NF_GENERAL;
}

/* Parse cellXfs into a per-style-index kind/pad table. Absent styles.xml or
 * cellXfs is fine (everything classifies as General). */
static BOOL parse_styles(const char *xml, size_t len, Styles *out, wchar_t *err, size_t errcch)
{
    const char *end = xml + len;
    const char *cx = mem_find(xml, xml + len, "<cellXfs", 8);
    const char *cxgt;
    const char *cxend;
    const char *p;
    uint32_t cap = 0;

    if (cx == NULL)
    {
        return TRUE;
    }
    cxgt = memchr(cx, '>', (size_t)(end - cx));
    if (cxgt == NULL)
    {
        return TRUE;
    }
    cxend = mem_find(cxgt + 1, end, "</cellXfs>", 10);
    if (cxend == NULL)
    {
        cxend = end;
    }
    p = cxgt + 1;
    for (;;)
    {
        const char *xf = mem_find(p, cxend, "<xf", 3);
        const char *xfgt;
        const char *nv;
        size_t nvlen;
        int id = 0;
        int pad = 0;
        NumFmtKind kind;

        if (xf == NULL || (xf[3] != ' ' && xf[3] != '>' && xf[3] != '/'))
        {
            if (xf == NULL)
            {
                break;
            }
            p = xf + 3;
            continue;
        }
        xfgt = memchr(xf, '>', (size_t)(cxend - xf));
        if (xfgt == NULL)
        {
            break;
        }
        if (tag_attr(xf, xfgt, "numFmtId", &nv, &nvlen))
        {
            size_t j;
            for (j = 0; j < nvlen && nv[j] >= '0' && nv[j] <= '9'; j++)
            {
                id = id * 10 + (nv[j] - '0');
            }
        }
        kind = (id >= 164) ? classify_custom_numfmt(xml, len, id, &pad) : builtin_numfmt_kind(id);

        if (out->xf_count == cap)
        {
            uint32_t ncap = cap ? cap * 2 : 64;
            NumFmtKind *nk = (NumFmtKind *)realloc(out->xf_kind, (size_t)ncap * sizeof(NumFmtKind));
            int *np;
            if (nk == NULL)
            {
                goto oom;
            }
            out->xf_kind = nk;
            np = (int *)realloc(out->xf_pad, (size_t)ncap * sizeof(int));
            if (np == NULL)
            {
                goto oom;
            }
            out->xf_pad = np;
            cap = ncap;
        }
        out->xf_kind[out->xf_count] = kind;
        out->xf_pad[out->xf_count] = pad;
        out->xf_count++;
        p = xfgt + 1;
    }
    return TRUE;

oom:
    styles_free(out);
    xlsx_err(err, errcch, L"Out of memory reading styles.");
    return FALSE;
}

/* -------------------------------------------------------------------------- */
/* Worksheet -> rows                                                          */
/* -------------------------------------------------------------------------- */

typedef struct RowBuilder
{
    const char **cells; /* pointer per column */
    Buf arena;          /* backing bytes for inline/number cells this row */
    size_t *starts;     /* arena offset for each arena-backed cell */
    uint32_t cap;
} RowBuilder;

static void rb_free(RowBuilder *rb)
{
    free(rb->cells);
    free(rb->starts);
    buf_free(&rb->arena);
    rb->cells = NULL;
    rb->starts = NULL;
    rb->cap = 0;
}

static BOOL rb_ensure(RowBuilder *rb, uint32_t cols)
{
    if (cols > rb->cap)
    {
        uint32_t ncap = rb->cap ? rb->cap : 64;
        const char **nc;
        size_t *ns;
        while (ncap < cols)
        {
            ncap *= 2;
        }
        nc = (const char **)realloc(rb->cells, (size_t)ncap * sizeof(char *));
        if (nc == NULL)
        {
            return FALSE;
        }
        rb->cells = nc;
        ns = (size_t *)realloc(rb->starts, (size_t)ncap * sizeof(size_t));
        if (ns == NULL)
        {
            return FALSE;
        }
        rb->starts = ns;
        rb->cap = ncap;
    }
    return TRUE;
}

typedef struct XlsxReadCtx
{
    const SharedStrings *shared;
    const Styles *styles;
    BOOL date1904;
    EeXlsxRowSink sink;
    void *sink_ctx;
    volatile LONG *cancel;
    EeLoadProgressFn progress;
    void *progress_user;
    uint64_t bytes_total;
    uint32_t rows;
    BOOL aborted;
} XlsxReadCtx;

/* Resolve one <c ...>...</c> cell's text. Arena-backed results are appended to
 * rb->arena and referenced by offset (patched to pointers after the row is
 * complete, since the arena may realloc). Shared-string results point directly
 * into the shared pool (stable) and are marked with start == (size_t)-1. */
static BOOL cell_value(const XlsxReadCtx *ctx,
                       const char *tag,
                       const char *tagend,
                       const char *cellend,
                       RowBuilder *rb,
                       uint32_t col,
                       long style_idx,
                       BOOL *is_shared,
                       const char **shared_ptr)
{
    const SharedStrings *shared = ctx->shared;
    const char *tv;
    size_t tvlen;
    char type = 'n';
    *is_shared = FALSE;
    *shared_ptr = "";

    if (tag_attr(tag, tagend, "t", &tv, &tvlen) && tvlen > 0)
    {
        /* first char distinguishes s / str / inlineStr / b / e */
        if (tvlen >= 9 && memcmp(tv, "inlineStr", 9) == 0)
            type = 'i';
        else if (tvlen == 1 && tv[0] == 's')
            type = 's';
        else if (tvlen == 3 && memcmp(tv, "str", 3) == 0)
            type = 'S';
        else if (tvlen == 1 && tv[0] == 'b')
            type = 'b';
        else if (tvlen == 1 && tv[0] == 'e')
            type = 'e';
        else
            type = 'n';
    }

    if (type == 'i')
    {
        const char *is = mem_find(tagend, cellend, "<is", 3);
        rb->starts[col] = rb->arena.len;
        if (is != NULL)
        {
            const char *gt = memchr(is, '>', (size_t)(cellend - is));
            if (gt != NULL && !collect_t_text(gt + 1, cellend, &rb->arena))
            {
                return FALSE;
            }
        }
        if (!buf_append(&rb->arena, "", 0))
        {
            return FALSE;
        }
        rb->arena.p[rb->arena.len++] = '\0';
        return TRUE;
    }

    /* everything else reads <v>...</v> */
    {
        const char *v = mem_find(tagend, cellend, "<v", 2);
        const char *vstart = NULL;
        const char *vend = NULL;
        if (v != NULL)
        {
            const char *gt = memchr(v, '>', (size_t)(cellend - v));
            if (gt != NULL && gt[-1] != '/')
            {
                const char *vc = mem_find(gt + 1, cellend, "</v>", 4);
                if (vc != NULL)
                {
                    vstart = gt + 1;
                    vend = vc;
                }
            }
        }
        if (type == 's')
        {
            long idx = -1;
            if (vstart != NULL)
            {
                idx = 0;
                for (; vstart < vend && *vstart >= '0' && *vstart <= '9'; vstart++)
                {
                    idx = idx * 10 + (*vstart - '0');
                }
            }
            *is_shared = TRUE;
            *shared_ptr = shared_at(shared, idx);
            return TRUE;
        }
        rb->starts[col] = rb->arena.len;
        if (type == 'b')
        {
            BOOL t = (vstart != NULL && vstart < vend && *vstart != '0');
            if (!buf_append(&rb->arena, t ? "TRUE" : "FALSE", t ? 4 : 5))
            {
                return FALSE;
            }
        }
        else if (type == 'n')
        {
            /* Number: apply the cell's style so date serials become dates and
             * zero-padded formats (e.g. a ZIP "00000") keep their leading zeros;
             * otherwise emit the stored value text. */
            NumFmtKind kind = NF_GENERAL;
            int pad = 0;
            int r = 0;
            if (ctx->styles != NULL && style_idx >= 0 &&
                (uint32_t)style_idx < ctx->styles->xf_count)
            {
                kind = ctx->styles->xf_kind[style_idx];
                pad = ctx->styles->xf_pad[style_idx];
            }
            if (vstart != NULL && kind == NF_DATE)
            {
                r = append_serial_date(&rb->arena, vstart, (size_t)(vend - vstart), ctx->date1904);
            }
            else if (vstart != NULL && kind == NF_ZEROPAD)
            {
                r = append_zeropad(&rb->arena, vstart, (size_t)(vend - vstart), pad);
            }
            if (r == -1)
            {
                return FALSE;
            }
            if (r == 0 && vstart != NULL)
            {
                if (!xml_append_unescaped(&rb->arena, vstart, (size_t)(vend - vstart)))
                {
                    return FALSE;
                }
            }
        }
        else if (vstart != NULL)
        {
            /* str / e: emit the value text (unescape). */
            if (!xml_append_unescaped(&rb->arena, vstart, (size_t)(vend - vstart)))
            {
                return FALSE;
            }
        }
        if (!buf_append(&rb->arena, "", 0))
        {
            return FALSE;
        }
        rb->arena.p[rb->arena.len++] = '\0';
        return TRUE;
    }
}

static BOOL emit_row(XlsxReadCtx *ctx, RowBuilder *rb, uint32_t width)
{
    /* Patch arena-backed cells (offsets) to real pointers now the arena is
     * final for this row; shared cells already hold stable pointers. */
    uint32_t i;
    for (i = 0; i < width; i++)
    {
        if (rb->starts[i] != (size_t)-1)
        {
            rb->cells[i] = rb->arena.p + rb->starts[i];
        }
    }
    if (!ctx->sink(ctx->sink_ctx, rb->cells, width))
    {
        ctx->aborted = TRUE;
        return FALSE;
    }
    return TRUE;
}

static EeLoadStatus parse_worksheet(const char *xml,
                                    size_t len,
                                    XlsxReadCtx *ctx,
                                    wchar_t *err,
                                    size_t errcch)
{
    const char *end = xml + len;
    const char *sd = mem_find(xml, end, "<sheetData", 10);
    const char *p;
    RowBuilder rb = {0};
    EeLoadStatus status = EeLoadStatus_Ok;
    uint32_t last_pct = 0;

    if (sd == NULL)
    {
        return EeLoadStatus_Ok; /* empty sheet */
    }
    p = memchr(sd, '>', (size_t)(end - sd));
    if (p == NULL)
    {
        return EeLoadStatus_Ok;
    }
    p++;

    for (;;)
    {
        const char *row = mem_find(p, end, "<row", 4);
        const char *rowgt;
        const char *rowend;
        uint32_t width = 0;
        const char *c;

        rb.arena.len = 0; /* reuse the arena for each row */

        if (row == NULL || (row[4] != ' ' && row[4] != '>' && row[4] != '/'))
        {
            if (row == NULL)
            {
                break;
            }
            p = row + 4;
            continue;
        }
        rowgt = memchr(row, '>', (size_t)(end - row));
        if (rowgt == NULL)
        {
            break;
        }
        if (rowgt[-1] == '/')
        {
            /* empty <row/> -> emit a zero-width row so row numbering is kept? We
             * simply skip; the ingest pipeline tolerates it. */
            p = rowgt + 1;
            continue;
        }
        rowend = mem_find(rowgt + 1, end, "</row>", 6);
        if (rowend == NULL)
        {
            rowend = end;
        }

        /* Walk cells in this row, placing each by its column index. */
        c = rowgt + 1;
        for (;;)
        {
            const char *cell = mem_find(c, rowend, "<c", 2);
            const char *cgt;
            const char *cellend;
            const char *rv;
            size_t rvlen;
            uint32_t col;
            BOOL is_shared = FALSE;
            const char *sptr = "";

            if (cell == NULL || (cell[2] != ' ' && cell[2] != '>' && cell[2] != '/'))
            {
                if (cell == NULL)
                {
                    break;
                }
                c = cell + 2;
                continue;
            }
            cgt = memchr(cell, '>', (size_t)(rowend - cell));
            if (cgt == NULL)
            {
                break;
            }
            /* determine column index */
            if (tag_attr(cell, cgt, "r", &rv, &rvlen))
            {
                col = col_from_ref(rv, rvlen);
                if (col == (uint32_t)-1)
                {
                    col = width; /* fall back to positional */
                }
            }
            else
            {
                col = width;
            }
            if (col >= XLSX_MAX_COLS)
            {
                c = cgt + 1;
                continue;
            }
            if (!rb_ensure(&rb, col + 1))
            {
                status = EeLoadStatus_Error;
                xlsx_err(err, errcch, L"Out of memory reading a worksheet row.");
                goto done;
            }
            /* new columns since last high-water get empty defaults */
            while (width <= col)
            {
                rb.cells[width] = "";
                rb.starts[width] = (size_t)-1;
                width++;
            }

            if (cgt[-1] == '/')
            {
                cellend = cgt; /* self-closed empty cell */
            }
            else
            {
                cellend = mem_find(cgt + 1, rowend, "</c>", 4);
                if (cellend == NULL)
                {
                    cellend = rowend;
                }
            }

            if (cgt[-1] != '/')
            {
                long style_idx = -1;
                const char *sv;
                size_t svlen;
                if (tag_attr(cell, cgt, "s", &sv, &svlen))
                {
                    size_t j;
                    style_idx = 0;
                    for (j = 0; j < svlen && sv[j] >= '0' && sv[j] <= '9'; j++)
                    {
                        style_idx = style_idx * 10 + (sv[j] - '0');
                    }
                }
                if (!cell_value(ctx, cell, cgt, cellend, &rb, col, style_idx, &is_shared, &sptr))
                {
                    status = EeLoadStatus_Error;
                    xlsx_err(err, errcch, L"Out of memory reading a cell.");
                    goto done;
                }
                if (is_shared)
                {
                    rb.cells[col] = sptr;
                    rb.starts[col] = (size_t)-1;
                }
                /* else: arena-backed; pointer patched in emit_row */
            }

            c = (cellend < rowend) ? cellend + 4 : rowend;
        }

        if (width > 0)
        {
            if (!emit_row(ctx, &rb, width))
            {
                status = EeLoadStatus_Cancelled;
                goto done;
            }
        }
        ctx->rows++;

        /* progress + cancel */
        if ((ctx->rows & 0x3FF) == 0)
        {
            if (ctx->cancel != NULL && *ctx->cancel != 0)
            {
                status = EeLoadStatus_Cancelled;
                goto done;
            }
            if (ctx->progress != NULL && ctx->bytes_total > 0)
            {
                uint64_t done_bytes = (uint64_t)(p - xml);
                uint32_t pct = (uint32_t)((done_bytes * 99ull) / ctx->bytes_total);
                if (pct > 99u)
                {
                    pct = 99u;
                }
                if (pct != last_pct)
                {
                    EeLoadProgress pr;
                    last_pct = pct;
                    pr.percent = pct;
                    pr.rows_loaded = ctx->rows;
                    pr.bytes_read = done_bytes;
                    pr.bytes_total = ctx->bytes_total;
                    if (!ctx->progress(&pr, ctx->progress_user) && ctx->cancel != NULL)
                    {
                        InterlockedExchange(ctx->cancel, 1);
                    }
                }
            }
        }

        p = (rowend < end) ? rowend + 6 : end;
    }

done:
    rb_free(&rb);
    return status;
}

/* -------------------------------------------------------------------------- */
/* Workbook + relationships -> sheet part paths                               */
/* -------------------------------------------------------------------------- */

typedef struct SheetRef
{
    char name_utf8[192];
    char rid[64];
} SheetRef;

static int parse_workbook_sheets(const char *xml, size_t len, SheetRef *sheets, int max)
{
    const char *p = xml;
    const char *end = xml + len;
    int n = 0;
    while (n < max)
    {
        const char *s = mem_find(p, end, "<sheet", 6);
        const char *gt;
        const char *nv;
        size_t nvlen;
        if (s == NULL || (s[6] != ' ' && s[6] != '\t'))
        {
            if (s == NULL)
            {
                break;
            }
            p = s + 6;
            continue;
        }
        gt = memchr(s, '>', (size_t)(end - s));
        if (gt == NULL)
        {
            break;
        }
        sheets[n].name_utf8[0] = '\0';
        sheets[n].rid[0] = '\0';
        if (tag_attr(s, gt, "name", &nv, &nvlen))
        {
            Buf b = {0};
            if (xml_append_unescaped(&b, nv, nvlen) && b.p != NULL)
            {
                size_t cc = b.len < sizeof(sheets[n].name_utf8) - 1
                                ? b.len
                                : sizeof(sheets[n].name_utf8) - 1;
                memcpy(sheets[n].name_utf8, b.p, cc);
                sheets[n].name_utf8[cc] = '\0';
            }
            buf_free(&b);
        }
        if (tag_attr(s, gt, "r:id", &nv, &nvlen))
        {
            size_t cc = nvlen < sizeof(sheets[n].rid) - 1 ? nvlen : sizeof(sheets[n].rid) - 1;
            memcpy(sheets[n].rid, nv, cc);
            sheets[n].rid[cc] = '\0';
        }
        n++;
        p = gt + 1;
    }
    return n;
}

/* Given the rels XML, resolve @p rid to a part path "xl/<target>" in @p out. */
static BOOL resolve_rid_target(const char *xml,
                               size_t len,
                               const char *rid,
                               char *out,
                               size_t outcch)
{
    const char *p = xml;
    const char *end = xml + len;
    size_t ridlen = strlen(rid);
    while (p < end)
    {
        const char *rel = mem_find(p, end, "<Relationship", 13);
        const char *gt;
        const char *iv;
        size_t ivlen;
        const char *tv;
        size_t tvlen;
        if (rel == NULL)
        {
            break;
        }
        gt = memchr(rel, '>', (size_t)(end - rel));
        if (gt == NULL)
        {
            break;
        }
        if (tag_attr(rel, gt, "Id", &iv, &ivlen) && ivlen == ridlen && memcmp(iv, rid, ridlen) == 0)
        {
            if (tag_attr(rel, gt, "Target", &tv, &tvlen))
            {
                const char *t = tv;
                size_t tl = tvlen;
                if (tl > 0 && t[0] == '/')
                {
                    /* package-absolute: strip leading slash */
                    t++;
                    tl--;
                    if (tl >= 3 && memcmp(t, "xl/", 3) == 0)
                    {
                        return SUCCEEDED(StringCchCopyNA(out, outcch, t, tl)) ? TRUE : FALSE;
                    }
                }
                /* relative to xl/ */
                if (SUCCEEDED(StringCchCopyA(out, outcch, "xl/")) &&
                    SUCCEEDED(StringCchCatNA(out, outcch, t, tl)))
                {
                    return TRUE;
                }
            }
            return FALSE;
        }
        p = gt + 1;
    }
    return FALSE;
}

/* -------------------------------------------------------------------------- */
/* Public API                                                                 */
/* -------------------------------------------------------------------------- */

EeLoadStatus EeXlsx_ListSheets(const wchar_t *path,
                               wchar_t names[][EE_XLSX_SHEET_NAME_CCH],
                               int max_sheets,
                               int *out_count,
                               wchar_t *error_message,
                               size_t error_cch)
{
    unsigned char *file = NULL;
    size_t file_size = 0;
    mz_zip_archive zip;
    char *wb = NULL;
    size_t wb_size = 0;
    SheetRef sheets[EE_XLSX_MAX_SHEETS];
    int n = 0;
    int i;
    EeLoadStatus status = EeLoadStatus_Error;

    if (out_count != NULL)
    {
        *out_count = 0;
    }
    if (path == NULL || names == NULL || out_count == NULL || max_sheets <= 0)
    {
        xlsx_err(error_message, error_cch, L"Invalid arguments.");
        return EeLoadStatus_Error;
    }
    if (!read_entire_file(path, &file, &file_size, error_message, error_cch))
    {
        return EeLoadStatus_Error;
    }
    mz_zip_zero_struct(&zip);
    if (!mz_zip_reader_init_mem(&zip, file, file_size, 0))
    {
        free(file);
        xlsx_err(error_message, error_cch, L"Not a valid .xlsx (ZIP) file.");
        return EeLoadStatus_Error;
    }
    wb = extract_part(&zip, "xl/workbook.xml", &wb_size);
    if (wb == NULL)
    {
        xlsx_err(error_message, error_cch, L"Missing xl/workbook.xml.");
        goto cleanup;
    }
    n = parse_workbook_sheets(wb, wb_size, sheets, EE_XLSX_MAX_SHEETS);
    if (n <= 0)
    {
        xlsx_err(error_message, error_cch, L"No worksheets found.");
        goto cleanup;
    }
    if (n > max_sheets)
    {
        n = max_sheets;
    }
    for (i = 0; i < n; i++)
    {
        int wc = MultiByteToWideChar(CP_UTF8,
                                     0,
                                     sheets[i].name_utf8,
                                     -1,
                                     names[i],
                                     EE_XLSX_SHEET_NAME_CCH);
        if (wc == 0)
        {
            names[i][0] = L'\0';
        }
    }
    *out_count = n;
    status = EeLoadStatus_Ok;

cleanup:
    if (wb != NULL)
    {
        mz_free(wb);
    }
    mz_zip_reader_end(&zip);
    free(file);
    return status;
}

EeLoadStatus EeXlsx_ReadSheet(const wchar_t *path,
                              int sheet_index,
                              EeXlsxRowSink sink,
                              void *sink_ctx,
                              volatile LONG *cancel_flag,
                              EeLoadProgressFn progress_fn,
                              void *progress_user,
                              wchar_t *error_message,
                              size_t error_cch)
{
    unsigned char *file = NULL;
    size_t file_size = 0;
    mz_zip_archive zip;
    char *wb = NULL, *rels = NULL, *shared_xml = NULL, *sheet_xml = NULL, *styles_xml = NULL;
    size_t wb_size = 0, rels_size = 0, shared_size = 0, sheet_size = 0, styles_size = 0;
    SheetRef sheets[EE_XLSX_MAX_SHEETS];
    SharedStrings shared = {0};
    Styles styles = {0};
    BOOL date1904 = FALSE;
    char sheet_part[260];
    int n;
    EeLoadStatus status = EeLoadStatus_Error;
    XlsxReadCtx ctx;

    if (path == NULL || sink == NULL || sheet_index < 0)
    {
        xlsx_err(error_message, error_cch, L"Invalid arguments.");
        return EeLoadStatus_Error;
    }
    if (!read_entire_file(path, &file, &file_size, error_message, error_cch))
    {
        return EeLoadStatus_Error;
    }
    mz_zip_zero_struct(&zip);
    if (!mz_zip_reader_init_mem(&zip, file, file_size, 0))
    {
        free(file);
        xlsx_err(error_message, error_cch, L"Not a valid .xlsx (ZIP) file.");
        return EeLoadStatus_Error;
    }

    wb = extract_part(&zip, "xl/workbook.xml", &wb_size);
    rels = extract_part(&zip, "xl/_rels/workbook.xml.rels", &rels_size);
    if (wb == NULL || rels == NULL)
    {
        xlsx_err(error_message, error_cch, L"Missing workbook parts.");
        goto cleanup;
    }
    n = parse_workbook_sheets(wb, wb_size, sheets, EE_XLSX_MAX_SHEETS);
    if (sheet_index >= n)
    {
        xlsx_err(error_message, error_cch, L"Worksheet index out of range.");
        goto cleanup;
    }
    /* Date system: default 1900; some workbooks use the 1904 system. */
    if (mem_find(wb, wb + wb_size, "date1904=\"1\"", 12) != NULL ||
        mem_find(wb, wb + wb_size, "date1904=\"true\"", 15) != NULL)
    {
        date1904 = TRUE;
    }
    if (!resolve_rid_target(rels,
                            rels_size,
                            sheets[sheet_index].rid,
                            sheet_part,
                            ARRAYSIZE(sheet_part)))
    {
        xlsx_err(error_message, error_cch, L"Could not locate the worksheet part.");
        goto cleanup;
    }

    /* Shared strings are optional (files using inline strings have none). */
    shared_xml = extract_part(&zip, "xl/sharedStrings.xml", &shared_size);
    if (shared_xml != NULL)
    {
        if (!parse_shared_strings(shared_xml, shared_size, &shared, error_message, error_cch))
        {
            goto cleanup;
        }
    }

    /* Styles are optional; they tell dates from plain numbers (Phase 3). */
    styles_xml = extract_part(&zip, "xl/styles.xml", &styles_size);
    if (styles_xml != NULL)
    {
        if (!parse_styles(styles_xml, styles_size, &styles, error_message, error_cch))
        {
            goto cleanup;
        }
    }

    sheet_xml = extract_part(&zip, sheet_part, &sheet_size);
    if (sheet_xml == NULL)
    {
        xlsx_err(error_message, error_cch, L"Could not read the worksheet.");
        goto cleanup;
    }

    ctx.shared = &shared;
    ctx.styles = &styles;
    ctx.date1904 = date1904;
    ctx.sink = sink;
    ctx.sink_ctx = sink_ctx;
    ctx.cancel = cancel_flag;
    ctx.progress = progress_fn;
    ctx.progress_user = progress_user;
    ctx.bytes_total = sheet_size;
    ctx.rows = 0;
    ctx.aborted = FALSE;

    status = parse_worksheet(sheet_xml, sheet_size, &ctx, error_message, error_cch);

cleanup:
    shared_free(&shared);
    styles_free(&styles);
    if (sheet_xml != NULL)
        mz_free(sheet_xml);
    if (styles_xml != NULL)
        mz_free(styles_xml);
    if (shared_xml != NULL)
        mz_free(shared_xml);
    if (rels != NULL)
        mz_free(rels);
    if (wb != NULL)
        mz_free(wb);
    mz_zip_reader_end(&zip);
    free(file);
    return status;
}
