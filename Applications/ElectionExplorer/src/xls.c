/**
 * @file xls.c
 * @brief Read tabular data from legacy .xls (Excel 97-2003, BIFF8) workbooks. See xls.h.
 *
 * Two layers. An OLE2 compound file (MS-CFB) stores named streams as chains of sectors
 * linked by a sector allocation table (FAT); streams under 4 KB live in a "mini stream"
 * of 64-byte sectors with their own table. The "Workbook" stream is a sequence of BIFF8
 * records (MS-XLS): the workbook globals give the sheet list, the shared string table
 * (SST), number formats (FORMAT) and cell formats (XF); each worksheet's records give its
 * cells. A record longer than 8 KB continues in CONTINUE records, and a string split
 * across one restarts with an option byte, so strings are read through a cursor that
 * crosses them. Cells may arrive in any order: a sheet's cells are collected, sorted by
 * row and column, and emitted row by row with the .xlsx reader's cell text conventions.
 * Every offset, length and chain read from the file is bounds-checked.
 */

#include "xls.h"

#include <stdlib.h>
#include <string.h>
#include <strsafe.h>

#define XLS_MAX_COLS   16384 /* same cap as the .xlsx reader */
#define CFB_ENDOFCHAIN 0xFFFFFFFEu
#define CFB_FREESECT   0xFFFFFFFFu
#define CFB_MINI_SECT  64u

/* BIFF8 record types */
#define BR_FORMULA    0x0006
#define BR_EOF        0x000A
#define BR_DATEMODE   0x0022
#define BR_FILEPASS   0x002F
#define BR_CONTINUE   0x003C
#define BR_BOUNDSHEET 0x0085
#define BR_MULRK      0x00BD
#define BR_RSTRING    0x00D6
#define BR_XF         0x00E0
#define BR_SST        0x00FC
#define BR_LABELSST   0x00FD
#define BR_NUMBER     0x0203
#define BR_LABEL      0x0204
#define BR_BOOLERR    0x0205
#define BR_STRING     0x0207
#define BR_RK         0x027E
#define BR_FORMAT     0x041E
#define BR_BOF        0x0809

static uint16_t rd16(const uint8_t *p)
{
    return (uint16_t)(p[0] | (p[1] << 8));
}

static uint32_t rd32(const uint8_t *p)
{
    return (uint32_t)p[0] | ((uint32_t)p[1] << 8) | ((uint32_t)p[2] << 16) | ((uint32_t)p[3] << 24);
}

static void xls_err(wchar_t *dst, size_t cch, const wchar_t *msg)
{
    if (dst != NULL && cch > 0)
    {
        StringCchCopyW(dst, cch, msg);
    }
}

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

static BOOL buf_append(Buf *b, const void *s, size_t n)
{
    if (n > ((size_t)-1) - b->len - 1)
    {
        return FALSE;
    }
    if (b->len + n + 1 > b->cap)
    {
        size_t ncap = b->cap ? b->cap : 256;
        char *np;
        while (ncap < b->len + n + 1)
        {
            if (ncap > ((size_t)-1) / 2)
            {
                return FALSE;
            }
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
    if (n > 0)
    {
        memcpy(b->p + b->len, s, n); /* SAFE: capacity ensured above */
    }
    b->len += n;
    b->p[b->len] = '\0';
    return TRUE;
}

/* Append a NUL that is part of the content (string pools). */
static BOOL buf_append_nul(Buf *b)
{
    if (!buf_append(b, "", 0))
    {
        return FALSE;
    }
    b->len++; /* keep the terminator buf_append wrote; it now separates entries */
    return buf_append(b, "", 0);
}

/* Grow a heap array of @p elem-byte items to hold @p need items. */
static BOOL grow(void **arr, uint32_t *cap, uint32_t need, size_t elem)
{
    if (need > *cap)
    {
        uint32_t ncap = *cap ? *cap : 64;
        void *np;
        while (ncap < need)
        {
            if (ncap > 0x40000000u)
            {
                return FALSE;
            }
            ncap *= 2;
        }
        np = realloc(*arr, (size_t)ncap * elem);
        if (np == NULL)
        {
            return FALSE;
        }
        *arr = np;
        *cap = ncap;
    }
    return TRUE;
}

/* -------------------------------------------------------------------------- */
/* OLE2 compound file                                                         */
/* -------------------------------------------------------------------------- */

typedef struct Cfb
{
    const uint8_t *data;
    size_t size;
    uint32_t ssz; /* sector size: 512 or 4096 */
    uint32_t *fat;
    uint32_t nfat;
    uint32_t *minifat;
    uint32_t nminifat;
    uint8_t *ministream;  /* the root entry's stream: the mini sectors */
    size_t ministream_size;
    uint32_t mini_cutoff; /* streams smaller than this live in the mini stream */
    uint8_t *dir;         /* directory entries, 128 bytes each */
    uint32_t ndir;
} Cfb;

static void cfb_free(Cfb *c)
{
    free(c->fat);
    free(c->minifat);
    free(c->ministream);
    free(c->dir);
    ZeroMemory(c, sizeof(*c));
}

/* Sector @p sid's bytes, or NULL past the end of the file; *avail = bytes present (the
 * file's last sector may be short). */
static const uint8_t *cfb_sector(const Cfb *c, uint32_t sid, size_t *avail)
{
    uint64_t off = ((uint64_t)sid + 1u) * c->ssz;
    if (sid >= 0xFFFFFFFAu || off >= (uint64_t)c->size)
    {
        return NULL;
    }
    *avail = (c->size - (size_t)off < c->ssz) ? c->size - (size_t)off : c->ssz;
    return c->data + (size_t)off;
}

/* Read the FAT chain from @p start into a new buffer: @p want bytes, or the whole chain
 * when @p want is (size_t)-1. A chain that ends early, loops or leaves the file fails. */
static BOOL cfb_read_chain(const Cfb *c,
                           uint32_t start,
                           size_t want,
                           uint8_t **out,
                           size_t *out_len)
{
    Buf b = {0};
    uint32_t sid = start;
    uint32_t steps = 0;
    *out = NULL;
    *out_len = 0;
    while (sid != CFB_ENDOFCHAIN && b.len < want)
    {
        size_t avail = 0;
        size_t take;
        const uint8_t *p;
        if (sid >= c->nfat || ++steps > c->nfat)
        {
            goto fail;
        }
        p = cfb_sector(c, sid, &avail);
        if (p == NULL)
        {
            goto fail;
        }
        take = (want - b.len < avail) ? want - b.len : avail;
        if (!buf_append(&b, p, take))
        {
            goto fail;
        }
        sid = c->fat[sid];
    }
    if (want != (size_t)-1 && b.len < want)
    {
        goto fail;
    }
    *out = (uint8_t *)b.p;
    *out_len = b.len;
    return TRUE;

fail:
    buf_free(&b);
    return FALSE;
}

/* Read @p want bytes of a mini-stream chain from @p start. */
static BOOL cfb_read_mini(const Cfb *c, uint32_t start, size_t want, uint8_t **out, size_t *out_len)
{
    Buf b = {0};
    uint32_t sid = start;
    uint32_t steps = 0;
    *out = NULL;
    *out_len = 0;
    while (b.len < want)
    {
        uint64_t off = (uint64_t)sid * CFB_MINI_SECT;
        size_t take = CFB_MINI_SECT;
        if (sid >= c->nminifat || ++steps > c->nminifat || off >= (uint64_t)c->ministream_size)
        {
            buf_free(&b);
            return FALSE;
        }
        if (c->ministream_size - (size_t)off < take)
        {
            take = c->ministream_size - (size_t)off;
        }
        if (want - b.len < take)
        {
            take = want - b.len;
        }
        if (!buf_append(&b, c->ministream + (size_t)off, take))
        {
            buf_free(&b);
            return FALSE;
        }
        sid = c->minifat[sid];
    }
    *out = (uint8_t *)b.p;
    *out_len = b.len;
    return TRUE;
}

/* Little-endian table bytes -> a new array of @p n entries. */
static uint32_t *table_from_bytes(const uint8_t *p, size_t n)
{
    uint32_t *t = (uint32_t *)malloc((n ? n : 1) * sizeof(uint32_t));
    size_t i;
    if (t != NULL)
    {
        for (i = 0; i < n; i++)
        {
            t[i] = rd32(p + 4 * i);
        }
    }
    return t;
}

static BOOL cfb_open(Cfb *c, const uint8_t *data, size_t size, wchar_t *err, size_t errcch)
{
    static const wchar_t k_damaged[] =
        L"The .xls file is damaged (its compound-file structure could not be read).";
    uint16_t shift;
    uint32_t nfatsec, first_dir, first_mini, nmini, first_difat;
    uint32_t *fatsecs = NULL;
    uint32_t nfs = 0;
    uint32_t per;
    uint32_t i;
    size_t dirlen = 0;
    uint64_t max_sectors;

    ZeroMemory(c, sizeof(*c));
    c->data = data;
    c->size = size;
    if (size < 512 || !EeXls_IsCompoundFile(data, size))
    {
        xls_err(err, errcch, L"Not an Excel 97-2003 (.xls) workbook.");
        return FALSE;
    }
    shift = rd16(data + 0x1E);
    if (shift != 9 && shift != 12)
    {
        goto bad;
    }
    c->ssz = 1u << shift;
    max_sectors = (uint64_t)size / c->ssz + 1u;
    nfatsec = rd32(data + 0x2C);
    first_dir = rd32(data + 0x30);
    c->mini_cutoff = rd32(data + 0x38);
    first_mini = rd32(data + 0x3C);
    nmini = rd32(data + 0x40);
    first_difat = rd32(data + 0x44);
    if (nfatsec == 0 || nfatsec > max_sectors)
    {
        goto bad;
    }

    /* FAT sector list: 109 in the header, the rest in the DIFAT chain. */
    fatsecs = (uint32_t *)malloc((size_t)nfatsec * sizeof(uint32_t));
    if (fatsecs == NULL)
    {
        goto oom;
    }
    for (i = 0; i < 109 && nfs < nfatsec; i++)
    {
        fatsecs[nfs++] = rd32(data + 0x4C + 4 * i);
    }
    per = c->ssz / 4 - 1;
    {
        uint32_t d = first_difat;
        uint64_t steps = 0;
        while (nfs < nfatsec && d != CFB_ENDOFCHAIN && d != CFB_FREESECT)
        {
            size_t avail = 0;
            const uint8_t *p = cfb_sector(c, d, &avail);
            uint32_t k;
            if (p == NULL || avail < c->ssz || ++steps > max_sectors)
            {
                goto bad;
            }
            for (k = 0; k < per && nfs < nfatsec; k++)
            {
                fatsecs[nfs++] = rd32(p + 4 * k);
            }
            d = rd32(p + 4 * per);
        }
    }
    if (nfs < nfatsec)
    {
        goto bad;
    }

    /* The FAT itself. */
    c->nfat = nfatsec * (c->ssz / 4);
    c->fat = (uint32_t *)malloc((size_t)c->nfat * sizeof(uint32_t));
    if (c->fat == NULL)
    {
        goto oom;
    }
    for (i = 0; i < nfatsec; i++)
    {
        size_t avail = 0;
        const uint8_t *p = cfb_sector(c, fatsecs[i], &avail);
        uint32_t k;
        if (p == NULL)
        {
            goto bad;
        }
        for (k = 0; k < c->ssz / 4; k++)
        {
            c->fat[i * (c->ssz / 4) + k] =
                ((size_t)k * 4 + 4 <= avail) ? rd32(p + 4 * k) : CFB_FREESECT;
        }
    }
    free(fatsecs);
    fatsecs = NULL;

    /* Directory. */
    if (!cfb_read_chain(c, first_dir, (size_t)-1, &c->dir, &dirlen) || dirlen < 128 ||
        c->dir[66] != 5)
    {
        goto bad;
    }
    c->ndir = (uint32_t)(dirlen / 128);

    /* Mini stream (the root entry's stream) and its allocation table; optional until a
     * small stream is read. */
    {
        uint32_t rstart = rd32(c->dir + 116);
        uint32_t rsize = rd32(c->dir + 120);
        if (rsize > 0 && rsize <= size)
        {
            if (!cfb_read_chain(c, rstart, rsize, &c->ministream, &c->ministream_size))
            {
                c->ministream = NULL;
                c->ministream_size = 0;
            }
        }
    }
    if (nmini > 0 && nmini <= max_sectors && first_mini != CFB_ENDOFCHAIN)
    {
        uint8_t *mf = NULL;
        size_t mfl = 0;
        if (cfb_read_chain(c, first_mini, (size_t)nmini * c->ssz, &mf, &mfl))
        {
            c->nminifat = (uint32_t)(mfl / 4);
            c->minifat = table_from_bytes(mf, c->nminifat);
            if (c->minifat == NULL)
            {
                c->nminifat = 0;
            }
            free(mf);
        }
    }
    return TRUE;

oom:
    free(fatsecs);
    cfb_free(c);
    xls_err(err, errcch, L"Out of memory reading the .xls file.");
    return FALSE;
bad:
    free(fatsecs);
    cfb_free(c);
    xls_err(err, errcch, k_damaged);
    return FALSE;
}

/* Whether directory entry @p e is named @p want (ASCII, case-insensitive). */
static BOOL cfb_name_is(const uint8_t *e, const char *want)
{
    size_t n = strlen(want);
    size_t k;
    if (rd16(e + 64) != (n + 1) * 2)
    {
        return FALSE;
    }
    for (k = 0; k < n; k++)
    {
        uint16_t ch = rd16(e + 2 * k);
        char w = want[k];
        if (ch >= 'A' && ch <= 'Z')
        {
            ch = (uint16_t)(ch - 'A' + 'a');
        }
        if (w >= 'A' && w <= 'Z')
        {
            w = (char)(w - 'A' + 'a');
        }
        if (ch != (uint16_t)(unsigned char)w)
        {
            return FALSE;
        }
    }
    return TRUE;
}

/* Directory index of the stream named @p want among the root storage's children, or -1.
 * The children form a binary tree (left / right sibling links), walked with an explicit
 * stack; each entry is visited once, so a damaged tree cannot loop. */
static int cfb_find_stream(const Cfb *c, const char *want)
{
    uint32_t *stack = (uint32_t *)malloc((size_t)c->ndir * sizeof(uint32_t));
    uint8_t *seen = (uint8_t *)calloc(c->ndir, 1);
    uint32_t sp = 0;
    uint32_t child = rd32(c->dir + 76);
    int found = -1;
    if (stack != NULL && seen != NULL && child < c->ndir)
    {
        stack[sp++] = child;
    }
    while (sp > 0 && found < 0)
    {
        uint32_t i = stack[--sp];
        const uint8_t *e = c->dir + (size_t)i * 128;
        uint32_t lr[2];
        int k;
        if (seen[i])
        {
            continue;
        }
        seen[i] = 1;
        if (e[66] == 2 && cfb_name_is(e, want))
        {
            found = (int)i;
        }
        lr[0] = rd32(e + 68);
        lr[1] = rd32(e + 72);
        for (k = 0; k < 2; k++)
        {
            if (lr[k] < c->ndir && !seen[lr[k]] && sp < c->ndir)
            {
                stack[sp++] = lr[k];
            }
        }
    }
    free(stack);
    free(seen);
    return found;
}

/* Read directory entry @p idx's stream into a new buffer. */
static BOOL cfb_read_stream(const Cfb *c, int idx, uint8_t **out, size_t *out_len)
{
    const uint8_t *e = c->dir + (size_t)idx * 128;
    uint32_t start = rd32(e + 116);
    uint32_t size = rd32(e + 120);
    if (c->ssz == 4096 && rd32(e + 124) != 0)
    {
        return FALSE; /* over 4 GB */
    }
    if ((size_t)size > c->size)
    {
        return FALSE;
    }
    if (size < c->mini_cutoff)
    {
        return c->ministream != NULL && c->minifat != NULL &&
               cfb_read_mini(c, start, size, out, out_len);
    }
    return cfb_read_chain(c, start, size, out, out_len);
}

/* -------------------------------------------------------------------------- */
/* BIFF8 record cursor (crosses CONTINUE records)                             */
/* -------------------------------------------------------------------------- */

typedef struct Rec
{
    const uint8_t *s; /* the Workbook stream */
    size_t len;
    size_t pos;       /* read position */
    size_t end;       /* end of the current record's (or CONTINUE's) data */
} Rec;

static void rec_at(Rec *r, const uint8_t *s, size_t len, size_t start, size_t end)
{
    r->s = s;
    r->len = len;
    r->pos = start;
    r->end = end;
}

/* The current record is used up: move into the next record if it is a CONTINUE. */
static BOOL rec_continue(Rec *r)
{
    size_t n;
    if (r->end + 4 > r->len || rd16(r->s + r->end) != BR_CONTINUE)
    {
        return FALSE;
    }
    n = rd16(r->s + r->end + 2);
    if (r->end + 4 + n > r->len)
    {
        return FALSE;
    }
    r->pos = r->end + 4;
    r->end = r->pos + n;
    return TRUE;
}

/* Read (or, with @p out NULL, skip) @p n plain bytes, which may straddle CONTINUEs. */
static BOOL rec_read(Rec *r, void *out, size_t n)
{
    uint8_t *o = (uint8_t *)out;
    while (n > 0)
    {
        size_t k;
        if (r->pos >= r->end)
        {
            if (!rec_continue(r))
            {
                return FALSE;
            }
            continue;
        }
        k = r->end - r->pos;
        if (k > n)
        {
            k = n;
        }
        if (o != NULL)
        {
            memcpy(o, r->s + r->pos, k); /* SAFE: k <= bytes left in the record and in out */
            o += k;
        }
        r->pos += k;
        n -= k;
    }
    return TRUE;
}

/* Convert @p n UTF-16 units to UTF-8 and append them. */
static BOOL append_utf16(Buf *out, const wchar_t *w, int n)
{
    char tmp[1024];
    int got;
    if (n <= 0)
    {
        return TRUE;
    }
    got = WideCharToMultiByte(CP_UTF8, 0, w, n, tmp, (int)sizeof(tmp), NULL, NULL);
    return got > 0 && buf_append(out, tmp, (size_t)got);
}

/* Append @p cch characters of a BIFF8 string at the cursor as UTF-8. @p high: 2 bytes per
 * character. A CONTINUE in the middle of the characters starts with its own option byte
 * (bit 0: 2 bytes per character for the rest). */
static BOOL rec_chars(Rec *r, uint32_t cch, BOOL high, Buf *out)
{
    wchar_t tmp[256];
    int nt = 0;
    while (cch > 0)
    {
        size_t avail;
        size_t k;
        size_t i;
        if (r->pos >= r->end)
        {
            if (!rec_continue(r))
            {
                return FALSE;
            }
            if (r->pos < r->end)
            {
                high = (r->s[r->pos++] & 1) != 0;
            }
            continue;
        }
        avail = (r->end - r->pos) / (high ? 2u : 1u);
        if (avail == 0)
        {
            return FALSE; /* half a character before the record ends */
        }
        k = (avail < cch) ? avail : cch;
        for (i = 0; i < k; i++)
        {
            tmp[nt++] = high ? (wchar_t)rd16(r->s + r->pos + 2 * i) : (wchar_t)r->s[r->pos + i];
            if (nt == (int)ARRAYSIZE(tmp))
            {
                /* keep a trailing high surrogate with its pair */
                int keep = (tmp[nt - 1] >= 0xD800 && tmp[nt - 1] <= 0xDBFF) ? 1 : 0;
                if (!append_utf16(out, tmp, nt - keep))
                {
                    return FALSE;
                }
                if (keep)
                {
                    tmp[0] = tmp[nt - 1];
                }
                nt = keep;
            }
        }
        r->pos += k * (high ? 2u : 1u);
        cch -= (uint32_t)k;
    }
    return append_utf16(out, tmp, nt);
}

/* Read an XLUnicodeString / XLUnicodeRichExtendedString (@p long_cch: a 2-byte character
 * count, else ShortXLUnicodeString's 1-byte count) and append its text as UTF-8; rich
 * text runs and phonetic data are skipped. */
static BOOL rec_xlstring(Rec *r, BOOL long_cch, Buf *out)
{
    uint8_t b[4];
    uint32_t cch;
    uint8_t flags;
    uint32_t runs = 0;
    uint32_t ext = 0;
    if (!rec_read(r, b, long_cch ? 2u : 1u))
    {
        return FALSE;
    }
    cch = long_cch ? rd16(b) : b[0];
    if (!rec_read(r, &flags, 1))
    {
        return FALSE;
    }
    if (flags & 0x08)
    {
        if (!rec_read(r, b, 2))
        {
            return FALSE;
        }
        runs = rd16(b);
    }
    if (flags & 0x04)
    {
        if (!rec_read(r, b, 4))
        {
            return FALSE;
        }
        ext = rd32(b);
    }
    if (!rec_chars(r, cch, (flags & 1) != 0, out))
    {
        return FALSE;
    }
    return rec_read(r, NULL, (size_t)runs * 4u + ext);
}

/* -------------------------------------------------------------------------- */
/* Number formats (same rules as the .xlsx reader)                            */
/* -------------------------------------------------------------------------- */

enum
{
    NF_GENERAL = 0,
    NF_DATE,
    NF_ZEROPAD
};

static uint8_t builtin_numfmt_kind(uint32_t id)
{
    if ((id >= 14 && id <= 22) || (id >= 27 && id <= 36) || (id >= 45 && id <= 47) ||
        (id >= 50 && id <= 58))
    {
        return NF_DATE;
    }
    return NF_GENERAL;
}

/* A format code: a date if it has a y / d token (outside quotes and [..] blocks); a
 * zero-pad if it is only '0' characters (a ZIP "00000"). */
static uint8_t classify_format_code(const char *code, size_t len, int *pad)
{
    size_t i;
    BOOL has_yd = FALSE;
    BOOL all_zero = (len > 0);
    for (i = 0; i < len; i++)
    {
        char ch = code[i];
        if (ch == '"')
        {
            all_zero = FALSE;
            for (i++; i < len && code[i] != '"'; i++)
            {
            }
            continue;
        }
        if (ch == '[')
        {
            all_zero = FALSE;
            for (; i < len && code[i] != ']'; i++)
            {
            }
            continue;
        }
        if (ch == '\\')
        {
            all_zero = FALSE;
            i++;
            continue;
        }
        if (ch == 'y' || ch == 'Y' || ch == 'd' || ch == 'D')
        {
            has_yd = TRUE;
        }
        if (ch != '0')
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

/* A number as the .xlsx reader writes it: an integer without ".0", else the shortest
 * decimal that reads back as the same double. */
static void fmt_general(double d, char *out, size_t cap)
{
    int prec;
    if (d >= -9007199254740992.0 && d <= 9007199254740992.0 && d == (double)(long long)d)
    {
        StringCchPrintfA(out, cap, "%lld", (long long)d);
        return;
    }
    for (prec = 15; prec <= 17; prec++)
    {
        StringCchPrintfA(out, cap, "%.*g", prec, d);
        if (strtod(out, NULL) == d)
        {
            return;
        }
    }
}

/* Format a number by its cell format (kind / pad): a date serial -> YYYY-MM-DD (the
 * time of day dropped, as the .xlsx reader does), a short integer under a zero-pad
 * format -> leading zeros, else General. */
static void fmt_number(double d, uint8_t kind, int pad, BOOL date1904, char *out, size_t cap)
{
    if (kind == NF_DATE && d >= 0.0 && d < 2958466.0)
    {
        int y, m, dd;
        long days = (long)(d + 1e-6) - (date1904 ? 24107L : 25569L); /* -> days since 1970-01-01 */
        civil_from_days(days, &y, &m, &dd);
        StringCchPrintfA(out, cap, "%04d-%02d-%02d", y, m, dd);
        return;
    }
    fmt_general(d, out, cap);
    if (kind == NF_ZEROPAD && d >= 0.0 && d == (double)(long long)d)
    {
        size_t n = strlen(out);
        if (pad > 0 && n < (size_t)pad && (size_t)pad < cap)
        {
            memmove(out + (pad - (int)n), out, n + 1);
            memset(out, '0', (size_t)pad - n);
        }
    }
}

/* RK: a compressed number (an integer or the top 30 bits of a double, optionally x100). */
static double rk_value(uint32_t rk)
{
    double v;
    if (rk & 2u)
    {
        v = (double)(int32_t)(rk & 0xFFFFFFFCu) / 4.0;
    }
    else
    {
        uint64_t bits = (uint64_t)(rk & 0xFFFFFFFCu) << 32;
        memcpy(&v, &bits, sizeof(v)); /* SAFE: same size */
    }
    return (rk & 1u) ? v / 100.0 : v;
}

static double ieee_double(const uint8_t *p)
{
    double v;
    uint64_t bits = (uint64_t)rd32(p) | ((uint64_t)rd32(p + 4) << 32);
    memcpy(&v, &bits, sizeof(v)); /* SAFE: same size */
    return v;
}

static const char *error_text(uint8_t code)
{
    switch (code)
    {
        case 0x00:
            return "#NULL!";
        case 0x07:
            return "#DIV/0!";
        case 0x0F:
            return "#VALUE!";
        case 0x17:
            return "#REF!";
        case 0x1D:
            return "#NAME?";
        case 0x24:
            return "#NUM!";
        case 0x2A:
            return "#N/A";
        default:
            return "#ERROR";
    }
}

/* -------------------------------------------------------------------------- */
/* Workbook globals                                                           */
/* -------------------------------------------------------------------------- */

typedef struct XlsFmt
{
    uint16_t id;
    uint8_t kind;
    uint8_t pad;
} XlsFmt;

typedef struct XlsSheet
{
    uint32_t pos; /* offset of the sheet's BOF in the Workbook stream */
    char name[192];
} XlsSheet;

typedef struct Globals
{
    BOOL date1904;
    Buf sst; /* shared strings, NUL-separated */
    size_t *sst_off;
    uint32_t nsst, sst_cap;
    uint16_t *xf_fmt; /* number format id per XF */
    uint32_t nxf, xf_cap;
    uint8_t *xf_kind; /* resolved per XF after the globals are read */
    uint8_t *xf_pad;
    XlsFmt *fmt;
    uint32_t nfmt, fmt_cap;
    XlsSheet *sheets; /* worksheets in workbook order (up to EE_XLSX_MAX_ALL_SHEETS) */
    int nsheets;
    uint32_t sheets_cap;
} Globals;

static void globals_free(Globals *g)
{
    buf_free(&g->sst);
    free(g->sst_off);
    free(g->xf_fmt);
    free(g->xf_kind);
    free(g->xf_pad);
    free(g->fmt);
    free(g->sheets);
    ZeroMemory(g, sizeof(*g));
}

/* Read the globals substream (from the stream's first BOF to its EOF). */
static BOOL parse_globals(const uint8_t *s, size_t len, Globals *g, wchar_t *err, size_t errcch)
{
    size_t pos = 0;
    BOOL first = TRUE;
    uint32_t k;

    while (pos + 4 <= len)
    {
        uint16_t id = rd16(s + pos);
        size_t n = rd16(s + pos + 2);
        const uint8_t *d = s + pos + 4;
        Rec r;
        if (pos + 4 + n > len)
        {
            break; /* truncated last record */
        }
        if (first)
        {
            if (id != BR_BOF || n < 4)
            {
                xls_err(err, errcch, L"The .xls file holds no Excel workbook.");
                return FALSE;
            }
            if (rd16(d) != 0x0600)
            {
                xls_err(
                    err,
                    errcch,
                    L"This .xls was saved in an Excel format older than Excel 97, which cannot be read; "
                    L"save it as .xlsx.");
                return FALSE;
            }
            first = FALSE;
        }
        else if (id == BR_EOF)
        {
            break;
        }
        else if (id == BR_FILEPASS)
        {
            xls_err(err, errcch, L"The .xls workbook is password-protected.");
            return FALSE;
        }
        else if (id == BR_DATEMODE && n >= 2)
        {
            g->date1904 = (rd16(d) == 1);
        }
        else if (id == BR_BOUNDSHEET && n >= 8 && g->nsheets < EE_XLSX_MAX_ALL_SHEETS)
        {
            if (d[5] == 0) /* a worksheet (not a chart, macro or VB module sheet) */
            {
                Buf nm = {0};
                XlsSheet *sh;
                if (!grow((void **)&g->sheets,
                          &g->sheets_cap,
                          (uint32_t)g->nsheets + 1,
                          sizeof(XlsSheet)))
                {
                    xls_err(err, errcch, L"Out of memory reading the .xls file.");
                    return FALSE;
                }
                sh = &g->sheets[g->nsheets];
                rec_at(&r, s, len, pos + 4 + 6, pos + 4 + n);
                sh->pos = rd32(d);
                sh->name[0] = '\0';
                if (rec_xlstring(&r, FALSE, &nm) && nm.p != NULL)
                {
                    StringCchCopyA(sh->name, ARRAYSIZE(sh->name), nm.p);
                }
                buf_free(&nm);
                g->nsheets++;
            }
        }
        else if (id == BR_XF && n >= 4)
        {
            if (!grow((void **)&g->xf_fmt, &g->xf_cap, g->nxf + 1, sizeof(uint16_t)))
            {
                goto oom;
            }
            g->xf_fmt[g->nxf++] = rd16(d + 2);
        }
        else if (id == BR_FORMAT && n >= 2)
        {
            Buf code = {0};
            int pad = 0;
            XlsFmt f;
            rec_at(&r, s, len, pos + 4 + 2, pos + 4 + n);
            f.id = rd16(d);
            f.kind = NF_GENERAL;
            if (rec_xlstring(&r, TRUE, &code))
            {
                f.kind = classify_format_code(code.p != NULL ? code.p : "", code.len, &pad);
            }
            buf_free(&code);
            f.pad = (uint8_t)(pad > 255 ? 255 : pad);
            if (!grow((void **)&g->fmt, &g->fmt_cap, g->nfmt + 1, sizeof(XlsFmt)))
            {
                goto oom;
            }
            g->fmt[g->nfmt++] = f;
        }
        else if (id == BR_SST && n >= 8)
        {
            uint32_t unique = rd32(d + 4);
            uint32_t i;
            rec_at(&r, s, len, pos + 4 + 8, pos + 4 + n);
            for (i = 0; i < unique; i++)
            {
                size_t off = g->sst.len;
                if (!rec_xlstring(&r, TRUE, &g->sst))
                {
                    break; /* data ends early: keep what was read */
                }
                if (!buf_append_nul(&g->sst) ||
                    !grow((void **)&g->sst_off, &g->sst_cap, g->nsst + 1, sizeof(size_t)))
                {
                    goto oom;
                }
                g->sst_off[g->nsst++] = off;
            }
        }
        pos += 4 + n;
    }
    if (first)
    {
        xls_err(err, errcch, L"The .xls file holds no Excel workbook.");
        return FALSE;
    }

    /* Resolve each XF's number format: a FORMAT record (custom, or a redefined built-in)
     * wins over the built-in table. */
    g->xf_kind = (uint8_t *)calloc(g->nxf ? g->nxf : 1, 1);
    g->xf_pad = (uint8_t *)calloc(g->nxf ? g->nxf : 1, 1);
    if (g->xf_kind == NULL || g->xf_pad == NULL)
    {
        goto oom;
    }
    for (k = 0; k < g->nxf; k++)
    {
        uint32_t f;
        uint8_t kind = builtin_numfmt_kind(g->xf_fmt[k]);
        uint8_t pad = 0;
        for (f = 0; f < g->nfmt; f++)
        {
            if (g->fmt[f].id == g->xf_fmt[k])
            {
                kind = g->fmt[f].kind;
                pad = g->fmt[f].pad;
            }
        }
        g->xf_kind[k] = kind;
        g->xf_pad[k] = pad;
    }
    return TRUE;

oom:
    xls_err(err, errcch, L"Out of memory reading the .xls file.");
    return FALSE;
}

/* Open the compound file, read the Workbook stream and its globals. On success the
 * caller frees *stream and the globals. */
static BOOL open_workbook(const void *data,
                          size_t size,
                          uint8_t **stream,
                          size_t *stream_len,
                          Globals *g,
                          wchar_t *err,
                          size_t errcch)
{
    Cfb cfb;
    int idx;
    BOOL ok;
    *stream = NULL;
    *stream_len = 0;
    ZeroMemory(g, sizeof(*g));
    if (!cfb_open(&cfb, (const uint8_t *)data, size, err, errcch))
    {
        return FALSE;
    }
    idx = cfb_find_stream(&cfb, "Workbook");
    if (idx < 0)
    {
        xls_err(
            err,
            errcch,
            cfb_find_stream(&cfb, "Book") >= 0
                ? L"This .xls was saved in an Excel format older than Excel 97, which cannot be read; "
                  L"save it as .xlsx."
                : L"The .xls file holds no Excel workbook.");
        cfb_free(&cfb);
        return FALSE;
    }
    ok = cfb_read_stream(&cfb, idx, stream, stream_len);
    cfb_free(&cfb);
    if (!ok)
    {
        xls_err(err, errcch, L"The .xls file is damaged (its workbook stream could not be read).");
        return FALSE;
    }
    if (!parse_globals(*stream, *stream_len, g, err, errcch))
    {
        free(*stream);
        *stream = NULL;
        globals_free(g);
        return FALSE;
    }
    return TRUE;
}

/* -------------------------------------------------------------------------- */
/* Worksheet cells                                                            */
/* -------------------------------------------------------------------------- */

typedef struct XlsCell
{
    uint32_t row;
    uint32_t col;
    uint32_t seq;    /* arrival order: a later record for the same cell wins */
    uint32_t in_sst; /* text is in the shared string pool, else in the cell pool */
    size_t off;
} XlsCell;

typedef struct SheetCells
{
    XlsCell *c;
    uint32_t n, cap;
    Buf pool;
} SheetCells;

static BOOL add_text(SheetCells *sc, uint32_t row, uint32_t col, const char *txt, size_t n)
{
    XlsCell *c;
    if (col >= XLS_MAX_COLS)
    {
        return TRUE;
    }
    if (!grow((void **)&sc->c, &sc->cap, sc->n + 1, sizeof(XlsCell)))
    {
        return FALSE;
    }
    c = &sc->c[sc->n];
    c->row = row;
    c->col = col;
    c->seq = sc->n;
    c->in_sst = 0;
    c->off = sc->pool.len;
    if (!buf_append(&sc->pool, txt, n) || !buf_append_nul(&sc->pool))
    {
        return FALSE;
    }
    sc->n++;
    return TRUE;
}

static BOOL add_sst(SheetCells *sc, const Globals *g, uint32_t row, uint32_t col, uint32_t isst)
{
    XlsCell *c;
    if (col >= XLS_MAX_COLS || isst >= g->nsst)
    {
        return TRUE;
    }
    if (!grow((void **)&sc->c, &sc->cap, sc->n + 1, sizeof(XlsCell)))
    {
        return FALSE;
    }
    c = &sc->c[sc->n];
    c->row = row;
    c->col = col;
    c->seq = sc->n;
    c->in_sst = 1;
    c->off = g->sst_off[isst];
    sc->n++;
    return TRUE;
}

static BOOL add_number(SheetCells *sc,
                       const Globals *g,
                       uint32_t row,
                       uint32_t col,
                       uint32_t xf,
                       double v)
{
    char out[64];
    uint8_t kind = (xf < g->nxf) ? g->xf_kind[xf] : NF_GENERAL;
    int pad = (xf < g->nxf) ? g->xf_pad[xf] : 0;
    fmt_number(v, kind, pad, g->date1904, out, sizeof(out));
    return add_text(sc, row, col, out, strlen(out));
}

/* Collect one worksheet's cells (from its BOF to the matching EOF; embedded chart
 * substreams nest their own BOF / EOF and are skipped). */
static EeLoadStatus read_cells(const uint8_t *s,
                               size_t len,
                               size_t pos,
                               const Globals *g,
                               SheetCells *sc,
                               volatile LONG *cancel,
                               wchar_t *err,
                               size_t errcch)
{
    int depth = 0;
    uint32_t nrec = 0;
    BOOL pending = FALSE; /* a FORMULA whose string result follows in a STRING record */
    uint32_t prow = 0, pcol = 0;

    while (pos + 4 <= len)
    {
        uint16_t id = rd16(s + pos);
        size_t n = rd16(s + pos + 2);
        const uint8_t *d = s + pos + 4;
        BOOL ok = TRUE;
        Rec r;
        if (pos + 4 + n > len)
        {
            break;
        }
        if ((++nrec & 0xFFFF) == 0 && cancel != NULL && *cancel != 0)
        {
            return EeLoadStatus_Cancelled;
        }
        if (id == BR_BOF)
        {
            depth++;
        }
        else if (id == BR_EOF)
        {
            if (--depth <= 0)
            {
                break;
            }
        }
        else if (depth != 1)
        {
            /* inside an embedded chart */
        }
        else if (id == BR_LABELSST && n >= 10)
        {
            ok = add_sst(sc, g, rd16(d), rd16(d + 2), rd32(d + 6));
        }
        else if ((id == BR_LABEL || id == BR_RSTRING) && n >= 8)
        {
            Buf t = {0};
            rec_at(&r, s, len, pos + 4 + 6, pos + 4 + n);
            if (rec_xlstring(&r, TRUE, &t))
            {
                ok = add_text(sc, rd16(d), rd16(d + 2), t.p != NULL ? t.p : "", t.len);
            }
            buf_free(&t);
        }
        else if (id == BR_NUMBER && n >= 14)
        {
            ok = add_number(sc, g, rd16(d), rd16(d + 2), rd16(d + 4), ieee_double(d + 6));
        }
        else if (id == BR_RK && n >= 10)
        {
            ok = add_number(sc, g, rd16(d), rd16(d + 2), rd16(d + 4), rk_value(rd32(d + 6)));
        }
        else if (id == BR_MULRK && n >= 6)
        {
            uint32_t row = rd16(d);
            uint32_t col = rd16(d + 2);
            size_t i;
            for (i = 0; ok && 4 + 6 * i + 6 <= n - 2; i++)
            {
                ok = add_number(sc,
                                g,
                                row,
                                col + (uint32_t)i,
                                rd16(d + 4 + 6 * i),
                                rk_value(rd32(d + 6 + 6 * i)));
            }
        }
        else if (id == BR_BOOLERR && n >= 8)
        {
            const char *t = d[7] ? error_text(d[6]) : (d[6] ? "TRUE" : "FALSE");
            ok = add_text(sc, rd16(d), rd16(d + 2), t, strlen(t));
        }
        else if (id == BR_FORMULA && n >= 20)
        {
            const uint8_t *v = d + 6; /* the cached result */
            pending = FALSE;
            if (v[6] == 0xFF && v[7] == 0xFF)
            {
                if (v[0] == 0)
                {
                    pending = TRUE;
                    prow = rd16(d);
                    pcol = rd16(d + 2);
                }
                else if (v[0] == 1 || v[0] == 2)
                {
                    const char *t = (v[0] == 2) ? error_text(v[2]) : (v[2] ? "TRUE" : "FALSE");
                    ok = add_text(sc, rd16(d), rd16(d + 2), t, strlen(t));
                }
            }
            else
            {
                ok = add_number(sc, g, rd16(d), rd16(d + 2), rd16(d + 4), ieee_double(v));
            }
        }
        else if (id == BR_STRING && pending)
        {
            Buf t = {0};
            rec_at(&r, s, len, pos + 4, pos + 4 + n);
            if (rec_xlstring(&r, TRUE, &t))
            {
                ok = add_text(sc, prow, pcol, t.p != NULL ? t.p : "", t.len);
            }
            buf_free(&t);
            pending = FALSE;
        }
        if (!ok)
        {
            xls_err(err, errcch, L"Out of memory reading the .xls file.");
            return EeLoadStatus_Error;
        }
        pos += 4 + n;
    }
    return EeLoadStatus_Ok;
}

static int __cdecl cell_cmp(const void *a, const void *b)
{
    const XlsCell *x = (const XlsCell *)a;
    const XlsCell *y = (const XlsCell *)b;
    if (x->row != y->row)
        return x->row < y->row ? -1 : 1;
    if (x->col != y->col)
        return x->col < y->col ? -1 : 1;
    return x->seq < y->seq ? -1 : (x->seq > y->seq);
}

/* -------------------------------------------------------------------------- */
/* Public API                                                                 */
/* -------------------------------------------------------------------------- */

BOOL EeXls_IsCompoundFile(const void *data, size_t size)
{
    static const uint8_t k_sig[8] = {0xD0, 0xCF, 0x11, 0xE0, 0xA1, 0xB1, 0x1A, 0xE1};
    return data != NULL && size >= sizeof(k_sig) && memcmp(data, k_sig, sizeof(k_sig)) == 0;
}

EeLoadStatus EeXls_ListSheetsMem(const void *data,
                                 size_t size,
                                 wchar_t names[][EE_XLSX_SHEET_NAME_CCH],
                                 int max_sheets,
                                 int *out_count,
                                 wchar_t *error_message,
                                 size_t error_cch)
{
    uint8_t *stream = NULL;
    size_t stream_len = 0;
    Globals g;
    int i;

    if (out_count != NULL)
    {
        *out_count = 0;
    }
    if (data == NULL || size == 0 || names == NULL || out_count == NULL || max_sheets <= 0)
    {
        xls_err(error_message, error_cch, L"Invalid arguments.");
        return EeLoadStatus_Error;
    }
    if (!open_workbook(data, size, &stream, &stream_len, &g, error_message, error_cch))
    {
        return EeLoadStatus_Error;
    }
    if (g.nsheets == 0)
    {
        xls_err(error_message, error_cch, L"No worksheets found.");
        free(stream);
        globals_free(&g);
        return EeLoadStatus_Error;
    }
    for (i = 0; i < g.nsheets && i < max_sheets; i++)
    {
        if (MultiByteToWideChar(CP_UTF8,
                                0,
                                g.sheets[i].name,
                                -1,
                                names[i],
                                EE_XLSX_SHEET_NAME_CCH) == 0)
        {
            names[i][0] = L'\0';
        }
    }
    *out_count = i;
    free(stream);
    globals_free(&g);
    return EeLoadStatus_Ok;
}

/* Read worksheet @p sheet_index of an open workbook and stream its rows to @p sink. */
static EeLoadStatus emit_sheet(const uint8_t *stream,
                               size_t stream_len,
                               const Globals *g,
                               int sheet_index,
                               EeXlsxRowSink sink,
                               void *sink_ctx,
                               volatile LONG *cancel_flag,
                               EeLoadProgressFn progress_fn,
                               void *progress_user,
                               wchar_t *error_message,
                               size_t error_cch)
{
    SheetCells sc = {0};
    const char **row_cells = NULL;
    EeLoadStatus status = EeLoadStatus_Error;
    uint32_t i;
    uint32_t rows = 0;
    uint32_t last_pct = 0;

    if (g->sheets[sheet_index].pos >= stream_len)
    {
        xls_err(error_message,
                error_cch,
                L"The .xls file is damaged (a worksheet could not be found).");
        goto cleanup;
    }
    status = read_cells(stream,
                        stream_len,
                        g->sheets[sheet_index].pos,
                        g,
                        &sc,
                        cancel_flag,
                        error_message,
                        error_cch);
    if (status != EeLoadStatus_Ok)
    {
        goto cleanup;
    }
    if (sc.n > 1)
    {
        qsort(sc.c, sc.n, sizeof(XlsCell), cell_cmp);
    }
    row_cells = (const char **)malloc(XLS_MAX_COLS * sizeof(char *));
    if (row_cells == NULL)
    {
        xls_err(error_message, error_cch, L"Out of memory reading the .xls file.");
        status = EeLoadStatus_Error;
        goto cleanup;
    }

    /* Emit row by row: a row's width is its last cell + 1, gaps are "". */
    for (i = 0; i < sc.n;)
    {
        uint32_t row = sc.c[i].row;
        uint32_t width = 0;
        uint32_t j;
        for (j = i; j < sc.n && sc.c[j].row == row; j++)
        {
            const XlsCell *c = &sc.c[j];
            while (width <= c->col)
            {
                row_cells[width++] = "";
            }
            row_cells[c->col] = c->in_sst ? g->sst.p + c->off : sc.pool.p + c->off; /* later wins */
        }
        i = j;
        if (!sink(sink_ctx, row_cells, width))
        {
            status = EeLoadStatus_Cancelled;
            goto cleanup;
        }
        rows++;
        if ((rows & 0x3FF) == 0)
        {
            if (cancel_flag != NULL && *cancel_flag != 0)
            {
                status = EeLoadStatus_Cancelled;
                goto cleanup;
            }
            if (progress_fn != NULL)
            {
                uint32_t pct = (uint32_t)(((uint64_t)i * 99u) / sc.n);
                if (pct != last_pct)
                {
                    EeLoadProgress pr;
                    last_pct = pct;
                    pr.percent = pct;
                    pr.rows_loaded = rows;
                    pr.bytes_read = (uint64_t)i;
                    pr.bytes_total = sc.n;
                    if (!progress_fn(&pr, progress_user) && cancel_flag != NULL)
                    {
                        InterlockedExchange(cancel_flag, 1);
                    }
                }
            }
        }
    }
    status = EeLoadStatus_Ok;

cleanup:
    free(row_cells);
    free(sc.c);
    buf_free(&sc.pool);
    return status;
}

EeLoadStatus EeXls_ReadSheetMem(const void *data,
                                size_t size,
                                int sheet_index,
                                EeXlsxRowSink sink,
                                void *sink_ctx,
                                volatile LONG *cancel_flag,
                                EeLoadProgressFn progress_fn,
                                void *progress_user,
                                wchar_t *error_message,
                                size_t error_cch)
{
    uint8_t *stream = NULL;
    size_t stream_len = 0;
    Globals g;
    EeLoadStatus status = EeLoadStatus_Error;

    if (data == NULL || size == 0 || sink == NULL || sheet_index < 0)
    {
        xls_err(error_message, error_cch, L"Invalid arguments.");
        return EeLoadStatus_Error;
    }
    if (!open_workbook(data, size, &stream, &stream_len, &g, error_message, error_cch))
    {
        return EeLoadStatus_Error;
    }
    if (sheet_index >= g.nsheets)
    {
        xls_err(error_message, error_cch, L"Worksheet index out of range.");
    }
    else
    {
        status = emit_sheet(stream, stream_len, &g, sheet_index, sink, sink_ctx, cancel_flag,
                            progress_fn, progress_user, error_message, error_cch);
    }
    free(stream);
    globals_free(&g);
    return status;
}

EeLoadStatus EeXls_ReadAllSheetsMem(const void *data,
                                    size_t size,
                                    EeXlsxSheetFn sheet_fn,
                                    EeXlsxRowSink sink,
                                    void *sink_ctx,
                                    volatile LONG *cancel_flag,
                                    wchar_t *error_message,
                                    size_t error_cch)
{
    uint8_t *stream = NULL;
    size_t stream_len = 0;
    Globals g;
    EeLoadStatus status = EeLoadStatus_Ok;
    int i;

    if (data == NULL || size == 0 || sheet_fn == NULL || sink == NULL)
    {
        xls_err(error_message, error_cch, L"Invalid arguments.");
        return EeLoadStatus_Error;
    }
    if (!open_workbook(data, size, &stream, &stream_len, &g, error_message, error_cch))
    {
        return EeLoadStatus_Error;
    }
    for (i = 0; i < g.nsheets && status == EeLoadStatus_Ok; i++)
    {
        if (!sheet_fn(sink_ctx, i, g.sheets[i].name))
        {
            status = EeLoadStatus_Cancelled;
            break;
        }
        status = emit_sheet(stream, stream_len, &g, i, sink, sink_ctx, cancel_flag, NULL, NULL,
                            error_message, error_cch);
    }
    free(stream);
    globals_free(&g);
    return status;
}
