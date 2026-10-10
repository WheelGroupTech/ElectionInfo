/**
 * @file pdf_reader.c
 * @brief Minimal read-only PDF text extractor. See pdf_reader.h.
 *
 * Layout:
 *   - a bump arena for parsed objects (reset per page / per font / per tree node);
 *   - a lexer + recursive object parser over an in-memory byte range;
 *   - a 256 KB read window over the file (objects of a page sit close together in
 *     report PDFs, so most object loads are memory hits);
 *   - cross-reference loading (classic tables and xref streams, /Prev chains) with
 *     two repairs: a negative `startxref` (a 32-bit writer overflowed) is taken modulo
 *     2^32, and any object/section offset that does not land on its header is retried
 *     at +4 GiB multiples (32-bit offsets wrapped in files larger than 4 GiB). If the
 *     cross-reference data is unusable the object table is rebuilt by scanning;
 *   - FlateDecode (vendored miniz) with PNG predictors;
 *   - fonts: simple fonts map bytes through WinAnsi (+ /Differences), Type0 fonts
 *     read 2-byte codes; a ToUnicode CMap overrides either;
 *   - a content-stream interpreter tracking the CTM, clip rectangles and the text
 *     matrix, emitting one run per show operator.
 */

#include "pdf_reader.h"

#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strsafe.h>

#include "third_party/miniz/miniz.h"

#define PDF_WIN_SIZE      (256u * 1024u)
#define PDF_ARENA_CHUNK   (64u * 1024u)
#define PDF_MAX_OBJ_BYTES (512u * 1024u * 1024u) /* cap one parsed object's window */
#define PDF_4GIB          (0x100000000ull)
#define PDF_GSTACK_MAX    64

/* -------------------------------------------------------------------------- */
/* Growable byte buffer                                                       */
/* -------------------------------------------------------------------------- */

typedef struct ByteBuf
{
    unsigned char *p;
    size_t len;
    size_t cap;
} ByteBuf;

static BOOL bb_reserve(ByteBuf *b, size_t need)
{
    size_t nc;
    unsigned char *np;
    if (need <= b->cap)
    {
        return TRUE;
    }
    nc = b->cap ? b->cap : 4096;
    while (nc < need)
    {
        if (nc > ((size_t)-1) / 2)
        {
            return FALSE;
        }
        nc *= 2;
    }
    np = (unsigned char *)realloc(b->p, nc);
    if (np == NULL)
    {
        return FALSE;
    }
    b->p = np;
    b->cap = nc;
    return TRUE;
}

static BOOL bb_append(ByteBuf *b, const void *src, size_t n)
{
    if (!bb_reserve(b, b->len + n + 1))
    {
        return FALSE;
    }
    if (n > 0)
    {
        memcpy(b->p + b->len, src, n);
    }
    b->len += n;
    b->p[b->len] = 0;
    return TRUE;
}

static void bb_free(ByteBuf *b)
{
    free(b->p);
    b->p = NULL;
    b->len = b->cap = 0;
}

/* -------------------------------------------------------------------------- */
/* Arena                                                                      */
/* -------------------------------------------------------------------------- */

typedef struct ArenaChunk
{
    struct ArenaChunk *next;
    size_t used;
    size_t cap;
    unsigned char data[];
} ArenaChunk;

typedef struct Arena
{
    ArenaChunk *head;
} Arena;

static void *arena_alloc(Arena *a, size_t n)
{
    void *p;
    n = (n + 7u) & ~(size_t)7u;
    if (a->head == NULL || a->head->used + n > a->head->cap)
    {
        size_t cap = (n > PDF_ARENA_CHUNK) ? n : PDF_ARENA_CHUNK;
        ArenaChunk *c = (ArenaChunk *)malloc(offsetof(ArenaChunk, data) + cap);
        if (c == NULL)
        {
            return NULL;
        }
        c->next = a->head;
        c->used = 0;
        c->cap = cap;
        a->head = c;
    }
    p = a->head->data + a->head->used;
    a->head->used += n;
    return p;
}

/* Free everything except one standard-size chunk, which is kept for reuse. */
static void arena_reset(Arena *a)
{
    ArenaChunk *keep = NULL;
    ArenaChunk *c = a->head;
    while (c != NULL)
    {
        ArenaChunk *next = c->next;
        if (keep == NULL && c->cap == PDF_ARENA_CHUNK)
        {
            keep = c;
            keep->used = 0;
            keep->next = NULL;
        }
        else
        {
            free(c);
        }
        c = next;
    }
    a->head = keep;
}

static void arena_free(Arena *a)
{
    ArenaChunk *c = a->head;
    while (c != NULL)
    {
        ArenaChunk *next = c->next;
        free(c);
        c = next;
    }
    a->head = NULL;
}

/* -------------------------------------------------------------------------- */
/* Objects                                                                    */
/* -------------------------------------------------------------------------- */

typedef enum PoType
{
    PO_NULL = 0,
    PO_BOOL,
    PO_INT,
    PO_REAL,
    PO_STR,
    PO_NAME,
    PO_ARRAY,
    PO_DICT,
    PO_REF,
    PO_KEYWORD
} PoType;

typedef struct PdfObj
{
    PoType t;
    uint32_t n; /* str/name/keyword: byte length; array: item count; dict: pair count */
    union
    {
        int64_t i;
        double r;
        const unsigned char *s; /* NUL-terminated copy (str/name/keyword) */
        struct PdfObj *items;   /* array items, or dict key/value pairs (2n) */
        struct
        {
            uint32_t num;
            uint32_t gen;
        } ref;
    } u;
} PdfObj;

static double obj_num(const PdfObj *o)
{
    if (o == NULL)
    {
        return 0.0;
    }
    if (o->t == PO_INT)
    {
        return (double)o->u.i;
    }
    if (o->t == PO_REAL)
    {
        return o->u.r;
    }
    return 0.0;
}

static BOOL obj_is_name(const PdfObj *o, const char *name)
{
    return o != NULL && o->t == PO_NAME && strcmp((const char *)o->u.s, name) == 0;
}

static const PdfObj *dict_get(const PdfObj *d, const char *key)
{
    uint32_t i;
    if (d == NULL || d->t != PO_DICT)
    {
        return NULL;
    }
    for (i = 0; i < d->n; i++)
    {
        const PdfObj *k = &d->u.items[2 * i];
        if (k->t == PO_NAME && strcmp((const char *)k->u.s, key) == 0)
        {
            return &d->u.items[2 * i + 1];
        }
    }
    return NULL;
}

/* -------------------------------------------------------------------------- */
/* Lexer / parser                                                             */
/* -------------------------------------------------------------------------- */

typedef struct Lex
{
    const unsigned char *p;
    const unsigned char *end;
    int truncated; /* ran off the end of the buffer mid-object */
} Lex;

static int is_ws(unsigned char c)
{
    return c == 0 || c == 9 || c == 10 || c == 12 || c == 13 || c == 32;
}

static int is_delim(unsigned char c)
{
    return c == '(' || c == ')' || c == '<' || c == '>' || c == '[' || c == ']' || c == '{' ||
           c == '}' || c == '/' || c == '%';
}

static void lex_skip_ws(Lex *lx)
{
    while (lx->p < lx->end)
    {
        unsigned char c = *lx->p;
        if (is_ws(c))
        {
            lx->p++;
        }
        else if (c == '%')
        {
            while (lx->p < lx->end && *lx->p != '\r' && *lx->p != '\n')
            {
                lx->p++;
            }
        }
        else
        {
            break;
        }
    }
}

static int hexval(unsigned char c)
{
    if (c >= '0' && c <= '9')
        return c - '0';
    if (c >= 'a' && c <= 'f')
        return c - 'a' + 10;
    if (c >= 'A' && c <= 'F')
        return c - 'A' + 10;
    return -1;
}

static unsigned char *arena_copy(Arena *a, const unsigned char *src, size_t n)
{
    unsigned char *d = (unsigned char *)arena_alloc(a, n + 1);
    if (d == NULL)
    {
        return NULL;
    }
    if (n > 0)
    {
        memcpy(d, src, n);
    }
    d[n] = 0;
    return d;
}

/* Parse an unsigned decimal integer at lx->p (no sign); FALSE if none. */
static BOOL lex_uint(Lex *lx, uint64_t *out)
{
    uint64_t v = 0;
    const unsigned char *s = lx->p;
    while (lx->p < lx->end && *lx->p >= '0' && *lx->p <= '9')
    {
        v = v * 10u + (uint64_t)(*lx->p - '0');
        lx->p++;
    }
    if (lx->p == s)
    {
        return FALSE;
    }
    *out = v;
    return TRUE;
}

static BOOL parse_obj(Lex *lx, Arena *a, PdfObj *out, int depth);

/* Literal string "(...)": the opening '(' is at lx->p. Decoded into the arena (the
 * decoded form is never longer than the raw span). */
static BOOL parse_literal(Lex *lx, Arena *a, PdfObj *out)
{
    const unsigned char *p = lx->p + 1;
    const unsigned char *q = p;
    unsigned char *d;
    size_t o = 0;
    int level = 1;
    /* pass 1: find the closing parenthesis */
    while (q < lx->end)
    {
        unsigned char c = *q++;
        if (c == '\\')
        {
            if (q < lx->end)
            {
                q++;
            }
        }
        else if (c == '(')
        {
            level++;
        }
        else if (c == ')' && --level == 0)
        {
            break;
        }
    }
    if (level != 0)
    {
        lx->truncated = 1;
        return FALSE;
    }
    d = (unsigned char *)arena_alloc(a, (size_t)(q - p) + 1);
    if (d == NULL)
    {
        return FALSE;
    }
    /* pass 2: decode escapes; q - 1 is the closing ')' */
    while (p < q - 1)
    {
        unsigned char c = *p++;
        if (c == '\\' && p < q - 1)
        {
            unsigned char e = *p++;
            switch (e)
            {
                case 'n':
                    c = '\n';
                    break;
                case 'r':
                    c = '\r';
                    break;
                case 't':
                    c = '\t';
                    break;
                case 'b':
                    c = '\b';
                    break;
                case 'f':
                    c = '\f';
                    break;
                case '\r':
                    if (p < q - 1 && *p == '\n')
                    {
                        p++;
                    }
                    continue;
                case '\n':
                    continue;
                default:
                    if (e >= '0' && e <= '7')
                    {
                        int v = e - '0';
                        int k;
                        for (k = 0; k < 2 && p < q - 1 && *p >= '0' && *p <= '7'; k++)
                        {
                            v = v * 8 + (*p++ - '0');
                        }
                        c = (unsigned char)(v & 0xFF);
                    }
                    else
                    {
                        c = e; /* \( \) \\ and unknown escapes */
                    }
                    break;
            }
        }
        d[o++] = c;
    }
    d[o] = 0;
    out->t = PO_STR;
    out->n = (uint32_t)o;
    out->u.s = d;
    lx->p = q;
    return TRUE;
}

/* Hex string "<...>": the opening '<' is at lx->p. */
static BOOL parse_hex(Lex *lx, Arena *a, PdfObj *out)
{
    const unsigned char *p = lx->p + 1;
    const unsigned char *q;
    size_t ndig = 0;
    unsigned char *d;
    size_t o = 0;
    int hi = -1;
    for (q = p; q < lx->end && *q != '>'; q++)
    {
        if (hexval(*q) >= 0)
        {
            ndig++;
        }
    }
    if (q >= lx->end)
    {
        lx->truncated = 1;
        return FALSE;
    }
    d = (unsigned char *)arena_alloc(a, ndig / 2 + 2);
    if (d == NULL)
    {
        return FALSE;
    }
    for (; p < q; p++)
    {
        int v = hexval(*p);
        if (v < 0)
        {
            continue;
        }
        if (hi < 0)
        {
            hi = v;
        }
        else
        {
            d[o++] = (unsigned char)(hi * 16 + v);
            hi = -1;
        }
    }
    if (hi >= 0)
    {
        d[o++] = (unsigned char)(hi * 16); /* odd digit count: trailing 0 */
    }
    d[o] = 0;
    out->t = PO_STR;
    out->n = (uint32_t)o;
    out->u.s = d;
    lx->p = q + 1;
    return TRUE;
}

/* Parse items until @p close (']' or '>>'); dict pairs are stored flat. */
static BOOL parse_seq(Lex *lx, Arena *a, PdfObj *out, int is_dict, int depth)
{
    PdfObj local[24];
    PdfObj *items = local;
    uint32_t n = 0, cap = (uint32_t)ARRAYSIZE(local);
    BOOL ok = TRUE;
    for (;;)
    {
        lex_skip_ws(lx);
        if (lx->p >= lx->end)
        {
            lx->truncated = 1;
            ok = FALSE;
            break;
        }
        if (!is_dict && *lx->p == ']')
        {
            lx->p++;
            break;
        }
        if (is_dict && *lx->p == '>')
        {
            if (lx->p + 1 >= lx->end)
            {
                lx->truncated = 1;
                ok = FALSE;
                break;
            }
            if (lx->p[1] == '>')
            {
                lx->p += 2;
                break;
            }
        }
        if (n == cap)
        {
            uint32_t nc = cap * 2;
            PdfObj *ni = (PdfObj *)malloc((size_t)nc * sizeof(PdfObj));
            if (ni == NULL)
            {
                ok = FALSE;
                break;
            }
            memcpy(ni, items, (size_t)n * sizeof(PdfObj));
            if (items != local)
            {
                free(items);
            }
            items = ni;
            cap = nc;
        }
        if (!parse_obj(lx, a, &items[n], depth + 1))
        {
            ok = FALSE;
            break;
        }
        n++;
    }
    if (ok)
    {
        if (is_dict && (n & 1u))
        {
            n--; /* dangling key: drop it */
        }
        out->t = is_dict ? PO_DICT : PO_ARRAY;
        out->n = is_dict ? n / 2 : n;
        out->u.items = (PdfObj *)arena_alloc(a, (size_t)(n ? n : 1) * sizeof(PdfObj));
        if (out->u.items == NULL)
        {
            ok = FALSE;
        }
        else if (n > 0)
        {
            memcpy(out->u.items, items, (size_t)n * sizeof(PdfObj));
        }
    }
    if (items != local)
    {
        free(items);
    }
    return ok;
}

static BOOL parse_obj(Lex *lx, Arena *a, PdfObj *out, int depth)
{
    unsigned char c;
    ZeroMemory(out, sizeof(*out));
    if (depth > 64)
    {
        return FALSE;
    }
    lex_skip_ws(lx);
    if (lx->p >= lx->end)
    {
        lx->truncated = 1;
        return FALSE;
    }
    c = *lx->p;
    if (c == '/')
    {
        const unsigned char *p = lx->p + 1;
        const unsigned char *q = p;
        unsigned char *d;
        size_t o = 0;
        while (q < lx->end && !is_ws(*q) && !is_delim(*q))
        {
            q++;
        }
        d = (unsigned char *)arena_alloc(a, (size_t)(q - p) + 1);
        if (d == NULL)
        {
            return FALSE;
        }
        while (p < q)
        {
            unsigned char ch = *p++;
            if (ch == '#' && p + 1 < q && hexval(p[0]) >= 0 && hexval(p[1]) >= 0)
            {
                ch = (unsigned char)(hexval(p[0]) * 16 + hexval(p[1]));
                p += 2;
            }
            d[o++] = ch;
        }
        d[o] = 0;
        out->t = PO_NAME;
        out->n = (uint32_t)o;
        out->u.s = d;
        lx->p = q;
        return TRUE;
    }
    if (c == '(')
    {
        return parse_literal(lx, a, out);
    }
    if (c == '<')
    {
        if (lx->p + 1 >= lx->end)
        {
            lx->truncated = 1;
            return FALSE;
        }
        if (lx->p[1] == '<')
        {
            lx->p += 2;
            return parse_seq(lx, a, out, 1, depth);
        }
        return parse_hex(lx, a, out);
    }
    if (c == '[')
    {
        lx->p++;
        return parse_seq(lx, a, out, 0, depth);
    }
    if (c == ']' || c == '>' || c == ')' || c == '{' || c == '}')
    {
        /* Stray delimiter: consume it as a keyword so callers make progress. */
        out->t = PO_KEYWORD;
        out->n = 1;
        out->u.s = arena_copy(a, lx->p, 1);
        lx->p++;
        return out->u.s != NULL;
    }
    if ((c >= '0' && c <= '9') || c == '+' || c == '-' || c == '.')
    {
        const unsigned char *s = lx->p;
        const unsigned char *p = lx->p;
        int neg = 0, has_dot = 0, ndig = 0;
        double frac = 0.0, scale = 1.0;
        int64_t iv = 0;
        if (*p == '+' || *p == '-')
        {
            neg = (*p == '-');
            p++;
        }
        while (p < lx->end && ((*p >= '0' && *p <= '9') || *p == '.'))
        {
            if (*p == '.')
            {
                if (has_dot)
                {
                    break;
                }
                has_dot = 1;
            }
            else if (!has_dot)
            {
                iv = iv * 10 + (*p - '0');
                ndig++;
            }
            else
            {
                scale /= 10.0;
                frac += (*p - '0') * scale;
                ndig++;
            }
            p++;
        }
        if (p >= lx->end && depth > 0)
        {
            /* A number touching the buffer end inside a container may be cut. */
            lx->truncated = 1;
            return FALSE;
        }
        (void)s;
        lx->p = p;
        if (ndig == 0)
        {
            out->t = PO_INT;
            out->u.i = 0;
            return TRUE;
        }
        if (has_dot)
        {
            out->t = PO_REAL;
            out->u.r = neg ? -((double)iv + frac) : ((double)iv + frac);
            return TRUE;
        }
        out->t = PO_INT;
        out->u.i = neg ? -iv : iv;
        /* "num gen R" lookahead (only for plain non-negative integers). */
        if (!neg && *s != '+')
        {
            Lex save = *lx;
            uint64_t gen = 0;
            lex_skip_ws(lx);
            if (lx->p < lx->end && *lx->p >= '0' && *lx->p <= '9' && lex_uint(lx, &gen))
            {
                lex_skip_ws(lx);
                if (lx->p < lx->end && *lx->p == 'R' &&
                    (lx->p + 1 >= lx->end || is_ws(lx->p[1]) || is_delim(lx->p[1])))
                {
                    lx->p++;
                    out->t = PO_REF;
                    out->u.ref.num = (uint32_t)iv;
                    out->u.ref.gen = (uint32_t)gen;
                    return TRUE;
                }
            }
            *lx = save;
        }
        return TRUE;
    }
    /* keyword */
    {
        const unsigned char *s = lx->p;
        const unsigned char *p = lx->p;
        while (p < lx->end && !is_ws(*p) && !is_delim(*p))
        {
            p++;
        }
        if (p == s)
        {
            lx->p++; /* unknown byte: skip */
            out->t = PO_NULL;
            return TRUE;
        }
        lx->p = p;
        if ((size_t)(p - s) == 4 && memcmp(s, "true", 4) == 0)
        {
            out->t = PO_BOOL;
            out->u.i = 1;
            return TRUE;
        }
        if ((size_t)(p - s) == 5 && memcmp(s, "false", 5) == 0)
        {
            out->t = PO_BOOL;
            out->u.i = 0;
            return TRUE;
        }
        if ((size_t)(p - s) == 4 && memcmp(s, "null", 4) == 0)
        {
            out->t = PO_NULL;
            return TRUE;
        }
        out->t = PO_KEYWORD;
        out->n = (uint32_t)(p - s);
        out->u.s = arena_copy(a, s, (size_t)(p - s));
        return out->u.s != NULL;
    }
}

static BOOL obj_is_kw(const PdfObj *o, const char *kw)
{
    return o != NULL && o->t == PO_KEYWORD && strcmp((const char *)o->u.s, kw) == 0;
}

/* -------------------------------------------------------------------------- */
/* Document                                                                   */
/* -------------------------------------------------------------------------- */

typedef struct CMapRange
{
    uint32_t lo;
    uint32_t hi;
    uint32_t dst;     /* offset into units */
    uint16_t dst_len; /* UTF-16 units */
    uint16_t incr;    /* 1: last unit increments with (code - lo) */
} CMapRange;

typedef struct PdfFont
{
    uint32_t num;
    int two_byte;
    uint16_t enc[256];
    CMapRange *map;
    uint32_t nmap;
    uint32_t cap_map;
    uint16_t *units;
    uint32_t nunits;
    uint32_t cap_units;
} PdfFont;

struct EePdf
{
    HANDLE file;
    uint64_t size;

    /* read window */
    unsigned char *win;
    uint64_t win_off;
    size_t win_len;
    ByteBuf big; /* reads larger than the window */

    /* cross-reference table */
    uint8_t *xtype; /* 0xFF unset, 0 free, 1 offset, 2 in object stream */
    uint64_t *xoff; /* type 1: file offset; type 2: object stream number */
    uint32_t *xidx; /* type 2: index within the object stream */
    uint32_t nobj;
    uint32_t root;
    uint32_t info;
    int encrypted;

    /* pages */
    uint32_t *pages;
    uint32_t npages;
    uint32_t cap_pages;

    /* object stream cache */
    uint32_t os_num;
    ByteBuf os_data;
    uint32_t os_count;
    uint32_t os_first;
    uint32_t *os_nums;
    uint32_t *os_offs;

    /* fonts */
    PdfFont **fonts;
    uint32_t nfonts;
    uint32_t cap_fonts;
    uint32_t *font_hash; /* slot -> index + 1 */
    uint32_t font_hash_cap;

    /* scratch */
    Arena arena;     /* per page */
    Arena tmp_arena; /* per font / tree node / xref section */
    Arena os_arena;  /* object stream dictionaries */
    ByteBuf content; /* decoded page content */
    EePdfPageText scratch_text; /* text sink for EePdf_GetPageImage */
    ByteBuf part;    /* one decoded stream */
};

static void set_err(wchar_t *err, size_t cch, const wchar_t *msg)
{
    if (err != NULL && cch > 0)
    {
        StringCchCopyW(err, cch, msg);
    }
}

/* Read up to @p len bytes at @p off into @p dst; returns the count read. */
static size_t file_read(EePdf *pdf, uint64_t off, void *dst, size_t len)
{
    size_t total = 0;
    while (total < len)
    {
        OVERLAPPED ov;
        DWORD want = (DWORD)(((len - total) > 0x40000000u) ? 0x40000000u : (len - total));
        DWORD got = 0;
        uint64_t at = off + total;
        ZeroMemory(&ov, sizeof(ov));
        ov.Offset = (DWORD)(at & 0xFFFFFFFFu);
        ov.OffsetHigh = (DWORD)(at >> 32);
        if (!ReadFile(pdf->file, (unsigned char *)dst + total, want, &got, &ov) || got == 0)
        {
            break;
        }
        total += got;
    }
    return total;
}

/* Return a pointer to the bytes at [off, off+want) (clamped at EOF; *avail receives
 * the available count). Valid until the next pdf_view call. */
static const unsigned char *pdf_view(EePdf *pdf, uint64_t off, size_t want, size_t *avail)
{
    size_t can;
    if (off >= pdf->size)
    {
        *avail = 0;
        return pdf->win;
    }
    can = (pdf->size - off < (uint64_t)want) ? (size_t)(pdf->size - off) : want;
    if (off >= pdf->win_off && off + can <= pdf->win_off + pdf->win_len)
    {
        *avail = can;
        return pdf->win + (off - pdf->win_off);
    }
    if (can <= PDF_WIN_SIZE)
    {
        size_t n = (pdf->size - off < PDF_WIN_SIZE) ? (size_t)(pdf->size - off) : PDF_WIN_SIZE;
        pdf->win_len = file_read(pdf, off, pdf->win, n);
        pdf->win_off = off;
        *avail = (can < pdf->win_len) ? can : pdf->win_len;
        return pdf->win;
    }
    if (!bb_reserve(&pdf->big, can + 1))
    {
        *avail = 0;
        return pdf->win;
    }
    *avail = file_read(pdf, off, pdf->big.p, can);
    return pdf->big.p;
}

/* -------------------------------------------------------------------------- */
/* Indirect objects                                                           */
/* -------------------------------------------------------------------------- */

/* Parse "num gen obj <value> [stream]" at @p off. When @p expect != UINT32_MAX the
 * header's object number must match. On a stream, *stream_off receives the file
 * offset of the stream data (else 0). */
static BOOL load_obj_at(EePdf *pdf,
                        uint64_t off,
                        uint32_t expect,
                        Arena *a,
                        PdfObj *out,
                        uint64_t *stream_off,
                        uint32_t *out_num)
{
    size_t want = 4096;
    for (;;)
    {
        size_t avail = 0;
        const unsigned char *b = pdf_view(pdf, off, want, &avail);
        Lex lx;
        uint64_t num = 0, gen = 0;
        if (avail == 0)
        {
            return FALSE;
        }
        lx.p = b;
        lx.end = b + avail;
        lx.truncated = 0;
        lex_skip_ws(&lx);
        if (!lex_uint(&lx, &num))
        {
            return FALSE;
        }
        lex_skip_ws(&lx);
        if (!lex_uint(&lx, &gen))
        {
            return FALSE;
        }
        lex_skip_ws(&lx);
        if (lx.end - lx.p < 3 || memcmp(lx.p, "obj", 3) != 0)
        {
            return FALSE;
        }
        lx.p += 3;
        if (expect != UINT32_MAX && num != expect)
        {
            return FALSE;
        }
        if (parse_obj(&lx, a, out, 0))
        {
            Lex after = lx;
            lex_skip_ws(&after);
            if (after.end - after.p < 16 && avail == want && want < PDF_MAX_OBJ_BYTES)
            {
                want *= 4; /* the "stream" keyword may lie past the window */
                continue;
            }
            if (stream_off != NULL)
            {
                *stream_off = 0;
            }
            if (out_num != NULL)
            {
                *out_num = (uint32_t)num;
            }
            if (out->t == PO_DICT && after.end - after.p >= 6 && memcmp(after.p, "stream", 6) == 0)
            {
                const unsigned char *d = after.p + 6;
                if (d < after.end && *d == '\r')
                {
                    d++;
                }
                if (d < after.end && *d == '\n')
                {
                    d++;
                }
                if (stream_off != NULL)
                {
                    *stream_off = off + (uint64_t)(d - b);
                }
            }
            return TRUE;
        }
        if (!lx.truncated || avail < want || want >= PDF_MAX_OBJ_BYTES)
        {
            return FALSE;
        }
        want *= 4; /* object larger than the window: retry with more bytes */
    }
}

static BOOL os_load(EePdf *pdf, uint32_t osnum);

/* Load object @p num into @p out (arena @p a). *stream_off as in load_obj_at. */
static BOOL pdf_load(EePdf *pdf, uint32_t num, Arena *a, PdfObj *out, uint64_t *stream_off)
{
    if (stream_off != NULL)
    {
        *stream_off = 0;
    }
    if (num >= pdf->nobj)
    {
        return FALSE;
    }
    if (pdf->xtype[num] == 1)
    {
        uint64_t off = pdf->xoff[num];
        uint64_t k;
        for (k = 0; off + k * PDF_4GIB < pdf->size; k++)
        {
            if (load_obj_at(pdf, off + k * PDF_4GIB, num, a, out, stream_off, NULL))
            {
                pdf->xoff[num] = off + k * PDF_4GIB; /* remember the repaired offset */
                return TRUE;
            }
            if (off >= PDF_4GIB)
            {
                break; /* a 64-bit offset cannot have wrapped */
            }
        }
        return FALSE;
    }
    if (pdf->xtype[num] == 2)
    {
        uint32_t osnum = (uint32_t)pdf->xoff[num];
        uint32_t idx = pdf->xidx[num];
        uint32_t i;
        Lex lx;
        if (!os_load(pdf, osnum))
        {
            return FALSE;
        }
        /* Prefer the stated index; fall back to a search by object number. */
        if (idx >= pdf->os_count || pdf->os_nums[idx] != num)
        {
            for (i = 0; i < pdf->os_count && pdf->os_nums[i] != num; i++)
            {
            }
            if (i >= pdf->os_count)
            {
                return FALSE;
            }
            idx = i;
        }
        if ((size_t)pdf->os_first + pdf->os_offs[idx] >= pdf->os_data.len)
        {
            return FALSE;
        }
        lx.p = pdf->os_data.p + pdf->os_first + pdf->os_offs[idx];
        lx.end = pdf->os_data.p + pdf->os_data.len;
        lx.truncated = 0;
        return parse_obj(&lx, a, out, 0);
    }
    return FALSE;
}

/* If @p o is a reference, load it into @p tmp and return tmp; else return @p o.
 * Returns NULL for a dangling reference. */
static const PdfObj *resolve(EePdf *pdf, Arena *a, const PdfObj *o, PdfObj *tmp)
{
    int hops = 0;
    while (o != NULL && o->t == PO_REF && hops++ < 8)
    {
        PdfObj *n = (PdfObj *)arena_alloc(a, sizeof(PdfObj));
        if (n == NULL || !pdf_load(pdf, o->u.ref.num, a, n, NULL))
        {
            return NULL;
        }
        o = n;
    }
    if (o != NULL && tmp != NULL && o != tmp)
    {
        *tmp = *o;
        return tmp;
    }
    return o;
}

/* -------------------------------------------------------------------------- */
/* Streams                                                                    */
/* -------------------------------------------------------------------------- */

static BOOL inflate_into(const unsigned char *src, size_t n, ByteBuf *out, int raw)
{
    mz_stream zs;
    int st;
    BOOL ok = TRUE;
    ZeroMemory(&zs, sizeof(zs));
    if ((raw ? mz_inflateInit2(&zs, -MZ_DEFAULT_WINDOW_BITS) : mz_inflateInit(&zs)) != MZ_OK)
    {
        return FALSE;
    }
    zs.next_in = src;
    zs.avail_in = (mz_uint32)((n > 0xFFFFFFFFu) ? 0xFFFFFFFFu : n);
    for (;;)
    {
        if (out->cap - out->len < 65536)
        {
            if (!bb_reserve(out, out->len + 65536 + (out->len / 2)))
            {
                ok = FALSE;
                break;
            }
        }
        zs.next_out = out->p + out->len;
        zs.avail_out = (mz_uint32)(out->cap - out->len - 1);
        st = mz_inflate(&zs, MZ_NO_FLUSH);
        out->len = (size_t)(zs.next_out - out->p);
        if (st == MZ_STREAM_END)
        {
            break;
        }
        if (st == MZ_OK)
        {
            continue;
        }
        if (st == MZ_BUF_ERROR && zs.avail_in == 0)
        {
            break;           /* truncated input: keep what was produced */
        }
        ok = (out->len > 0); /* data error after output: keep the partial result */
        break;
    }
    mz_inflateEnd(&zs);
    if (out->p != NULL)
    {
        out->p[out->len] = 0;
    }
    return ok;
}

/* Undo PNG predictors (Predictor >= 10) in place-ish into a new buffer. */
static BOOL png_unpredict(ByteBuf *b, int colors, int bpc, int columns)
{
    size_t bpp = (size_t)((colors * bpc + 7) / 8);
    size_t row = (size_t)((colors * bpc * columns + 7) / 8);
    size_t nrows;
    ByteBuf o = {0};
    unsigned char *prev;
    size_t r;
    if (bpp == 0)
    {
        bpp = 1;
    }
    if (row == 0)
    {
        return FALSE;
    }
    nrows = b->len / (row + 1);
    prev = (unsigned char *)calloc(row, 1);
    if (prev == NULL || !bb_reserve(&o, nrows * row + 1))
    {
        free(prev);
        bb_free(&o);
        return FALSE;
    }
    for (r = 0; r < nrows; r++)
    {
        const unsigned char *in = b->p + r * (row + 1);
        unsigned char *cur = o.p + r * row;
        int ft = in[0];
        size_t i;
        in++;
        for (i = 0; i < row; i++)
        {
            unsigned a = (i >= bpp) ? cur[i - bpp] : 0;
            unsigned up = prev[i];
            unsigned c = (i >= bpp) ? prev[i - bpp] : 0;
            unsigned v = in[i];
            switch (ft)
            {
                case 1:
                    v += a;
                    break;
                case 2:
                    v += up;
                    break;
                case 3:
                    v += (a + up) / 2;
                    break;
                case 4:
                {
                    int p = (int)a + (int)up - (int)c;
                    int pa = abs(p - (int)a), pb = abs(p - (int)up), pc = abs(p - (int)c);
                    v += (pa <= pb && pa <= pc) ? a : (pb <= pc ? up : c);
                    break;
                }
                default:
                    break;
            }
            cur[i] = (unsigned char)(v & 0xFF);
        }
        memcpy(prev, cur, row);
    }
    o.len = nrows * row;
    free(prev);
    bb_free(b);
    *b = o;
    return TRUE;
}

/* Find "endstream" at or after @p from; returns its offset or UINT64_MAX. */
static uint64_t find_endstream(EePdf *pdf, uint64_t from)
{
    uint64_t off = from;
    while (off < pdf->size)
    {
        size_t avail = 0;
        const unsigned char *b = pdf_view(pdf, off, PDF_WIN_SIZE, &avail);
        size_t i;
        if (avail < 9)
        {
            break;
        }
        for (i = 0; i + 9 <= avail; i++)
        {
            if (b[i] == 'e' && memcmp(b + i, "endstream", 9) == 0)
            {
                return off + i;
            }
        }
        off += avail - 8;
    }
    return UINT64_MAX;
}

/* Decode the stream whose dictionary is @p dict and data starts at @p data_off into
 * @p out (replaced). Supports no filter and FlateDecode (+ PNG predictors). */
static BOOL stream_decode_ex(EePdf *pdf, Arena *a, const PdfObj *dict, uint64_t data_off,
                             ByteBuf *out, int *is_dct)
{
    PdfObj tmp;
    const PdfObj *lenobj = resolve(pdf, a, dict_get(dict, "Length"), &tmp);
    const PdfObj *filt = resolve(pdf, a, dict_get(dict, "Filter"), NULL);
    const PdfObj *parms = resolve(pdf, a, dict_get(dict, "DecodeParms"), NULL);
    int64_t len = (lenobj != NULL && lenobj->t == PO_INT) ? lenobj->u.i : -1;
    int flate = 0;
    const unsigned char *src;
    size_t avail = 0;

    out->len = 0;
    if (is_dct != NULL)
    {
        *is_dct = 0;
    }
    if (filt != NULL && filt->t == PO_ARRAY)
    {
        if (filt->n == 0)
        {
            filt = NULL;
        }
        else if (filt->n == 1)
        {
            filt = &filt->u.items[0];
            if (parms != NULL && parms->t == PO_ARRAY)
            {
                parms = (parms->n > 0) ? resolve(pdf, a, &parms->u.items[0], NULL) : NULL;
            }
        }
        else
        {
            return FALSE; /* filter chains are not needed for report PDFs */
        }
    }
    if (filt != NULL)
    {
        if (obj_is_name(filt, "FlateDecode") || obj_is_name(filt, "Fl"))
        {
            flate = 1;
        }
        else if (is_dct != NULL && (obj_is_name(filt, "DCTDecode") || obj_is_name(filt, "DCT")))
        {
            *is_dct = 1; /* JPEG: returned as the encoded bytes */
        }
        else
        {
            return FALSE;
        }
    }

    /* Validate /Length against an "endstream" marker; recover it by search if bad. */
    if (len < 0 || data_off + (uint64_t)len > pdf->size)
    {
        len = -1;
    }
    else
    {
        size_t av = 0;
        const unsigned char *t = pdf_view(pdf, data_off + (uint64_t)len, 32, &av);
        size_t i = 0;
        while (i < av && is_ws(t[i]))
        {
            i++;
        }
        if (av - i < 9 || memcmp(t + i, "endstream", 9) != 0)
        {
            len = -1;
        }
    }
    if (len < 0)
    {
        uint64_t es = find_endstream(pdf, data_off);
        if (es == UINT64_MAX)
        {
            return FALSE;
        }
        len = (int64_t)(es - data_off);
        /* drop the EOL before "endstream" */
        if (len > 0)
        {
            size_t av = 0;
            const unsigned char *t = pdf_view(pdf, data_off + (uint64_t)len - 1, 1, &av);
            if (av == 1 && t[0] == '\n')
            {
                len--;
                if (len > 0)
                {
                    t = pdf_view(pdf, data_off + (uint64_t)len - 1, 1, &av);
                    if (av == 1 && t[0] == '\r')
                    {
                        len--;
                    }
                }
            }
            else if (av == 1 && t[0] == '\r')
            {
                len--;
            }
        }
    }

    src = pdf_view(pdf, data_off, (size_t)len, &avail);
    if (!flate)
    {
        return bb_append(out, src, avail);
    }
    if (!inflate_into(src, avail, out, 0))
    {
        out->len = 0;
        if (!inflate_into(src, avail, out, 1))
        {
            return FALSE;
        }
    }
    if (parms != NULL && parms->t == PO_DICT)
    {
        int pred = (int)obj_num(dict_get(parms, "Predictor"));
        if (pred >= 10)
        {
            const PdfObj *co = dict_get(parms, "Colors");
            const PdfObj *bo = dict_get(parms, "BitsPerComponent");
            const PdfObj *cl = dict_get(parms, "Columns");
            int colors = co ? (int)obj_num(co) : 1;
            int bpc = bo ? (int)obj_num(bo) : 8;
            int columns = cl ? (int)obj_num(cl) : 1;
            if (!png_unpredict(out, colors, bpc, columns))
            {
                return FALSE;
            }
        }
        else if (pred > 1)
        {
            return FALSE; /* TIFF predictor: not needed for report PDFs */
        }
    }
    return TRUE;
}

static BOOL stream_decode(EePdf *pdf, Arena *a, const PdfObj *dict, uint64_t data_off,
                          ByteBuf *out)
{
    return stream_decode_ex(pdf, a, dict, data_off, out, NULL);
}

/* Load and index object stream @p osnum into the single-entry cache. */
static BOOL os_load(EePdf *pdf, uint32_t osnum)
{
    PdfObj d;
    uint64_t soff = 0;
    uint32_t n, i;
    Lex lx;
    if (pdf->os_num == osnum && pdf->os_data.p != NULL)
    {
        return TRUE;
    }
    pdf->os_num = 0;
    if (osnum >= pdf->nobj || pdf->xtype[osnum] != 1)
    {
        return FALSE; /* object streams cannot nest */
    }
    arena_reset(&pdf->os_arena);
    if (!pdf_load(pdf, osnum, &pdf->os_arena, &d, &soff) || soff == 0 ||
        !stream_decode(pdf, &pdf->os_arena, &d, soff, &pdf->os_data))
    {
        return FALSE;
    }
    n = (uint32_t)obj_num(dict_get(&d, "N"));
    pdf->os_first = (uint32_t)obj_num(dict_get(&d, "First"));
    free(pdf->os_nums);
    free(pdf->os_offs);
    pdf->os_nums = (uint32_t *)calloc(n ? n : 1, sizeof(uint32_t));
    pdf->os_offs = (uint32_t *)calloc(n ? n : 1, sizeof(uint32_t));
    if (pdf->os_nums == NULL || pdf->os_offs == NULL)
    {
        return FALSE;
    }
    lx.p = pdf->os_data.p;
    lx.end = pdf->os_data.p + pdf->os_data.len;
    lx.truncated = 0;
    for (i = 0; i < n; i++)
    {
        uint64_t a = 0, b = 0;
        lex_skip_ws(&lx);
        if (!lex_uint(&lx, &a))
        {
            break;
        }
        lex_skip_ws(&lx);
        if (!lex_uint(&lx, &b))
        {
            break;
        }
        pdf->os_nums[i] = (uint32_t)a;
        pdf->os_offs[i] = (uint32_t)b;
    }
    pdf->os_count = i;
    pdf->os_num = osnum;
    return TRUE;
}

/* -------------------------------------------------------------------------- */
/* Cross-reference                                                            */
/* -------------------------------------------------------------------------- */

static BOOL xref_ensure(EePdf *pdf, uint64_t need)
{
    uint32_t nc;
    uint8_t *nt;
    uint64_t *no;
    uint32_t *ni;
    if (need <= pdf->nobj)
    {
        return TRUE;
    }
    if (need > 0x7FFFFFF0u)
    {
        return FALSE;
    }
    nc = pdf->nobj ? pdf->nobj : 1024;
    while (nc < need)
    {
        nc = (nc > 0x3FFFFFF0u) ? 0x7FFFFFF0u : nc * 2;
    }
    nt = (uint8_t *)realloc(pdf->xtype, nc);
    if (nt == NULL)
    {
        return FALSE;
    }
    pdf->xtype = nt;
    no = (uint64_t *)realloc(pdf->xoff, (size_t)nc * sizeof(uint64_t));
    if (no == NULL)
    {
        return FALSE;
    }
    pdf->xoff = no;
    ni = (uint32_t *)realloc(pdf->xidx, (size_t)nc * sizeof(uint32_t));
    if (ni == NULL)
    {
        return FALSE;
    }
    pdf->xidx = ni;
    memset(pdf->xtype + pdf->nobj, 0xFF, nc - pdf->nobj);
    memset(pdf->xoff + pdf->nobj, 0, (size_t)(nc - pdf->nobj) * sizeof(uint64_t));
    memset(pdf->xidx + pdf->nobj, 0, (size_t)(nc - pdf->nobj) * sizeof(uint32_t));
    pdf->nobj = nc;
    return TRUE;
}

static void xref_set(EePdf *pdf, uint64_t num, uint8_t type, uint64_t a, uint32_t b)
{
    if (num >= 0x7FFFFFF0u || !xref_ensure(pdf, num + 1))
    {
        return;
    }
    if (pdf->xtype[num] != 0xFF)
    {
        return; /* newer section already defined it */
    }
    pdf->xtype[num] = type;
    pdf->xoff[num] = a;
    pdf->xidx[num] = b;
}

static void trailer_take(EePdf *pdf, const PdfObj *tr)
{
    const PdfObj *r = dict_get(tr, "Root");
    const PdfObj *i = dict_get(tr, "Info");
    const PdfObj *s = dict_get(tr, "Size");
    if (pdf->root == 0 && r != NULL && r->t == PO_REF)
    {
        pdf->root = r->u.ref.num;
    }
    if (pdf->info == 0 && i != NULL && i->t == PO_REF)
    {
        pdf->info = i->u.ref.num;
    }
    if (dict_get(tr, "Encrypt") != NULL)
    {
        pdf->encrypted = 1;
    }
    if (s != NULL && s->t == PO_INT && s->u.i > 0 && s->u.i < 0x7FFFFFF0)
    {
        xref_ensure(pdf, (uint64_t)s->u.i);
    }
}

/* Parse a classic "xref" section at @p off. *prev receives /Prev (or -1). */
static BOOL xref_classic(EePdf *pdf, uint64_t off, int64_t *prev, int64_t *xrefstm)
{
    uint64_t pos = off;
    size_t avail = 0;
    const unsigned char *b = pdf_view(pdf, pos, 64, &avail);
    Lex lx;
    lx.p = b;
    lx.end = b + avail;
    lx.truncated = 0;
    lex_skip_ws(&lx);
    if (lx.end - lx.p < 4 || memcmp(lx.p, "xref", 4) != 0)
    {
        return FALSE;
    }
    pos += (uint64_t)(lx.p - b) + 4;
    for (;;)
    {
        uint64_t start = 0, count = 0, i;
        b = pdf_view(pdf, pos, 128, &avail);
        lx.p = b;
        lx.end = b + avail;
        lx.truncated = 0;
        lex_skip_ws(&lx);
        if (lx.end - lx.p >= 7 && memcmp(lx.p, "trailer", 7) == 0)
        {
            pos += (uint64_t)(lx.p - b) + 7;
            break;
        }
        if (!lex_uint(&lx, &start))
        {
            return FALSE;
        }
        lex_skip_ws(&lx);
        if (!lex_uint(&lx, &count))
        {
            return FALSE;
        }
        pos += (uint64_t)(lx.p - b);
        if (count > 0x7FFFFFF0u || start + count > 0x7FFFFFF0u)
        {
            return FALSE;
        }
        b = pdf_view(pdf, pos, (size_t)count * 22u + 64u, &avail);
        lx.p = b;
        lx.end = b + avail;
        for (i = 0; i < count; i++)
        {
            uint64_t o = 0, g = 0;
            unsigned char t;
            lex_skip_ws(&lx);
            if (!lex_uint(&lx, &o))
            {
                return FALSE;
            }
            lex_skip_ws(&lx);
            if (!lex_uint(&lx, &g))
            {
                return FALSE;
            }
            lex_skip_ws(&lx);
            if (lx.p >= lx.end)
            {
                return FALSE;
            }
            t = *lx.p++;
            xref_set(pdf, start + i, (t == 'n') ? 1 : 0, o, 0);
        }
        pos += (uint64_t)(lx.p - b);
    }
    /* trailer dictionary */
    {
        PdfObj tr;
        size_t want = 4096;
        for (;;)
        {
            b = pdf_view(pdf, pos, want, &avail);
            lx.p = b;
            lx.end = b + avail;
            lx.truncated = 0;
            arena_reset(&pdf->tmp_arena);
            if (parse_obj(&lx, &pdf->tmp_arena, &tr, 0))
            {
                break;
            }
            if (!lx.truncated || avail < want || want >= PDF_MAX_OBJ_BYTES)
            {
                return FALSE;
            }
            want *= 4;
        }
        if (tr.t != PO_DICT)
        {
            return FALSE;
        }
        trailer_take(pdf, &tr);
        {
            const PdfObj *p = dict_get(&tr, "Prev");
            const PdfObj *x = dict_get(&tr, "XRefStm");
            *prev = (p != NULL && p->t == PO_INT) ? p->u.i : -1;
            *xrefstm = (x != NULL && x->t == PO_INT) ? x->u.i : -1;
        }
    }
    return TRUE;
}

static uint64_t be_field(const unsigned char *p, int w)
{
    uint64_t v = 0;
    int i;
    for (i = 0; i < w; i++)
    {
        v = (v << 8) | p[i];
    }
    return v;
}

/* Parse an xref stream object at @p off. */
static BOOL xref_stream(EePdf *pdf, uint64_t off, int64_t *prev)
{
    PdfObj d;
    uint64_t soff = 0;
    ByteBuf data = {0};
    const PdfObj *w, *idx, *sz, *pv;
    int wf[3];
    size_t ew, pos = 0;
    uint32_t k;
    BOOL ok = FALSE;

    arena_reset(&pdf->tmp_arena);
    if (!load_obj_at(pdf, off, UINT32_MAX, &pdf->tmp_arena, &d, &soff, NULL) || soff == 0 ||
        !obj_is_name(dict_get(&d, "Type"), "XRef"))
    {
        return FALSE;
    }
    if (!stream_decode(pdf, &pdf->tmp_arena, &d, soff, &data))
    {
        goto done;
    }
    w = dict_get(&d, "W");
    if (w == NULL || w->t != PO_ARRAY || w->n < 3)
    {
        goto done;
    }
    for (k = 0; k < 3; k++)
    {
        wf[k] = (int)obj_num(&w->u.items[k]);
        if (wf[k] < 0 || wf[k] > 8)
        {
            goto done;
        }
    }
    ew = (size_t)(wf[0] + wf[1] + wf[2]);
    if (ew == 0)
    {
        goto done;
    }
    trailer_take(pdf, &d);
    sz = dict_get(&d, "Size");
    idx = dict_get(&d, "Index");
    {
        uint32_t nsub = (idx != NULL && idx->t == PO_ARRAY) ? idx->n / 2 : 1;
        uint32_t s;
        for (s = 0; s < nsub; s++)
        {
            uint64_t start =
                (idx != NULL && idx->t == PO_ARRAY) ? (uint64_t)obj_num(&idx->u.items[2 * s]) : 0;
            uint64_t count = (idx != NULL && idx->t == PO_ARRAY)
                                 ? (uint64_t)obj_num(&idx->u.items[2 * s + 1])
                                 : (uint64_t)obj_num(sz);
            uint64_t i;
            for (i = 0; i < count && pos + ew <= data.len; i++, pos += ew)
            {
                const unsigned char *e = data.p + pos;
                uint64_t type = wf[0] ? be_field(e, wf[0]) : 1;
                uint64_t f2 = be_field(e + wf[0], wf[1]);
                uint64_t f3 = be_field(e + wf[0] + wf[1], wf[2]);
                if (type == 1)
                {
                    xref_set(pdf, start + i, 1, f2, 0);
                }
                else if (type == 2)
                {
                    xref_set(pdf, start + i, 2, f2, (uint32_t)f3);
                }
                else
                {
                    xref_set(pdf, start + i, 0, 0, 0);
                }
            }
        }
    }
    pv = dict_get(&d, "Prev");
    *prev = (pv != NULL && pv->t == PO_INT) ? pv->u.i : -1;
    ok = TRUE;
done:
    bb_free(&data);
    return ok;
}

/* Load one xref section (classic or stream) at @p off, retrying at +4 GiB multiples
 * when a 32-bit writer wrapped the offset. */
static BOOL xref_section(EePdf *pdf, uint64_t off, int64_t *prev)
{
    uint64_t k;
    for (k = 0; off + k * PDF_4GIB < pdf->size; k++)
    {
        int64_t xs = -1;
        uint64_t at = off + k * PDF_4GIB;
        if (xref_classic(pdf, at, prev, &xs))
        {
            if (xs > 0)
            {
                int64_t dummy = -1;
                xref_stream(pdf, (uint64_t)xs, &dummy); /* hybrid file */
            }
            return TRUE;
        }
        if (xref_stream(pdf, at, prev))
        {
            return TRUE;
        }
        if (off >= PDF_4GIB)
        {
            break;
        }
    }
    return FALSE;
}

/* Rebuild the object table by scanning for "N G obj" headers at line starts, and
 * recover /Root from the last "/Root N G R" in the file. */
static BOOL xref_rebuild(EePdf *pdf)
{
    uint64_t off = 0;
    const size_t chunk = 4u * 1024u * 1024u;
    unsigned char *buf = (unsigned char *)malloc(chunk);
    if (buf == NULL)
    {
        return FALSE;
    }
    memset(pdf->xtype, 0xFF, pdf->nobj);
    pdf->root = 0;
    while (off < pdf->size)
    {
        size_t n = file_read(pdf, off, buf, chunk);
        size_t i;
        if (n < 8)
        {
            break;
        }
        for (i = 1; i + 4 < n; i++)
        {
            if (buf[i] == 'o' && buf[i + 1] == 'b' && buf[i + 2] == 'j' &&
                (is_ws(buf[i + 3]) || is_delim(buf[i + 3])) && is_ws(buf[i - 1]))
            {
                /* walk back: ws gen ws num, preceded by an EOL (or file start) */
                size_t j = i - 1;
                size_t gend, nend;
                uint64_t num = 0, mul = 1;
                while (j > 0 && (buf[j] == ' ' || buf[j] == '\t'))
                {
                    j--;
                }
                gend = j;
                while (j > 0 && buf[j] >= '0' && buf[j] <= '9')
                {
                    j--;
                }
                if (j == gend || !(buf[j] == ' ' || buf[j] == '\t'))
                {
                    continue;
                }
                while (j > 0 && (buf[j] == ' ' || buf[j] == '\t'))
                {
                    j--;
                }
                nend = j;
                while (j > 0 && buf[j] >= '0' && buf[j] <= '9')
                {
                    num += (uint64_t)(buf[j] - '0') * mul;
                    mul *= 10;
                    j--;
                }
                if (j == nend || mul > 10000000000ull)
                {
                    continue;
                }
                if (buf[j] != '\n' && buf[j] != '\r' && !(j == 0 && off == 0))
                {
                    continue;
                }
                if (num < 0x7FFFFFF0u && xref_ensure(pdf, num + 1))
                {
                    /* later definitions override earlier ones */
                    pdf->xtype[num] = 1;
                    pdf->xoff[num] = off + j + ((buf[j] == '\n' || buf[j] == '\r') ? 1 : 0);
                    pdf->xidx[num] = 0;
                }
            }
            else if (buf[i] == '/' && i + 5 < n && memcmp(buf + i, "/Root", 5) == 0)
            {
                Lex lx;
                uint64_t r = 0;
                lx.p = buf + i + 5;
                lx.end = buf + n;
                lx.truncated = 0;
                lex_skip_ws(&lx);
                if (lex_uint(&lx, &r) && r > 0 && r < 0x7FFFFFF0u)
                {
                    pdf->root = (uint32_t)r;
                }
            }
            else if (buf[i] == '/' && i + 8 < n && memcmp(buf + i, "/Encrypt", 8) == 0)
            {
                pdf->encrypted = 1;
            }
        }
        if (off + n >= pdf->size)
        {
            break;
        }
        off += n - 64; /* overlap so a header spanning chunks is seen */
    }
    free(buf);
    return pdf->root != 0;
}

static BOOL xref_load(EePdf *pdf)
{
    size_t avail = 0;
    size_t tail = (pdf->size < 4096) ? (size_t)pdf->size : 4096;
    const unsigned char *b = pdf_view(pdf, pdf->size - tail, tail, &avail);
    int64_t sx = -1;
    size_t i;
    BOOL any = FALSE;
    if (!xref_ensure(pdf, 1024))
    {
        return FALSE;
    }
    for (i = avail; i >= 9; i--)
    {
        if (memcmp(b + i - 9, "startxref", 9) == 0)
        {
            const unsigned char *p = b + i;
            int neg = 0;
            int64_t v = 0;
            int nd = 0;
            while (p < b + avail && is_ws(*p))
            {
                p++;
            }
            if (p < b + avail && *p == '-')
            {
                neg = 1;
                p++;
            }
            while (p < b + avail && *p >= '0' && *p <= '9' && nd < 19)
            {
                v = v * 10 + (*p - '0');
                p++;
                nd++;
            }
            if (nd > 0)
            {
                sx = neg ? -v : v;
            }
            break;
        }
    }
    if (sx < 0 && sx >= -(int64_t)PDF_4GIB)
    {
        sx += (int64_t)PDF_4GIB; /* a 32-bit signed writer overflowed */
    }
    if (sx >= 0)
    {
        int64_t at = sx;
        int guard = 0;
        while (at >= 0 && guard++ < 256)
        {
            int64_t prev = -1;
            if (!xref_section(pdf, (uint64_t)at, &prev))
            {
                break;
            }
            any = TRUE;
            if (prev < 0 && prev >= -(int64_t)PDF_4GIB && prev != -1)
            {
                prev += (int64_t)PDF_4GIB;
            }
            at = prev;
        }
    }
    if (!any || pdf->root == 0)
    {
        return xref_rebuild(pdf);
    }
    return TRUE;
}

/* -------------------------------------------------------------------------- */
/* Page tree                                                                  */
/* -------------------------------------------------------------------------- */

static BOOL pages_push(EePdf *pdf, uint32_t num)
{
    if (pdf->npages == pdf->cap_pages)
    {
        uint32_t nc = pdf->cap_pages ? pdf->cap_pages * 2 : 1024;
        uint32_t *np = (uint32_t *)realloc(pdf->pages, (size_t)nc * sizeof(uint32_t));
        if (np == NULL)
        {
            return FALSE;
        }
        pdf->pages = np;
        pdf->cap_pages = nc;
    }
    pdf->pages[pdf->npages++] = num;
    return TRUE;
}

static BOOL pages_collect(EePdf *pdf)
{
    PdfObj cat, tmp;
    const PdfObj *pr;
    uint32_t *stack = NULL;
    size_t sn = 0, scap = 0;
    uint64_t visits = 0;
    BOOL ok = TRUE;

    arena_reset(&pdf->tmp_arena);
    if (!pdf_load(pdf, pdf->root, &pdf->tmp_arena, &cat, NULL) || cat.t != PO_DICT)
    {
        return FALSE;
    }
    pr = dict_get(&cat, "Pages");
    if (pr == NULL || pr->t != PO_REF)
    {
        return FALSE;
    }
    scap = 64;
    stack = (uint32_t *)malloc(scap * sizeof(uint32_t));
    if (stack == NULL)
    {
        return FALSE;
    }
    stack[sn++] = pr->u.ref.num;
    while (sn > 0 && ok)
    {
        uint32_t num = stack[--sn];
        PdfObj node;
        const PdfObj *kids;
        if (++visits > (uint64_t)pdf->nobj + 16u)
        {
            break; /* cycle guard */
        }
        arena_reset(&pdf->tmp_arena);
        if (!pdf_load(pdf, num, &pdf->tmp_arena, &node, NULL) || node.t != PO_DICT)
        {
            continue;
        }
        kids = resolve(pdf, &pdf->tmp_arena, dict_get(&node, "Kids"), &tmp);
        if (obj_is_name(dict_get(&node, "Type"), "Page") || kids == NULL || kids->t != PO_ARRAY)
        {
            ok = pages_push(pdf, num);
            continue;
        }
        if (sn + kids->n > scap)
        {
            size_t nc = scap;
            uint32_t *ns;
            while (nc < sn + kids->n)
            {
                nc *= 2;
            }
            ns = (uint32_t *)realloc(stack, nc * sizeof(uint32_t));
            if (ns == NULL)
            {
                ok = FALSE;
                break;
            }
            stack = ns;
            scap = nc;
        }
        {
            uint32_t k = kids->n;
            while (k-- > 0) /* reverse push so kids pop in document order */
            {
                const PdfObj *kid = &kids->u.items[k];
                if (kid->t == PO_REF)
                {
                    stack[sn++] = kid->u.ref.num;
                }
            }
        }
    }
    free(stack);
    return ok && pdf->npages > 0;
}

/* -------------------------------------------------------------------------- */
/* Text encoding                                                              */
/* -------------------------------------------------------------------------- */

static const uint16_t k_WinAnsi80[32] = {
    0x20AC, 0x0081, 0x201A, 0x0192, 0x201E, 0x2026, 0x2020, 0x2021, 0x02C6, 0x2030, 0x0160,
    0x2039, 0x0152, 0x008D, 0x017D, 0x008F, 0x0090, 0x2018, 0x2019, 0x201C, 0x201D, 0x2022,
    0x2013, 0x2014, 0x02DC, 0x2122, 0x0161, 0x203A, 0x0153, 0x009D, 0x017E, 0x0178};

static void enc_winansi(uint16_t *enc)
{
    int c;
    for (c = 0; c < 256; c++)
    {
        enc[c] = (uint16_t)c;
    }
    for (c = 0; c < 32; c++)
    {
        enc[0x80 + c] = k_WinAnsi80[c];
    }
}

typedef struct GlyphName
{
    const char *name;
    uint16_t cp;
} GlyphName;

static const GlyphName k_Glyphs[] = {
    {"space", 0x20},
    {"exclam", 0x21},
    {"quotedbl", 0x22},
    {"numbersign", 0x23},
    {"dollar", 0x24},
    {"percent", 0x25},
    {"ampersand", 0x26},
    {"quotesingle", 0x27},
    {"parenleft", 0x28},
    {"parenright", 0x29},
    {"asterisk", 0x2A},
    {"plus", 0x2B},
    {"comma", 0x2C},
    {"hyphen", 0x2D},
    {"period", 0x2E},
    {"slash", 0x2F},
    {"zero", 0x30},
    {"one", 0x31},
    {"two", 0x32},
    {"three", 0x33},
    {"four", 0x34},
    {"five", 0x35},
    {"six", 0x36},
    {"seven", 0x37},
    {"eight", 0x38},
    {"nine", 0x39},
    {"colon", 0x3A},
    {"semicolon", 0x3B},
    {"less", 0x3C},
    {"equal", 0x3D},
    {"greater", 0x3E},
    {"question", 0x3F},
    {"at", 0x40},
    {"bracketleft", 0x5B},
    {"backslash", 0x5C},
    {"bracketright", 0x5D},
    {"asciicircum", 0x5E},
    {"underscore", 0x5F},
    {"grave", 0x60},
    {"braceleft", 0x7B},
    {"bar", 0x7C},
    {"braceright", 0x7D},
    {"asciitilde", 0x7E},
    {"quoteleft", 0x2018},
    {"quoteright", 0x2019},
    {"quotedblleft", 0x201C},
    {"quotedblright", 0x201D},
    {"endash", 0x2013},
    {"emdash", 0x2014},
    {"bullet", 0x2022},
    {"nbspace", 0xA0},
    {"section", 0xA7},
    {"copyright", 0xA9},
    {"registered", 0xAE},
    {"degree", 0xB0},
    {"Agrave", 0xC0},
    {"Aacute", 0xC1},
    {"Adieresis", 0xC4},
    {"Ccedilla", 0xC7},
    {"Egrave", 0xC8},
    {"Eacute", 0xC9},
    {"Iacute", 0xCD},
    {"Ntilde", 0xD1},
    {"Oacute", 0xD3},
    {"Odieresis", 0xD6},
    {"Uacute", 0xDA},
    {"Udieresis", 0xDC},
    {"agrave", 0xE0},
    {"aacute", 0xE1},
    {"adieresis", 0xE4},
    {"ccedilla", 0xE7},
    {"egrave", 0xE8},
    {"eacute", 0xE9},
    {"iacute", 0xED},
    {"ntilde", 0xF1},
    {"oacute", 0xF3},
    {"odieresis", 0xF6},
    {"uacute", 0xFA},
    {"udieresis", 0xFC},
};

static uint16_t glyph_to_unicode(const char *n)
{
    size_t len = strlen(n);
    size_t i;
    if (len == 1 && ((n[0] >= 'A' && n[0] <= 'Z') || (n[0] >= 'a' && n[0] <= 'z')))
    {
        return (uint16_t)n[0];
    }
    if (len == 7 && n[0] == 'u' && n[1] == 'n' && n[2] == 'i')
    {
        uint16_t v = 0;
        for (i = 3; i < 7; i++)
        {
            int h = hexval((unsigned char)n[i]);
            if (h < 0)
            {
                return 0;
            }
            v = (uint16_t)(v * 16 + h);
        }
        return v;
    }
    for (i = 0; i < ARRAYSIZE(k_Glyphs); i++)
    {
        if (strcmp(n, k_Glyphs[i].name) == 0)
        {
            return k_Glyphs[i].cp;
        }
    }
    return 0;
}

static BOOL font_add_units(PdfFont *f,
                           const unsigned char *be,
                           uint32_t nbytes,
                           uint32_t *off,
                           uint16_t *n)
{
    uint32_t k, cnt = nbytes / 2;
    if (f->nunits + cnt > f->cap_units)
    {
        uint32_t nc = f->cap_units ? f->cap_units * 2 : 256;
        uint16_t *nu;
        while (nc < f->nunits + cnt)
        {
            nc *= 2;
        }
        nu = (uint16_t *)realloc(f->units, (size_t)nc * sizeof(uint16_t));
        if (nu == NULL)
        {
            return FALSE;
        }
        f->units = nu;
        f->cap_units = nc;
    }
    *off = f->nunits;
    for (k = 0; k < cnt; k++)
    {
        f->units[f->nunits++] = (uint16_t)((be[2 * k] << 8) | be[2 * k + 1]);
    }
    *n = (uint16_t)cnt;
    return TRUE;
}

static BOOL font_add_range(PdfFont *f,
                           uint32_t lo,
                           uint32_t hi,
                           const unsigned char *dst,
                           uint32_t dstlen,
                           int incr)
{
    CMapRange *r;
    if (f->nmap == f->cap_map)
    {
        uint32_t nc = f->cap_map ? f->cap_map * 2 : 64;
        CMapRange *nm = (CMapRange *)realloc(f->map, (size_t)nc * sizeof(CMapRange));
        if (nm == NULL)
        {
            return FALSE;
        }
        f->map = nm;
        f->cap_map = nc;
    }
    r = &f->map[f->nmap];
    r->lo = lo;
    r->hi = hi;
    r->incr = (uint16_t)incr;
    if (!font_add_units(f, dst, dstlen, &r->dst, &r->dst_len))
    {
        return FALSE;
    }
    f->nmap++;
    return TRUE;
}

static uint32_t str_code(const PdfObj *o)
{
    uint32_t v = 0, i;
    for (i = 0; i < o->n && i < 4; i++)
    {
        v = (v << 8) | o->u.s[i];
    }
    return v;
}

static int __cdecl cmap_cmp(const void *a, const void *b)
{
    const CMapRange *x = (const CMapRange *)a;
    const CMapRange *y = (const CMapRange *)b;
    return (x->lo < y->lo) ? -1 : (x->lo > y->lo ? 1 : 0);
}

/* Parse a ToUnicode CMap (bfchar / bfrange) into @p f. */
static void cmap_parse(PdfFont *f, const ByteBuf *data, Arena *a)
{
    Lex lx;
    lx.p = data->p;
    lx.end = data->p + data->len;
    lx.truncated = 0;
    for (;;)
    {
        PdfObj o;
        lex_skip_ws(&lx);
        if (lx.p >= lx.end || !parse_obj(&lx, a, &o, 0))
        {
            break;
        }
        if (obj_is_kw(&o, "beginbfchar"))
        {
            for (;;)
            {
                PdfObj s, d;
                if (!parse_obj(&lx, a, &s, 0) || s.t != PO_STR || !parse_obj(&lx, a, &d, 0))
                {
                    break; /* endbfchar (or junk) */
                }
                if (d.t == PO_STR)
                {
                    uint32_t c = str_code(&s);
                    font_add_range(f, c, c, d.u.s, d.n, 0);
                }
            }
        }
        else if (obj_is_kw(&o, "beginbfrange"))
        {
            for (;;)
            {
                PdfObj lo, hi, d;
                if (!parse_obj(&lx, a, &lo, 0) || lo.t != PO_STR || !parse_obj(&lx, a, &hi, 0) ||
                    hi.t != PO_STR || !parse_obj(&lx, a, &d, 0))
                {
                    break;
                }
                {
                    uint32_t l = str_code(&lo), h = str_code(&hi);
                    if (h < l || h - l > 0xFFFF)
                    {
                        continue;
                    }
                    if (d.t == PO_STR)
                    {
                        font_add_range(f, l, h, d.u.s, d.n, 1);
                    }
                    else if (d.t == PO_ARRAY)
                    {
                        uint32_t k;
                        for (k = 0; k < d.n && l + k <= h; k++)
                        {
                            if (d.u.items[k].t == PO_STR)
                            {
                                font_add_range(f,
                                               l + k,
                                               l + k,
                                               d.u.items[k].u.s,
                                               d.u.items[k].n,
                                               0);
                            }
                        }
                    }
                }
            }
        }
    }
    if (f->nmap > 1)
    {
        qsort(f->map, f->nmap, sizeof(CMapRange), cmap_cmp);
    }
}

static void font_free(PdfFont *f)
{
    if (f != NULL)
    {
        free(f->map);
        free(f->units);
        free(f);
    }
}

/* Build a font from its dictionary @p fd (in arena @p a). */
static PdfFont *font_build(EePdf *pdf, Arena *a, const PdfObj *fd, uint32_t num)
{
    PdfFont *f = (PdfFont *)calloc(1, sizeof(PdfFont));
    const PdfObj *sub, *enc, *tu;
    PdfObj tmp;
    if (f == NULL)
    {
        return NULL;
    }
    f->num = num;
    enc_winansi(f->enc);
    sub = dict_get(fd, "Subtype");
    f->two_byte = obj_is_name(sub, "Type0");
    enc = resolve(pdf, a, dict_get(fd, "Encoding"), &tmp);
    if (!f->two_byte && enc != NULL && enc->t == PO_DICT)
    {
        const PdfObj *diff = resolve(pdf, a, dict_get(enc, "Differences"), NULL);
        if (diff != NULL && diff->t == PO_ARRAY)
        {
            uint32_t k;
            int code = 0;
            for (k = 0; k < diff->n; k++)
            {
                const PdfObj *it = &diff->u.items[k];
                if (it->t == PO_INT)
                {
                    code = (int)it->u.i;
                }
                else if (it->t == PO_NAME)
                {
                    if (code >= 0 && code < 256)
                    {
                        uint16_t u = glyph_to_unicode((const char *)it->u.s);
                        if (u != 0)
                        {
                            f->enc[code] = u;
                        }
                    }
                    code++;
                }
            }
        }
    }
    tu = dict_get(fd, "ToUnicode");
    if (tu != NULL && tu->t == PO_REF)
    {
        PdfObj sd;
        uint64_t soff = 0;
        ByteBuf data = {0};
        if (pdf_load(pdf, tu->u.ref.num, a, &sd, &soff) && soff != 0 &&
            stream_decode(pdf, a, &sd, soff, &data))
        {
            cmap_parse(f, &data, a);
        }
        bb_free(&data);
    }
    return f;
}

static PdfFont *font_get(EePdf *pdf, const PdfObj *ref_or_dict)
{
    uint32_t num = 0, slot;
    PdfFont *f;
    PdfObj fd;
    if (ref_or_dict == NULL)
    {
        return NULL;
    }
    if (ref_or_dict->t == PO_REF)
    {
        num = ref_or_dict->u.ref.num;
        if (pdf->font_hash_cap != 0)
        {
            slot = (num * 2654435761u) & (pdf->font_hash_cap - 1);
            while (pdf->font_hash[slot] != 0)
            {
                PdfFont *c = pdf->fonts[pdf->font_hash[slot] - 1];
                if (c->num == num)
                {
                    return c;
                }
                slot = (slot + 1) & (pdf->font_hash_cap - 1);
            }
        }
        arena_reset(&pdf->tmp_arena);
        if (!pdf_load(pdf, num, &pdf->tmp_arena, &fd, NULL) || fd.t != PO_DICT)
        {
            return NULL;
        }
        f = font_build(pdf, &pdf->tmp_arena, &fd, num);
    }
    else if (ref_or_dict->t == PO_DICT)
    {
        /* Inline font dictionary: built per use (not cached). Rare in practice. */
        f = font_build(pdf, &pdf->arena, ref_or_dict, 0);
    }
    else
    {
        return NULL;
    }
    if (f == NULL)
    {
        return NULL;
    }
    /* Cache (inline fonts too, keyed 0 so they are never found again but freed). */
    if (pdf->nfonts == pdf->cap_fonts)
    {
        uint32_t nc = pdf->cap_fonts ? pdf->cap_fonts * 2 : 16;
        PdfFont **nf = (PdfFont **)realloc(pdf->fonts, (size_t)nc * sizeof(PdfFont *));
        if (nf == NULL)
        {
            font_free(f);
            return NULL;
        }
        pdf->fonts = nf;
        pdf->cap_fonts = nc;
    }
    pdf->fonts[pdf->nfonts++] = f;
    if (num != 0)
    {
        if ((pdf->nfonts + 1) * 2 > pdf->font_hash_cap)
        {
            uint32_t nc = pdf->font_hash_cap ? pdf->font_hash_cap * 2 : 64;
            uint32_t *nh = (uint32_t *)calloc(nc, sizeof(uint32_t));
            uint32_t i;
            if (nh != NULL)
            {
                for (i = 0; i < pdf->nfonts; i++)
                {
                    if (pdf->fonts[i]->num != 0)
                    {
                        uint32_t s = (pdf->fonts[i]->num * 2654435761u) & (nc - 1);
                        while (nh[s] != 0)
                        {
                            s = (s + 1) & (nc - 1);
                        }
                        nh[s] = i + 1;
                    }
                }
                free(pdf->font_hash);
                pdf->font_hash = nh;
                pdf->font_hash_cap = nc;
            }
        }
        else
        {
            slot = (num * 2654435761u) & (pdf->font_hash_cap - 1);
            while (pdf->font_hash[slot] != 0)
            {
                slot = (slot + 1) & (pdf->font_hash_cap - 1);
            }
            pdf->font_hash[slot] = pdf->nfonts;
        }
    }
    return f;
}

/* Append code point @p cp as UTF-8 to @p pt->text. */
static BOOL text_put_cp(EePdfPageText *pt, uint32_t cp)
{
    unsigned char u[4];
    size_t n;
    if (cp < 0x80)
    {
        u[0] = (unsigned char)cp;
        n = 1;
    }
    else if (cp < 0x800)
    {
        u[0] = (unsigned char)(0xC0 | (cp >> 6));
        u[1] = (unsigned char)(0x80 | (cp & 0x3F));
        n = 2;
    }
    else if (cp < 0x10000)
    {
        u[0] = (unsigned char)(0xE0 | (cp >> 12));
        u[1] = (unsigned char)(0x80 | ((cp >> 6) & 0x3F));
        u[2] = (unsigned char)(0x80 | (cp & 0x3F));
        n = 3;
    }
    else
    {
        u[0] = (unsigned char)(0xF0 | (cp >> 18));
        u[1] = (unsigned char)(0x80 | ((cp >> 12) & 0x3F));
        u[2] = (unsigned char)(0x80 | ((cp >> 6) & 0x3F));
        u[3] = (unsigned char)(0x80 | (cp & 0x3F));
        n = 4;
    }
    if (pt->text_len + n + 1 > pt->text_cap)
    {
        size_t nc = pt->text_cap ? pt->text_cap * 2 : 4096;
        char *nt;
        while (nc < pt->text_len + n + 1)
        {
            nc *= 2;
        }
        nt = (char *)realloc(pt->text, nc);
        if (nt == NULL)
        {
            return FALSE;
        }
        pt->text = nt;
        pt->text_cap = nc;
    }
    memcpy(pt->text + pt->text_len, u, n);
    pt->text_len += n;
    pt->text[pt->text_len] = 0;
    return TRUE;
}

/* Append UTF-16 units (with surrogate pairing) as UTF-8. */
static BOOL text_put_units(EePdfPageText *pt, const uint16_t *u, uint32_t n)
{
    uint32_t i;
    for (i = 0; i < n; i++)
    {
        uint32_t cp = u[i];
        if (cp >= 0xD800 && cp <= 0xDBFF && i + 1 < n && u[i + 1] >= 0xDC00 && u[i + 1] <= 0xDFFF)
        {
            cp = 0x10000 + ((cp - 0xD800) << 10) + (u[i + 1] - 0xDC00);
            i++;
        }
        if (!text_put_cp(pt, cp))
        {
            return FALSE;
        }
    }
    return TRUE;
}

static const CMapRange *cmap_find(const PdfFont *f, uint32_t code)
{
    uint32_t lo = 0, hi = f->nmap, k, guard;
    while (lo < hi) /* lo = count of ranges with .lo <= code */
    {
        uint32_t mid = (lo + hi) / 2;
        if (f->map[mid].lo <= code)
        {
            lo = mid + 1;
        }
        else
        {
            hi = mid;
        }
    }
    for (k = lo, guard = 0; k > 0 && guard < 64; k--, guard++)
    {
        if (code <= f->map[k - 1].hi)
        {
            return &f->map[k - 1];
        }
    }
    return NULL;
}

/* Decode string bytes through @p f and append to the page text. */
static BOOL font_decode(const PdfFont *f, const unsigned char *s, uint32_t n, EePdfPageText *pt)
{
    uint32_t i;
    if (f == NULL)
    {
        for (i = 0; i < n; i++)
        {
            if (!text_put_cp(pt, s[i]))
            {
                return FALSE;
            }
        }
        return TRUE;
    }
    if (f->two_byte)
    {
        for (i = 0; i + 1 < n; i += 2)
        {
            uint32_t code = ((uint32_t)s[i] << 8) | s[i + 1];
            const CMapRange *r = (f->nmap > 0) ? cmap_find(f, code) : NULL;
            BOOL ok;
            if (r == NULL || r->dst_len == 0)
            {
                ok = text_put_cp(pt, 0xFFFD);
            }
            else if (r->incr && r->dst_len >= 1)
            {
                uint16_t tmp[16];
                uint32_t k, m = (r->dst_len > 16) ? 16 : r->dst_len;
                for (k = 0; k < m; k++)
                {
                    tmp[k] = f->units[r->dst + k];
                }
                tmp[m - 1] = (uint16_t)(tmp[m - 1] + (code - r->lo));
                ok = text_put_units(pt, tmp, m);
            }
            else
            {
                ok = text_put_units(pt, f->units + r->dst, r->dst_len);
            }
            if (!ok)
            {
                return FALSE;
            }
        }
        return TRUE;
    }
    for (i = 0; i < n; i++)
    {
        const CMapRange *r = (f->nmap > 0) ? cmap_find(f, s[i]) : NULL;
        BOOL ok;
        if (r != NULL && r->dst_len > 0)
        {
            if (r->incr)
            {
                uint16_t u = (uint16_t)(f->units[r->dst + r->dst_len - 1] + (s[i] - r->lo));
                ok = text_put_units(pt, f->units + r->dst, (uint32_t)r->dst_len - 1) &&
                     text_put_units(pt, &u, 1);
            }
            else
            {
                ok = text_put_units(pt, f->units + r->dst, r->dst_len);
            }
        }
        else
        {
            ok = text_put_cp(pt, f->enc[s[i]]);
        }
        if (!ok)
        {
            return FALSE;
        }
    }
    return TRUE;
}

/* -------------------------------------------------------------------------- */
/* Content interpretation                                                     */
/* -------------------------------------------------------------------------- */

typedef struct GState
{
    double ctm[6];
    double cx0, cy0, cx1, cy1;
    int has_clip;
    uint32_t clip_id;
} GState;

static void mat_mul(const double *m, const double *n, double *out)
{
    /* out = m x n (row-vector convention used by PDF) */
    double r[6];
    r[0] = m[0] * n[0] + m[1] * n[2];
    r[1] = m[0] * n[1] + m[1] * n[3];
    r[2] = m[2] * n[0] + m[3] * n[2];
    r[3] = m[2] * n[1] + m[3] * n[3];
    r[4] = m[4] * n[0] + m[5] * n[2] + n[4];
    r[5] = m[4] * n[1] + m[5] * n[3] + n[5];
    memcpy(out, r, sizeof(r));
}

static void mat_ident(double *m)
{
    m[0] = 1;
    m[1] = 0;
    m[2] = 0;
    m[3] = 1;
    m[4] = 0;
    m[5] = 0;
}

typedef struct Interp
{
    EePdf *pdf;
    EePdfPageText *pt;
    const PdfObj *fonts; /* resources /Font dict (resolved) */
    GState gs[PDF_GSTACK_MAX];
    int gsp;
    int gs_overflow;
    double tm[6];
    double tlm[6];
    double tl;
    PdfFont *font;
    /* path bbox */
    int has_path;
    double px0, py0, px1, py1;
    int pending_clip;
    uint32_t next_clip;
    /* largest XObject drawn with "Do" (for image-only pages) */
    char best_xobj[128];
    double best_area;
} Interp;

static void path_add(Interp *in, double x, double y)
{
    const double *c = in->gs[in->gsp].ctm;
    double tx = x * c[0] + y * c[2] + c[4];
    double ty = x * c[1] + y * c[3] + c[5];
    if (!in->has_path)
    {
        in->px0 = in->px1 = tx;
        in->py0 = in->py1 = ty;
        in->has_path = 1;
        return;
    }
    if (tx < in->px0)
        in->px0 = tx;
    if (tx > in->px1)
        in->px1 = tx;
    if (ty < in->py0)
        in->py0 = ty;
    if (ty > in->py1)
        in->py1 = ty;
}

static void path_end(Interp *in)
{
    if (in->pending_clip && in->has_path)
    {
        /* Report the INNERMOST clip path's bounds, not its intersection with the outer
         * clips: report generators lay out each cell as its own clip rectangle, and
         * Reporting Services sometimes places a page's header cells outside the body
         * clip, where an intersection would be empty and lose the cell's position. */
        GState *g = &in->gs[in->gsp];
        g->cx0 = in->px0;
        g->cy0 = in->py0;
        g->cx1 = in->px1;
        g->cy1 = in->py1;
        g->has_clip = 1;
        g->clip_id = ++in->next_clip;
    }
    in->pending_clip = 0;
    in->has_path = 0;
}

static BOOL show_text(Interp *in, const PdfObj *str_or_array)
{
    EePdfPageText *pt = in->pt;
    const GState *g = &in->gs[in->gsp];
    EePdfTextRun *r;
    size_t start = pt->text_len;
    if (pt->nruns == pt->cap_runs)
    {
        uint32_t nc = pt->cap_runs ? pt->cap_runs * 2 : 256;
        EePdfTextRun *nr = (EePdfTextRun *)realloc(pt->runs, (size_t)nc * sizeof(EePdfTextRun));
        if (nr == NULL)
        {
            return FALSE;
        }
        pt->runs = nr;
        pt->cap_runs = nc;
    }
    if (str_or_array->t == PO_STR)
    {
        if (!font_decode(in->font, str_or_array->u.s, str_or_array->n, pt))
        {
            return FALSE;
        }
    }
    else if (str_or_array->t == PO_ARRAY)
    {
        uint32_t k;
        for (k = 0; k < str_or_array->n; k++)
        {
            const PdfObj *it = &str_or_array->u.items[k];
            if (it->t == PO_STR)
            {
                if (!font_decode(in->font, it->u.s, it->n, pt))
                {
                    return FALSE;
                }
            }
            else if ((it->t == PO_INT || it->t == PO_REAL) && obj_num(it) < -200.0)
            {
                /* a large negative kern in TJ is a word gap */
                if (!text_put_cp(pt, ' '))
                {
                    return FALSE;
                }
            }
        }
    }
    r = &pt->runs[pt->nruns++];
    r->x = (float)(in->tm[4] * g->ctm[0] + in->tm[5] * g->ctm[2] + g->ctm[4]);
    r->y = (float)(in->tm[4] * g->ctm[1] + in->tm[5] * g->ctm[3] + g->ctm[5]);
    r->has_clip = g->has_clip;
    r->clip_x0 = (float)g->cx0;
    r->clip_y0 = (float)g->cy0;
    r->clip_x1 = (float)g->cx1;
    r->clip_y1 = (float)g->cy1;
    r->clip_id = g->has_clip ? g->clip_id : 0;
    r->text_off = (uint32_t)start;
    r->text_len = (uint32_t)(pt->text_len - start);
    return TRUE;
}

static void text_newline(Interp *in, double tx, double ty)
{
    double t[6] = {1, 0, 0, 1, 0, 0};
    t[4] = tx;
    t[5] = ty;
    mat_mul(t, in->tlm, in->tlm);
    memcpy(in->tm, in->tlm, sizeof(in->tm));
}

/* Skip inline image data after "ID" up to and including "EI". */
static void skip_inline_image(Lex *lx)
{
    const unsigned char *p = lx->p;
    while (p + 2 < lx->end)
    {
        if (p[0] == 'E' && p[1] == 'I' && is_ws(*(p - 1)) && (p + 2 >= lx->end || is_ws(p[2])))
        {
            lx->p = p + 2;
            return;
        }
        p++;
    }
    lx->p = lx->end;
}

#define OP_MAX 64

static BOOL interpret(Interp *in, const unsigned char *data, size_t len)
{
    Lex lx;
    PdfObj ops[OP_MAX];
    int nops = 0;
    lx.p = data;
    lx.end = data + len;
    lx.truncated = 0;
    for (;;)
    {
        PdfObj o;
        lex_skip_ws(&lx);
        if (lx.p >= lx.end)
        {
            break;
        }
        if (!parse_obj(&lx, &in->pdf->arena, &o, 0))
        {
            if (lx.truncated)
            {
                break;
            }
            return FALSE;
        }
        if (o.t != PO_KEYWORD)
        {
            if (nops < OP_MAX)
            {
                ops[nops++] = o;
            }
            continue;
        }
        {
            const char *op = (const char *)o.u.s;
            GState *g = &in->gs[in->gsp];
            switch (op[0])
            {
                case 'q':
                    if (op[1] == 0)
                    {
                        if (in->gsp + 1 < PDF_GSTACK_MAX)
                        {
                            in->gs[in->gsp + 1] = in->gs[in->gsp];
                            in->gsp++;
                        }
                        else
                        {
                            in->gs_overflow++;
                        }
                    }
                    break;
                case 'Q':
                    if (op[1] == 0)
                    {
                        if (in->gs_overflow > 0)
                        {
                            in->gs_overflow--;
                        }
                        else if (in->gsp > 0)
                        {
                            in->gsp--;
                        }
                    }
                    break;
                case 'c':
                    if (op[1] == 'm' && op[2] == 0 && nops >= 6)
                    {
                        double m[6];
                        int k;
                        for (k = 0; k < 6; k++)
                        {
                            m[k] = obj_num(&ops[nops - 6 + k]);
                        }
                        mat_mul(m, g->ctm, g->ctm);
                    }
                    else if (op[1] == 0 && nops >= 6)
                    {
                        path_add(in, obj_num(&ops[nops - 6]), obj_num(&ops[nops - 5]));
                        path_add(in, obj_num(&ops[nops - 4]), obj_num(&ops[nops - 3]));
                        path_add(in, obj_num(&ops[nops - 2]), obj_num(&ops[nops - 1]));
                    }
                    break;
                case 'r':
                    if (op[1] == 'e' && op[2] == 0 && nops >= 4)
                    {
                        double x = obj_num(&ops[nops - 4]), y = obj_num(&ops[nops - 3]);
                        double w = obj_num(&ops[nops - 2]), h = obj_num(&ops[nops - 1]);
                        path_add(in, x, y);
                        path_add(in, x + w, y);
                        path_add(in, x, y + h);
                        path_add(in, x + w, y + h);
                    }
                    break;
                case 'm':
                case 'l':
                    if (op[1] == 0 && nops >= 2)
                    {
                        path_add(in, obj_num(&ops[nops - 2]), obj_num(&ops[nops - 1]));
                    }
                    break;
                case 'v':
                case 'y':
                    if (op[1] == 0 && nops >= 4)
                    {
                        path_add(in, obj_num(&ops[nops - 4]), obj_num(&ops[nops - 3]));
                        path_add(in, obj_num(&ops[nops - 2]), obj_num(&ops[nops - 1]));
                    }
                    break;
                case 'W':
                    if (op[1] == 0 || (op[1] == '*' && op[2] == 0))
                    {
                        in->pending_clip = 1;
                    }
                    break;
                case 'n':
                case 'f':
                case 'F':
                case 'S':
                case 's':
                case 'B':
                case 'b':
                    if (op[1] == 0 || (op[1] == '*' && op[2] == 0))
                    {
                        path_end(in);
                    }
                    else if (op[0] == 'B' && op[1] == 'T' && op[2] == 0)
                    {
                        mat_ident(in->tm);
                        mat_ident(in->tlm);
                    }
                    else if (op[0] == 'B' && op[1] == 'I' && op[2] == 0)
                    {
                        /* inline image: skip "<dict> ID <data> EI" */
                        const unsigned char *idp = lx.p;
                        while (idp + 2 < lx.end && !(idp[0] == 'I' && idp[1] == 'D' &&
                                                     is_ws(*(idp - 1)) && is_ws(idp[2])))
                        {
                            idp++;
                        }
                        lx.p = (idp + 3 <= lx.end) ? idp + 3 : lx.end;
                        skip_inline_image(&lx);
                    }
                    break;
                case 'D':
                    if (op[1] == 'o' && op[2] == 0 && nops >= 1 && ops[nops - 1].t == PO_NAME)
                    {
                        /* XObject painted in the unit square under the CTM: keep the
                         * largest (a scanned page is one full-page image). */
                        const double *m = g->ctm;
                        double area = m[0] * m[3] - m[1] * m[2];
                        if (area < 0)
                        {
                            area = -area;
                        }
                        if (area > in->best_area)
                        {
                            in->best_area = area;
                            StringCchCopyA(in->best_xobj, ARRAYSIZE(in->best_xobj),
                                           (const char *)ops[nops - 1].u.s);
                        }
                    }
                    break;
                case 'T':
                    if (op[1] == 'f' && op[2] == 0 && nops >= 2 && ops[nops - 2].t == PO_NAME)
                    {
                        in->font =
                            font_get(in->pdf, dict_get(in->fonts, (const char *)ops[nops - 2].u.s));
                    }
                    else if (op[1] == 'L' && op[2] == 0 && nops >= 1)
                    {
                        in->tl = obj_num(&ops[nops - 1]);
                    }
                    else if (op[1] == 'd' && op[2] == 0 && nops >= 2)
                    {
                        text_newline(in, obj_num(&ops[nops - 2]), obj_num(&ops[nops - 1]));
                    }
                    else if (op[1] == 'D' && op[2] == 0 && nops >= 2)
                    {
                        in->tl = -obj_num(&ops[nops - 1]);
                        text_newline(in, obj_num(&ops[nops - 2]), obj_num(&ops[nops - 1]));
                    }
                    else if (op[1] == 'm' && op[2] == 0 && nops >= 6)
                    {
                        int k;
                        for (k = 0; k < 6; k++)
                        {
                            in->tm[k] = in->tlm[k] = obj_num(&ops[nops - 6 + k]);
                        }
                    }
                    else if (op[1] == '*' && op[2] == 0)
                    {
                        text_newline(in, 0, -in->tl);
                    }
                    else if (op[1] == 'j' && op[2] == 0 && nops >= 1)
                    {
                        if (!show_text(in, &ops[nops - 1]))
                        {
                            return FALSE;
                        }
                    }
                    else if (op[1] == 'J' && op[2] == 0 && nops >= 1 && ops[nops - 1].t == PO_ARRAY)
                    {
                        if (!show_text(in, &ops[nops - 1]))
                        {
                            return FALSE;
                        }
                    }
                    break;
                case '\'':
                    if (op[1] == 0 && nops >= 1)
                    {
                        text_newline(in, 0, -in->tl);
                        if (!show_text(in, &ops[nops - 1]))
                        {
                            return FALSE;
                        }
                    }
                    break;
                case '"':
                    if (op[1] == 0 && nops >= 3)
                    {
                        text_newline(in, 0, -in->tl);
                        if (!show_text(in, &ops[nops - 1]))
                        {
                            return FALSE;
                        }
                    }
                    break;
                default:
                    break;
            }
        }
        nops = 0;
    }
    return TRUE;
}

/* -------------------------------------------------------------------------- */
/* Public API                                                                 */
/* -------------------------------------------------------------------------- */

void EePdf_Close(EePdf *pdf)
{
    uint32_t i;
    if (pdf == NULL)
    {
        return;
    }
    if (pdf->file != INVALID_HANDLE_VALUE && pdf->file != NULL)
    {
        CloseHandle(pdf->file);
    }
    free(pdf->win);
    bb_free(&pdf->big);
    free(pdf->xtype);
    free(pdf->xoff);
    free(pdf->xidx);
    free(pdf->pages);
    bb_free(&pdf->os_data);
    free(pdf->os_nums);
    free(pdf->os_offs);
    for (i = 0; i < pdf->nfonts; i++)
    {
        font_free(pdf->fonts[i]);
    }
    free(pdf->fonts);
    free(pdf->font_hash);
    arena_free(&pdf->arena);
    arena_free(&pdf->tmp_arena);
    arena_free(&pdf->os_arena);
    bb_free(&pdf->content);
    bb_free(&pdf->part);
    EePdf_PageTextFree(&pdf->scratch_text);
    free(pdf);
}

BOOL EePdf_Open(const wchar_t *path, EePdf **out, wchar_t *err, size_t errcch)
{
    EePdf *pdf;
    LARGE_INTEGER sz;
    size_t avail = 0;
    const unsigned char *hdr;
    size_t i;
    BOOL has_magic = FALSE;

    if (out == NULL || path == NULL)
    {
        set_err(err, errcch, L"Invalid arguments.");
        return FALSE;
    }
    *out = NULL;
    pdf = (EePdf *)calloc(1, sizeof(EePdf));
    if (pdf == NULL)
    {
        set_err(err, errcch, L"Out of memory.");
        return FALSE;
    }
    pdf->file = CreateFileW(path,
                            GENERIC_READ,
                            FILE_SHARE_READ,
                            NULL,
                            OPEN_EXISTING,
                            FILE_ATTRIBUTE_NORMAL,
                            NULL);
    if (pdf->file == INVALID_HANDLE_VALUE)
    {
        set_err(err, errcch, L"Could not open the PDF file.");
        EePdf_Close(pdf);
        return FALSE;
    }
    if (!GetFileSizeEx(pdf->file, &sz) || sz.QuadPart <= 0)
    {
        set_err(err, errcch, L"Could not read the PDF file.");
        EePdf_Close(pdf);
        return FALSE;
    }
    pdf->size = (uint64_t)sz.QuadPart;
    pdf->win = (unsigned char *)malloc(PDF_WIN_SIZE);
    pdf->win_off = UINT64_MAX;
    if (pdf->win == NULL)
    {
        set_err(err, errcch, L"Out of memory.");
        EePdf_Close(pdf);
        return FALSE;
    }
    hdr = pdf_view(pdf, 0, 1024, &avail);
    for (i = 0; i + 5 <= avail; i++)
    {
        if (memcmp(hdr + i, "%PDF-", 5) == 0)
        {
            has_magic = TRUE;
            break;
        }
    }
    if (!has_magic)
    {
        set_err(err, errcch, L"The file is not a PDF document.");
        EePdf_Close(pdf);
        return FALSE;
    }
    if (!xref_load(pdf))
    {
        set_err(err, errcch, L"The PDF's object index could not be read.");
        EePdf_Close(pdf);
        return FALSE;
    }
    if (pdf->encrypted)
    {
        set_err(err, errcch, L"Encrypted PDF documents are not supported.");
        EePdf_Close(pdf);
        return FALSE;
    }
    if (!pages_collect(pdf))
    {
        /* The xref may point at the wrong objects: rebuild once and retry. */
        if (!xref_rebuild(pdf) || !pages_collect(pdf))
        {
            set_err(err, errcch, L"The PDF's pages could not be read.");
            EePdf_Close(pdf);
            return FALSE;
        }
    }
    *out = pdf;
    return TRUE;
}

uint32_t EePdf_PageCount(const EePdf *pdf)
{
    return pdf ? pdf->npages : 0u;
}

BOOL EePdf_GetInfoText(EePdf *pdf, const char *key, char *buf, size_t cap)
{
    PdfObj info, tmp;
    const PdfObj *v;
    size_t o = 0;
    uint32_t i;
    if (buf == NULL || cap == 0)
    {
        return FALSE;
    }
    buf[0] = 0;
    if (pdf == NULL || pdf->info == 0)
    {
        return FALSE;
    }
    arena_reset(&pdf->tmp_arena);
    if (!pdf_load(pdf, pdf->info, &pdf->tmp_arena, &info, NULL) || info.t != PO_DICT)
    {
        return FALSE;
    }
    v = resolve(pdf, &pdf->tmp_arena, dict_get(&info, key), &tmp);
    if (v == NULL || v->t != PO_STR)
    {
        return FALSE;
    }
    if (v->n >= 2 && v->u.s[0] == 0xFE && v->u.s[1] == 0xFF)
    {
        /* UTF-16BE */
        for (i = 2; i + 1 < v->n; i += 2)
        {
            uint32_t cp = ((uint32_t)v->u.s[i] << 8) | v->u.s[i + 1];
            wchar_t w = (wchar_t)cp;
            char u8[8];
            int n = WideCharToMultiByte(CP_UTF8, 0, &w, 1, u8, (int)sizeof(u8), NULL, NULL);
            if (n <= 0 || o + (size_t)n + 1 > cap)
            {
                break;
            }
            memcpy(buf + o, u8, (size_t)n);
            o += (size_t)n;
        }
    }
    else
    {
        for (i = 0; i < v->n; i++)
        {
            unsigned c = v->u.s[i];
            if (c < 0x80)
            {
                if (o + 2 > cap)
                    break;
                buf[o++] = (char)c;
            }
            else
            {
                if (o + 3 > cap)
                    break;
                buf[o++] = (char)(0xC0 | (c >> 6));
                buf[o++] = (char)(0x80 | (c & 0x3F));
            }
        }
    }
    buf[o] = 0;
    return TRUE;
}

void EePdf_PageTextInit(EePdfPageText *pt)
{
    if (pt != NULL)
    {
        ZeroMemory(pt, sizeof(*pt));
    }
}

void EePdf_PageTextFree(EePdfPageText *pt)
{
    if (pt != NULL)
    {
        free(pt->runs);
        free(pt->text);
        ZeroMemory(pt, sizeof(*pt));
    }
}

/* Append the decoded data of content stream reference/object @p c to pdf->content. */
static BOOL content_append(EePdf *pdf, const PdfObj *c)
{
    PdfObj sd;
    uint64_t soff = 0;
    if (c == NULL || c->t != PO_REF)
    {
        return TRUE;
    }
    if (!pdf_load(pdf, c->u.ref.num, &pdf->arena, &sd, &soff) || soff == 0)
    {
        return FALSE;
    }
    if (!stream_decode(pdf, &pdf->arena, &sd, soff, &pdf->part))
    {
        return FALSE;
    }
    return bb_append(&pdf->content, pdf->part.p, pdf->part.len) &&
           bb_append(&pdf->content, "\n", 1);
}

/* Load page @p page_index, decode its content and interpret it into @p out. When
 * @p best_xobj is given it receives the name of the largest XObject drawn (or "") and
 * @p xobjects the page's resolved /XObject resource dictionary (valid until the next
 * page call). */
static BOOL page_interpret(EePdf *pdf, uint32_t page_index, EePdfPageText *out,
                           char *best_xobj, size_t best_cap, const PdfObj **xobjects)
{
    PdfObj page, tmp;
    const PdfObj *res = NULL, *contents, *fonts = NULL;
    Interp *in = NULL;
    BOOL ok = FALSE;
    int depth;

    if (best_xobj != NULL && best_cap > 0)
    {
        best_xobj[0] = 0;
    }
    if (xobjects != NULL)
    {
        *xobjects = NULL;
    }
    if (pdf == NULL || out == NULL || page_index >= pdf->npages)
    {
        return FALSE;
    }
    out->nruns = 0;
    out->text_len = 0;
    if (out->text != NULL)
    {
        out->text[0] = 0;
    }
    arena_reset(&pdf->arena);
    pdf->content.len = 0;
    if (!pdf_load(pdf, pdf->pages[page_index], &pdf->arena, &page, NULL) || page.t != PO_DICT)
    {
        return FALSE;
    }
    /* Resources, inherited through /Parent when absent. */
    {
        const PdfObj *node = &page;
        PdfObj parent;
        for (depth = 0; depth < 32 && node != NULL; depth++)
        {
            const PdfObj *r = dict_get(node, "Resources");
            const PdfObj *p;
            if (r != NULL)
            {
                res = resolve(pdf, &pdf->arena, r, NULL);
                break;
            }
            p = dict_get(node, "Parent");
            if (p == NULL || p->t != PO_REF ||
                !pdf_load(pdf, p->u.ref.num, &pdf->arena, &parent, NULL))
            {
                break;
            }
            {
                PdfObj *keep = (PdfObj *)arena_alloc(&pdf->arena, sizeof(PdfObj));
                if (keep == NULL)
                {
                    return FALSE;
                }
                *keep = parent;
                node = keep;
            }
        }
    }
    if (res != NULL && res->t == PO_DICT)
    {
        fonts = resolve(pdf, &pdf->arena, dict_get(res, "Font"), NULL);
        if (fonts != NULL && fonts->t == PO_DICT)
        {
            /* Copy the font dict out of any temporary so later loads cannot clobber. */
            PdfObj *keep = (PdfObj *)arena_alloc(&pdf->arena, sizeof(PdfObj));
            if (keep == NULL)
            {
                return FALSE;
            }
            *keep = *fonts;
            fonts = keep;
        }
        if (xobjects != NULL)
        {
            const PdfObj *xo = resolve(pdf, &pdf->arena, dict_get(res, "XObject"), NULL);
            if (xo != NULL && xo->t == PO_DICT)
            {
                PdfObj *keep = (PdfObj *)arena_alloc(&pdf->arena, sizeof(PdfObj));
                if (keep == NULL)
                {
                    return FALSE;
                }
                *keep = *xo;
                *xobjects = keep;
            }
        }
    }
    contents = resolve(pdf, &pdf->arena, dict_get(&page, "Contents"), &tmp);
    if (contents != NULL)
    {
        const PdfObj *raw = dict_get(&page, "Contents");
        if (contents->t == PO_ARRAY)
        {
            uint32_t k;
            for (k = 0; k < contents->n; k++)
            {
                if (!content_append(pdf, &contents->u.items[k]))
                {
                    return FALSE;
                }
            }
        }
        else if (raw != NULL && raw->t == PO_REF)
        {
            if (!content_append(pdf, raw))
            {
                return FALSE;
            }
        }
    }
    if (pdf->content.len == 0)
    {
        return TRUE; /* no content: no text */
    }
    in = (Interp *)calloc(1, sizeof(Interp));
    if (in == NULL)
    {
        return FALSE;
    }
    in->pdf = pdf;
    in->pt = out;
    in->fonts = fonts;
    mat_ident(in->gs[0].ctm);
    mat_ident(in->tm);
    mat_ident(in->tlm);
    ok = interpret(in, pdf->content.p, pdf->content.len);
    if (ok && best_xobj != NULL && best_cap > 0)
    {
        StringCchCopyA(best_xobj, best_cap, in->best_xobj);
    }
    free(in);
    return ok;
}

BOOL EePdf_ExtractPageText(EePdf *pdf, uint32_t page_index, EePdfPageText *out)
{
    return page_interpret(pdf, page_index, out, NULL, 0, NULL);
}

void EePdf_ImageFree(EePdfImage *img)
{
    if (img != NULL)
    {
        free(img->data);
        ZeroMemory(img, sizeof(*img));
    }
}

BOOL EePdf_GetPageImage(EePdf *pdf, uint32_t page_index, EePdfImage *out)
{
    char name[128];
    const PdfObj *xobjects = NULL;
    const PdfObj *ref, *sub, *cs, *bpc;
    PdfObj d;
    uint64_t soff = 0;
    int is_dct = 0;
    uint32_t w, h, ch;
    if (pdf == NULL || out == NULL)
    {
        return FALSE;
    }
    out->kind = EE_PDF_IMAGE_NONE;
    out->len = 0;
    out->width = out->height = out->channels = out->stride = 0;
    if (!page_interpret(pdf, page_index, &pdf->scratch_text, name, sizeof(name), &xobjects))
    {
        return FALSE;
    }
    if (name[0] == 0 || xobjects == NULL)
    {
        return TRUE; /* no image on the page */
    }
    ref = dict_get(xobjects, name);
    if (ref == NULL || ref->t != PO_REF ||
        !pdf_load(pdf, ref->u.ref.num, &pdf->arena, &d, &soff) || soff == 0 || d.t != PO_DICT)
    {
        return TRUE; /* not a stream XObject: treat as no image */
    }
    sub = dict_get(&d, "Subtype");
    if (!obj_is_name(sub, "Image"))
    {
        return TRUE; /* a form XObject (not followed) */
    }
    w = (uint32_t)obj_num(resolve(pdf, &pdf->arena, dict_get(&d, "Width"), NULL));
    h = (uint32_t)obj_num(resolve(pdf, &pdf->arena, dict_get(&d, "Height"), NULL));
    cs = resolve(pdf, &pdf->arena, dict_get(&d, "ColorSpace"), NULL);
    bpc = resolve(pdf, &pdf->arena, dict_get(&d, "BitsPerComponent"), NULL);
    {
        ByteBuf b;
        b.p = out->data;
        b.len = 0;
        b.cap = out->cap;
        if (!stream_decode_ex(pdf, &pdf->arena, &d, soff, &b, &is_dct))
        {
            out->data = b.p;
            out->cap = b.cap;
            return FALSE; /* unsupported filter (e.g. JPX, CCITT) */
        }
        out->data = b.p;
        out->cap = b.cap;
        out->len = b.len;
    }
    out->width = w;
    out->height = h;
    if (is_dct)
    {
        out->kind = EE_PDF_IMAGE_ENCODED;
        return TRUE;
    }
    /* Raw samples: support 8-bit gray / RGB. */
    ch = obj_is_name(cs, "DeviceGray") ? 1u : (obj_is_name(cs, "DeviceRGB") ? 3u : 0u);
    if (ch == 0 || (bpc != NULL && obj_num(bpc) != 8.0) || w == 0 || h == 0 ||
        (uint64_t)w * h * ch > out->len)
    {
        out->len = 0;
        return FALSE;
    }
    out->kind = EE_PDF_IMAGE_PIXELS;
    out->channels = ch;
    out->stride = w * ch;
    return TRUE;
}
