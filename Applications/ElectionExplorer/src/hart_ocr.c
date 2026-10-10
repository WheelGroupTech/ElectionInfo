/**
 * @file hart_ocr.c
 * @brief OCR reader for image-only Hart CVR Report PDFs. See hart_ocr.h.
 *
 * Geometry is in OCR image pixels (origin top-left, y down). Report distances are
 * expressed in PDF points and scaled by px_per_pt = image height / 792 (letter page).
 */

#include "hart_ocr.h"

#include <math.h>
#include <stdlib.h>
#include <string.h>
#include <strsafe.h>

/* -------------------------------------------------------------------------- */
/* Store                                                                      */
/* -------------------------------------------------------------------------- */

static void store_clear(HartOcrStore *s)
{
    HartOcr_StoreFree(s);
}

void HartOcr_StoreFree(HartOcrStore *s)
{
    if (s != NULL)
    {
        free(s->pool);
        free(s->recs);
        free(s->rows);
        ZeroMemory(s, sizeof(*s));
    }
}

const char *HartOcr_Str(const HartOcrStore *s, uint32_t off)
{
    return (s->pool != NULL && off < s->pool_len) ? s->pool + off : "";
}

/* Append [p,n) + NUL; returns its offset, or (uint32_t)-1 on OOM. Offset 0 is "". */
static uint32_t pool_add(HartOcrStore *s, const char *p, size_t n)
{
    uint32_t off;
    if (s->pool_len == 0)
    {
        s->pool = (char *)malloc(65536);
        if (s->pool == NULL)
        {
            return (uint32_t)-1;
        }
        s->pool_cap = 65536;
        s->pool[0] = '\0';
        s->pool_len = 1;
    }
    if (n == 0)
    {
        return 0;
    }
    if (s->pool_len + n + 1 > 0xFFFFFFF0u)
    {
        return (uint32_t)-1;
    }
    if (s->pool_len + n + 1 > s->pool_cap)
    {
        size_t nc = s->pool_cap * 2;
        char *np;
        while (nc < s->pool_len + n + 1)
        {
            nc *= 2;
        }
        np = (char *)realloc(s->pool, nc);
        if (np == NULL)
        {
            return (uint32_t)-1;
        }
        s->pool = np;
        s->pool_cap = nc;
    }
    off = (uint32_t)s->pool_len;
    memcpy(s->pool + off, p, n);
    s->pool[off + n] = '\0';
    s->pool_len += n + 1;
    return off;
}

static HartOcrRecord *store_new_record(HartOcrStore *s)
{
    HartOcrRecord *r;
    if (s->nrecs == s->cap_recs)
    {
        uint32_t nc = s->cap_recs ? s->cap_recs * 2 : 256;
        HartOcrRecord *nr = (HartOcrRecord *)realloc(s->recs, (size_t)nc * sizeof(HartOcrRecord));
        if (nr == NULL)
        {
            return NULL;
        }
        s->recs = nr;
        s->cap_recs = nc;
    }
    r = &s->recs[s->nrecs++];
    ZeroMemory(r, sizeof(*r));
    r->first_row = s->nrows;
    return r;
}

static BOOL store_add_row(HartOcrStore *s, uint32_t title, uint32_t option)
{
    if (s->nrows == s->cap_rows)
    {
        uint32_t nc = s->cap_rows ? s->cap_rows * 2 : 4096;
        HartOcrRow *nr = (HartOcrRow *)realloc(s->rows, (size_t)nc * sizeof(HartOcrRow));
        if (nr == NULL)
        {
            return FALSE;
        }
        s->rows = nr;
        s->cap_rows = nc;
    }
    s->rows[s->nrows].title = title;
    s->rows[s->nrows].option = option;
    s->nrows++;
    return TRUE;
}

/* -------------------------------------------------------------------------- */
/* Text helpers                                                               */
/* -------------------------------------------------------------------------- */

/* Canonical form for matching labels: lowercase letters only, with the common OCR
 * look-alikes folded (l, 1, |, ! -> i; 0 -> o). */
static size_t canon_label(const char *s, size_t n, char *out, size_t cap)
{
    size_t o = 0, i;
    for (i = 0; i < n && o + 1 < cap; i++)
    {
        char c = s[i];
        if (c >= 'A' && c <= 'Z')
        {
            c = (char)(c - 'A' + 'a');
        }
        if (c == 'l' || c == '1' || c == '|' || c == '!')
        {
            c = 'i';
        }
        else if (c == '0')
        {
            c = 'o';
        }
        if (c >= 'a' && c <= 'z')
        {
            out[o++] = c;
        }
    }
    out[o] = '\0';
    return o;
}

/* Levenshtein distance of two short strings (capped at 64 chars each). */
static int edit_distance(const char *a, const char *b)
{
    int la = (int)strlen(a), lb = (int)strlen(b), i, j;
    int prev[65], cur[65];
    if (la > 64)
        la = 64;
    if (lb > 64)
        lb = 64;
    for (j = 0; j <= lb; j++)
    {
        prev[j] = j;
    }
    for (i = 1; i <= la; i++)
    {
        cur[0] = i;
        for (j = 1; j <= lb; j++)
        {
            int c = prev[j - 1] + (a[i - 1] != b[j - 1]);
            int d = prev[j] + 1;
            int e = cur[j - 1] + 1;
            cur[j] = (c < d) ? (c < e ? c : e) : (d < e ? d : e);
        }
        memcpy(prev, cur, (size_t)(lb + 1) * sizeof(int));
    }
    return prev[lb];
}

static int canon_matches(const char *text, size_t n, const char *label)
{
    char a[96], b[96];
    size_t lb;
    canon_label(text, n, a, sizeof(a));
    lb = canon_label(label, strlen(label), b, sizeof(b));
    if (strcmp(a, b) == 0)
    {
        return 1;
    }
    return edit_distance(a, b) <= ((lb >= 8) ? 2 : 1);
}

/* Trim spaces; returns pointer and length. */
static const char *trim(const char *s, size_t n, size_t *out)
{
    while (n > 0 && (*s == ' ' || *s == '\t'))
    {
        s++;
        n--;
    }
    while (n > 0 && (s[n - 1] == ' ' || s[n - 1] == '\t'))
    {
        n--;
    }
    *out = n;
    return s;
}

/* Repair an OCR'd GUID: fold look-alikes into hex, drop spaces/hyphens, require 32 hex
 * digits; output canonical lowercase 8-4-4-4-12. *repaired set when characters were
 * changed (beyond case and spacing). */
static BOOL repair_guid(const char *s, size_t n, char *out, size_t cap, int *repaired)
{
    char hex[33];
    size_t h = 0, i;
    int fix = 0;
    if (cap < 37)
    {
        return FALSE;
    }
    for (i = 0; i < n; i++)
    {
        char c = s[i];
        char m = 0;
        if (c == ' ' || c == '-' || c == '\t')
        {
            continue;
        }
        if ((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f'))
            m = c;
        else if (c >= 'A' && c <= 'F')
            m = (char)(c - 'A' + 'a');
        else if (c == 'O' || c == 'o' || c == 'Q')
            m = '0';
        else if (c == 'I' || c == 'l' || c == 'i' || c == '|' || c == '!' || c == 'L')
            m = '1';
        else if (c == 'S' || c == 's')
            m = '5';
        else if (c == 'Z' || c == 'z')
            m = '2';
        else if (c == 'G')
            m = '6';
        else if (c == 'T')
            m = '7';
        else if (c == 'g')
            m = '9';
        else
            return FALSE;
        if (m != c && !(c >= 'A' && c <= 'F'))
        {
            fix = 1;
        }
        if (h >= 32)
        {
            return FALSE;
        }
        hex[h++] = m;
    }
    if (h != 32)
    {
        return FALSE;
    }
    hex[32] = '\0';
    StringCchPrintfA(out,
                     cap,
                     "%.8s-%.4s-%.4s-%.4s-%.12s",
                     hex,
                     hex + 8,
                     hex + 12,
                     hex + 16,
                     hex + 20);
    if (repaired != NULL)
    {
        *repaired = fix;
    }
    return TRUE;
}

/* Hex digits differing between two canonical GUIDs. */
static int guid_distance(const char *a, const char *b)
{
    int d = 0;
    for (; *a && *b; a++, b++)
    {
        if (*a != *b)
        {
            d++;
        }
    }
    return d + (int)strlen(a) + (int)strlen(b);
}

/* TRUE if [s,n) contains an ASCII letter or digit. */
static BOOL has_alnum(const char *s, size_t n)
{
    size_t i;
    for (i = 0; i < n; i++)
    {
        char c = s[i];
        if ((c >= '0' && c <= '9') || (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z'))
        {
            return TRUE;
        }
    }
    return FALSE;
}

/* Fold digit look-alikes in a numeric field (Central Batch Id). */
static void fix_number(char *s)
{
    for (; *s; s++)
    {
        if (*s == 'O' || *s == 'o')
            *s = '0';
        else if (*s == 'I' || *s == 'l' || *s == '|')
            *s = '1';
    }
}

/* "3156 - 008" -> "3156-008" (matches the XML / text-PDF form). */
static void normalize_precinct(char *p)
{
    char *w = p;
    const char *r = p;
    while (*r)
    {
        if (r[0] == ' ' && r[1] == '-' && r[2] == ' ')
        {
            *w++ = '-';
            r += 3;
            continue;
        }
        *w++ = *r++;
    }
    *w = '\0';
}

/* -------------------------------------------------------------------------- */
/* Page cells                                                                 */
/* -------------------------------------------------------------------------- */

typedef struct OCell
{
    float x0, y0, x1, y1;
    uint32_t off;
    uint32_t len;
    int label;   /* HOF_* when a header label, -1 otherwise */
    int is_guid; /* header value repaired into a GUID */
    int used;
} OCell;

typedef struct PageCells
{
    OCell *c;
    uint32_t n;
    uint32_t cap;
    char *txt;
    size_t len;
    size_t cap_txt;
    float w, h, ppt; /* image size; pixels per PDF point */
} PageCells;

static void cells_free(PageCells *p)
{
    free(p->c);
    free(p->txt);
    ZeroMemory(p, sizeof(*p));
}

static BOOL cells_text(PageCells *p, const char *s, size_t n)
{
    if (p->len + n + 2 > p->cap_txt)
    {
        size_t nc = p->cap_txt ? p->cap_txt * 2 : 8192;
        char *nt;
        while (nc < p->len + n + 2)
        {
            nc *= 2;
        }
        nt = (char *)realloc(p->txt, nc);
        if (nt == NULL)
        {
            return FALSE;
        }
        p->txt = nt;
        p->cap_txt = nc;
    }
    memcpy(p->txt + p->len, s, n);
    p->len += n;
    p->txt[p->len] = '\0';
    return TRUE;
}

/* Split each OCR line into cells at wide word gaps (columns). */
static BOOL build_cells(const EeOcrText *t, PageCells *p)
{
    uint32_t li;
    p->n = 0;
    p->len = 0;
    p->w = (float)t->width;
    p->h = (float)t->height;
    p->ppt = (t->height > 0) ? (float)t->height / 792.0f : 1.0f;
    for (li = 0; li < t->nlines; li++)
    {
        const EeOcrLine *ln = &t->lines[li];
        uint32_t k;
        OCell *cur = NULL;
        for (k = 0; k < ln->nwords; k++)
        {
            const EeOcrWord *w = &t->words[ln->first_word + k];
            float gap_thr = 9.0f * p->ppt; /* ~ a few character widths */
            if (cur == NULL || w->x0 - cur->x1 > gap_thr)
            {
                if (p->n == p->cap)
                {
                    uint32_t nc = p->cap ? p->cap * 2 : 256;
                    OCell *ncl = (OCell *)realloc(p->c, (size_t)nc * sizeof(OCell));
                    if (ncl == NULL)
                    {
                        return FALSE;
                    }
                    p->c = ncl;
                    p->cap = nc;
                }
                cur = &p->c[p->n++];
                ZeroMemory(cur, sizeof(*cur));
                cur->x0 = w->x0;
                cur->y0 = w->y0;
                cur->x1 = w->x1;
                cur->y1 = w->y1;
                cur->off = (uint32_t)p->len;
                cur->label = -1;
            }
            else
            {
                if (!cells_text(p, " ", 1))
                {
                    return FALSE;
                }
                cur->len++;
                if (w->x1 > cur->x1)
                    cur->x1 = w->x1;
                if (w->y0 < cur->y0)
                    cur->y0 = w->y0;
                if (w->y1 > cur->y1)
                    cur->y1 = w->y1;
            }
            if (!cells_text(p, t->text + w->text_off, w->text_len))
            {
                return FALSE;
            }
            cur->len += w->text_len;
        }
    }
    return TRUE;
}

static int __cdecl cell_cmp(const void *a, const void *b)
{
    const OCell *x = (const OCell *)a;
    const OCell *y = (const OCell *)b;
    float cx = (x->y0 + x->y1) * 0.5f, cy = (y->y0 + y->y1) * 0.5f;
    if (cx < cy - 4.0f)
        return -1;
    if (cx > cy + 4.0f)
        return 1;
    return (x->x0 < y->x0) ? -1 : (x->x0 > y->x0 ? 1 : 0);
}

static const char *cell_str(const PageCells *p, const OCell *c, size_t *n)
{
    return trim(p->txt + c->off, c->len, n);
}

/* -------------------------------------------------------------------------- */
/* Header labels                                                              */
/* -------------------------------------------------------------------------- */

typedef struct LabelDef
{
    const char *name;
    int field;
} LabelDef;

static const LabelDef k_Labels[] = {
    {"Precinct", HOF_PRECINCT},
    {"Party", HOF_PARTY},
    {"Polling Place", HOF_PPLACE},
    {"Voting Type", HOF_VTYPE},
    {"Central Batch Id", HOF_BATCH},
    {"Device Type", HOF_DTYPE},
    {"Device Serial", HOF_DSERIAL},
    {"Device Data Id", HOF_DDATA},
    {"Cvr Id", HOF_GUID},
};

/* Classify a cell as a header label; value text (after the colon) via *val/*vn. */
static int match_label(const char *s, size_t n, const char **val, size_t *vn)
{
    size_t i, colon = n;
    uint32_t k;
    for (i = 0; i < n && i < 24; i++)
    {
        if (s[i] == ':' || s[i] == ';')
        {
            colon = i;
            break;
        }
    }
    if (colon == n)
    {
        return -1; /* OCR keeps the colon after a header label */
    }
    for (k = 0; k < ARRAYSIZE(k_Labels); k++)
    {
        if (canon_matches(s, colon, k_Labels[k].name))
        {
            *val = trim(s + colon + 1, n - colon - 1, vn);
            return k_Labels[k].field;
        }
    }
    return -1;
}

static BOOL is_table_header(const char *s, size_t n)
{
    return canon_matches(s, n, "Contest Title");
}

static BOOL is_option_header(const char *s, size_t n)
{
    char a[32];
    if (n > 12)
    {
        return FALSE;
    }
    canon_label(s, n, a, sizeof(a));
    return strcmp(a, "option") == 0 || edit_distance(a, "option") <= 1;
}

/* Canonical special Option values (OCR spellings vary). Writes into @p out. */
static BOOL special_option(const char *s, size_t n, char *out, size_t cap)
{
    char c[64];
    size_t cn = canon_label(s, n > 40 ? 40 : n, c, sizeof(c));
    if (cn >= 9 && edit_distance(c, "undervotes") <= 2)
    {
        /* trailing count: digits (with look-alikes) after the word */
        int v = 0, any = 0;
        size_t i;
        for (i = 0; i < n; i++)
        {
            char ch = s[i];
            if (ch == 'O' || ch == 'o')
                ch = '0';
            if ((ch == 'I' || ch == 'l' || ch == '|') && i > 9)
                ch = '1';
            if (ch >= '0' && ch <= '9' && i > 8)
            {
                v = v * 10 + (ch - '0');
                any = 1;
            }
        }
        StringCchPrintfA(out, cap, "Undervotes: %d", any && v > 0 ? v : 1);
        return TRUE;
    }
    if (cn >= 7 && cn <= 10 && edit_distance(c, "overvote") <= 1)
    {
        StringCchCopyA(out, cap, "Overvote");
        return TRUE;
    }
    if (cn >= 6 && cn <= 9 && edit_distance(c, "writein") <= 1)
    {
        StringCchCopyA(out, cap, "Write-in");
        return TRUE;
    }
    return FALSE;
}

/* -------------------------------------------------------------------------- */
/* Page parsing                                                               */
/* -------------------------------------------------------------------------- */

/* Record-level OCR issue flags (folded into the status after correction). */
enum
{
    RF_GUID_REPAIRED = 1,
    RF_GUID_INVALID = 2,
    RF_MISSING_OPTION = 4,
    RF_ORPHAN_OPTION = 8,
    RF_VALUE_SNAPPED = 16,
    RF_RARE_VALUE = 32
};

typedef struct ParseState
{
    HartOcrStore *store;
    int32_t cur; /* current record index, -1 = none */
    char cur_guid[40];
    uint32_t page;
    uint32_t unreadable; /* counter for unreadable Cvr Ids */
    uint32_t issues;     /* OCR problems found on the current page (retry trigger) */
    uint32_t cur_page;   /* last page the current record appeared on */
} ParseState;

/* Space-free uppercase key for comparing OCR'd titles. */
static void title_key(const char *s, char *out, size_t cap)
{
    size_t o = 0;
    for (; *s && o + 1 < cap; s++)
    {
        char c = *s;
        if (c == ' ' || c == '\t')
        {
            continue;
        }
        if (c >= 'a' && c <= 'z')
        {
            c = (char)(c - 'a' + 'A');
        }
        out[o++] = c;
    }
    out[o] = '\0';
}

/* TRUE if @p title could start the continuation of the current record: the record has
 * no contest titled like it, or it is the record's last contest (the next seat row of a
 * vote-for-N contest split by the page break). */
static BOOL title_continues_record(const ParseState *ps, const char *title)
{
    const HartOcrStore *s = ps->store;
    const HartOcrRecord *r = &s->recs[ps->cur];
    char a[128], b[128];
    uint32_t q;
    title_key(title, a, sizeof(a));
    for (q = r->first_row; q < r->first_row + r->nrows; q++)
    {
        title_key(HartOcr_Str(s, s->rows[q].title), b, sizeof(b));
        if (strcmp(a, b) == 0)
        {
            return q + 1 == r->first_row + r->nrows;
        }
    }
    return TRUE;
}

/* Start or continue a record from a header block's fields. @p first_on_page: the block
 * is the first record header on its page; @p first_title: its table's first contest. */
static BOOL begin_block(ParseState *ps,
                        char fields[HOF_COUNT][256],
                        int guid_ok,
                        int flags,
                        BOOL first_on_page,
                        const char *first_title)
{
    HartOcrStore *s = ps->store;
    HartOcrRecord *r;
    int k;
    if (guid_ok && ps->cur >= 0 && guid_distance(fields[HOF_GUID], ps->cur_guid) <= 3)
    {
        /* repeated header of a record continued from the previous page */
        s->recs[ps->cur].status |= (uint8_t)flags;
        ps->cur_page = ps->page;
        return TRUE;
    }
    if (guid_ok && first_on_page && ps->cur >= 0 && ps->cur_page + 1 == ps->page &&
        strncmp(ps->cur_guid, "unreadable", 10) == 0 &&
        strcmp(fields[HOF_PRECINCT], HartOcr_Str(s, s->recs[ps->cur].f[HOF_PRECINCT])) == 0 &&
        strcmp(fields[HOF_BATCH], HartOcr_Str(s, s->recs[ps->cur].f[HOF_BATCH])) == 0 &&
        first_title != NULL && first_title[0] != '\0' && title_continues_record(ps, first_title))
    {
        /* The previous page's header Cvr Id was unreadable but this repeated header's is
         * fine: same continuation test as below, and the record takes the readable id. */
        uint32_t off = pool_add(s, fields[HOF_GUID], strlen(fields[HOF_GUID]));
        if (off == (uint32_t)-1)
        {
            return FALSE;
        }
        s->recs[ps->cur].f[HOF_GUID] = off;
        s->recs[ps->cur].status =
            (uint8_t)((s->recs[ps->cur].status & ~RF_GUID_INVALID) | flags | RF_GUID_REPAIRED);
        StringCchCopyA(ps->cur_guid, ARRAYSIZE(ps->cur_guid), fields[HOF_GUID]);
        ps->cur_page = ps->page;
        return TRUE;
    }
    if (!guid_ok && first_on_page && ps->cur >= 0 && ps->cur_page + 1 == ps->page &&
        strcmp(fields[HOF_PRECINCT], HartOcr_Str(s, s->recs[ps->cur].f[HOF_PRECINCT])) == 0 &&
        strcmp(fields[HOF_BATCH], HartOcr_Str(s, s->recs[ps->cur].f[HOF_BATCH])) == 0 &&
        first_title != NULL && first_title[0] != '\0' && title_continues_record(ps, first_title))
    {
        /* The repeated header at the top of a page whose Cvr Id OCR could not read, but
         * whose precinct and batch match the record cut off at the bottom of the previous
         * page and whose first contest continues it (not already on the record, or the
         * record's last contest): the same ballot continued. A new ballot would repeat
         * the record's earlier contests. (Voting Type is not compared: OCR sometimes
         * truncates it.) */
        s->recs[ps->cur].status |= (uint8_t)(flags & ~RF_GUID_INVALID) | RF_GUID_REPAIRED;
        ps->cur_page = ps->page;
        return TRUE;
    }
    if (!guid_ok)
    {
        ps->issues++;
        StringCchPrintfA(fields[HOF_GUID],
                         256,
                         "unreadable-p%u-%u",
                         ps->page + 1,
                         ++ps->unreadable);
    }
    r = store_new_record(s);
    if (r == NULL)
    {
        return FALSE;
    }
    r->page = ps->page;
    r->status = (uint8_t)flags;
    for (k = 0; k < HOF_COUNT; k++)
    {
        uint32_t off = pool_add(s, fields[k], strlen(fields[k]));
        if (off == (uint32_t)-1)
        {
            return FALSE;
        }
        s->recs[s->nrecs - 1].f[k] = off;
    }
    ps->cur = (int32_t)(s->nrecs - 1);
    ps->cur_page = ps->page;
    StringCchCopyA(ps->cur_guid, ARRAYSIZE(ps->cur_guid), fields[HOF_GUID]);
    return TRUE;
}

/* Add the rows in [y_from, y_to) to the current record. */
static BOOL parse_rows(ParseState *ps, PageCells *p, float y_from, float y_to, float split)
{
    HartOcrStore *s = ps->store;
    uint32_t i, nt = 0, no = 0;
    uint32_t *t_idx, *o_idx;
    float *t_y0, *t_y1;
    char *t_text = NULL;
    BOOL ok = TRUE;
    size_t cap = (size_t)p->n + 1;

    t_idx = (uint32_t *)malloc(cap * sizeof(uint32_t));
    o_idx = (uint32_t *)malloc(cap * sizeof(uint32_t));
    t_y0 = (float *)malloc(cap * sizeof(float));
    t_y1 = (float *)malloc(cap * sizeof(float));
    if (t_idx == NULL || o_idx == NULL || t_y0 == NULL || t_y1 == NULL)
    {
        ok = FALSE;
        goto done;
    }
    for (i = 0; i < p->n; i++)
    {
        OCell *c = &p->c[i];
        float cy = (c->y0 + c->y1) * 0.5f;
        if (c->used || cy < y_from || cy >= y_to)
        {
            continue;
        }
        c->used = 1;
        if (c->x0 < split)
        {
            t_idx[nt++] = i;
        }
        else
        {
            o_idx[no++] = i;
        }
    }
    if (nt == 0 && no == 0)
    {
        goto done;
    }
    if (ps->cur < 0)
    {
        /* table rows before any header on the first page: cannot attribute */
        goto done;
    }
    {
        /* Merge wrapped title lines: a title line close below the previous one with no
         * option beside it continues that title. Titles are kept as a merged list. */
        uint32_t m = 0, k;
        size_t tl = 0, tcap = 4096;
        uint32_t *t_off = (uint32_t *)malloc(cap * sizeof(uint32_t));
        uint32_t *t_len = (uint32_t *)malloc(cap * sizeof(uint32_t));
        t_text = (char *)malloc(tcap);
        if (t_off == NULL || t_len == NULL || t_text == NULL)
        {
            free(t_off);
            free(t_len);
            ok = FALSE;
            goto done;
        }
        for (i = 0; i < nt; i++)
        {
            const OCell *c = &p->c[t_idx[i]];
            size_t n;
            const char *txt = cell_str(p, c, &n);
            BOOL merge = FALSE;
            if (m > 0)
            {
                float lh = t_y1[m - 1] - t_y0[m - 1];
                float gap = c->y0 - t_y1[m - 1];
                float cy = (c->y0 + c->y1) * 0.5f;
                BOOL has_opt = FALSE;
                for (k = 0; k < no; k++)
                {
                    const OCell *o = &p->c[o_idx[k]];
                    float oy = (o->y0 + o->y1) * 0.5f;
                    if (oy > cy - 0.5f * (c->y1 - c->y0) && oy < cy + 0.5f * (c->y1 - c->y0))
                    {
                        has_opt = TRUE;
                        break;
                    }
                }
                /* a wrapped line sits ~13.3pt below; the next row ~17.3pt */
                merge = (!has_opt && gap < 0.85f * (lh > 1.0f ? lh : 1.0f) &&
                         c->y0 - t_y0[m - 1] < 15.5f * p->ppt);
            }
            if (tl + n + 2 > tcap)
            {
                char *nt2;
                while (tl + n + 2 > tcap)
                    tcap *= 2;
                nt2 = (char *)realloc(t_text, tcap);
                if (nt2 == NULL)
                {
                    free(t_off);
                    free(t_len);
                    ok = FALSE;
                    goto done;
                }
                t_text = nt2;
            }
            if (merge)
            {
                t_text[t_off[m - 1] + t_len[m - 1]] = ' ';
                memcpy(t_text + tl, txt, n); /* contiguous: previous title ends at tl-1 */
                t_len[m - 1] += (uint32_t)(n + 1);
                tl += n;
                t_text[tl++] = '\0';
                t_y1[m - 1] = c->y1;
            }
            else
            {
                t_off[m] = (uint32_t)tl;
                t_len[m] = (uint32_t)n;
                memcpy(t_text + tl, txt, n);
                tl += n;
                t_text[tl++] = '\0';
                t_y0[m] = c->y0;
                t_y1[m] = c->y1;
                m++;
            }
        }
        /* Each option to the title whose (padded) span holds its centre, else nearest. */
        {
            uint32_t *opt_of = (uint32_t *)malloc(cap * sizeof(uint32_t));
            uint32_t *nopt = (uint32_t *)calloc(m + 1, sizeof(uint32_t));
            uint32_t r;
            if (opt_of == NULL || nopt == NULL)
            {
                free(opt_of);
                free(nopt);
                free(t_off);
                free(t_len);
                ok = FALSE;
                goto done;
            }
            for (k = 0; k < no; k++)
            {
                const OCell *o = &p->c[o_idx[k]];
                float oy = (o->y0 + o->y1) * 0.5f;
                float best = 1e9f;
                uint32_t bi = UINT32_MAX;
                for (r = 0; r < m; r++)
                {
                    float pad = 0.5f * (t_y1[r] - t_y0[r]);
                    float d;
                    if (oy >= t_y0[r] - pad && oy <= t_y1[r] + pad)
                    {
                        d = 0.0f;
                    }
                    else
                    {
                        float tc = (t_y0[r] + t_y1[r]) * 0.5f;
                        d = (oy > tc) ? oy - tc : tc - oy;
                        if (d > 9.0f * p->ppt)
                        {
                            continue;
                        }
                    }
                    if (d < best)
                    {
                        best = d;
                        bi = r;
                    }
                }
                opt_of[k] = bi;
                if (bi != UINT32_MAX)
                {
                    nopt[bi]++;
                }
                else
                {
                    s->recs[ps->cur].status |= RF_ORPHAN_OPTION;
                    ps->issues++;
                }
            }
            for (r = 0; r < m && ok; r++)
            {
                uint32_t toff = pool_add(s, t_text + t_off[r], t_len[r]);
                if (toff == (uint32_t)-1)
                {
                    ok = FALSE;
                    break;
                }
                if (nopt[r] == 0)
                {
                    s->recs[ps->cur].status |= RF_MISSING_OPTION;
                    ps->issues++;
                    ok = store_add_row(s, toff, 0);
                    continue;
                }
                for (k = 0; k < no && ok; k++)
                {
                    if (opt_of[k] == r)
                    {
                        size_t n;
                        char sp[64];
                        const char *v = cell_str(p, &p->c[o_idx[k]], &n);
                        uint32_t voff;
                        if (special_option(v, n, sp, sizeof(sp)))
                        {
                            voff = pool_add(s, sp, strlen(sp));
                        }
                        else
                        {
                            voff = pool_add(s, v, n);
                        }
                        ok = (voff != (uint32_t)-1) && store_add_row(s, toff, voff);
                    }
                }
            }
            if (ok)
            {
                s->recs[ps->cur].nrows = s->nrows - s->recs[ps->cur].first_row;
            }
            free(opt_of);
            free(nopt);
        }
        free(t_off);
        free(t_len);
    }
done:
    free(t_idx);
    free(o_idx);
    free(t_y0);
    free(t_y1);
    free(t_text);
    return ok;
}

/* Parse one OCR'd page into the store. */
static BOOL parse_page(ParseState *ps, PageCells *p)
{
    uint32_t i;
    float furniture = 0.0f;
    float split = p->w * 0.5f;
    uint32_t tbl[64];
    uint32_t ntbl = 0;

    if (p->n == 0)
    {
        return TRUE;
    }
    qsort(p->c, p->n, sizeof(OCell), cell_cmp);

    /* Page banner (report title, run date, "Page N of M", ballot/precinct counters):
     * everything down to the last "<n> of <m>" counter in the top quarter. */
    for (i = 0; i < p->n; i++)
    {
        size_t n;
        const char *s = cell_str(p, &p->c[i], &n);
        if (p->c[i].y1 < p->h * 0.25f && n >= 6)
        {
            size_t k;
            for (k = 1; k + 3 < n; k++)
            {
                if (s[k] == ' ' && s[k + 1] == 'o' && s[k + 2] == 'f' && s[k + 3] == ' ' &&
                    s[k - 1] >= '0' && s[k - 1] <= '9')
                {
                    if (p->c[i].y1 > furniture)
                    {
                        furniture = p->c[i].y1;
                    }
                    break;
                }
            }
        }
    }
    if (furniture == 0.0f)
    {
        furniture = 0.15f * p->h;
    }
    for (i = 0; i < p->n; i++)
    {
        if ((p->c[i].y0 + p->c[i].y1) * 0.5f < furniture + 2.0f)
        {
            p->c[i].used = 1;
        }
    }

    /* Table headers ("Contest Title"); the Option header sets the column split. */
    for (i = 0; i < p->n && ntbl < ARRAYSIZE(tbl); i++)
    {
        size_t n;
        const char *s;
        if (p->c[i].used)
        {
            continue;
        }
        s = cell_str(p, &p->c[i], &n);
        if (is_table_header(s, n))
        {
            uint32_t k;
            tbl[ntbl++] = i;
            p->c[i].used = 1;
            for (k = 0; k < p->n; k++)
            {
                const OCell *o = &p->c[k];
                size_t on;
                const char *os;
                if (o->used || k == i)
                {
                    continue;
                }
                os = cell_str(p, o, &on);
                if (fabsf(((o->y0 + o->y1) - (p->c[i].y0 + p->c[i].y1)) * 0.5f) < 6.0f * p->ppt &&
                    is_option_header(os, on))
                {
                    p->c[k].used = 1;
                    split = ((p->c[i].x0 + p->c[i].x1) * 0.5f + (o->x0 + o->x1) * 0.5f) * 0.5f;
                    break;
                }
            }
        }
    }

    /* Header labels and Cvr Id values. */
    for (i = 0; i < p->n; i++)
    {
        size_t n, vn = 0;
        const char *s, *v = NULL;
        char g[40];
        int rep = 0;
        if (p->c[i].used)
        {
            continue;
        }
        s = cell_str(p, &p->c[i], &n);
        p->c[i].label = match_label(s, n, &v, &vn);
        /* A Cvr Id is recognized by its value even when OCR garbles the label. */
        {
            const char *gv = s;
            size_t gn = n;
            size_t k;
            for (k = 0; k < n && k < 24; k++)
            {
                if (s[k] == ':' || s[k] == ';')
                {
                    gv = s + k + 1;
                    gn = n - k - 1;
                    break;
                }
            }
            if (repair_guid(gv, gn, g, sizeof(g), &rep))
            {
                p->c[i].label = HOF_GUID;
                p->c[i].is_guid = 1;
            }
        }
    }

    /* Walk tables top to bottom: the header block above each table, then its rows. */
    {
        float rows_from = furniture + 2.0f;
        uint32_t t;
        for (t = 0; t <= ntbl; t++)
        {
            float hdr_top, hdr_bot;
            if (t < ntbl)
            {
                const OCell *th = &p->c[tbl[t]];
                float win = 90.0f * p->ppt; /* header block is ~5 rows of 14.4pt */
                hdr_bot = th->y0;
                hdr_top = hdr_bot;
                for (i = 0; i < p->n; i++)
                {
                    const OCell *c = &p->c[i];
                    if (c->label >= 0 && !c->used && c->y0 >= hdr_bot - win &&
                        c->y1 <= hdr_bot + 1.0f && c->y0 < hdr_top)
                    {
                        hdr_top = c->y0;
                    }
                }
            }
            else
            {
                hdr_top = hdr_bot = p->h + 1.0f;
            }
            /* rows of the previous table (or continuation rows at the page top) */
            if (!parse_rows(ps, p, rows_from, hdr_top - 1.0f, split))
            {
                return FALSE;
            }
            if (t == ntbl)
            {
                break;
            }
            /* header block */
            {
                char fields[HOF_COUNT][256];
                int guid_ok = 0, flags = 0;
                ZeroMemory(fields, sizeof(fields));
                for (i = 0; i < p->n; i++)
                {
                    OCell *c = &p->c[i];
                    float cy = (c->y0 + c->y1) * 0.5f;
                    size_t n, vn = 0;
                    const char *s, *v = NULL;
                    if (c->used || cy < hdr_top - 1.0f || cy > hdr_bot + 1.0f)
                    {
                        continue;
                    }
                    c->used = 1;
                    s = cell_str(p, c, &n);
                    if (c->is_guid)
                    {
                        const char *gv = s;
                        size_t gn = n, q;
                        int rep = 0;
                        for (q = 0; q < n && q < 24; q++)
                        {
                            if (s[q] == ':' || s[q] == ';')
                            {
                                gv = s + q + 1;
                                gn = n - q - 1;
                                break;
                            }
                        }
                        if (!guid_ok && repair_guid(gv, gn, fields[HOF_GUID], 256, &rep))
                        {
                            guid_ok = 1;
                            if (rep)
                            {
                                flags |= RF_GUID_REPAIRED;
                            }
                        }
                        continue;
                    }
                    if (c->label >= 0 && c->label != HOF_GUID)
                    {
                        match_label(s, n, &v, &vn);
                        if (vn >= 256)
                        {
                            vn = 255;
                        }
                        if (v != NULL && fields[c->label][0] == '\0' && has_alnum(v, vn))
                        {
                            /* (a value with no letter or digit is OCR reading a
                             * redaction box -- e.g. "?" or "-" -- and is left blank) */
                            memcpy(fields[c->label], v, vn);
                            fields[c->label][vn] = '\0';
                        }
                    }
                }
                normalize_precinct(fields[HOF_PRECINCT]);
                fix_number(fields[HOF_BATCH]);
                if (!guid_ok)
                {
                    flags |= RF_GUID_INVALID;
                }
                {
                    /* first contest title of this table (topmost title-column cell) */
                    char first_title[256];
                    float best = 1e9f, ty = p->c[tbl[t]].y1;
                    first_title[0] = '\0';
                    for (i = 0; i < p->n; i++)
                    {
                        const OCell *c = &p->c[i];
                        if (!c->used && c->x0 < split && c->y0 > ty && c->y0 < best)
                        {
                            size_t n;
                            const char *s = cell_str(p, c, &n);
                            if (n >= sizeof(first_title))
                            {
                                n = sizeof(first_title) - 1;
                            }
                            memcpy(first_title, s, n);
                            first_title[n] = '\0';
                            best = c->y0;
                        }
                    }
                    if (!begin_block(ps, fields, guid_ok, flags, t == 0, first_title))
                    {
                        return FALSE;
                    }
                }
            }
            rows_from = p->c[tbl[t]].y1 + 1.0f;
        }
    }
    return TRUE;
}

/* -------------------------------------------------------------------------- */
/* OCR of one page                                                            */
/* -------------------------------------------------------------------------- */

typedef struct OcrPage
{
    EePdfImage img;
    EeOcrText text;
    PageCells cells;
    EePdfPageText pt;
} OcrPage;

static void ocrpage_free(OcrPage *op)
{
    EePdf_ImageFree(&op->img);
    EeOcr_TextFree(&op->text);
    cells_free(&op->cells);
    EePdf_PageTextFree(&op->pt);
}

static BOOL ocr_page(EeOcr *ocr,
                     EePdf *pdf,
                     uint32_t page,
                     double scale,
                     OcrPage *op,
                     wchar_t *err,
                     size_t errcch)
{
    BOOL ok;
    if (!EePdf_GetPageImage(pdf, page, &op->img))
    {
        if (err != NULL)
        {
            StringCchPrintfW(err, errcch, L"page %u: the page image could not be read.", page + 1);
        }
        return FALSE;
    }
    if (op->img.kind == EE_PDF_IMAGE_NONE)
    {
        op->text.nwords = op->text.nlines = 0;
        op->cells.n = 0;
        return TRUE;
    }
    if (op->img.kind == EE_PDF_IMAGE_ENCODED)
    {
        ok = EeOcr_RecognizeEncoded(ocr, op->img.data, op->img.len, scale, &op->text, err, errcch);
    }
    else
    {
        ok = EeOcr_RecognizePixels(ocr,
                                   op->img.data,
                                   op->img.width,
                                   op->img.height,
                                   op->img.stride,
                                   (int)op->img.channels,
                                   scale,
                                   &op->text,
                                   err,
                                   errcch);
    }
    return ok && build_cells(&op->text, &op->cells);
}

BOOL HartOcr_PageIsImageOnly(EePdf *pdf, uint32_t page)
{
    EePdfPageText pt;
    EePdfImage img;
    BOOL r = FALSE;
    EePdf_PageTextInit(&pt);
    ZeroMemory(&img, sizeof(img));
    if (EePdf_ExtractPageText(pdf, page, &pt) && pt.nruns == 0 &&
        EePdf_GetPageImage(pdf, page, &img) && img.kind != EE_PDF_IMAGE_NONE)
    {
        r = TRUE;
    }
    EePdf_PageTextFree(&pt);
    EePdf_ImageFree(&img);
    return r;
}

BOOL HartOcr_ProbeHart(EeOcr *ocr, EePdf *pdf, uint32_t page, wchar_t *err, size_t errcch)
{
    OcrPage op;
    uint32_t i;
    BOOL title = FALSE, table = FALSE, guid = FALSE;
    ZeroMemory(&op, sizeof(op));
    if (!ocr_page(ocr, pdf, page, 1.0, &op, err, errcch))
    {
        ocrpage_free(&op);
        return FALSE;
    }
    for (i = 0; i < op.cells.n; i++)
    {
        size_t n;
        const char *s = cell_str(&op.cells, &op.cells.c[i], &n);
        char g[40];
        if (canon_matches(s, n, "CVR Report"))
        {
            title = TRUE;
        }
        else if (is_table_header(s, n))
        {
            table = TRUE;
        }
        else
        {
            size_t k;
            const char *gv = s;
            size_t gn = n;
            for (k = 0; k < n && k < 24; k++)
            {
                if (s[k] == ':' || s[k] == ';')
                {
                    gv = s + k + 1;
                    gn = n - k - 1;
                    break;
                }
            }
            if (repair_guid(gv, gn, g, sizeof(g), NULL))
            {
                guid = TRUE;
            }
        }
    }
    ocrpage_free(&op);
    if (!(title && table && guid) && err != NULL)
    {
        StringCchCopyW(err, errcch, L"the scanned page does not look like a Hart CVR Report.");
    }
    return title && table && guid;
}

/* -------------------------------------------------------------------------- */
/* Cross-record correction                                                    */
/* -------------------------------------------------------------------------- */

typedef struct VEnt
{
    uint32_t off;   /* string */
    uint32_t group; /* comparison group (field / contest) */
    uint32_t count;
    uint32_t map;   /* corrected entry index (self when unchanged) */
    char key[96];   /* uppercase, spaces removed */
    char digits[16];
    char vkey[96];  /* key with OCR look-alikes folded (visual_key) */
} VEnt;

typedef struct Vocab
{
    VEnt *e;
    uint32_t n;
    uint32_t cap;
    uint32_t *hash; /* slot -> index + 1 */
    uint32_t hcap;
} Vocab;

static uint32_t vhash(const char *s, uint32_t group)
{
    uint32_t h = 2166136261u ^ group * 16777619u;
    for (; *s; s++)
    {
        h ^= (unsigned char)*s;
        h *= 16777619u;
    }
    return h;
}

/* Fold the characters and pairs OCR confuses, and drop punctuation, so that two
 * readings of the same printed text compare equal ("MARKI_JM" ~ "MARKUM",
 * "ME-RTTON" ~ "MERTTON", "0" ~ "O", "1"/"L"/"|" ~ "I", "RN" ~ "M"). Input is the
 * uppercase, space-free key. */
static void visual_key(const char *k, char *out, size_t cap)
{
    size_t o = 0;
    for (; *k && o + 1 < cap; k++)
    {
        char c = *k;
        if (c == '_' || c == '-' || c == '.' || c == ',' || c == '\'' || c == '`')
        {
            continue;
        }
        if ((c == 'I' || c == 'L') && k[1] == 'J')
        {
            out[o++] = 'U';
            k++;
            continue;
        }
        if (c == 'I' && k[1] == '_' && k[2] == 'J')
        {
            out[o++] = 'U';
            k += 2;
            continue;
        }
        if (c == 'R' && k[1] == 'N')
        {
            out[o++] = 'M';
            k++;
            continue;
        }
        if (c == 'V' && k[1] == 'V')
        {
            out[o++] = 'W';
            k++;
            continue;
        }
        if (c == '0')
            c = 'O';
        else if (c == '1' || c == 'L' || c == '|' || c == '!')
            c = 'I';
        else if (c == '5')
            c = 'S';
        else if (c == '8')
            c = 'B';
        out[o++] = c;
    }
    out[o] = '\0';
}

static void vocab_free(Vocab *v)
{
    free(v->e);
    free(v->hash);
    ZeroMemory(v, sizeof(*v));
}

static BOOL vocab_rehash(Vocab *v, const HartOcrStore *s, uint32_t ncap)
{
    uint32_t *nh = (uint32_t *)calloc(ncap, sizeof(uint32_t));
    uint32_t i;
    if (nh == NULL)
    {
        return FALSE;
    }
    for (i = 0; i < v->n; i++)
    {
        uint32_t sl = vhash(HartOcr_Str(s, v->e[i].off), v->e[i].group) & (ncap - 1);
        while (nh[sl] != 0)
        {
            sl = (sl + 1) & (ncap - 1);
        }
        nh[sl] = i + 1;
    }
    free(v->hash);
    v->hash = nh;
    v->hcap = ncap;
    return TRUE;
}

/* Count string @p off in @p group; returns the entry index or UINT32_MAX on OOM. */
static uint32_t vocab_add(Vocab *v, const HartOcrStore *s, uint32_t off, uint32_t group)
{
    const char *str = HartOcr_Str(s, off);
    uint32_t sl;
    if (v->hcap == 0 || (v->n + 1) * 2 > v->hcap)
    {
        if (!vocab_rehash(v, s, v->hcap ? v->hcap * 2 : 256))
        {
            return UINT32_MAX;
        }
    }
    sl = vhash(str, group) & (v->hcap - 1);
    while (v->hash[sl] != 0)
    {
        VEnt *e = &v->e[v->hash[sl] - 1];
        if (e->group == group && strcmp(HartOcr_Str(s, e->off), str) == 0)
        {
            e->count++;
            return v->hash[sl] - 1;
        }
        sl = (sl + 1) & (v->hcap - 1);
    }
    if (v->n == v->cap)
    {
        uint32_t nc = v->cap ? v->cap * 2 : 256;
        VEnt *ne = (VEnt *)realloc(v->e, (size_t)nc * sizeof(VEnt));
        if (ne == NULL)
        {
            return UINT32_MAX;
        }
        v->e = ne;
        v->cap = nc;
    }
    {
        VEnt *e = &v->e[v->n];
        size_t k = 0, d = 0;
        e->off = off;
        e->group = group;
        e->count = 1;
        e->map = v->n;
        for (; *str && k + 1 < sizeof(e->key); str++)
        {
            char c = *str;
            if (c == ' ' || c == '\t')
            {
                continue;
            }
            if (c >= 'a' && c <= 'z')
            {
                c = (char)(c - 'a' + 'A');
            }
            e->key[k++] = c;
            if (c >= '0' && c <= '9' && d + 1 < sizeof(e->digits))
            {
                e->digits[d++] = c;
            }
        }
        e->key[k] = '\0';
        e->digits[d] = '\0';
        visual_key(e->key, e->vkey, sizeof(e->vkey));
    }
    v->hash[sl] = v->n + 1;
    return v->n++;
}

/* Map rare entries onto an unambiguous, much more frequent close spelling in the same
 * group (same digits, small edit distance on the space-free uppercase form). */
static void vocab_resolve(Vocab *v)
{
    uint32_t i, j;
    for (i = 0; i < v->n; i++)
    {
        VEnt *a = &v->e[i];
        uint32_t best = UINT32_MAX;
        int bestd = 1000, ties = 0;
        size_t la = strlen(a->key);
        int thr = (la <= 4) ? 0 : (la <= 10 ? 1 : (la <= 24 ? 2 : 3));
        /* 1. Same text once OCR look-alikes are folded (e.g. "JOY MARKI_JM" for
         *    "JOY MARKUM", which OCR produced on 52 of 122 ballots): the same value,
         *    whatever the counts -- map to the most frequent such spelling. */
        if (strlen(a->vkey) >= 5)
        {
            uint32_t vb = UINT32_MAX;
            for (j = 0; j < v->n; j++)
            {
                VEnt *b = &v->e[j];
                if (j != i && b->group == a->group && b->count > a->count &&
                    strcmp(a->vkey, b->vkey) == 0 &&
                    (vb == UINT32_MAX || b->count > v->e[vb].count))
                {
                    vb = j;
                }
            }
            if (vb != UINT32_MAX)
            {
                a->map = vb;
                continue;
            }
        }
        /* 2. A rare spelling close to a much more common one. */
        if (la <= 4)
        {
            continue;
        }
        for (j = 0; j < v->n; j++)
        {
            VEnt *b = &v->e[j];
            int d;
            if (j == i || b->group != a->group || b->count < 3 || b->count < 4 * a->count ||
                strcmp(a->digits, b->digits) != 0)
            {
                continue;
            }
            d = (strcmp(a->key, b->key) == 0) ? 0 : edit_distance(a->key, b->key);
            if (d > thr)
            {
                /* OCR dropped a word: a long fragment of exactly one common value
                 * ("D. HARRIS TIM WALZ" of "KAMALA D. HARRIS TIM WALZ") */
                size_t lb = strlen(b->key);
                if (la >= 8 && la * 10 >= lb * 6 && strstr(b->key, a->key) != NULL)
                {
                    d = thr + 1; /* ranks after any genuine near-spelling */
                }
                else
                {
                    continue;
                }
            }
            if (d < bestd)
            {
                bestd = d;
                best = j;
                ties = 0;
            }
            else if (d == bestd)
            {
                ties++;
            }
        }
        if (best != UINT32_MAX && ties == 0)
        {
            a->map = best;
        }
    }
}

/* Group max count, for the "rare value" review rule. */
static uint32_t vocab_group_max(const Vocab *v, uint32_t group)
{
    uint32_t i, m = 0;
    for (i = 0; i < v->n; i++)
    {
        if (v->e[i].group == group && v->e[i].map == i && v->e[i].count > m)
        {
            m = v->e[i].count;
        }
    }
    return m;
}

static BOOL correct_store(HartOcrStore *s)
{
    Vocab vt, vo, vf;
    uint32_t r, k;
    uint32_t *row_title_e = NULL;
    BOOL ok = FALSE;
    static const int k_Fields[] = {HOF_PRECINCT, HOF_PARTY, HOF_PPLACE, HOF_VTYPE, HOF_DTYPE};

    ZeroMemory(&vt, sizeof(vt));
    ZeroMemory(&vo, sizeof(vo));
    ZeroMemory(&vf, sizeof(vf));
    row_title_e = (uint32_t *)malloc(((size_t)s->nrows + 1) * sizeof(uint32_t));
    if (row_title_e == NULL)
    {
        goto done;
    }
    /* 1. contest titles */
    for (r = 0; r < s->nrows; r++)
    {
        row_title_e[r] = vocab_add(&vt, s, s->rows[r].title, 0);
        if (row_title_e[r] == UINT32_MAX)
        {
            goto done;
        }
    }
    vocab_resolve(&vt);
    for (r = 0; r < s->nrows; r++)
    {
        VEnt *e = &vt.e[row_title_e[r]];
        if (e->map != row_title_e[r])
        {
            s->rows[r].title = vt.e[e->map].off;
            s->corrected_values++;
        }
    }
    /* 2. options, grouped by the (corrected) contest's vocabulary entry -- not by the
     *    row's title offset: every row holds its own copy of the title string. */
    for (r = 0; r < s->nrows; r++)
    {
        if (s->rows[r].option != 0 &&
            vocab_add(&vo, s, s->rows[r].option, vt.e[row_title_e[r]].map) == UINT32_MAX)
        {
            goto done;
        }
    }
    vocab_resolve(&vo);
    /* 3. header text fields */
    for (r = 0; r < s->nrecs; r++)
    {
        for (k = 0; k < ARRAYSIZE(k_Fields); k++)
        {
            uint32_t off = s->recs[r].f[k_Fields[k]];
            if (off != 0 && vocab_add(&vf, s, off, (uint32_t)k_Fields[k]) == UINT32_MAX)
            {
                goto done;
            }
        }
    }
    vocab_resolve(&vf);

    /* Apply option + field corrections per record and set statuses. */
    for (r = 0; r < s->nrecs; r++)
    {
        HartOcrRecord *rec = &s->recs[r];
        uint32_t flags = rec->status;
        uint32_t q;
        for (q = rec->first_row; q < rec->first_row + rec->nrows; q++)
        {
            HartOcrRow *row = &s->rows[q];
            uint32_t grp = vt.e[row_title_e[q]].map;
            uint32_t sl;
            if (row_title_e[q] != vt.e[row_title_e[q]].map)
            {
                flags |= RF_VALUE_SNAPPED;
            }
            if (row->option == 0)
            {
                continue;
            }
            sl = vhash(HartOcr_Str(s, row->option), grp) & (vo.hcap - 1);
            while (vo.hash[sl] != 0)
            {
                VEnt *e = &vo.e[vo.hash[sl] - 1];
                if (e->group == grp &&
                    strcmp(HartOcr_Str(s, e->off), HartOcr_Str(s, row->option)) == 0)
                {
                    uint32_t idx = vo.hash[sl] - 1;
                    if (e->map != idx)
                    {
                        row->option = vo.e[e->map].off;
                        s->corrected_values++;
                        flags |= RF_VALUE_SNAPPED;
                    }
                    else if (e->count <= 2 && vocab_group_max(&vo, e->group) >= 20 &&
                             strncmp(HartOcr_Str(s, row->option), "Undervotes:", 11) != 0)
                    {
                        flags |= RF_RARE_VALUE;
                    }
                    break;
                }
                sl = (sl + 1) & (vo.hcap - 1);
            }
        }
        for (k = 0; k < ARRAYSIZE(k_Fields); k++)
        {
            uint32_t off = rec->f[k_Fields[k]];
            uint32_t sl;
            if (off == 0)
            {
                continue;
            }
            sl = vhash(HartOcr_Str(s, off), (uint32_t)k_Fields[k]) & (vf.hcap - 1);
            while (vf.hash[sl] != 0)
            {
                VEnt *e = &vf.e[vf.hash[sl] - 1];
                if (e->group == (uint32_t)k_Fields[k] &&
                    strcmp(HartOcr_Str(s, e->off), HartOcr_Str(s, off)) == 0)
                {
                    if (e->map != vf.hash[sl] - 1)
                    {
                        rec->f[k_Fields[k]] = vf.e[e->map].off;
                        s->corrected_values++;
                        flags |= RF_VALUE_SNAPPED;
                    }
                    break;
                }
                sl = (sl + 1) & (vf.hcap - 1);
            }
        }
        if (rec->nrows == 0)
        {
            /* a header with no readable contest rows: the county redacted this
             * ballot's votes (e.g. a tiny precinct), or the table was unreadable */
            rec->status = HOCR_NOVOTES;
            s->novote_records++;
        }
        else if (flags & (RF_GUID_INVALID | RF_MISSING_OPTION | RF_ORPHAN_OPTION | RF_RARE_VALUE))
        {
            rec->status = HOCR_REVIEW;
            s->review_records++;
        }
        else if (flags & (RF_GUID_REPAIRED | RF_VALUE_SNAPPED))
        {
            rec->status = HOCR_CORRECTED;
            s->corrected_records++;
        }
        else
        {
            rec->status = HOCR_OK;
        }
    }
    ok = TRUE;
done:
    free(row_title_e);
    vocab_free(&vt);
    vocab_free(&vo);
    vocab_free(&vf);
    return ok;
}

/* -------------------------------------------------------------------------- */
/* Whole document                                                             */
/* -------------------------------------------------------------------------- */

BOOL HartOcr_ReadPdf(EeOcr *ocr,
                     EePdf *pdf,
                     HartOcrStore *store,
                     HartOcrTickFn tick,
                     void *tick_ctx,
                     BOOL *cancelled,
                     wchar_t *err,
                     size_t errcch)
{
    OcrPage op;
    ParseState ps;
    uint32_t pg, n = EePdf_PageCount(pdf);
    BOOL ok = TRUE;
    if (cancelled != NULL)
    {
        *cancelled = FALSE;
    }
    store_clear(store);
    if (pool_add(store, "", 0) == (uint32_t)-1)
    {
        return FALSE;
    }
    ZeroMemory(&op, sizeof(op));
    ZeroMemory(&ps, sizeof(ps));
    ps.store = store;
    ps.cur = -1;
    for (pg = 0; pg < n && ok; pg++)
    {
        /* OCR at native resolution; if the page shows problems (unreadable Cvr Id, a
         * contest without its option, an option without a contest), re-read it enlarged
         * -- OCR often resolves a character at a different scale -- and keep the reading
         * with the fewest problems. */
        static const double k_Scales[] = {1.0, 1.5, 2.0};
        ParseState snap_ps = ps;
        uint32_t snap_recs = store->nrecs, snap_rows = store->nrows;
        size_t snap_pool = store->pool_len;
        HartOcrRecord snap_cur;
        uint32_t best_issues = UINT32_MAX;
        double best_scale = 1.0;
        uint32_t a;
        ZeroMemory(&snap_cur, sizeof(snap_cur));
        if (ps.cur >= 0)
        {
            snap_cur = store->recs[ps.cur];
        }
        for (a = 0; a <= ARRAYSIZE(k_Scales) && ok; a++)
        {
            double sc = (a < ARRAYSIZE(k_Scales)) ? k_Scales[a] : best_scale;
            if (a == ARRAYSIZE(k_Scales) && best_scale == k_Scales[ARRAYSIZE(k_Scales) - 1])
            {
                break; /* the last attempt is already the best one */
            }
            if (a > 0)
            {
                /* roll back the previous attempt */
                ps = snap_ps;
                store->nrecs = snap_recs;
                store->nrows = snap_rows;
                store->pool_len = snap_pool;
                if (ps.cur >= 0)
                {
                    store->recs[ps.cur] = snap_cur;
                }
            }
            ps.page = pg;
            ps.issues = 0;
            if (!ocr_page(ocr, pdf, pg, sc, &op, err, errcch))
            {
                ok = FALSE;
                break;
            }
            if (!parse_page(&ps, &op.cells))
            {
                if (err != NULL)
                {
                    StringCchCopyW(err, errcch, L"Out of memory reading the scanned report.");
                }
                ok = FALSE;
                break;
            }
            if (a == ARRAYSIZE(k_Scales))
            {
                break; /* re-ran the best scale */
            }
            if (ps.issues < best_issues)
            {
                best_issues = ps.issues;
                best_scale = sc;
            }
            if (ps.issues == 0)
            {
                break;
            }
            if (a + 1 == ARRAYSIZE(k_Scales) && best_scale == sc)
            {
                break; /* the current state is already the best reading */
            }
        }
        if (!ok)
        {
            break;
        }
        store->pages++;
        if (tick != NULL && !tick(tick_ctx))
        {
            if (cancelled != NULL)
            {
                *cancelled = TRUE;
            }
            ok = FALSE;
        }
    }
    ocrpage_free(&op);
    if (ok && !correct_store(store))
    {
        if (err != NULL)
        {
            StringCchCopyW(err, errcch, L"Out of memory reading the scanned report.");
        }
        ok = FALSE;
    }
    return ok;
}
