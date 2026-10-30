/**
 * @file hart_hvs.c
 * @brief Reader for the Hart HVS "Votes by Precinct" CVR report. See hart_hvs.h.
 */

#include "hart_hvs.h"

#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strsafe.h>

#define HVS_CELL 512 /* longest line text kept */

/* -------------------------------------------------------------------------- */
/* Store: interned strings, records, rows                                     */
/* -------------------------------------------------------------------------- */

typedef struct Intern
{
    HartOcrStore *s;
    uint32_t *slot; /* pool offset, 0 = empty */
    uint32_t cap;   /* power of two */
    uint32_t n;
} Intern;

static uint32_t str_hash(const char *p, size_t n)
{
    uint32_t h = 2166136261u;
    size_t i;
    for (i = 0; i < n; i++)
    {
        h ^= (unsigned char)p[i];
        h *= 16777619u;
    }
    return h;
}

/* Append [p,n) + NUL to the pool; its offset, or UINT32_MAX on OOM. Offset 0 is "". */
static uint32_t pool_raw(HartOcrStore *s, const char *p, size_t n)
{
    uint32_t off;
    if (s->pool_len == 0)
    {
        s->pool = (char *)malloc(65536);
        if (s->pool == NULL)
        {
            return UINT32_MAX;
        }
        s->pool_cap = 65536;
        s->pool[0] = '\0';
        s->pool_len = 1;
    }
    if (s->pool_len + n + 1 > 0xFFFFFFF0u)
    {
        return UINT32_MAX;
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
            return UINT32_MAX;
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

static BOOL intern_grow(Intern *in)
{
    uint32_t nc = in->cap ? in->cap * 2 : 1024;
    uint32_t *ns = (uint32_t *)calloc(nc, sizeof(uint32_t));
    uint32_t i;
    if (ns == NULL)
    {
        return FALSE;
    }
    for (i = 0; i < in->cap; i++)
    {
        uint32_t off = in->slot[i];
        if (off != 0)
        {
            const char *p = in->s->pool + off;
            uint32_t k = str_hash(p, strlen(p)) & (nc - 1);
            while (ns[k] != 0)
            {
                k = (k + 1) & (nc - 1);
            }
            ns[k] = off;
        }
    }
    free(in->slot);
    in->slot = ns;
    in->cap = nc;
    return TRUE;
}

/* The pool offset of the string [p,n) (added once); 0 for "", UINT32_MAX on OOM. */
static uint32_t intern(Intern *in, const char *p, size_t n)
{
    uint32_t k;
    if (n == 0)
    {
        return 0;
    }
    if ((in->n + 1) * 2 > in->cap && !intern_grow(in))
    {
        return UINT32_MAX;
    }
    k = str_hash(p, n) & (in->cap - 1);
    while (in->slot[k] != 0)
    {
        const char *q = in->s->pool + in->slot[k];
        if (strlen(q) == n && memcmp(q, p, n) == 0)
        {
            return in->slot[k];
        }
        k = (k + 1) & (in->cap - 1);
    }
    in->slot[k] = pool_raw(in->s, p, n);
    if (in->slot[k] == UINT32_MAX)
    {
        in->slot[k] = 0;
        return UINT32_MAX;
    }
    in->n++;
    return in->slot[k];
}

static HartOcrRecord *rec_add(HartOcrStore *s)
{
    if (s->nrecs == s->cap_recs)
    {
        uint32_t nc = s->cap_recs ? s->cap_recs * 2 : 1024;
        HartOcrRecord *nr = (HartOcrRecord *)realloc(s->recs, (size_t)nc * sizeof(HartOcrRecord));
        if (nr == NULL)
        {
            return NULL;
        }
        s->recs = nr;
        s->cap_recs = nc;
    }
    ZeroMemory(&s->recs[s->nrecs], sizeof(HartOcrRecord));
    return &s->recs[s->nrecs++];
}

static BOOL row_add(HartOcrStore *s, uint32_t title, uint32_t option)
{
    if (s->nrows == s->cap_rows)
    {
        uint32_t nc = s->cap_rows ? s->cap_rows * 2 : 16384;
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
/* Lines of a page                                                            */
/* -------------------------------------------------------------------------- */

typedef struct HLine
{
    uint32_t first; /* into the sorted run index */
    uint32_t count;
} HLine;

typedef struct HPage
{
    uint32_t *ord; /* run indices: kept runs by baseline (top first), then x */
    uint32_t nord;
    uint32_t cap_ord;
    HLine *lines;
    uint32_t nlines;
    uint32_t cap_lines;
} HPage;

static float run_width(const EePdfTextRun *r)
{
    return r->has_width ? r->width : 0.55f * r->font_size * (float)r->text_len;
}

/* Bytes of a space at @p s (an ASCII space or tab, or a UTF-8 no-break space), else 0. */
static size_t space_len(const char *s, size_t n)
{
    if (n >= 1 && (s[0] == ' ' || s[0] == '\t' || s[0] == '\r' || s[0] == '\n'))
    {
        return 1;
    }
    if (n >= 2 && (unsigned char)s[0] == 0xC2 && (unsigned char)s[1] == 0xA0)
    {
        return 2;
    }
    return 0;
}

/* A run worth reading: text of normal size with something besides spaces (PostScript copies
 * draw 1-point spaces at the table's edges, and no-break spaces at the page's). */
static BOOL run_kept(const EePdfPageText *pt, const EePdfTextRun *r)
{
    uint32_t i = 0;
    if (r->font_size < 3.0f)
    {
        return FALSE;
    }
    while (i < r->text_len)
    {
        size_t sp = space_len(pt->text + r->text_off + i, r->text_len - i);
        if (sp == 0)
        {
            return TRUE;
        }
        i += (uint32_t)sp;
    }
    return FALSE;
}

static int __cdecl run_cmp(void *ctx, const void *a, const void *b)
{
    const EePdfPageText *pt = (const EePdfPageText *)ctx;
    const EePdfTextRun *ra = &pt->runs[*(const uint32_t *)a];
    const EePdfTextRun *rb = &pt->runs[*(const uint32_t *)b];
    if (ra->y != rb->y)
    {
        return ra->y > rb->y ? -1 : 1;
    }
    if (ra->x != rb->x)
    {
        return ra->x < rb->x ? -1 : 1;
    }
    return *(const uint32_t *)a < *(const uint32_t *)b ? -1 : 1;
}

static int __cdecl x_cmp(void *ctx, const void *a, const void *b)
{
    const EePdfPageText *pt = (const EePdfPageText *)ctx;
    const EePdfTextRun *ra = &pt->runs[*(const uint32_t *)a];
    const EePdfTextRun *rb = &pt->runs[*(const uint32_t *)b];
    if (ra->x != rb->x)
    {
        return ra->x < rb->x ? -1 : 1;
    }
    return *(const uint32_t *)a < *(const uint32_t *)b ? -1 : 1;
}

/* Sort the page's kept runs and group them into lines (baselines within 2 points). */
static BOOL page_lines(HPage *hp, const EePdfPageText *pt)
{
    uint32_t i;
    hp->nord = hp->nlines = 0;
    if (pt->nruns > hp->cap_ord)
    {
        uint32_t *no = (uint32_t *)realloc(hp->ord, (size_t)pt->nruns * sizeof(uint32_t));
        HLine *nl = (HLine *)realloc(hp->lines, (size_t)pt->nruns * sizeof(HLine));
        if (no != NULL)
        {
            hp->ord = no;
        }
        if (nl != NULL)
        {
            hp->lines = nl;
        }
        if (no == NULL || nl == NULL)
        {
            return FALSE;
        }
        hp->cap_ord = hp->cap_lines = pt->nruns;
    }
    for (i = 0; i < pt->nruns; i++)
    {
        if (run_kept(pt, &pt->runs[i]))
        {
            hp->ord[hp->nord++] = i;
        }
    }
    qsort_s(hp->ord, hp->nord, sizeof(uint32_t), run_cmp, (void *)pt);
    for (i = 0; i < hp->nord; i++)
    {
        float y = pt->runs[hp->ord[i]].y;
        if (hp->nlines > 0 && fabsf(pt->runs[hp->ord[hp->lines[hp->nlines - 1].first]].y - y) <= 2.0f)
        {
            hp->lines[hp->nlines - 1].count++;
        }
        else
        {
            hp->lines[hp->nlines].first = i;
            hp->lines[hp->nlines].count = 1;
            hp->nlines++;
        }
    }
    for (i = 0; i < hp->nlines; i++)
    {
        qsort_s(hp->ord + hp->lines[i].first, hp->lines[i].count, sizeof(uint32_t), x_cmp, (void *)pt);
    }
    return TRUE;
}

/* The text of runs [first, first+n) of the sorted index (already in x order), a space where
 * the runs are apart, whitespace collapsed and trimmed. */
static void runs_text(const EePdfPageText *pt, const uint32_t *idx, uint32_t n, char *out, size_t cap)
{
    char tmp[HVS_CELL];
    size_t len = 0, k = 0;
    float end = 0.0f;
    uint32_t i;
    BOOL space = FALSE;
    for (i = 0; i < n; i++)
    {
        const EePdfTextRun *r = &pt->runs[idx[i]];
        size_t m = r->text_len;
        if (i > 0 && r->x - end > 1.5f && len + 1 < sizeof(tmp))
        {
            tmp[len++] = ' ';
        }
        if (len + m >= sizeof(tmp))
        {
            m = sizeof(tmp) - 1 - len;
        }
        memcpy(tmp + len, pt->text + r->text_off, m);
        len += m;
        end = r->x + run_width(r);
    }
    tmp[len] = '\0';
    if (cap == 0)
    {
        return;
    }
    for (i = 0; tmp[i] != '\0'; i++)
    {
        char ch = tmp[i];
        size_t sp = space_len(tmp + i, len - i);
        if (sp > 0)
        {
            space = k > 0;
            i += (uint32_t)sp - 1;
            continue;
        }
        if (space && k + 1 < cap)
        {
            out[k++] = ' ';
        }
        space = FALSE;
        if (k + 1 < cap)
        {
            out[k++] = ch;
        }
    }
    out[k] = '\0';
}

/* @p t with the first @p label removed and the rest trimmed ("Precinct: 12", "12Precinct:"). */
static void take_value(const char *t, const char *label, char *out, size_t cap)
{
    const char *p = strstr(t, label);
    char tmp[HVS_CELL];
    size_t n = p - t;
    if (n >= sizeof(tmp))
    {
        n = sizeof(tmp) - 1;
    }
    memcpy(tmp, t, n);
    StringCchCopyA(tmp + n, sizeof(tmp) - n, p + strlen(label));
    {
        char *s = tmp;
        size_t m;
        while (*s == ' ')
        {
            s++;
        }
        m = strlen(s);
        while (m > 0 && s[m - 1] == ' ')
        {
            s[--m] = '\0';
        }
        StringCchCopyA(out, cap, s);
    }
}

/* The footer: "11/20/2024  1:51:31PM ... Page 1 of 21815". */
static BOOL is_footer(const char *t)
{
    const char *p = strstr(t, "Page ");
    unsigned a = 0, b = 0;
    return p != NULL && sscanf_s(p, "Page %u of %u", &a, &b) == 2;
}

/* -------------------------------------------------------------------------- */
/* Page parsing                                                               */
/* -------------------------------------------------------------------------- */

typedef struct HHead
{
    BOOL title;    /* "Votes by Precinct" */
    BOOL precinct; /* a "Precinct:" line */
    BOOL header;   /* the "Contest Title" table header */
    char prec[96];
    char party[64];
    char vtype[64];
    float split;   /* x where the Candidate/Option column starts */
} HHead;

static const char *const k_VoteTypes[] = {"Early Voting", "Election Day", "Absentee", "Mail", "Provisional"};

/* Read the page's header lines into @p h. Returns the index of the first table line (after
 * the "Contest Title" header), or nlines when there is none. */
static uint32_t parse_head(const HPage *hp, const EePdfPageText *pt, HHead *h)
{
    uint32_t li;
    ZeroMemory(h, sizeof(*h));
    h->split = 312.0f;
    for (li = 0; li < hp->nlines; li++)
    {
        const HLine *ln = &hp->lines[li];
        char t[HVS_CELL];
        runs_text(pt, hp->ord + ln->first, ln->count, t, sizeof(t));
        if (strstr(t, "Votes by Precinct") != NULL)
        {
            h->title = TRUE;
        }
        else if (strstr(t, "Precinct:") != NULL)
        {
            h->precinct = TRUE;
            take_value(t, "Precinct:", h->prec, sizeof(h->prec));
        }
        else if (strstr(t, "Party:") != NULL)
        {
            take_value(t, "Party:", h->party, sizeof(h->party));
        }
        else if (strncmp(t, "Contest Title", 13) == 0)
        {
            uint32_t k;
            h->header = TRUE;
            for (k = 0; k < ln->count; k++)
            {
                const EePdfTextRun *r = &pt->runs[hp->ord[ln->first + k]];
                if (r->x > 100.0f && r->text_len >= 9 && memcmp(pt->text + r->text_off, "Candidate", 9) == 0)
                {
                    h->split = r->x - 5.0f;
                }
            }
            return li + 1;
        }
        else if (h->vtype[0] == '\0')
        {
            size_t v;
            for (v = 0; v < ARRAYSIZE(k_VoteTypes); v++)
            {
                if (strstr(t, k_VoteTypes[v]) != NULL)
                {
                    StringCchCopyA(h->vtype, sizeof(h->vtype), k_VoteTypes[v]);
                    break;
                }
            }
        }
    }
    return hp->nlines;
}

BOOL HartHvs_IsReportPage(const EePdfPageText *pt)
{
    HPage hp;
    HHead h;
    BOOL ok = FALSE;
    ZeroMemory(&hp, sizeof(hp));
    if (page_lines(&hp, pt))
    {
        (void)parse_head(&hp, pt, &h);
        ok = h.title && h.precinct && h.header;
    }
    free(hp.ord);
    free(hp.lines);
    return ok;
}

/* The store's option text for a printed choice: "No Selection" is an undervote, "Write-in"
 * (with the written name beside it) the write-in marker. */
static uint32_t option_of(Intern *in, const char *choice)
{
    if (strcmp(choice, "No Selection") == 0)
    {
        return intern(in, "Undervotes: 1", 13);
    }
    if (_strnicmp(choice, "Write-in", 8) == 0)
    {
        return intern(in, "Write-in", 8);
    }
    if (_stricmp(choice, "Overvote") == 0 || _stricmp(choice, "Over Vote") == 0 || _stricmp(choice, "Overvoted") == 0)
    {
        return intern(in, "Overvote", 8);
    }
    return intern(in, choice, strlen(choice));
}

/* Add the page's table rows, after header line @p from, to the store (the open record). */
static BOOL page_rows(const HPage *hp, const EePdfPageText *pt, uint32_t from, float split, Intern *in,
                      HartHvsStats *st)
{
    uint32_t li;
    BOOL after_writein = FALSE; /* the line before was a write-in row */
    for (li = from; li < hp->nlines; li++)
    {
        const HLine *ln = &hp->lines[li];
        uint32_t k, nleft = 0;
        char all[HVS_CELL], title[HVS_CELL], choice[HVS_CELL];
        uint32_t t, o;
        runs_text(pt, hp->ord + ln->first, ln->count, all, sizeof(all));
        if (is_footer(all))
        {
            continue;
        }
        for (k = 0; k < ln->count && pt->runs[hp->ord[ln->first + k]].x < split; k++)
        {
            nleft++;
        }
        runs_text(pt, hp->ord + ln->first, nleft, title, sizeof(title));
        runs_text(pt, hp->ord + ln->first + nleft, ln->count - nleft, choice, sizeof(choice));
        if (title[0] == '\0' && choice[0] != '\0' && after_writein)
        {
            continue; /* a long written-in name wrapped onto a second line ("RANDALL TERRY STEPH" / "BRO") */
        }
        if (choice[0] == '\0' || title[0] == '\0')
        {
            st->stray_lines++;
            after_writein = FALSE;
            continue;
        }
        t = intern(in, title, strlen(title));
        o = option_of(in, choice);
        after_writein = _strnicmp(choice, "Write-in", 8) == 0;
        if (t == UINT32_MAX || o == UINT32_MAX || !row_add(in->s, t, o))
        {
            return FALSE;
        }
    }
    return TRUE;
}

/* -------------------------------------------------------------------------- */
/* Cut-short contest titles                                                   */
/* -------------------------------------------------------------------------- */

typedef struct TitleInfo
{
    uint32_t off;
    double pos_sum; /* row index on its ballots, summed */
    uint32_t count;
    int short_title; /* the start of a longer title it never shares a ballot with */
    uint32_t *cand;  /* the full titles it can stand for, in ballot order */
    uint32_t ncand;
} TitleInfo;

typedef struct IdMap
{
    uint32_t *key; /* key + 1 (0 = empty) */
    uint32_t *val;
    uint32_t cap;
    uint32_t n;
} IdMap;

static BOOL idmap_grow(IdMap *m)
{
    uint32_t nc = m->cap ? m->cap * 2 : 256;
    uint32_t *nk = (uint32_t *)calloc(nc, sizeof(uint32_t));
    uint32_t *nv = (uint32_t *)calloc(nc, sizeof(uint32_t));
    uint32_t i;
    if (nk == NULL || nv == NULL)
    {
        free(nk);
        free(nv);
        return FALSE;
    }
    for (i = 0; i < m->cap; i++)
    {
        if (m->key[i] != 0)
        {
            uint32_t k = (m->key[i] * 2654435761u) & (nc - 1);
            while (nk[k] != 0)
            {
                k = (k + 1) & (nc - 1);
            }
            nk[k] = m->key[i];
            nv[k] = m->val[i];
        }
    }
    free(m->key);
    free(m->val);
    m->key = nk;
    m->val = nv;
    m->cap = nc;
    return TRUE;
}

/* The dense id of @p key (assigned in order of first sight), or UINT32_MAX on OOM. */
static uint32_t idmap_get(IdMap *m, uint32_t key)
{
    uint32_t k;
    if ((m->n + 1) * 2 > m->cap && !idmap_grow(m))
    {
        return UINT32_MAX;
    }
    k = ((key + 1) * 2654435761u) & (m->cap - 1);
    while (m->key[k] != 0)
    {
        if (m->key[k] == key + 1)
        {
            return m->val[k];
        }
        k = (k + 1) & (m->cap - 1);
    }
    m->key[k] = key + 1;
    m->val[k] = m->n++;
    return m->val[k];
}

static int __cdecl cand_cmp(void *ctx, const void *a, const void *b)
{
    const TitleInfo *ti = (const TitleInfo *)ctx;
    const TitleInfo *x = &ti[*(const uint32_t *)a];
    const TitleInfo *y = &ti[*(const uint32_t *)b];
    double px = x->pos_sum / (x->count ? x->count : 1);
    double py = y->pos_sum / (y->count ? y->count : 1);
    if (px != py)
    {
        return px < py ? -1 : 1;
    }
    return *(const uint32_t *)a < *(const uint32_t *)b ? -1 : 1;
}

/* Complete the cut-short titles (see hart_hvs.h). FALSE only on OOM. */
static BOOL repair_titles(HartOcrStore *s, HartHvsStats *st)
{
    IdMap tmap, kmap, pmap, ymap;
    TitleInfo *ti = NULL;
    uint32_t *rtid = NULL; /* row -> title id */
    uint8_t *with = NULL;  /* ntitles x ntitles: on one ballot together */
    uint8_t *inkey = NULL; /* ntitles x nkeys: printed for that precinct and party */
    uint32_t *rkey = NULL; /* record -> precinct key id */
    uint32_t nt, nk, r, q, a, b;
    BOOL ok = FALSE;
    ZeroMemory(&tmap, sizeof(tmap));
    ZeroMemory(&kmap, sizeof(kmap));
    ZeroMemory(&pmap, sizeof(pmap));
    ZeroMemory(&ymap, sizeof(ymap));
    rtid = (uint32_t *)malloc((size_t)(s->nrows ? s->nrows : 1) * sizeof(uint32_t));
    rkey = (uint32_t *)malloc((size_t)(s->nrecs ? s->nrecs : 1) * sizeof(uint32_t));
    if (rtid == NULL || rkey == NULL)
    {
        goto done;
    }
    for (q = 0; q < s->nrows; q++)
    {
        if ((rtid[q] = idmap_get(&tmap, s->rows[q].title)) == UINT32_MAX)
        {
            goto done;
        }
    }
    for (r = 0; r < s->nrecs; r++)
    {
        /* the ballot's precinct and party, as one key */
        uint32_t pid = idmap_get(&pmap, s->recs[r].f[HOF_PRECINCT]);
        uint32_t yid = idmap_get(&ymap, s->recs[r].f[HOF_PARTY]);
        if (pid == UINT32_MAX || yid == UINT32_MAX || pid >= 0x100000u || yid >= 0x1000u ||
            (rkey[r] = idmap_get(&kmap, pid << 12 | yid)) == UINT32_MAX)
        {
            goto done;
        }
    }
    nt = tmap.n;
    nk = kmap.n;
    if (nt < 2 || nt > 4000)
    {
        ok = TRUE; /* nothing to repair (or too many titles to compare: kept as printed) */
        goto done;
    }
    ti = (TitleInfo *)calloc(nt, sizeof(TitleInfo));
    with = (uint8_t *)calloc((size_t)nt * nt, 1);
    inkey = (uint8_t *)calloc((size_t)nt * (nk ? nk : 1), 1);
    if (ti == NULL || with == NULL || inkey == NULL)
    {
        goto done;
    }
    for (q = 0; q < s->nrows; q++)
    {
        ti[rtid[q]].off = s->rows[q].title;
    }
    for (r = 0; r < s->nrecs; r++)
    {
        const HartOcrRecord *rec = &s->recs[r];
        for (a = 0; a < rec->nrows; a++)
        {
            uint32_t ta = rtid[rec->first_row + a];
            ti[ta].pos_sum += a;
            ti[ta].count++;
            inkey[(size_t)ta * nk + rkey[r]] = 1;
            for (b = 0; b < rec->nrows; b++)
            {
                with[(size_t)ta * nt + rtid[rec->first_row + b]] = 1;
            }
        }
    }
    /* t is cut short when a longer title u starts with it, is printed for a precinct t is,
     * and never shares a ballot with it */
    for (a = 0; a < nt; a++)
    {
        const char *ta = HartOcr_Str(s, ti[a].off);
        size_t la = strlen(ta);
        for (b = 0; b < nt && !ti[a].short_title; b++)
        {
            const char *tb = HartOcr_Str(s, ti[b].off);
            uint32_t k;
            if (b == a || with[(size_t)a * nt + b] || strlen(tb) <= la || memcmp(ta, tb, la) != 0)
            {
                continue;
            }
            for (k = 0; k < nk; k++)
            {
                if (inkey[(size_t)a * nk + k] && inkey[(size_t)b * nk + k])
                {
                    ti[a].short_title = 1;
                    break;
                }
            }
        }
    }
    for (a = 0; a < nt; a++)
    {
        const char *ta = HartOcr_Str(s, ti[a].off);
        size_t la = strlen(ta);
        if (!ti[a].short_title)
        {
            continue;
        }
        ti[a].cand = (uint32_t *)malloc((size_t)nt * sizeof(uint32_t));
        if (ti[a].cand == NULL)
        {
            goto done;
        }
        for (b = 0; b < nt; b++)
        {
            const char *tb = HartOcr_Str(s, ti[b].off);
            uint32_t k;
            BOOL shared = FALSE;
            if (b == a || ti[b].short_title || with[(size_t)a * nt + b] || strlen(tb) <= la ||
                memcmp(ta, tb, la) != 0)
            {
                continue;
            }
            for (k = 0; k < nk && !shared; k++)
            {
                shared = inkey[(size_t)a * nk + k] && inkey[(size_t)b * nk + k];
            }
            if (shared)
            {
                ti[a].cand[ti[a].ncand++] = b;
            }
        }
        qsort_s(ti[a].cand, ti[a].ncand, sizeof(uint32_t), cand_cmp, ti);
    }
    /* each ballot: the k-th cut title is the k-th of its full titles, when it has them all */
    for (r = 0; r < s->nrecs; r++)
    {
        const HartOcrRecord *rec = &s->recs[r];
        uint32_t seen[64];
        uint32_t used[64];
        uint32_t nseen = 0;
        for (a = 0; a < rec->nrows; a++)
        {
            uint32_t ta = rtid[rec->first_row + a];
            uint32_t j, cnt = 0, k = 0;
            if (!ti[ta].short_title)
            {
                continue;
            }
            for (j = 0; j < rec->nrows; j++)
            {
                cnt += rtid[rec->first_row + j] == ta;
            }
            for (j = 0; j < nseen && seen[j] != ta; j++)
            {
            }
            if (j == nseen && nseen < ARRAYSIZE(seen))
            {
                seen[nseen] = ta;
                used[nseen++] = 0;
            }
            if (j < nseen)
            {
                k = used[j]++;
            }
            if (ti[ta].ncand == 1)
            {
                s->rows[rec->first_row + a].title = ti[ti[ta].cand[0]].off;
                st->cut_titles++;
            }
            else if (ti[ta].ncand > 1 && cnt == ti[ta].ncand && k < ti[ta].ncand)
            {
                s->rows[rec->first_row + a].title = ti[ti[ta].cand[k]].off;
                st->cut_titles++;
            }
            else
            {
                st->cut_unresolved++;
            }
        }
    }
    ok = TRUE;
done:
    if (ti != NULL)
    {
        for (a = 0; a < tmap.n; a++)
        {
            free(ti[a].cand);
        }
    }
    free(ti);
    free(with);
    free(inkey);
    free(rtid);
    free(rkey);
    free(tmap.key);
    free(tmap.val);
    free(kmap.key);
    free(kmap.val);
    free(pmap.key);
    free(pmap.val);
    free(ymap.key);
    free(ymap.val);
    return ok;
}

/* -------------------------------------------------------------------------- */
/* Reading a file                                                             */
/* -------------------------------------------------------------------------- */

BOOL HartHvs_ReadPdf(EePdf *pdf,
                     const wchar_t *name,
                     HartOcrStore *store,
                     HartOcrTickFn tick,
                     void *tick_ctx,
                     BOOL *cancelled,
                     HartHvsStats *stats,
                     wchar_t *err,
                     size_t errcch)
{
    EePdfPageText pt;
    HPage hp;
    Intern in;
    HartHvsStats st;
    uint32_t page, npages;
    char stem[128];
    BOOL ok = TRUE;
    BOOL open = FALSE;

    HartOcr_StoreFree(store);
    ZeroMemory(&hp, sizeof(hp));
    ZeroMemory(&in, sizeof(in));
    ZeroMemory(&st, sizeof(st));
    in.s = store;
    if (cancelled != NULL)
    {
        *cancelled = FALSE;
    }
    if (err != NULL && errcch > 0)
    {
        err[0] = L'\0';
    }
    {
        /* the record id names the file without its extension */
        int n = WideCharToMultiByte(CP_UTF8, 0, name, -1, stem, (int)sizeof(stem), NULL, NULL);
        char *dot;
        if (n <= 0)
        {
            stem[0] = '\0';
        }
        dot = strrchr(stem, '.');
        if (dot != NULL && _stricmp(dot, ".pdf") == 0)
        {
            *dot = '\0';
        }
    }
    EePdf_PageTextInit(&pt);
    npages = EePdf_PageCount(pdf);
    if (pool_raw(store, "", 0) == UINT32_MAX) /* offset 0 = "" */
    {
        ok = FALSE;
    }
    for (page = 0; ok && page < npages; page++)
    {
        HHead h;
        uint32_t from;
        if (!EePdf_ExtractPageText(pdf, page, &pt) || !page_lines(&hp, &pt))
        {
            if (err != NULL && errcch > 0)
            {
                StringCchPrintfW(err, errcch, L"page %u could not be read.", page + 1);
            }
            ok = FALSE;
            break;
        }
        from = parse_head(&hp, &pt, &h);
        if (h.precinct)
        {
            HartOcrRecord *rec = rec_add(store);
            char id[192];
            if (rec == NULL)
            {
                ok = FALSE;
                break;
            }
            StringCchPrintfA(id, sizeof(id), "%s p.%u", stem, page + 1);
            rec->f[HOF_GUID] = intern(&in, id, strlen(id));
            rec->f[HOF_PRECINCT] = intern(&in, h.prec, strlen(h.prec));
            rec->f[HOF_PARTY] = intern(&in, h.party, strlen(h.party));
            rec->f[HOF_VTYPE] = intern(&in, h.vtype, strlen(h.vtype));
            rec->first_row = store->nrows;
            rec->page = page;
            rec->status = HOCR_OK;
            if (rec->f[HOF_GUID] == UINT32_MAX || rec->f[HOF_PRECINCT] == UINT32_MAX ||
                rec->f[HOF_PARTY] == UINT32_MAX || rec->f[HOF_VTYPE] == UINT32_MAX)
            {
                ok = FALSE;
                break;
            }
            open = TRUE;
            st.ballots++;
        }
        else if (open && from < hp.nlines)
        {
            st.continued++; /* a ballot too long for one page: more of the one before */
        }
        else
        {
            from = hp.nlines; /* a page that is not a ballot (no precinct, no table) */
        }
        if (from < hp.nlines)
        {
            uint32_t before = store->nrows;
            if (!page_rows(&hp, &pt, from, h.split, &in, &st))
            {
                ok = FALSE;
                break;
            }
            store->recs[store->nrecs - 1].nrows += store->nrows - before;
        }
        st.pages++;
        if (tick != NULL && !tick(tick_ctx))
        {
            if (cancelled != NULL)
            {
                *cancelled = TRUE;
            }
            ok = FALSE;
            break;
        }
    }
    if (ok && st.ballots == 0)
    {
        if (err != NULL && errcch > 0)
        {
            StringCchCopyW(err, errcch, L"no ballot pages were found in the Votes by Precinct report.");
        }
        ok = FALSE;
    }
    if (ok && !repair_titles(store, &st))
    {
        ok = FALSE;
    }
    if (!ok && err != NULL && errcch > 0 && err[0] == L'\0' && (cancelled == NULL || !*cancelled))
    {
        StringCchCopyW(err, errcch, L"out of memory reading the report.");
    }
    store->pages = st.pages;
    if (stats != NULL)
    {
        *stats = st;
    }
    EePdf_PageTextFree(&pt);
    free(hp.ord);
    free(hp.lines);
    free(in.slot);
    return ok;
}
