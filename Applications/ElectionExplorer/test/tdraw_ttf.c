/**
 * @file tdraw_ttf.c
 * @brief Test fixtures' text drawing on Linux (see tdraw.h) with no library: glyph outlines read
 *        from a TrueType font file and filled by a scanline rasterizer, 4 x 4 subsamples a pixel.
 *
 * The font is Liberation Sans (Arial's metrics), else DejaVu Sans, from the usual places, or
 * the file the TDRAW_FONT environment variable names. Only what the fixtures need is read:
 * the cmap (format 4), loca / glyf (simple glyphs), hhea / hmtx and head.
 */

#include <math.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "tdraw.h"

#define SUB 4 /* subsamples a pixel, each way */

typedef struct Ttf
{
    uint8_t *d;
    size_t n;
    uint32_t glyf, loca, hmtx, cmap4;
    int loca_long;
    uint16_t upem, nglyphs, nhm;
    int16_t ascender;
} Ttf;

static Ttf g_ttf;
static int g_state; /* 0 not tried, 1 loaded, -1 no font */

static uint16_t u16(const uint8_t *p)
{
    return (uint16_t)((p[0] << 8) | p[1]);
}

static int16_t s16(const uint8_t *p)
{
    return (int16_t)u16(p);
}

static uint32_t u32(const uint8_t *p)
{
    return ((uint32_t)p[0] << 24) | ((uint32_t)p[1] << 16) | ((uint32_t)p[2] << 8) | p[3];
}

static int ttf_parse(uint8_t *d, size_t n)
{
    uint32_t head = 0, maxp = 0, hhea = 0, cmap = 0, i;
    uint16_t nt;
    if (n < 12)
    {
        return 0;
    }
    memset(&g_ttf, 0, sizeof(g_ttf));
    nt = u16(d + 4);
    if (12 + 16u * nt > n)
    {
        return 0;
    }
    for (i = 0; i < nt; i++)
    {
        const uint8_t *r = d + 12 + 16 * i;
        uint32_t off = u32(r + 8);
        if (off >= n)
        {
            return 0;
        }
        if (memcmp(r, "head", 4) == 0)
            head = off;
        else if (memcmp(r, "maxp", 4) == 0)
            maxp = off;
        else if (memcmp(r, "hhea", 4) == 0)
            hhea = off;
        else if (memcmp(r, "cmap", 4) == 0)
            cmap = off;
        else if (memcmp(r, "glyf", 4) == 0)
            g_ttf.glyf = off;
        else if (memcmp(r, "loca", 4) == 0)
            g_ttf.loca = off;
        else if (memcmp(r, "hmtx", 4) == 0)
            g_ttf.hmtx = off;
    }
    if (head == 0 || maxp == 0 || hhea == 0 || cmap == 0 || g_ttf.glyf == 0 || g_ttf.loca == 0 ||
        g_ttf.hmtx == 0)
    {
        return 0;
    }
    g_ttf.d = d;
    g_ttf.n = n;
    g_ttf.upem = u16(d + head + 18);
    g_ttf.loca_long = s16(d + head + 50) != 0;
    g_ttf.nglyphs = u16(d + maxp + 4);
    g_ttf.ascender = s16(d + hhea + 4);
    g_ttf.nhm = u16(d + hhea + 34);
    for (i = 0; i < u16(d + cmap + 2); i++)
    {
        const uint8_t *r = d + cmap + 4 + 8 * i;
        uint16_t plat = u16(r), enc = u16(r + 2);
        uint32_t off = cmap + u32(r + 4);
        if (off + 14 < n && u16(d + off) == 4 && ((plat == 3 && enc == 1) || plat == 0))
        {
            g_ttf.cmap4 = off;
            break;
        }
    }
    return g_ttf.cmap4 != 0 && g_ttf.upem != 0 && g_ttf.nhm != 0;
}

static int ttf_load(void)
{
    static const char *const k_fonts[] = {
        "/usr/share/fonts/truetype/liberation/LiberationSans-Regular.ttf",
        "/usr/share/fonts/truetype/liberation2/LiberationSans-Regular.ttf",
        "/usr/share/fonts/liberation-sans/LiberationSans-Regular.ttf",
        "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
        "/usr/share/fonts/dejavu/DejaVuSans.ttf",
        "/usr/share/fonts/TTF/DejaVuSans.ttf",
    };
    size_t k;
    if (g_state != 0)
    {
        return g_state > 0;
    }
    g_state = -1;
    for (k = 0; k <= sizeof(k_fonts) / sizeof(k_fonts[0]); k++)
    {
        const char *path = k == 0 ? getenv("TDRAW_FONT") : k_fonts[k - 1];
        FILE *f;
        long len;
        uint8_t *d;
        if (path == NULL || (f = fopen(path, "rb")) == NULL)
        {
            continue;
        }
        fseek(f, 0, SEEK_END);
        len = ftell(f);
        fseek(f, 0, SEEK_SET);
        d = len > 0 ? (uint8_t *)malloc((size_t)len) : NULL;
        if (d != NULL && fread(d, 1, (size_t)len, f) == (size_t)len && ttf_parse(d, (size_t)len))
        {
            fclose(f);
            g_state = 1;
            return 1;
        }
        free(d);
        fclose(f);
    }
    return 0;
}

/* The glyph for character @p ch (cmap format 4), 0 when none. */
static uint16_t ttf_glyph(uint32_t ch)
{
    const uint8_t *t = g_ttf.d + g_ttf.cmap4;
    uint16_t seg2 = u16(t + 6), s;
    const uint8_t *ends = t + 14, *starts = ends + seg2 + 2, *deltas = starts + seg2,
                  *ros = deltas + seg2;
    if (ch > 0xFFFF)
    {
        return 0;
    }
    for (s = 0; s < seg2 / 2; s++)
    {
        uint16_t end = u16(ends + 2 * s), start, delta, ro;
        if (ch > end)
        {
            continue;
        }
        start = u16(starts + 2 * s);
        if (ch < start)
        {
            return 0;
        }
        delta = u16(deltas + 2 * s);
        ro = u16(ros + 2 * s);
        if (ro == 0)
        {
            return (uint16_t)(ch + delta);
        }
        {
            const uint8_t *g = ros + 2 * s + ro + 2 * (ch - start);
            uint16_t gid;
            if (g + 2 > g_ttf.d + g_ttf.n)
            {
                return 0;
            }
            gid = u16(g);
            return gid != 0 ? (uint16_t)(gid + delta) : 0;
        }
    }
    return 0;
}

static uint16_t ttf_advance(uint16_t gid)
{
    uint16_t i = gid < g_ttf.nhm ? gid : (uint16_t)(g_ttf.nhm - 1);
    return u16(g_ttf.d + g_ttf.hmtx + 4u * i);
}

/* An outline element in font units, y up: 0 move to (x0, y0), 1 line to (x0, y0), 2 quadratic
 * curve through (x0, y0) to (x1, y1), 3 close. */
typedef void (*OutFn)(void *u, int op, double x0, double y0, double x1, double y1);

/* Walk glyph @p gid's outline (simple glyphs; a composite glyph draws nothing). */
static int ttf_outline(uint16_t gid, OutFn fn, void *u)
{
    uint32_t off, next;
    const uint8_t *p, *q, *end;
    int16_t nc;
    uint16_t npts, i, c, first;
    uint8_t *flags;
    double *xs, *ys;
    int x = 0, y = 0;
    if (gid >= g_ttf.nglyphs)
    {
        return 0;
    }
    off = g_ttf.loca_long ? u32(g_ttf.d + g_ttf.loca + 4u * gid)
                          : 2u * u16(g_ttf.d + g_ttf.loca + 2u * gid);
    next = g_ttf.loca_long ? u32(g_ttf.d + g_ttf.loca + 4u * (gid + 1u))
                           : 2u * u16(g_ttf.d + g_ttf.loca + 2u * (gid + 1u));
    if (next <= off)
    {
        return 1; /* (no outline: a space) */
    }
    p = g_ttf.d + g_ttf.glyf + off;
    end = g_ttf.d + g_ttf.glyf + next;
    if (end > g_ttf.d + g_ttf.n)
    {
        return 0;
    }
    nc = s16(p);
    if (nc <= 0)
    {
        return 1; /* (composite glyphs are not read) */
    }
    npts = (uint16_t)(u16(p + 10 + 2 * (nc - 1)) + 1);
    q = p + 10 + 2 * nc;
    q += 2 + u16(q); /* instructions */
    flags = (uint8_t *)malloc(npts);
    xs = (double *)malloc(npts * sizeof(double));
    ys = (double *)malloc(npts * sizeof(double));
    if (flags == NULL || xs == NULL || ys == NULL)
    {
        free(flags);
        free(xs);
        free(ys);
        return 0;
    }
    for (i = 0; i < npts && q < end;)
    {
        uint8_t f = *q++;
        flags[i++] = f;
        if ((f & 8) && q < end)
        {
            uint8_t r = *q++;
            while (r-- > 0 && i < npts)
            {
                flags[i++] = f;
            }
        }
    }
    for (i = 0; i < npts; i++)
    {
        if (flags[i] & 2)
        {
            x += (flags[i] & 16) ? *q : -*q;
            q++;
        }
        else if (!(flags[i] & 16))
        {
            x += s16(q);
            q += 2;
        }
        xs[i] = x;
    }
    for (i = 0; i < npts; i++)
    {
        if (flags[i] & 4)
        {
            y += (flags[i] & 32) ? *q : -*q;
            q++;
        }
        else if (!(flags[i] & 32))
        {
            y += s16(q);
            q += 2;
        }
        ys[i] = y;
    }
    for (c = 0, first = 0; c < (uint16_t)nc; c++)
    {
        uint16_t last = u16(p + 10 + 2 * c), n = (uint16_t)(last - first + 1), k, start = 0;
        double sx, sy, cx = 0, cy = 0;
        int on_found = 0, ctrl = 0;
        for (k = 0; k < n; k++)
        {
            if (flags[first + k] & 1)
            {
                start = k;
                on_found = 1;
                break;
            }
        }
        if (on_found)
        {
            sx = xs[first + start];
            sy = ys[first + start];
        }
        else
        {
            /* all points off the curve: start between the last and the first */
            sx = (xs[first + n - 1] + xs[first]) / 2.0;
            sy = (ys[first + n - 1] + ys[first]) / 2.0;
            start = (uint16_t)(n - 1);
        }
        fn(u, 0, sx, sy, 0, 0);
        for (k = 1; k <= n; k++)
        {
            uint16_t j = (uint16_t)(first + (start + k) % n);
            double px = xs[j], py = ys[j];
            if (k == n && on_found)
            {
                break; /* (back at the start point) */
            }
            if (flags[j] & 1)
            {
                if (ctrl)
                {
                    fn(u, 2, cx, cy, px, py);
                }
                else
                {
                    fn(u, 1, px, py, 0, 0);
                }
                ctrl = 0;
            }
            else
            {
                if (ctrl)
                {
                    fn(u, 2, cx, cy, (cx + px) / 2.0, (cy + py) / 2.0);
                }
                cx = px;
                cy = py;
                ctrl = 1;
            }
        }
        if (ctrl)
        {
            fn(u, 2, cx, cy, sx, sy);
        }
        else
        {
            fn(u, 1, sx, sy, 0, 0);
        }
        fn(u, 3, 0, 0, 0, 0);
        first = (uint16_t)(last + 1);
    }
    free(flags);
    free(xs);
    free(ys);
    return 1;
}

/* ---- the page ------------------------------------------------------------ */

typedef struct Edge
{
    double x0, y0, x1, y1;
} Edge;

struct TDrawPage
{
    int w, h;
    double px;    /* pixels to the em */
    uint8_t *cov; /* subsamples covered, 0..SUB*SUB, a pixel */
};

typedef struct Build
{
    Edge *e;
    size_t n, cap;
    double ox, oy, k; /* glyph origin (subsample units, y down) and font units to subsamples */
    double cx, cy;    /* pen (subsample units) */
    int ok;
} Build;

static void add_edge(Build *b, double x0, double y0, double x1, double y1)
{
    if (!b->ok || y0 == y1)
    {
        return;
    }
    if (b->n == b->cap)
    {
        size_t nc = b->cap ? b->cap * 2 : 256;
        Edge *ne = (Edge *)realloc(b->e, nc * sizeof(Edge));
        if (ne == NULL)
        {
            b->ok = 0;
            return;
        }
        b->e = ne;
        b->cap = nc;
    }
    b->e[b->n].x0 = x0;
    b->e[b->n].y0 = y0;
    b->e[b->n].x1 = x1;
    b->e[b->n].y1 = y1;
    b->n++;
}

/* OutFn: the outline as edges in subsample units (y down). */
static void to_edges(void *u, int op, double x0, double y0, double x1, double y1)
{
    Build *b = (Build *)u;
    double px = b->ox + x0 * b->k, py = b->oy - y0 * b->k;
    if (op == 0)
    {
        b->cx = px;
        b->cy = py;
    }
    else if (op == 1)
    {
        add_edge(b, b->cx, b->cy, px, py);
        b->cx = px;
        b->cy = py;
    }
    else if (op == 2)
    {
        double ex = b->ox + x1 * b->k, ey = b->oy - y1 * b->k;
        double len = hypot(px - b->cx, py - b->cy) + hypot(ex - px, ey - py);
        int s, ns = (int)ceil(len / 2.0);
        double lx = b->cx, ly = b->cy;
        if (ns < 1)
            ns = 1;
        if (ns > 64)
            ns = 64;
        for (s = 1; s <= ns; s++)
        {
            double t = (double)s / ns, m = 1.0 - t;
            double qx = m * m * b->cx + 2 * m * t * px + t * t * ex;
            double qy = m * m * b->cy + 2 * m * t * py + t * t * ey;
            add_edge(b, lx, ly, qx, qy);
            lx = qx;
            ly = qy;
        }
        b->cx = ex;
        b->cy = ey;
    }
}

static int cmp_double(const void *a, const void *b)
{
    double x = *(const double *)a, y = *(const double *)b;
    return x < y ? -1 : (x > y ? 1 : 0);
}

typedef struct Cross
{
    double x;
    int dir;
} Cross;

static int cmp_cross(const void *a, const void *b)
{
    const Cross *x = (const Cross *)a, *y = (const Cross *)b;
    return cmp_double(&x->x, &y->x);
}

/* Fill the edges (nonzero winding) into the page's subsample coverage. */
static void fill(TDrawPage *p, const Edge *e, size_t n)
{
    double ymin = 1e30, ymax = -1e30;
    Cross *cr = (Cross *)malloc((n + 1) * sizeof(Cross));
    size_t i;
    int y, y0, y1, H = p->h * SUB, W = p->w * SUB;
    if (cr == NULL)
    {
        return;
    }
    for (i = 0; i < n; i++)
    {
        ymin = fmin(ymin, fmin(e[i].y0, e[i].y1));
        ymax = fmax(ymax, fmax(e[i].y0, e[i].y1));
    }
    y0 = (int)floor(ymin);
    y1 = (int)ceil(ymax);
    if (y0 < 0)
        y0 = 0;
    if (y1 > H - 1)
        y1 = H - 1;
    for (y = y0; y <= y1; y++)
    {
        double yc = y + 0.5;
        size_t nc = 0, c;
        int wind = 0;
        double xs = 0;
        for (i = 0; i < n; i++)
        {
            double lo = fmin(e[i].y0, e[i].y1), hi = fmax(e[i].y0, e[i].y1);
            if (yc >= lo && yc < hi)
            {
                cr[nc].x = e[i].x0 + (yc - e[i].y0) * (e[i].x1 - e[i].x0) / (e[i].y1 - e[i].y0);
                cr[nc].dir = e[i].y1 > e[i].y0 ? 1 : -1;
                nc++;
            }
        }
        qsort(cr, nc, sizeof(Cross), cmp_cross);
        for (c = 0; c < nc; c++)
        {
            int before = wind;
            wind += cr[c].dir;
            if (before == 0 && wind != 0)
            {
                xs = cr[c].x;
            }
            else if (before != 0 && wind == 0)
            {
                int xa = (int)ceil(xs - 0.5), xb = (int)ceil(cr[c].x - 0.5), x;
                if (xa < 0)
                    xa = 0;
                if (xb > W)
                    xb = W;
                for (x = xa; x < xb; x++)
                {
                    uint8_t *cv = &p->cov[(size_t)(y / SUB) * (size_t)p->w + (size_t)(x / SUB)];
                    if (*cv < SUB * SUB)
                    {
                        (*cv)++;
                    }
                }
            }
        }
    }
    free(cr);
}

int TDraw_Available(void)
{
    return ttf_load();
}

TDrawPage *TDraw_PageBegin(int w, int h, double font_px)
{
    TDrawPage *p;
    if (!ttf_load() || w <= 0 || h <= 0)
    {
        return NULL;
    }
    p = (TDrawPage *)calloc(1, sizeof(TDrawPage));
    if (p == NULL)
    {
        return NULL;
    }
    p->w = w;
    p->h = h;
    p->px = font_px;
    p->cov = (uint8_t *)calloc((size_t)w * (size_t)h, 1);
    if (p->cov == NULL)
    {
        free(p);
        return NULL;
    }
    return p;
}

/* The next character of UTF-8 text, advancing @p s. */
static uint32_t next_char(const char **s)
{
    const unsigned char *u = (const unsigned char *)*s;
    uint32_t c = *u++;
    if (c >= 0xC0 && c < 0xE0 && (*u & 0xC0) == 0x80)
    {
        c = ((c & 0x1F) << 6) | (*u++ & 0x3F);
    }
    else if (c >= 0xE0 && c < 0xF0 && (u[0] & 0xC0) == 0x80 && (u[1] & 0xC0) == 0x80)
    {
        c = ((c & 0x0F) << 12) | ((u[0] & 0x3F) << 6) | (u[1] & 0x3F);
        u += 2;
    }
    *s = (const char *)u;
    return c;
}

void TDraw_Text(TDrawPage *p, int x, int y, const char *utf8)
{
    double k, pen;
    Build b;
    if (p == NULL || utf8 == NULL)
    {
        return;
    }
    k = p->px / g_ttf.upem * SUB; /* font units to subsamples */
    pen = (double)x * SUB;
    memset(&b, 0, sizeof(b));
    /* GDI's (x, y) is the cell's top-left: the baseline is the ascent below it */
    b.oy = ((double)y + g_ttf.ascender * p->px / g_ttf.upem) * SUB;
    b.k = k;
    while (*utf8 != '\0')
    {
        uint16_t gid = ttf_glyph(next_char(&utf8));
        b.n = 0;
        b.ok = 1;
        b.ox = pen;
        if (gid != 0 && ttf_outline(gid, to_edges, &b) && b.ok && b.n > 0)
        {
            fill(p, b.e, b.n);
        }
        pen += ttf_advance(gid) * k;
    }
    free(b.e);
}

unsigned char *TDraw_PageEnd(TDrawPage *p)
{
    unsigned char *g;
    size_t i, n;
    if (p == NULL)
    {
        return NULL;
    }
    n = (size_t)p->w * (size_t)p->h;
    g = (unsigned char *)malloc(n);
    if (g != NULL)
    {
        for (i = 0; i < n; i++)
        {
            g[i] = (unsigned char)(255 - p->cov[i] * 255 / (SUB * SUB));
        }
    }
    free(p->cov);
    free(p);
    return g;
}

typedef struct PathCx
{
    TDrawPathFn fn;
    void *user;
    double k; /* font units to ems */
    double cx, cy;
    int ok;
} PathCx;

/* OutFn: the outline in ems, quadratic curves as the cubic that draws them. */
static void to_path(void *u, int op, double x0, double y0, double x1, double y1)
{
    PathCx *c = (PathCx *)u;
    double k = c->k;
    if (!c->ok)
    {
        return;
    }
    if (op == 0 || op == 1)
    {
        c->ok = c->fn(c->user, op, x0 * k, y0 * k, 0, 0, 0, 0);
        c->cx = x0 * k;
        c->cy = y0 * k;
    }
    else if (op == 2)
    {
        double qx = x0 * k, qy = y0 * k, ex = x1 * k, ey = y1 * k;
        c->ok = c->fn(c->user,
                      2,
                      c->cx + 2.0 / 3.0 * (qx - c->cx),
                      c->cy + 2.0 / 3.0 * (qy - c->cy),
                      ex + 2.0 / 3.0 * (qx - ex),
                      ey + 2.0 / 3.0 * (qy - ey),
                      ex,
                      ey);
        c->cx = ex;
        c->cy = ey;
    }
    else
    {
        c->ok = c->fn(c->user, 3, 0, 0, 0, 0, 0, 0);
    }
}

int TDraw_GlyphOutline(char ch, TDrawPathFn fn, void *user)
{
    PathCx c;
    uint16_t gid;
    if (!ttf_load())
    {
        return 0;
    }
    gid = ttf_glyph((uint32_t)(unsigned char)ch);
    if (gid == 0)
    {
        return 0;
    }
    c.fn = fn;
    c.user = user;
    c.k = 1.0 / g_ttf.upem;
    c.cx = c.cy = 0.0;
    c.ok = 1;
    return ttf_outline(gid, to_path, &c) && c.ok;
}
