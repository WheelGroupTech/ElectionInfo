/**
 * @file ee_glyph_ocr.c
 * @brief Characters for a PDF's unnamed Type 3 glyphs, learned through OCR. See
 *        ee_glyph_ocr.h.
 */

#include "ee_glyph_ocr.h"
#include "ocr_win.h"

#include <math.h>
#include <stdlib.h>
#include <string.h>
#include <strsafe.h>

#define GO_SCALE    3.0f  /* bitmap pixels per point */
#define GO_SUBPIXEL 16.0f /* GDI coordinates per pixel (world transform scales back) */

typedef struct GoVote
{
    uint32_t fp;
    uint32_t cp;
    uint32_t n;
} GoVote;

typedef struct GoOutline
{
    uint32_t proc;
    BOOL ok;
    EePdfOutline o;
} GoOutline;

typedef struct GoCtx
{
    EePdf *pdf;
    GoVote *votes;
    size_t nvotes, cap_votes;
    GoOutline *outl;
    size_t noutl, cap_outl;
    BOOL oom;
} GoCtx;

static BOOL go_grow(void **p, size_t *cap, size_t need, size_t elem)
{
    size_t nc;
    void *np;
    if (need <= *cap)
    {
        return TRUE;
    }
    nc = *cap ? *cap * 2 : 64;
    while (nc < need)
    {
        nc *= 2;
    }
    np = realloc(*p, nc * elem);
    if (np == NULL)
    {
        return FALSE;
    }
    *p = np;
    *cap = nc;
    return TRUE;
}

static const EePdfOutline *go_outline(GoCtx *cx, uint32_t proc)
{
    size_t i;
    for (i = 0; i < cx->noutl; i++)
    {
        if (cx->outl[i].proc == proc)
        {
            return cx->outl[i].ok ? &cx->outl[i].o : NULL;
        }
    }
    if (!go_grow((void **)&cx->outl, &cx->cap_outl, cx->noutl + 1, sizeof(GoOutline)))
    {
        cx->oom = TRUE;
        return NULL;
    }
    ZeroMemory(&cx->outl[cx->noutl], sizeof(GoOutline));
    cx->outl[cx->noutl].proc = proc;
    cx->outl[cx->noutl].ok = EePdf_GlyphOutline(cx->pdf, proc, &cx->outl[cx->noutl].o);
    cx->noutl++;
    return cx->outl[cx->noutl - 1].ok ? &cx->outl[cx->noutl - 1].o : NULL;
}

static void go_vote(GoCtx *cx, uint32_t fp, uint32_t cp)
{
    size_t i;
    for (i = 0; i < cx->nvotes; i++)
    {
        if (cx->votes[i].fp == fp && cx->votes[i].cp == cp)
        {
            cx->votes[i].n++;
            return;
        }
    }
    if (!go_grow((void **)&cx->votes, &cx->cap_votes, cx->nvotes + 1, sizeof(GoVote)))
    {
        cx->oom = TRUE;
        return;
    }
    cx->votes[cx->nvotes].fp = fp;
    cx->votes[cx->nvotes].cp = cp;
    cx->votes[cx->nvotes].n = 1;
    cx->nvotes++;
}

static int __cdecl go_x_cmp(const void *pa, const void *pb)
{
    const EePdfGlyph *a = *(const EePdfGlyph *const *)pa;
    const EePdfGlyph *b = *(const EePdfGlyph *const *)pb;
    return a->x0 < b->x0 ? -1 : (a->x0 > b->x0 ? 1 : 0);
}

/* Draw the page's glyphs (black on white) into an 8-bit gray bitmap of @p w x @p h pixels,
 * @p k pixels per point, the page @p ph points high. */
static BOOL go_draw(GoCtx *cx, const EePdfPageText *pt, uint8_t *gray, int w, int h, float k, float ph)
{
    BITMAPINFO bi;
    void *bits = NULL;
    HDC dc = CreateCompatibleDC(NULL);
    HBITMAP bm, old;
    XFORM xf;
    uint32_t g;
    int x, y;
    if (dc == NULL)
    {
        return FALSE;
    }
    ZeroMemory(&bi, sizeof(bi));
    bi.bmiHeader.biSize = sizeof(BITMAPINFOHEADER);
    bi.bmiHeader.biWidth = w;
    bi.bmiHeader.biHeight = -h; /* top-down */
    bi.bmiHeader.biPlanes = 1;
    bi.bmiHeader.biBitCount = 32;
    bi.bmiHeader.biCompression = BI_RGB;
    bm = CreateDIBSection(dc, &bi, DIB_RGB_COLORS, &bits, NULL, 0);
    if (bm == NULL || bits == NULL)
    {
        DeleteDC(dc);
        return FALSE;
    }
    old = (HBITMAP)SelectObject(dc, bm);
    memset(bits, 0xFF, (size_t)w * (size_t)h * 4);
    SetGraphicsMode(dc, GM_ADVANCED);
    xf.eM11 = 1.0f / GO_SUBPIXEL;
    xf.eM12 = 0.0f;
    xf.eM21 = 0.0f;
    xf.eM22 = 1.0f / GO_SUBPIXEL;
    xf.eDx = 0.0f;
    xf.eDy = 0.0f;
    SetWorldTransform(dc, &xf);
    SetPolyFillMode(dc, WINDING);
    SelectObject(dc, GetStockObject(BLACK_BRUSH));
    SelectObject(dc, GetStockObject(NULL_PEN));
    for (g = 0; g < pt->nglyphs && !cx->oom; g++)
    {
        const EePdfGlyph *gl = &pt->glyphs[g];
        const EePdfOutline *o = go_outline(cx, gl->proc);
        uint32_t i, p = 0;
        if (o == NULL || o->nops == 0)
        {
            continue;
        }
        BeginPath(dc);
        for (i = 0; i < o->nops; i++)
        {
            POINT q[3];
            int n = o->ops[i] == 2 ? 3 : (o->ops[i] == 3 ? 0 : 1), j;
            for (j = 0; j < n; j++, p++)
            {
                float gx = o->pts[2 * p], gy = o->pts[2 * p + 1];
                float ux = gx * gl->m[0] + gy * gl->m[2] + gl->m[4];
                float uy = gx * gl->m[1] + gy * gl->m[3] + gl->m[5];
                q[j].x = (LONG)lroundf(ux * k * GO_SUBPIXEL);
                q[j].y = (LONG)lroundf((ph - uy) * k * GO_SUBPIXEL);
            }
            if (o->ops[i] == 0)
            {
                MoveToEx(dc, q[0].x, q[0].y, NULL);
            }
            else if (o->ops[i] == 1)
            {
                LineTo(dc, q[0].x, q[0].y);
            }
            else if (o->ops[i] == 2)
            {
                PolyBezierTo(dc, q, 3);
            }
            else
            {
                CloseFigure(dc);
            }
        }
        EndPath(dc);
        FillPath(dc);
    }
    GdiFlush();
    for (y = 0; y < h; y++)
    {
        const uint8_t *row = (const uint8_t *)bits + (size_t)y * (size_t)w * 4;
        for (x = 0; x < w; x++)
        {
            gray[(size_t)y * (size_t)w + (size_t)x] = row[(size_t)x * 4]; /* black or white: any channel */
        }
    }
    SelectObject(dc, old);
    DeleteObject(bm);
    DeleteDC(dc);
    return TRUE;
}

/* Line up the recognized words with the glyphs under them and vote. */
static void go_align(GoCtx *cx, const EePdfPageText *pt, const EeOcrText *t, float k, float ph, float sx, float sy)
{
    const EePdfGlyph **cand = (const EePdfGlyph **)malloc(((size_t)pt->nglyphs + 1) * sizeof(EePdfGlyph *));
    uint32_t w, g;
    if (cand == NULL)
    {
        cx->oom = TRUE;
        return;
    }
    for (w = 0; w < t->nwords; w++)
    {
        const EeOcrWord *wd = &t->words[w];
        /* the word's box in user space (y up) */
        float x0 = wd->x0 * sx / k, x1 = wd->x1 * sx / k;
        float ytop = ph - wd->y0 * sy / k, ybot = ph - wd->y1 * sy / k;
        uint32_t cps[64];
        uint32_t ncp = 0, nc = 0, i;
        const unsigned char *s = (const unsigned char *)t->text + wd->text_off;
        const unsigned char *e = s + wd->text_len;
        /* the word's characters */
        while (s < e && ncp < 64)
        {
            uint32_t c = *s++;
            if (c >= 0xF0 && s + 3 <= e)
            {
                c = ((c & 0x07) << 18) | ((s[0] & 0x3F) << 12) | ((s[1] & 0x3F) << 6) | (s[2] & 0x3F);
                s += 3;
            }
            else if (c >= 0xE0 && s + 2 <= e)
            {
                c = ((c & 0x0F) << 12) | ((s[0] & 0x3F) << 6) | (s[1] & 0x3F);
                s += 2;
            }
            else if (c >= 0xC0 && s + 1 <= e)
            {
                c = ((c & 0x1F) << 6) | (s[0] & 0x3F);
                s += 1;
            }
            if (c != ' ')
            {
                cps[ncp++] = c;
            }
        }
        /* the glyphs whose middle is in the box and whose baseline is near its bottom */
        for (g = 0; g < pt->nglyphs; g++)
        {
            const EePdfGlyph *gl = &pt->glyphs[g];
            float cx0 = (gl->x0 + gl->x1) / 2.0f;
            if (cx0 >= x0 - 0.5f && cx0 <= x1 + 0.5f && gl->y0 <= ytop && gl->y0 >= ybot - 0.45f * gl->size)
            {
                cand[nc++] = gl;
            }
        }
        if (nc == 0 || nc != ncp)
        {
            continue;
        }
        qsort((void *)cand, nc, sizeof(EePdfGlyph *), go_x_cmp);
        for (i = 0; i < nc; i++)
        {
            if (!cand[i]->named)
            {
                go_vote(cx, cand[i]->fp, cps[i]);
            }
        }
    }
    free((void *)cand);
}

BOOL EeGlyphOcr_Learn(EePdf *pdf, uint32_t npages, EeGlyphOcrStats *st, wchar_t *err, size_t errcch)
{
    GoCtx cx;
    EeOcr *ocr = NULL;
    EeOcrText txt;
    EePdfPageText pt;
    uint32_t p, *fps = NULL, *cps = NULL, nmap = 0;
    size_t i, j;
    BOOL ok = FALSE;

    ZeroMemory(&cx, sizeof(cx));
    ZeroMemory(st, sizeof(*st));
    cx.pdf = pdf;
    if (!EeOcr_Create(&ocr, err, errcch))
    {
        return FALSE;
    }
    EeOcr_TextInit(&txt);
    EePdf_PageTextInit(&pt);
    pt.collect_glyphs = 1;
    for (p = 0; p < npages && !cx.oom; p++)
    {
        float maxx = 0.0f, maxy = 0.0f, k, ph;
        int w, h;
        uint8_t *gray;
        uint32_t g;
        if (!EePdf_ExtractPageText(pdf, p, &pt) || pt.nglyphs == 0)
        {
            continue;
        }
        for (g = 0; g < pt.nglyphs; g++)
        {
            maxx = pt.glyphs[g].x1 > maxx ? pt.glyphs[g].x1 : maxx;
            maxy = pt.glyphs[g].y0 + pt.glyphs[g].size > maxy ? pt.glyphs[g].y0 + pt.glyphs[g].size : maxy;
        }
        ph = maxy + 18.0f;
        /* within the engine's largest image (2,600 pixels when it does not say) */
        k = GO_SCALE;
        if ((maxx + 18.0f) * k > 2600.0f || ph * k > 2600.0f)
        {
            k = 2600.0f / ((maxx + 18.0f) > ph ? (maxx + 18.0f) : ph);
        }
        w = (int)((maxx + 18.0f) * k);
        h = (int)(ph * k);
        gray = (uint8_t *)malloc((size_t)w * (size_t)h);
        if (gray == NULL)
        {
            cx.oom = TRUE;
            break;
        }
        if (go_draw(&cx, &pt, gray, w, h, k, ph) &&
            EeOcr_RecognizePixels(ocr, gray, (uint32_t)w, (uint32_t)h, (uint32_t)w, 1, 1.0, &txt, NULL, 0))
        {
            float sx = txt.width != 0 ? (float)w / (float)txt.width : 1.0f;
            float sy = txt.height != 0 ? (float)h / (float)txt.height : 1.0f;
            go_align(&cx, &pt, &txt, k, ph, sx, sy);
            st->pages++;
        }
        free(gray);
    }
    /* each drawing's character: the one most of its occurrences were read as */
    fps = (uint32_t *)malloc((cx.nvotes + 1) * sizeof(uint32_t));
    cps = (uint32_t *)malloc((cx.nvotes + 1) * sizeof(uint32_t));
    if (fps == NULL || cps == NULL || cx.oom)
    {
        StringCchCopyW(err, errcch, L"Out of memory reading the glyphs.");
        goto done;
    }
    for (i = 0; i < cx.nvotes; i++)
    {
        uint32_t best = 0, total = 0, cp = 0;
        BOOL seen = FALSE;
        for (j = 0; j < i; j++)
        {
            seen = seen || cx.votes[j].fp == cx.votes[i].fp;
        }
        if (seen)
        {
            continue;
        }
        for (j = i; j < cx.nvotes; j++)
        {
            if (cx.votes[j].fp == cx.votes[i].fp)
            {
                total += cx.votes[j].n;
                if (cx.votes[j].n > best)
                {
                    best = cx.votes[j].n;
                    cp = cx.votes[j].cp;
                }
            }
        }
        st->drawings++;
        fps[nmap] = cx.votes[i].fp;
        cps[nmap] = cp;
        nmap++;
        st->learned++;
        if (total < 2 || best * 3 < total * 2)
        {
            st->uncertain++;
        }
    }
    if (nmap == 0)
    {
        StringCchCopyW(err, errcch, L"OCR read no words that line up with the report's glyphs.");
        goto done;
    }
    ok = EePdf_SetGlyphChars(pdf, fps, cps, nmap);
    if (!ok)
    {
        StringCchCopyW(err, errcch, L"Out of memory reading the glyphs.");
    }
done:
    free(fps);
    free(cps);
    for (i = 0; i < cx.noutl; i++)
    {
        EePdf_OutlineFree(&cx.outl[i].o);
    }
    free(cx.outl);
    free(cx.votes);
    EePdf_PageTextFree(&pt);
    EeOcr_TextFree(&txt);
    EeOcr_Destroy(ocr);
    return ok;
}
