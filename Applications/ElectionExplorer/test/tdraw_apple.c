/**
 * @file tdraw_apple.c
 * @brief macOS test fixtures' text drawing (see tdraw.h): CoreGraphics bitmaps and CoreText
 *        text and glyph outlines.
 */

#include <CoreGraphics/CoreGraphics.h>
#include <CoreText/CoreText.h>

#include <stdlib.h>
#include <string.h>

#include "tdraw.h"

struct TDrawPage
{
    CGContextRef ctx;
    CTFontRef font;
    unsigned char *px;
    int w, h;
};

int TDraw_Available(void)
{
    return 1;
}

TDrawPage *TDraw_PageBegin(int w, int h, double font_px)
{
    TDrawPage *p = (TDrawPage *)calloc(1, sizeof(TDrawPage));
    CGColorSpaceRef gs = CGColorSpaceCreateDeviceGray();
    if (p == NULL || gs == NULL)
    {
        free(p);
        if (gs != NULL)
        {
            CGColorSpaceRelease(gs);
        }
        return NULL;
    }
    p->w = w;
    p->h = h;
    p->px = (unsigned char *)malloc((size_t)w * (size_t)h);
    if (p->px != NULL)
    {
        p->ctx = CGBitmapContextCreate(p->px,
                                       (size_t)w,
                                       (size_t)h,
                                       8,
                                       (size_t)w,
                                       gs,
                                       (CGBitmapInfo)kCGImageAlphaNone);
    }
    CGColorSpaceRelease(gs);
    p->font = CTFontCreateWithName(CFSTR("Helvetica"), font_px, NULL);
    if (p->ctx == NULL || p->font == NULL)
    {
        if (p->ctx != NULL)
        {
            CGContextRelease(p->ctx);
        }
        if (p->font != NULL)
        {
            CFRelease(p->font);
        }
        free(p->px);
        free(p);
        return NULL;
    }
    CGContextSetGrayFillColor(p->ctx, 1.0, 1.0);
    CGContextFillRect(p->ctx, CGRectMake(0, 0, w, h));
    return p;
}

void TDraw_Text(TDrawPage *p, int x, int y, const char *utf8)
{
    CFStringRef s;
    CFStringRef keys[2];
    CFTypeRef vals[2];
    CFDictionaryRef attrs;
    CFAttributedStringRef as;
    CTLineRef line;
    CGColorRef black;
    if (p == NULL || utf8 == NULL)
    {
        return;
    }
    s = CFStringCreateWithCString(NULL, utf8, kCFStringEncodingUTF8);
    black = CGColorCreateGenericGray(0.0, 1.0);
    keys[0] = kCTFontAttributeName;
    vals[0] = p->font;
    keys[1] = kCTForegroundColorAttributeName;
    vals[1] = black;
    attrs = CFDictionaryCreate(NULL,
                               (const void **)keys,
                               (const void **)vals,
                               2,
                               &kCFTypeDictionaryKeyCallBacks,
                               &kCFTypeDictionaryValueCallBacks);
    as = CFAttributedStringCreate(NULL, s, attrs);
    line = CTLineCreateWithAttributedString(as);
    /* GDI's (x, y) is the cell's top-left; CoreText draws from the baseline, y up */
    CGContextSetTextPosition(p->ctx, x, (double)p->h - (double)y - CTFontGetAscent(p->font));
    CTLineDraw(line, p->ctx);
    CFRelease(line);
    CFRelease(as);
    CFRelease(attrs);
    CGColorRelease(black);
    CFRelease(s);
}

unsigned char *TDraw_PageEnd(TDrawPage *p)
{
    unsigned char *px;
    if (p == NULL)
    {
        return NULL;
    }
    CGContextFlush(p->ctx);
    px = p->px; /* (rows top-down: a bitmap context's first row is the image's top) */
    CGContextRelease(p->ctx);
    CFRelease(p->font);
    free(p);
    return px;
}

typedef struct TDrawPathCx
{
    TDrawPathFn fn;
    void *user;
    double scale; /* font units to ems */
    double cx, cy;
    int ok;
} TDrawPathCx;

static void path_element(void *info, const CGPathElement *e)
{
    TDrawPathCx *c = (TDrawPathCx *)info;
    const CGPoint *q = e->points;
    double s = c->scale;
    if (!c->ok)
    {
        return;
    }
    switch (e->type)
    {
        case kCGPathElementMoveToPoint:
            c->ok = c->fn(c->user, 0, q[0].x * s, q[0].y * s, 0, 0, 0, 0);
            c->cx = q[0].x * s;
            c->cy = q[0].y * s;
            break;
        case kCGPathElementAddLineToPoint:
            c->ok = c->fn(c->user, 1, q[0].x * s, q[0].y * s, 0, 0, 0, 0);
            c->cx = q[0].x * s;
            c->cy = q[0].y * s;
            break;
        case kCGPathElementAddQuadCurveToPoint:
        {
            /* a quadratic curve as the cubic that draws it */
            double qx = q[0].x * s, qy = q[0].y * s, ex = q[1].x * s, ey = q[1].y * s;
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
            break;
        }
        case kCGPathElementAddCurveToPoint:
            c->ok = c->fn(c->user,
                          2,
                          q[0].x * s,
                          q[0].y * s,
                          q[1].x * s,
                          q[1].y * s,
                          q[2].x * s,
                          q[2].y * s);
            c->cx = q[2].x * s;
            c->cy = q[2].y * s;
            break;
        case kCGPathElementCloseSubpath:
            c->ok = c->fn(c->user, 3, 0, 0, 0, 0, 0, 0);
            break;
    }
}

int TDraw_GlyphOutline(char ch, TDrawPathFn fn, void *user)
{
    CTFontRef font = CTFontCreateWithName(CFSTR("ArialMT"), 1000.0, NULL);
    UniChar u = (UniChar)(unsigned char)ch;
    CGGlyph g = 0;
    CGPathRef path;
    TDrawPathCx c;
    if (font == NULL)
    {
        return 0;
    }
    if (!CTFontGetGlyphsForCharacters(font, &u, &g, 1))
    {
        CFRelease(font);
        return 0;
    }
    path = CTFontCreatePathForGlyph(font, g, NULL);
    CFRelease(font);
    if (path == NULL)
    {
        return 0;
    }
    c.fn = fn;
    c.user = user;
    c.scale = 1.0 / 1000.0;
    c.cx = c.cy = 0.0;
    c.ok = 1;
    CGPathApply(path, &c, path_element);
    CGPathRelease(path);
    return c.ok;
}
