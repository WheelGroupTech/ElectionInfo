/**
 * @file ocr_vision.m
 * @brief Text recognition through Apple's Vision framework (VNRecognizeTextRequest), behind the
 *        engine interface in src/platform/posix/ocr_engine.h (for ocr_bridge.c). Compiled with ARC.
 *
 * The image is prepared as ocr_win.c prepares it for Windows OCR: decoded (ImageIO), scaled with
 * high-quality interpolation, made 8-bit gray, then turned. Vision then reads it in its accurate
 * mode, revision 2, with language correction off, so counts and names come back as printed.
 * Each observation is a line. Its words are the runs of characters between spaces, each boxed
 * with boundingBoxForRange.
 *
 * Where Vision differs from Windows OCR, which the readers were built on:
 * - Printed rules are whitened first (remove_rules): accurate mode skips a count on an
 *   underline.
 * - Word boxes are trimmed to their ink (tighten): Vision pads them to a line's height and a
 *   few pixels each side, which merges rows and moves numbers out of their columns.
 * - Lines turned sideways are left out: Windows OCR does not read them, and the readers turn a
 *   sideways page until it reads well.
 */

#import <CoreGraphics/CoreGraphics.h>
#import <Foundation/Foundation.h>
#import <ImageIO/ImageIO.h>
#import <Vision/Vision.h>

#include <stdlib.h>
#include <string.h>

#include "ocr_engine.h"

static void set_err(char *err, size_t cap, const char *msg)
{
    if (err != NULL && cap > 0)
    {
        snprintf(err, cap, "%s", msg);
    }
}

const char *EeOcrEngine_Name(void)
{
    return "Apple Vision";
}

void *EeOcrEngine_Open(char *err, size_t errcap)
{
    static int k_vision; /* (Vision keeps no state between images) */
    if (@available(macOS 10.15, *))
    {
        return &k_vision;
    }
    set_err(err, errcap, "Text recognition (Apple Vision) needs macOS 10.15 or later.");
    return NULL;
}

void EeOcrEngine_Close(void *engine)
{
    (void)engine;
}

/* The image's pixels as an 8-bit gray buffer of @p tw x @p th (caller frees), drawn with
 * high-quality interpolation on white. */
static uint8_t *gray_of(CGImageRef img, uint32_t tw, uint32_t th)
{
    CGColorSpaceRef gs = CGColorSpaceCreateDeviceGray();
    uint8_t *buf = (uint8_t *)calloc((size_t)tw * th, 1);
    CGContextRef ctx = NULL;
    if (gs != NULL && buf != NULL)
    {
        ctx = CGBitmapContextCreate(buf, tw, th, 8, tw, gs, (CGBitmapInfo)kCGImageAlphaNone);
    }
    if (ctx == NULL)
    {
        free(buf);
        buf = NULL;
    }
    else
    {
        CGContextSetGrayFillColor(ctx, 1.0, 1.0);
        CGContextFillRect(ctx, CGRectMake(0, 0, tw, th));
        CGContextSetInterpolationQuality(ctx, kCGInterpolationHigh);
        CGContextDrawImage(ctx, CGRectMake(0, 0, tw, th), img);
        CGContextRelease(ctx);
    }
    if (gs != NULL)
    {
        CGColorSpaceRelease(gs);
    }
    return buf;
}

/* @p src (w x h gray) turned @p q x 90 degrees clockwise into a new buffer (caller frees). */
static uint8_t *turned(const uint8_t *src, uint32_t w, uint32_t h, int q)
{
    uint8_t *dst = (uint8_t *)malloc((size_t)w * h);
    uint32_t x, y;
    if (dst == NULL)
    {
        return NULL;
    }
    for (y = 0; y < h; y++)
    {
        const uint8_t *row = src + (size_t)y * w;
        for (x = 0; x < w; x++)
        {
            size_t o;
            if (q == 1)
            {
                o = (size_t)x * h + (h - 1 - y); /* new size h x w: (h-1-y, x) */
            }
            else if (q == 2)
            {
                o = (size_t)(h - 1 - y) * w + (w - 1 - x);
            }
            else
            {
                o = (size_t)(w - 1 - x) * h + y; /* q == 3: (y, w-1-x) */
            }
            dst[o] = row[x];
        }
    }
    return dst;
}

/* Printed rules turned white: Vision's accurate mode does not read a count that sits on an
 * underline (the Secretary of State form's), where Windows OCR does. A dark run along a row
 * longer than 1/25 of the page (at least 60 pixels) is a rule, not part of a letter. */
static void remove_rules(uint8_t *g, uint32_t w, uint32_t h)
{
    uint32_t minrun = w / 25 > 60 ? w / 25 : 60, x, y;
    for (y = 0; y < h; y++)
    {
        uint8_t *row = g + (size_t)y * w;
        x = 0;
        while (x < w)
        {
            uint32_t s;
            if (row[x] >= 128)
            {
                x++;
                continue;
            }
            s = x;
            while (x < w && row[x] < 128)
            {
                x++;
            }
            if (x - s >= minrun)
            {
                memset(row + s, 255, x - s);
            }
        }
    }
}

/* Row @p row has ink (two dark pixels or more) in columns [xa, xb). */
static int ink_row(const uint8_t *row, int xa, int xb)
{
    int x, n = 0;
    for (x = xa; x < xb; x++)
    {
        if (row[x] < 160 && ++n >= 2)
        {
            return 1;
        }
    }
    return 0;
}

/* Vision's word boxes are a line's height, padded, about twice the letters'; Windows OCR's are
 * the letters' own, which the readers' row grouping expects. Keep the band of rows with ink
 * in the box's columns around the box's centre (gaps of one row bridged). */
static void tighten(const uint8_t *g, uint32_t w, uint32_t h, float *x0, float *y0, float *x1, float *y1)
{
    int xa = (int)*x0, xb = (int)*x1, ya = (int)*y0, yb = (int)*y1, yc, top, bot, y, miss;
    if (xa < 0)
        xa = 0;
    if (xb > (int)w)
        xb = (int)w;
    if (ya < 0)
        ya = 0;
    if (yb > (int)h)
        yb = (int)h;
    if (xb - xa < 1 || yb - ya < 3)
    {
        return;
    }
#define INKED(yy) ink_row(g + (size_t)(yy)*w, xa, xb)
    yc = (ya + yb) / 2;
    /* the inked row nearest the centre */
    for (y = 0; y <= (yb - ya) / 2; y++)
    {
        if (yc - y >= ya && INKED(yc - y))
        {
            yc -= y;
            break;
        }
        if (yc + y < yb && INKED(yc + y))
        {
            yc += y;
            break;
        }
    }
    if (!INKED(yc))
    {
        return;
    }
    top = bot = yc;
    for (y = yc - 1, miss = 0; y >= ya && miss < 2; y--)
    {
        if (INKED(y))
        {
            top = y;
            miss = 0;
        }
        else
        {
            miss++;
        }
    }
    for (y = yc + 1, miss = 0; y < yb && miss < 2; y++)
    {
        if (INKED(y))
        {
            bot = y;
            miss = 0;
        }
        else
        {
            miss++;
        }
    }
#undef INKED
    *y0 = (float)top;
    *y1 = (float)(bot + 1);
    /* and the first and last columns with ink in that band (padded about 7 pixels a side at
     * 2,200 pixels a page, which moves a number's right edge out of its column) */
    {
        int x, left = -1, right = -1;
        for (x = xa; x < xb && left < 0; x++)
        {
            for (y = top; y <= bot; y++)
            {
                if (g[(size_t)y * w + x] < 160)
                {
                    left = x;
                    break;
                }
            }
        }
        for (x = xb - 1; x >= xa && right < 0; x--)
        {
            for (y = top; y <= bot; y++)
            {
                if (g[(size_t)y * w + x] < 160)
                {
                    right = x;
                    break;
                }
            }
        }
        if (left >= 0 && right >= left)
        {
            *x0 = (float)left;
            *x1 = (float)(right + 1);
        }
    }
}

/* A CGImage over raw pixels (gray or RGB, 8 bits a sample), or NULL. */
static CGImageRef image_of_pixels(const uint8_t *px, uint32_t w, uint32_t h, uint32_t stride, int channels)
{
    CGColorSpaceRef cs = channels == 1 ? CGColorSpaceCreateDeviceGray() : CGColorSpaceCreateDeviceRGB();
    CGDataProviderRef prov = CGDataProviderCreateWithData(NULL, px, (size_t)stride * h, NULL);
    CGImageRef img = NULL;
    if (cs != NULL && prov != NULL)
    {
        img = CGImageCreate(w, h, 8, 8 * (size_t)channels, stride, cs, (CGBitmapInfo)kCGImageAlphaNone, prov, NULL,
                            false, kCGRenderingIntentDefault);
    }
    if (prov != NULL)
    {
        CGDataProviderRelease(prov);
    }
    if (cs != NULL)
    {
        CGColorSpaceRelease(cs);
    }
    return img;
}

int EeOcrEngine_Recognize(void *engine,
                          const void *data,
                          size_t len,
                          const uint8_t *px,
                          uint32_t w,
                          uint32_t h,
                          uint32_t stride,
                          int channels,
                          double scale,
                          uint32_t max_dim,
                          int quarter_turns,
                          EeOcrEngineWordFn fn,
                          void *user,
                          uint32_t *out_w,
                          uint32_t *out_h,
                          char *err,
                          size_t errcap)
{
    (void)engine;
    @autoreleasepool
    {
        CGImageRef img = NULL;
        uint8_t *gray = NULL, *t = NULL;
        uint32_t iw, ih, tw, th;
        int ok = 0, q = ((quarter_turns % 4) + 4) % 4;

        if (data != NULL)
        {
            NSData *d = [NSData dataWithBytesNoCopy:(void *)data length:len freeWhenDone:NO];
            CGImageSourceRef s = CGImageSourceCreateWithData((__bridge CFDataRef)d, NULL);
            if (s != NULL)
            {
                img = CGImageSourceCreateImageAtIndex(s, 0, NULL);
                CFRelease(s);
            }
            if (img == NULL)
            {
                set_err(err, errcap, "The page image could not be decoded");
                return 0;
            }
        }
        else
        {
            if (px == NULL || w == 0 || h == 0 || (channels != 1 && channels != 3) || stride < w * (uint32_t)channels)
            {
                set_err(err, errcap, "Invalid arguments.");
                return 0;
            }
            img = image_of_pixels(px, w, h, stride, channels);
            if (img == NULL)
            {
                set_err(err, errcap, "The page image could not be read");
                return 0;
            }
        }
        iw = (uint32_t)CGImageGetWidth(img);
        ih = (uint32_t)CGImageGetHeight(img);
        if (iw == 0 || ih == 0)
        {
            CGImageRelease(img);
            set_err(err, errcap, "The page image could not be read");
            return 0;
        }
        if (scale <= 0.0)
        {
            scale = 1.0;
        }
        tw = (uint32_t)(iw * scale + 0.5);
        th = (uint32_t)(ih * scale + 0.5);
        if (max_dim > 0 && (tw > max_dim || th > max_dim))
        {
            double f = (double)max_dim / (double)(tw > th ? tw : th);
            tw = (uint32_t)(tw * f);
            th = (uint32_t)(th * f);
        }
        if (tw == 0 || th == 0)
        {
            CGImageRelease(img);
            set_err(err, errcap, "The page image could not be scaled");
            return 0;
        }
        gray = gray_of(img, tw, th);
        CGImageRelease(img);
        img = NULL;
        if (gray == NULL)
        {
            set_err(err, errcap, "The page image could not be prepared for OCR");
            return 0;
        }
        if (q != 0)
        {
            t = turned(gray, tw, th, q);
            free(gray);
            gray = t;
            if (gray == NULL)
            {
                set_err(err, errcap, "The page image could not be turned");
                return 0;
            }
            if (q & 1)
            {
                uint32_t s = tw;
                tw = th;
                th = s;
            }
        }
        remove_rules(gray, tw, th);
        img = image_of_pixels(gray, tw, th, tw, 1);
        if (img == NULL)
        {
            free(gray);
            set_err(err, errcap, "The page image could not be prepared for OCR");
            return 0;
        }
        if (@available(macOS 10.15, *))
        {
            NSError *e = nil;
            VNRecognizeTextRequest *req = [[VNRecognizeTextRequest alloc] init];
            VNImageRequestHandler *handler = [[VNImageRequestHandler alloc] initWithCGImage:img options:@{}];
            req.recognitionLevel = VNRequestTextRecognitionLevelAccurate;
            req.usesLanguageCorrection = NO;
            req.recognitionLanguages = @[ @"en-US" ];
            req.minimumTextHeight = 0.004f; /* about 9 pixels of a 2,200-pixel page */
            if (@available(macOS 11.0, *))
            {
                /* revision 3 (macOS 13's default) drops letters at the ends of lines and invents
                 * words on some scans (Henderson's 2021 canvass report: "ROPOSITION 1 (HJR 14",
                 * "CONOUCT OHAR FOBEE"); revision 2 reads them */
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wdeprecated-declarations" /* (deprecated in macOS 15, still supported) */
                if ([[VNRecognizeTextRequest supportedRevisions] containsIndex:VNRecognizeTextRequestRevision2])
                {
                    req.revision = VNRecognizeTextRequestRevision2;
                }
#pragma clang diagnostic pop
            }
            if (![handler performRequests:@[ req ] error:&e])
            {
                set_err(err, errcap, e != nil ? e.localizedDescription.UTF8String : "Apple Vision text recognition failed");
            }
            else
            {
                ok = 1;
                for (VNRecognizedTextObservation *obs in req.results)
                {
                    VNRecognizedText *cand = [obs topCandidates:1].firstObject;
                    NSString *s = cand.string;
                    NSUInteger n = s.length, i = 0;
                    int first = 1;
                    /* a line turned sideways (its box much taller than wide): Windows OCR reads
                     * only upright text, and the readers count on that to turn a page scanned
                     * sideways (Midland's) until it reads well, so leave it out */
                    if (n >= 4 && obs.boundingBox.size.height * th > 1.5 * obs.boundingBox.size.width * tw)
                    {
                        continue;
                    }
                    while (ok && i < n)
                    {
                        NSUInteger j;
                        NSRange r;
                        VNRectangleObservation *box;
                        CGRect bb;
                        const char *u8;
                        while (i < n && [[NSCharacterSet whitespaceCharacterSet] characterIsMember:[s characterAtIndex:i]])
                        {
                            i++;
                        }
                        if (i >= n)
                        {
                            break;
                        }
                        j = i;
                        while (j < n && ![[NSCharacterSet whitespaceCharacterSet] characterIsMember:[s characterAtIndex:j]])
                        {
                            j++;
                        }
                        r = NSMakeRange(i, j - i);
                        box = [cand boundingBoxForRange:r error:nil];
                        bb = box != nil ? box.boundingBox : obs.boundingBox;
                        u8 = [s substringWithRange:r].UTF8String;
                        if (u8 != NULL)
                        {
                            float x0 = (float)(bb.origin.x * tw), x1 = (float)((bb.origin.x + bb.size.width) * tw);
                            float y0 = (float)((1.0 - bb.origin.y - bb.size.height) * th);
                            float y1 = (float)((1.0 - bb.origin.y) * th);
                            tighten(gray, tw, th, &x0, &y0, &x1, &y1);
                            ok = fn(user, first, u8, strlen(u8), x0, y0, x1, y1);
                            first = 0;
                        }
                        i = j;
                    }
                }
                if (!ok)
                {
                    set_err(err, errcap, "Out of memory reading the OCR result.");
                }
            }
        }
        CGImageRelease(img);
        free(gray);
        if (out_w != NULL)
        {
            *out_w = tw;
        }
        if (out_h != NULL)
        {
            *out_h = th;
        }
        return ok;
    }
}
