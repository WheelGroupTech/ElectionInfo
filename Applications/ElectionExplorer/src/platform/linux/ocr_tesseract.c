/**
 * @file ocr_tesseract.c
 * @brief Text recognition on Linux through Tesseract (libtesseract's C API) and Leptonica,
 *        behind the engine interface in src/platform/posix/ocr_engine.h (for ocr_bridge.c).
 *
 * The image is prepared as ocr_win.c prepares it for Windows OCR: decoded (Leptonica: JPEG,
 * PNG, TIFF including CCITT fax), made 8-bit gray, scaled (smoothed when made smaller, linear
 * when larger), then turned. Tesseract reads it with automatic page layout and English data
 * (the tesseract-ocr-eng package). Each word comes with its box; a word starting a Tesseract
 * text line starts a line.
 *
 * Where Tesseract differs from Windows OCR, which the readers were built on:
 * - It reads text turned sideways where it is; such words are left out, so a page scanned
 *   sideways reads poorly and the readers turn it, as on Windows (Midland's reports).
 * - It gives printed rules as words of spaces; those are left out.
 * - It takes a heading band printed in halftone dots for a picture and does not read it
 *   (Guadalupe's 2026 Electionware reports). Blurring and thresholding the page first reads the
 *   band but loses more elsewhere, so the page is read as scanned.
 *
 * Tesseract's OpenMP threads slow badly when several processes read at once on a machine's
 * cores (a batch of data_check runs): set OMP_THREAD_LIMIT=1 for those.
 */

#include <leptonica/allheaders.h>
#include <tesseract/capi.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "ocr_engine.h"

typedef struct TessEngine
{
    TessBaseAPI *api;
} TessEngine;

static void set_err(char *err, size_t cap, const char *msg)
{
    if (err != NULL && cap > 0)
    {
        snprintf(err, cap, "%s", msg);
    }
}

const char *EeOcrEngine_Name(void)
{
    return "Tesseract";
}

void *EeOcrEngine_Open(char *err, size_t errcap)
{
    TessEngine *e = (TessEngine *)calloc(1, sizeof(TessEngine));
    if (e == NULL)
    {
        set_err(err, errcap, "Out of memory.");
        return NULL;
    }
    setMsgSeverity(L_SEVERITY_NONE); /* (Leptonica's messages go to stderr) */
    e->api = TessBaseAPICreate();
    if (e->api == NULL || TessBaseAPIInit3(e->api, NULL, "eng") != 0)
    {
        set_err(
            err,
            errcap,
            "Tesseract could not load its English data. Install the tesseract-ocr-eng package.");
        if (e->api != NULL)
        {
            TessBaseAPIDelete(e->api);
        }
        free(e);
        return NULL;
    }
    TessBaseAPISetVariable(e->api, "debug_file", "/dev/null"); /* (its warnings too) */
    TessBaseAPISetPageSegMode(e->api, PSM_AUTO);
    return e;
}

void EeOcrEngine_Close(void *engine)
{
    TessEngine *e = (TessEngine *)engine;
    if (e != NULL)
    {
        TessBaseAPIEnd(e->api);
        TessBaseAPIDelete(e->api);
        free(e);
    }
}

/* Raw pixels as an 8-bit gray Pix (RGB averaged by luminance), or NULL. */
static PIX *pix_of_pixels(const uint8_t *px, uint32_t w, uint32_t h, uint32_t stride, int channels)
{
    PIX *p = pixCreate((l_int32)w, (l_int32)h, 8);
    l_uint32 *data;
    l_int32 wpl;
    uint32_t x, y;
    if (p == NULL)
    {
        return NULL;
    }
    data = pixGetData(p);
    wpl = pixGetWpl(p);
    for (y = 0; y < h; y++)
    {
        const uint8_t *row = px + (size_t)y * stride;
        l_uint32 *line = data + (size_t)y * (size_t)wpl;
        for (x = 0; x < w; x++)
        {
            unsigned v =
                channels == 1
                    ? row[x]
                    : (77u * row[3 * x] + 150u * row[3 * x + 1] + 29u * row[3 * x + 2]) >> 8;
            SET_DATA_BYTE(line, x, v);
        }
    }
    return p;
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
    TessEngine *e = (TessEngine *)engine;
    PIX *src, *gray, *pix;
    TessResultIterator *ri;
    l_int32 iw, ih, tw, th;
    int ok = 1, q = ((quarter_turns % 4) + 4) % 4;
    if (e == NULL)
    {
        set_err(err, errcap, "Invalid arguments.");
        return 0;
    }
    if (data != NULL)
    {
        src = pixReadMem((const l_uint8 *)data, len);
        if (src == NULL)
        {
            set_err(err, errcap, "The page image could not be decoded");
            return 0;
        }
    }
    else
    {
        if (px == NULL || w == 0 || h == 0 || (channels != 1 && channels != 3) ||
            stride < w * (uint32_t)channels)
        {
            set_err(err, errcap, "Invalid arguments.");
            return 0;
        }
        src = pix_of_pixels(px, w, h, stride, channels);
        if (src == NULL)
        {
            set_err(err, errcap, "The page image could not be read");
            return 0;
        }
    }
    gray = pixConvertTo8(src, 0);
    pixDestroy(&src);
    if (gray == NULL)
    {
        set_err(err, errcap, "The page image could not be prepared for OCR");
        return 0;
    }
    iw = pixGetWidth(gray);
    ih = pixGetHeight(gray);
    if (scale <= 0.0)
    {
        scale = 1.0;
    }
    tw = (l_int32)(iw * scale + 0.5);
    th = (l_int32)(ih * scale + 0.5);
    if (max_dim > 0 && ((uint32_t)tw > max_dim || (uint32_t)th > max_dim))
    {
        double f = (double)max_dim / (double)(tw > th ? tw : th);
        tw = (l_int32)(tw * f);
        th = (l_int32)(th * f);
    }
    if (tw <= 0 || th <= 0)
    {
        pixDestroy(&gray);
        set_err(err, errcap, "The page image could not be scaled");
        return 0;
    }
    if (tw != iw || th != ih)
    {
        pix = pixScaleToSize(gray, tw, th);
        pixDestroy(&gray);
    }
    else
    {
        pix = gray;
    }
    if (pix != NULL && q != 0)
    {
        PIX *t = pixRotateOrth(pix, q);
        pixDestroy(&pix);
        pix = t;
    }
    if (pix == NULL)
    {
        set_err(err, errcap, "The page image could not be prepared for OCR");
        return 0;
    }
    TessBaseAPISetImage2(e->api, pix);
    TessBaseAPISetSourceResolution(e->api, 200);
    if (TessBaseAPIRecognize(e->api, NULL) != 0)
    {
        TessBaseAPIClear(e->api);
        pixDestroy(&pix);
        set_err(err, errcap, "Tesseract text recognition failed");
        return 0;
    }
    ri = TessBaseAPIGetIterator(e->api);
    if (ri != NULL)
    {
        TessPageIterator *pi = TessResultIteratorGetPageIterator(ri);
        int line_start = 0;
        do
        {
            char *word = TessResultIteratorGetUTF8Text(ri, RIL_WORD);
            int l, t, r, b;
            line_start = line_start || TessPageIteratorIsAtBeginningOf(pi, RIL_TEXTLINE);
            if (word != NULL && word[strspn(word, " \t\r\n")] != '\0' &&
                TessPageIteratorBoundingBox(pi, RIL_WORD, &l, &t, &r, &b) &&
                !(strlen(word) >= 3 && (double)(b - t) > 1.5 * (double)(r - l)))
            {
                /* (a "word" of spaces only is a printed rule: left out) */
                /* (a word turned sideways, its box much taller than wide, is left out: Windows
                 * OCR does not read it, and the readers turn a page scanned sideways until it
                 * reads well) */
                ok = fn(user,
                        line_start,
                        word,
                        strlen(word),
                        (float)l,
                        (float)t,
                        (float)r,
                        (float)b);
                line_start = 0;
            }
            if (word != NULL)
            {
                TessDeleteText(word);
            }
        } while (ok && TessPageIteratorNext(pi, RIL_WORD));
        TessResultIteratorDelete(ri);
    }
    if (out_w != NULL)
    {
        *out_w = (uint32_t)pixGetWidth(pix);
    }
    if (out_h != NULL)
    {
        *out_h = (uint32_t)pixGetHeight(pix);
    }
    TessBaseAPIClear(e->api);
    pixDestroy(&pix);
    if (!ok)
    {
        set_err(err, errcap, "Out of memory reading the OCR result.");
    }
    return ok;
}
