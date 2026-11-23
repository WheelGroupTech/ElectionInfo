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
 *   (Guadalupe's 2026 Electionware reports). A picture block shaped like a band, in which the
 *   page's reading found no words, is read again on its own, blurred and thresholded so the dots
 *   turn white (picture_bands, read_band). Doing that to the whole page loses more elsewhere.
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

#define EE_TESS_MAX_BANDS 16

/* The words of Tesseract's last recognition, moved by (@p dx, @p dy), to @p fn. A "word" of
 * spaces only (a printed rule) is left out, and so is a word turned sideways (its box much
 * taller than wide): Windows OCR does not read it, and the readers turn a page scanned sideways
 * until it reads well. Returns 0 when @p fn stops. */
static int emit_words(TessBaseAPI *api,
                      EeOcrEngineWordFn fn,
                      void *user,
                      int dx,
                      int dy,
                      int up,
                      const BOX *bands,
                      int nbands,
                      int *inside)
{
    TessResultIterator *ri = TessBaseAPIGetIterator(api);
    int ok = 1;
    if (ri != NULL)
    {
        TessPageIterator *pi = TessResultIteratorGetPageIterator(ri);
        int line_start = 1;
        do
        {
            char *word = TessResultIteratorGetUTF8Text(ri, RIL_WORD);
            int l, t, r, b;
            line_start = line_start || TessPageIteratorIsAtBeginningOf(pi, RIL_TEXTLINE);
            if (word != NULL && word[strspn(word, " \t\r\n")] != '\0' &&
                TessPageIteratorBoundingBox(pi, RIL_WORD, &l, &t, &r, &b) &&
                !(strlen(word) >= 3 && (double)(b - t) > 1.5 * (double)(r - l)))
            {
                int k, cx = (l + r) / 2 + dx, cy = (t + b) / 2 + dy;
                ok = fn(user,
                        line_start,
                        word,
                        strlen(word),
                        (float)(l + dx) / up,
                        (float)(t + dy) / up,
                        (float)(r + dx) / up,
                        (float)(b + dy) / up);
                line_start = 0;
                for (k = 0; k < nbands; k++)
                {
                    /* (a word the page's own reading found in a picture band) */
                    inside[k] += cx >= bands[k].x && cx < bands[k].x + bands[k].w &&
                                 cy >= bands[k].y && cy < bands[k].y + bands[k].h;
                }
            }
            if (word != NULL)
            {
                TessDeleteText(word);
            }
        } while (ok && TessPageIteratorNext(pi, RIL_WORD));
        TessResultIteratorDelete(ri);
    }
    return ok;
}

/* The blocks Tesseract's page layout takes for pictures that are bands: a quarter of the page
 * wide or more, and four times as wide as high. (A table's column headings printed on halftone
 * dots, Guadalupe's 2026 Electionware reports; a seal or a photo is not a band.) */
static int picture_bands(TessBaseAPI *api, l_int32 page_w, BOX *bands)
{
    TessPageIterator *it = TessBaseAPIAnalyseLayout(api);
    int n = 0;
    if (it == NULL)
    {
        return 0;
    }
    do
    {
        TessPolyBlockType ty = TessPageIteratorBlockType(it);
        int l, t, r, b;
        if ((ty == PT_FLOWING_IMAGE || ty == PT_HEADING_IMAGE || ty == PT_PULLOUT_IMAGE) &&
            TessPageIteratorBoundingBox(it, RIL_BLOCK, &l, &t, &r, &b) && (r - l) * 4 >= page_w &&
            (r - l) >= 4 * (b - t) && b > t && n < EE_TESS_MAX_BANDS)
        {
            bands[n].x = l;
            bands[n].y = t;
            bands[n].w = r - l;
            bands[n].h = b - t;
            n++;
        }
    } while (TessPageIteratorNext(it, RIL_BLOCK));
    TessPageIteratorDelete(it);
    return n;
}

/* Read a picture band again on its own: blurred a little and thresholded, so its halftone
 * dots turn white and its letters stay black, as one block of text. */
static int read_band(TessBaseAPI *api,
                     PIX *page,
                     const BOX *band,
                     int up,
                     EeOcrEngineWordFn fn,
                     void *user)
{
    BOX *box = boxCreate(band->x, band->y, band->w, band->h);
    PIX *clip = box != NULL ? pixClipRectangle(page, box, NULL) : NULL;
    PIX *blur = clip != NULL ? pixBlockconv(clip, 2, 2) : NULL;
    PIX *bin = blur != NULL ? pixThresholdToBinary(blur, 140) : NULL;
    int ok = 1;
    if (bin != NULL)
    {
        TessBaseAPISetImage2(api, bin);
        TessBaseAPISetSourceResolution(api, 200);
        TessBaseAPISetPageSegMode(api, PSM_SINGLE_BLOCK);
        if (TessBaseAPIRecognize(api, NULL) == 0)
        {
            ok = emit_words(api, fn, user, band->x, band->y, up, NULL, 0, NULL);
        }
        TessBaseAPISetPageSegMode(api, PSM_AUTO);
    }
    pixDestroy(&bin);
    pixDestroy(&blur);
    pixDestroy(&clip);
    boxDestroy(&box);
    return ok;
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
    BOX bands[EE_TESS_MAX_BANDS];
    int inside[EE_TESS_MAX_BANDS];
    l_int32 iw, ih, tw, th;
    int ok = 1, q = ((quarter_turns % 4) + 4) % 4, nbands, k, up;
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
    if (out_w != NULL)
    {
        *out_w = (uint32_t)pixGetWidth(pix);
    }
    if (out_h != NULL)
    {
        *out_h = (uint32_t)pixGetHeight(pix);
    }
    {
        /* a small page (Hart's CVR Report read at 1,224 x 1,584, 10-pixel letters) is read at two
         * or three times its size, as Tesseract reads letters 20 or more pixels high (Windows OCR
         * reads them as they are); its words are scaled back to the page */
        l_int32 lng = pixGetWidth(pix) > pixGetHeight(pix) ? pixGetWidth(pix) : pixGetHeight(pix);
        up = lng >= 1800 ? 1 : (lng >= 900 ? 2 : 3);
        if (up > 1)
        {
            PIX *big = pixScale(pix, (l_float32)up, (l_float32)up);
            if (big != NULL)
            {
                pixDestroy(&pix);
                pix = big;
            }
            else
            {
                up = 1;
            }
        }
    }
    TessBaseAPISetImage2(e->api, pix);
    TessBaseAPISetSourceResolution(e->api, 200);
    nbands = picture_bands(e->api, pixGetWidth(pix), bands);
    if (TessBaseAPIRecognize(e->api, NULL) != 0)
    {
        TessBaseAPIClear(e->api);
        pixDestroy(&pix);
        set_err(err, errcap, "Tesseract text recognition failed");
        return 0;
    }
    memset(inside, 0, sizeof(inside));
    ok = emit_words(e->api, fn, user, 0, 0, up, bands, nbands, inside);
    for (k = 0; k < nbands && ok; k++)
    {
        if (inside[k] == 0) /* (a band the page's reading already reads is not read twice) */
        {
            ok = read_band(e->api, pix, &bands[k], up, fn, user);
        }
    }
    TessBaseAPIClear(e->api);
    pixDestroy(&pix);
    if (!ok)
    {
        set_err(err, errcap, "Out of memory reading the OCR result.");
    }
    return ok;
}
