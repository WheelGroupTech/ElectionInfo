/**
 * @file ocr_bridge.c
 * @brief macOS and Linux implementation of the OCR interface in ocr_win.h, on top of an OCR
 *        engine's plain C interface (ocr_engine.h): Apple Vision on macOS
 *        (src/platform/macos/ocr_vision.m), Tesseract on Linux
 *        (src/platform/linux/ocr_tesseract.c).
 *
 * Images are prepared as on Windows (scaled, capped at Windows OCR's 10,000-pixel side, made
 * gray, turned), so the readers built on Windows OCR's output get the same pages.
 */

#include "ocr_win.h"
#include "ocr_engine.h"

#include <stdlib.h>
#include <string.h>
#include <strsafe.h>

#define EE_OCR_MAX_DIM 10000u /* Windows OCR's MaxImageDimension */

struct EeOcr
{
    void *engine;
    int quarter_turns;
};

static void set_err(wchar_t *err, size_t cch, const char *msg)
{
    if (err != NULL && cch > 0)
    {
        if (MultiByteToWideChar(CP_UTF8, 0, msg, -1, err, (int)cch) <= 0)
        {
            StringCchCopyW(err, cch, L"Text recognition failed.");
        }
        err[cch - 1] = L'\0';
    }
}

const wchar_t *EeOcr_EngineName(void)
{
    static wchar_t name[64];
    if (name[0] == L'\0' &&
        MultiByteToWideChar(CP_UTF8, 0, EeOcrEngine_Name(), -1, name, (int)ARRAYSIZE(name)) <= 0)
    {
        StringCchCopyW(name, ARRAYSIZE(name), L"OCR");
    }
    return name;
}

void EeOcr_TextInit(EeOcrText *t)
{
    if (t != NULL)
    {
        ZeroMemory(t, sizeof(*t));
    }
}

void EeOcr_TextFree(EeOcrText *t)
{
    if (t != NULL)
    {
        free(t->words);
        free(t->lines);
        free(t->text);
        ZeroMemory(t, sizeof(*t));
    }
}

BOOL EeOcr_Create(EeOcr **out, wchar_t *err, size_t errcch)
{
    char e[256] = "";
    if (out == NULL)
    {
        return FALSE;
    }
    *out = (EeOcr *)calloc(1, sizeof(EeOcr));
    if (*out == NULL)
    {
        set_err(err, errcch, "Out of memory.");
        return FALSE;
    }
    (*out)->engine = EeOcrEngine_Open(e, sizeof(e));
    if ((*out)->engine == NULL)
    {
        set_err(err, errcch, e[0] != '\0' ? e : "Text recognition is not available.");
        free(*out);
        *out = NULL;
        return FALSE;
    }
    return TRUE;
}

void EeOcr_Destroy(EeOcr *ocr)
{
    if (ocr != NULL)
    {
        EeOcrEngine_Close(ocr->engine);
        free(ocr);
    }
}

void EeOcr_SetRotation(EeOcr *ocr, int quarter_turns)
{
    if (ocr != NULL)
    {
        ocr->quarter_turns = ((quarter_turns % 4) + 4) % 4;
    }
}

static BOOL text_append(EeOcrText *t, const char *s, size_t n)
{
    if (t->text_len + n + 1 > t->text_cap)
    {
        size_t nc = t->text_cap ? t->text_cap * 2 : 4096;
        char *np;
        while (nc < t->text_len + n + 1)
        {
            nc *= 2;
        }
        np = (char *)realloc(t->text, nc);
        if (np == NULL)
        {
            return FALSE;
        }
        t->text = np;
        t->text_cap = nc;
    }
    memcpy(t->text + t->text_len, s, n);
    t->text_len += n;
    t->text[t->text_len] = '\0';
    return TRUE;
}

/* EeOcrEngineWordFn: one word into the EeOcrText, a new line first when the engine starts one. */
static int add_word(void *user,
                    int new_line,
                    const char *utf8,
                    size_t len,
                    float x0,
                    float y0,
                    float x1,
                    float y1)
{
    EeOcrText *t = (EeOcrText *)user;
    EeOcrWord *wd;
    if (new_line || t->nlines == 0)
    {
        if (t->nlines == t->cap_lines)
        {
            uint32_t nc = t->cap_lines ? t->cap_lines * 2 : 64;
            EeOcrLine *nl = (EeOcrLine *)realloc(t->lines, (size_t)nc * sizeof(EeOcrLine));
            if (nl == NULL)
            {
                return 0;
            }
            t->lines = nl;
            t->cap_lines = nc;
        }
        t->lines[t->nlines].first_word = t->nwords;
        t->lines[t->nlines].nwords = 0;
        t->nlines++;
    }
    if (t->nwords == t->cap_words)
    {
        uint32_t nc = t->cap_words ? t->cap_words * 2 : 256;
        EeOcrWord *nw = (EeOcrWord *)realloc(t->words, (size_t)nc * sizeof(EeOcrWord));
        if (nw == NULL)
        {
            return 0;
        }
        t->words = nw;
        t->cap_words = nc;
    }
    if (len > 511)
    {
        len = 511; /* (Windows OCR's words are cut there too) */
    }
    wd = &t->words[t->nwords];
    wd->x0 = x0;
    wd->y0 = y0;
    wd->x1 = x1;
    wd->y1 = y1;
    wd->text_off = (uint32_t)t->text_len;
    wd->text_len = (uint32_t)len;
    wd->line = t->nlines - 1;
    if (!text_append(t, utf8, len))
    {
        return 0;
    }
    t->lines[t->nlines - 1].nwords++;
    t->nwords++;
    return 1;
}

static BOOL recognize(EeOcr *ocr,
                      const void *data,
                      size_t len,
                      const void *px,
                      uint32_t w,
                      uint32_t h,
                      uint32_t stride,
                      int channels,
                      double scale,
                      EeOcrText *out,
                      wchar_t *err,
                      size_t errcch)
{
    char e[256] = "";
    if (ocr == NULL || out == NULL)
    {
        set_err(err, errcch, "Invalid arguments.");
        return FALSE;
    }
    out->nwords = out->nlines = 0;
    out->text_len = 0;
    if (out->text != NULL)
    {
        out->text[0] = '\0';
    }
    out->width = out->height = 0;
    if (!EeOcrEngine_Recognize(ocr->engine,
                               data,
                               len,
                               (const uint8_t *)px,
                               w,
                               h,
                               stride,
                               channels,
                               scale,
                               EE_OCR_MAX_DIM,
                               ocr->quarter_turns,
                               add_word,
                               out,
                               &out->width,
                               &out->height,
                               e,
                               sizeof(e)))
    {
        set_err(err, errcch, e[0] != '\0' ? e : "Text recognition failed.");
        return FALSE;
    }
    return TRUE;
}

BOOL EeOcr_RecognizeEncoded(EeOcr *ocr,
                            const void *data,
                            size_t len,
                            double scale,
                            EeOcrText *out,
                            wchar_t *err,
                            size_t errcch)
{
    if (data == NULL || len == 0)
    {
        set_err(err, errcch, "Invalid arguments.");
        return FALSE;
    }
    return recognize(ocr, data, len, NULL, 0, 0, 0, 0, scale, out, err, errcch);
}

BOOL EeOcr_RecognizePixels(EeOcr *ocr,
                           const void *pixels,
                           uint32_t width,
                           uint32_t height,
                           uint32_t stride,
                           int channels,
                           double scale,
                           EeOcrText *out,
                           wchar_t *err,
                           size_t errcch)
{
    return recognize(ocr,
                     NULL,
                     0,
                     pixels,
                     width,
                     height,
                     stride,
                     channels,
                     scale,
                     out,
                     err,
                     errcch);
}
