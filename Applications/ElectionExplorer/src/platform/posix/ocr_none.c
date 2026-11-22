/**
 * @file ocr_none.c
 * @brief macOS / Linux placeholder for the OCR interface in ocr_win.h (Windows.Media.Ocr on
 *        Windows). EeOcr_Create reports that OCR is not available, so scanned documents are
 *        refused with that message and the callers' "no OCR" paths run; everything that reads
 *        text from files works as on Windows.
 *
 * To be replaced by Apple Vision (macOS) and Tesseract (Linux) behind the same interface.
 */

#include "ocr_win.h"

#include <stdlib.h>

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
    if (out != NULL)
    {
        *out = NULL;
    }
    if (err != NULL && errcch > 0)
    {
        StringCchCopyW(err,
                       errcch,
                       L"Text recognition (OCR) is not available on this platform yet.");
    }
    return FALSE;
}

void EeOcr_Destroy(EeOcr *ocr)
{
    (void)ocr;
}

void EeOcr_SetRotation(EeOcr *ocr, int quarter_turns)
{
    (void)ocr;
    (void)quarter_turns;
}

BOOL EeOcr_RecognizeEncoded(EeOcr *ocr,
                            const void *data,
                            size_t len,
                            double scale,
                            EeOcrText *out,
                            wchar_t *err,
                            size_t errcch)
{
    (void)ocr;
    (void)data;
    (void)len;
    (void)scale;
    (void)out;
    return EeOcr_Create(NULL, err, errcch);
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
    (void)ocr;
    (void)pixels;
    (void)width;
    (void)height;
    (void)stride;
    (void)channels;
    (void)scale;
    (void)out;
    return EeOcr_Create(NULL, err, errcch);
}
