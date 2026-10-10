/**
 * @file ocr_win.h
 * @brief Optical character recognition through the Windows built-in OCR engine.
 *
 * A thin C wrapper over Windows.Media.Ocr (WinRT) with WIC for image decoding. It is
 * part of Windows 10/11 and works for both unpackaged (developer build) and MSIX
 * packaged apps; it needs an OCR-capable language installed in Windows (English is on
 * typical English installs). No third-party code.
 *
 * Used for image-only PDFs (e.g. a county's redacted CVR Report flattened to page
 * images): the caller extracts each page's image and recognizes it here.
 *
 * Threading: create, use and destroy an EeOcr on ONE thread (it initializes the
 * Windows Runtime for that thread when needed).
 */
#pragma once

#include <windows.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C"
{
#endif

    /** Opaque OCR engine. */
    typedef struct EeOcr EeOcr;

    /** One recognized word; coordinates are image pixels, origin top-left. */
    typedef struct EeOcrWord
    {
        float x0;
        float y0;
        float x1;
        float y1;
        uint32_t text_off; /* UTF-8 text (not NUL-terminated) at EeOcrText.text + off */
        uint32_t text_len;
        uint32_t line;     /* index of its line in EeOcrText.lines */
    } EeOcrWord;

    /** One recognized line: a run of words, in reading order. */
    typedef struct EeOcrLine
    {
        uint32_t first_word;
        uint32_t nwords;
    } EeOcrLine;

    /** Recognition result for one image. Reused across images. */
    typedef struct EeOcrText
    {
        EeOcrWord *words;
        uint32_t nwords;
        uint32_t cap_words;
        EeOcrLine *lines;
        uint32_t nlines;
        uint32_t cap_lines;
        char *text;
        size_t text_len;
        size_t text_cap;
        uint32_t width; /* recognized image size (after any scaling), pixels */
        uint32_t height;
    } EeOcrText;

    /**
     * Create an OCR engine for the user's profile languages.
     *
     * @param out     Receives the engine (free with EeOcr_Destroy).
     * @param err     Optional; receives a message on failure (e.g. no OCR language).
     * @param errcch  Capacity of @p err.
     * @return TRUE on success.
     */
    BOOL EeOcr_Create(EeOcr **out, wchar_t *err, size_t errcch);

    /** Destroy an engine from EeOcr_Create (NULL is ignored). Same thread as create. */
    void EeOcr_Destroy(EeOcr *ocr);

    /**
     * Recognize text in an encoded image (JPEG, PNG, TIFF, BMP, ... -- any format WIC
     * decodes) held in memory.
     *
     * @param ocr     Engine.
     * @param data    Encoded image bytes.
     * @param len     Byte count.
     * @param scale   Resample factor before recognition (1.0 = as is; e.g. 2.0 enlarges
     *                low-resolution scans). The result is capped at the engine's maximum
     *                image dimension.
     * @param out     Receives the words/lines (cleared first).
     * @param err     Optional; receives a message on failure.
     * @param errcch  Capacity of @p err.
     * @return TRUE on success (an image with no text succeeds with zero words).
     */
    BOOL EeOcr_RecognizeEncoded(EeOcr *ocr,
                                const void *data,
                                size_t len,
                                double scale,
                                EeOcrText *out,
                                wchar_t *err,
                                size_t errcch);

    /**
     * Recognize text in raw 8-bit pixels: @p channels = 1 (gray) or 3 (RGB), rows
     * top-down, @p stride bytes apart. Same contract as EeOcr_RecognizeEncoded.
     */
    BOOL EeOcr_RecognizePixels(EeOcr *ocr,
                               const void *pixels,
                               uint32_t width,
                               uint32_t height,
                               uint32_t stride,
                               int channels,
                               double scale,
                               EeOcrText *out,
                               wchar_t *err,
                               size_t errcch);

    /** Initialize / release an EeOcrText. */
    void EeOcr_TextInit(EeOcrText *t);
    void EeOcr_TextFree(EeOcrText *t);

#ifdef __cplusplus
}
#endif
