/**
 * @file ocr_engine.h
 * @brief The plain C interface an OCR engine provides on macOS and Linux, for ocr_bridge.c
 *        (which implements ocr_win.h on top of it):
 *        - macOS: Apple Vision, src/platform/macos/ocr_vision.m;
 *        - Linux: Tesseract, src/platform/linux/ocr_tesseract.c.
 *
 * Standard C types only: the engines' own headers (Objective-C's BOOL, Leptonica's macros)
 * clash with the stand-in <windows.h>, so an engine file never sees the Election Explorer
 * headers, and ocr_bridge.c never sees the engine's.
 */
#pragma once

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C"
{
#endif

    /**
     * Called for each recognized word, in reading order. @p new_line is 1 for the first word of
     * a line. @p utf8 / @p len is the word (not NUL-terminated). The box is in pixels of the
     * recognized image, origin top-left. Return 0 to stop (out of memory).
     */
    typedef int (*EeOcrEngineWordFn)(void *user,
                                     int new_line,
                                     const char *utf8,
                                     size_t len,
                                     float x0,
                                     float y0,
                                     float x1,
                                     float y1);

    /** The engine's name for messages, UTF-8 ("Apple Vision", "Tesseract"). */
    const char *EeOcrEngine_Name(void);

    /** Open the engine for one thread (NULL with a message in @p err, UTF-8, when it cannot). */
    void *EeOcrEngine_Open(char *err, size_t errcap);

    /** Close an engine from EeOcrEngine_Open (NULL is ignored). */
    void EeOcrEngine_Close(void *engine);

    /**
     * Recognize the text of one image, prepared as Windows OCR's is: scaled by @p scale (and
     * down to @p max_dim on its long side), made 8-bit gray, then turned @p quarter_turns x 90
     * degrees clockwise.
     *
     * The image is either encoded (@p data / @p len: JPEG, PNG, TIFF including CCITT fax, ...)
     * or raw pixels (@p data NULL; @p px, @p w, @p h, @p stride, @p channels 1 gray or 3 RGB,
     * rows top-down).
     *
     * @param out_w, out_h  Receive the recognized image's size (after scaling and turning).
     * @return 1 on success (an image with no text succeeds with no words), 0 with a message in
     *         @p err (UTF-8).
     */
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
                              size_t errcap);

#ifdef __cplusplus
}
#endif
