/**
 * @file ocr_vision.h
 * @brief A plain C bridge to Apple's Vision text recognition (ocr_vision.m), for ocr_apple.c.
 *
 * Standard C types only: Objective-C's BOOL and the stand-in <windows.h>'s BOOL differ, so
 * the Objective-C file never sees the engine's headers, and ocr_apple.c never sees Apple's.
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
    typedef int (*EeVisionWordFn)(void *user,
                                  int new_line,
                                  const char *utf8,
                                  size_t len,
                                  float x0,
                                  float y0,
                                  float x1,
                                  float y1);

    /** 1 when text recognition is available; else 0 with a message in @p err (UTF-8). */
    int EeVision_Available(char *err, size_t errcap);

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
    int EeVision_Recognize(const void *data,
                           size_t len,
                           const uint8_t *px,
                           uint32_t w,
                           uint32_t h,
                           uint32_t stride,
                           int channels,
                           double scale,
                           uint32_t max_dim,
                           int quarter_turns,
                           EeVisionWordFn fn,
                           void *user,
                           uint32_t *out_w,
                           uint32_t *out_h,
                           char *err,
                           size_t errcap);

#ifdef __cplusplus
}
#endif
