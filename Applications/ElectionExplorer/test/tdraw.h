/**
 * @file tdraw.h
 * @brief Test fixtures' text drawing on macOS and Linux, in place of the GDI that smoke_load.c
 *        uses on Windows: page images with text for the OCR tests, and a font's glyph outlines
 *        for the Type 3 font of the PScript5 test.
 *        - macOS: tdraw_apple.c, CoreGraphics and CoreText (Helvetica, Arial).
 *        - Linux: tdraw_ttf.c, a TrueType font file read and filled with no library
 *          (Liberation Sans, else DejaVu Sans).
 *
 * Plain C types only, so the Apple file includes Apple's headers and never the stand-in
 * windows.h (as src/platform/posix/ocr_engine.h does for OCR).
 */
#pragma once

#ifdef __cplusplus
extern "C"
{
#endif

    typedef struct TDrawPage TDrawPage;

    /** 1 when text can be drawn (Linux: a TrueType font was found), else 0. */
    int TDraw_Available(void);

    /** A white @p w x @p h page whose text is drawn @p font_px pixels to the em. NULL on failure. */
    TDrawPage *TDraw_PageBegin(int w, int h, double font_px);

    /** Text (UTF-8) whose character cell's top-left corner is (@p x, @p y), y down, as GDI's TextOut. */
    void TDraw_Text(TDrawPage *p, int x, int y, const char *utf8);

    /** The page as 8-bit gray, rows top-down (malloc'd, caller frees; NULL on failure). Frees @p p. */
    unsigned char *TDraw_PageEnd(TDrawPage *p);

    /** One element of a glyph outline, in ems (1 em = 1), y up: @p op 0 move to (x0, y0), 1 line
     *  to (x0, y0), 2 cubic curve through (x0, y0), (x1, y1) to (x2, y2), 3 close. Return 0 to stop. */
    typedef int (*TDrawPathFn)(void *user,
                               int op,
                               double x0,
                               double y0,
                               double x1,
                               double y1,
                               double x2,
                               double y2);

    /** Arial's outline of character @p ch (quadratic curves given as cubic). 0 when the font or the
     *  glyph is missing. */
    int TDraw_GlyphOutline(char ch, TDrawPathFn fn, void *user);

#ifdef __cplusplus
}
#endif
