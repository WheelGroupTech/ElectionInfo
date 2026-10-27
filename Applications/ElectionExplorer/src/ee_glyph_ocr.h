/**
 * @file ee_glyph_ocr.h
 * @brief Characters for a PDF's unnamed Type 3 glyphs, learned through OCR.
 *
 * A report printed to PDF through the Windows PostScript driver (PScript5) draws its text in
 * Type 3 fonts whose glyphs are outlines named /0../255 in order of first use, with no
 * ToUnicode: the text cannot be extracted. Each glyph's drawing is the same wherever the
 * letter appears, so the drawing identifies the letter. This module draws each page's
 * glyphs into a bitmap, reads it with Windows OCR (ocr_win.c), lines up the recognized words
 * with the glyphs under them, and gives each drawing the character most of its occurrences
 * were read as. EePdf_SetGlyphChars then makes text extraction show those characters, so
 * the report reads like any other. A wrong letter in one word is outvoted by the drawing's
 * other occurrences.
 *
 * See docs/election-results-design.md (step 9c).
 */
#pragma once

#include <windows.h>
#include <stdint.h>
#include "pdf_reader.h"

#ifdef __cplusplus
extern "C"
{
#endif

    typedef struct EeGlyphOcrStats
    {
        uint32_t pages;     /* pages read */
        uint32_t drawings;  /* distinct glyph drawings without a character */
        uint32_t learned;   /* drawings given a character */
        uint32_t uncertain; /* learned with under two-thirds of their votes, or one vote */
    } EeGlyphOcrStats;

    /**
     * Learn characters for the unnamed Type 3 glyphs on pages 0..@p npages-1 of @p pdf and
     * set them (EePdf_SetGlyphChars).
     *
     * @return TRUE when at least one drawing was learned; FALSE (with a message) when OCR is
     *         not available or reads nothing that lines up.
     */
    BOOL EeGlyphOcr_Learn(EePdf *pdf, uint32_t npages, EeGlyphOcrStats *st, wchar_t *err, size_t errcch);

#ifdef __cplusplus
}
#endif
