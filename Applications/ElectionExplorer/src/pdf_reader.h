/**
 * @file pdf_reader.h
 * @brief Minimal read-only PDF text extractor (positioned text runs per page).
 *
 * Purpose-built for machine-generated report PDFs (e.g. Hart "CVR Report" exports
 * rendered by Microsoft Reporting Services, and voter rosters printed to PDF from Excel): it locates objects through the
 * cross-reference data, walks the page tree, decodes FlateDecode content streams,
 * and interprets the text/clipping operators of each page's content stream to
 * produce text runs with their page position and innermost clip rectangle.
 *
 * Supported: classic xref tables and xref streams (incl. /Prev chains, PNG
 * predictors, object streams), 64-bit file offsets, offsets that a 32-bit writer
 * wrapped (files > 2/4 GB), a full object-scan rebuild when the xref is unusable,
 * simple fonts (WinAnsi/Standard + /Differences) and Type0 Identity-H fonts, with
 * ToUnicode CMaps taking precedence. Each run reports its font size and, when the font
 * carries glyph widths (/Widths, or /W of a Type0 descendant), its advance width. Not
 * supported: encryption, filters other than FlateDecode, glyph-width-based layout (each
 * show operator's run starts at the current text position; runs are not split or
 * advanced by glyph widths).
 *
 * The file is read on demand (never wholly loaded), so multi-GB documents are fine.
 * An EePdf handle is not thread-safe; use one handle per thread.
 */
#pragma once

#include <windows.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C"
{
#endif

    /** Opaque open document. */
    typedef struct EePdf EePdf;

    /** One shown string (a Tj/TJ/'/" operator) on a page. Coordinates are PDF user
     *  space after the current transformation matrix (origin bottom-left). */
    typedef struct EePdfTextRun
    {
        float x;       /* text origin */
        float y;
        float clip_x0; /* bounds of the innermost clip path (valid when has_clip); not
                        * intersected with outer clips */
        float clip_y0;
        float clip_x1;
        float clip_y1;
        int has_clip;
        uint32_t clip_id;  /* identifies the clip established on this page (0 = none);
                           * runs drawn inside the same clip share an id */
        uint32_t text_off; /* UTF-8 text (not NUL-terminated) at EePdfPageText.text + off */
        uint32_t text_len;
        float font_size;   /* text size in user space (Tf size x text / CTM scale) */
        float width;       /* advance width of the shown string in user space, from the
                            * font's glyph widths (valid when has_width); the run's
                            * position does not include it (runs are not advanced) */
        int has_width;
        int rot; /* text direction in user space, degrees counterclockwise: 0 = left to
                  * right, 90 = bottom to top, 180, 270 = top to bottom (a page printed
                  * rotated, e.g. landscape through a portrait driver) */
        int type3; /* drawn in a Type 3 font (glyph outlines, e.g. through PScript5): its
                    * glyphs are shown one string each, so touching runs are one word */
    } EePdfTextRun;

    /** One glyph of a Type 3 font shown on a page (EePdfPageText.collect_glyphs). */
    typedef struct EePdfGlyph
    {
        uint32_t proc;  /* its CharProcs stream (EePdf_GlyphOutline) */
        uint32_t fp;    /* a hash of that stream: the same drawing in any font of the file */
        float m[6];     /* glyph space -> user space */
        float x0, y0;   /* origin in user space */
        float x1;       /* origin + advance (x) */
        float size;     /* text size in user space */
        BOOL named;     /* its glyph name already gives the character */
    } EePdfGlyph;

    /** A glyph's outline in glyph space: ops 0 move, 1 line, 2 curve (3 points), 3 close. */
    typedef struct EePdfOutline
    {
        uint8_t *ops;
        uint32_t nops;
        float *pts; /* x, y pairs: 1 per move/line, 3 per curve */
        uint32_t npts;
    } EePdfOutline;

    /** Text runs of one page, in content-stream order. Reused across pages. */
    typedef struct EePdfPageText
    {
        EePdfTextRun *runs;
        uint32_t nruns;
        uint32_t cap_runs;
        char *text; /* UTF-8 text pool referenced by runs */
        size_t text_len;
        size_t text_cap;
        int split_gaps; /* set by the caller (kept across pages): a TJ word-gap kern ends the
                         * run and the text after it starts a new run at its own position
                         * (tables printed through a PostScript driver draw each line as one
                         * TJ with the column gaps as kerns); off = the gap becomes a space */
        int std_widths; /* set by the caller (kept across pages): a font without widths (a
                         * standard 14 font) is measured as Helvetica, so split_gaps also
                         * splits its TJ gaps, and the text position moves on after each
                         * string, so strings shown one after another (an Acrobat edit, a
                         * tagged re-save) come at their own places; off = the position
                         * stays at the string's start */
        int collect_glyphs;  /* set by the caller: list Type 3 glyphs in glyphs[] */
        EePdfGlyph *glyphs;
        uint32_t nglyphs;
        uint32_t cap_glyphs;
        uint32_t unknown_glyphs; /* Type 3 glyphs on the page with no character (no glyph
                                  * name, not in EePdf_SetGlyphChars): shown as U+FFFD */
    } EePdfPageText;

    /**
     * Open a PDF and index it (cross-reference data + page tree).
     *
     * @param path      Wide path of the PDF file.
     * @param out       Receives the handle on success (close with EePdf_Close).
     * @param err       Optional; receives a message on failure.
     * @param errcch    Capacity of @p err.
     * @return TRUE on success; FALSE if the file cannot be read, is not a PDF, is
     *         encrypted, or has no readable page tree.
     */
    BOOL EePdf_Open(const wchar_t *path, EePdf **out, wchar_t *err, size_t errcch);

    /**
     * Open a PDF held in memory (a ZIP entry), like EePdf_Open. @p data must stay
     * valid until EePdf_Close.
     */
    BOOL EePdf_OpenMem(const void *data, size_t size, EePdf **out, wchar_t *err, size_t errcch);

    /** Close a handle from EePdf_Open (NULL is ignored). */
    void EePdf_Close(EePdf *pdf);

    /** Number of pages in the document. */
    uint32_t EePdf_PageCount(const EePdf *pdf);

    /**
     * Copy a document-information string (e.g. "Title", "Producer", "Creator") as
     * UTF-8 into @p buf. Returns FALSE (and sets buf to "") if absent.
     */
    BOOL EePdf_GetInfoText(EePdf *pdf, const char *key, char *buf, size_t cap);

    /** Initialize an empty page-text container. */
    void EePdf_PageTextInit(EePdfPageText *pt);

    /** Release a page-text container's memory. */
    void EePdf_PageTextFree(EePdfPageText *pt);

    /**
     * Extract the positioned text runs of page @p page_index (0-based) into @p out
     * (cleared first). Returns FALSE on a read/decode error or out of memory; a page
     * without text succeeds with zero runs.
     */
    BOOL EePdf_ExtractPageText(EePdf *pdf, uint32_t page_index, EePdfPageText *out);

    /**
     * Give Type 3 glyphs characters by their drawing (EePdfGlyph.fp -> code point), for a
     * file whose Type 3 fonts name glyphs /0../255 (PScript5): text extracted afterwards
     * shows them as these characters. Replaces an earlier map. Returns FALSE out of memory.
     */
    BOOL EePdf_SetGlyphChars(EePdf *pdf, const uint32_t *fps, const uint32_t *cps, uint32_t n);

    /** The outline a Type 3 glyph's stream @p proc draws (in glyph space). FALSE when it
     *  cannot be read or draws an image. Free with EePdf_OutlineFree. */
    BOOL EePdf_GlyphOutline(EePdf *pdf, uint32_t proc, EePdfOutline *out);
    void EePdf_OutlineFree(EePdfOutline *o);

    /** Kinds of page image returned by EePdf_GetPageImage. */
    enum
    {
        EE_PDF_IMAGE_NONE = 0,    /* the page draws no image */
        EE_PDF_IMAGE_ENCODED = 1, /* data = encoded file bytes (JPEG / DCTDecode) */
        EE_PDF_IMAGE_PIXELS = 2   /* data = raw 8-bit samples, top-down rows */
    };

    /** The largest image drawn on a page (for image-only / scanned pages). */
    typedef struct EePdfImage
    {
        int kind;          /* EE_PDF_IMAGE_* */
        uint32_t width;    /* pixels */
        uint32_t height;
        uint32_t channels; /* PIXELS: 1 (gray) or 3 (RGB) */
        uint32_t stride;   /* PIXELS: bytes per row */
        unsigned char *data;
        size_t len;
        size_t cap;
    } EePdfImage;

    /**
     * Find the largest image XObject drawn on page @p page_index and return its data:
     * JPEG (DCTDecode) images as their encoded bytes, CCITT fax images wrapped as a TIFF
     * (encoded bytes WIC decodes), Flate-compressed 8-bit gray/RGB images as raw pixels,
     * and 1-bit gray images expanded to 8-bit pixels. @p out (zero-initialize before first
     * use; reused across calls) gets kind EE_PDF_IMAGE_NONE when the page draws no image.
     * Returns FALSE on a read error or an unsupported image encoding (e.g. JPX, JBIG2,
     * indexed color).
     */
    BOOL EePdf_GetPageImage(EePdf *pdf, uint32_t page_index, EePdfImage *out);

    /** Release an EePdfImage's buffer. */
    void EePdf_ImageFree(EePdfImage *img);

#ifdef __cplusplus
}
#endif
