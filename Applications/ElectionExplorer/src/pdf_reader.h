/**
 * @file pdf_reader.h
 * @brief Minimal read-only PDF text extractor (positioned text runs per page).
 *
 * Purpose-built for machine-generated report PDFs (e.g. Hart "CVR Report" exports
 * rendered by Microsoft Reporting Services): it locates objects through the
 * cross-reference data, walks the page tree, decodes FlateDecode content streams,
 * and interprets the text/clipping operators of each page's content stream to
 * produce text runs with their page position and innermost clip rectangle.
 *
 * Supported: classic xref tables and xref streams (incl. /Prev chains, PNG
 * predictors, object streams), 64-bit file offsets, offsets that a 32-bit writer
 * wrapped (files > 2/4 GB), a full object-scan rebuild when the xref is unusable,
 * simple fonts (WinAnsi/Standard + /Differences) and Type0 Identity-H fonts, with
 * ToUnicode CMaps taking precedence. Not supported: encryption, filters other than
 * FlateDecode, glyph-width-based layout (each show operator's run starts at the
 * current text position; runs are not split or advanced by glyph widths).
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
    } EePdfTextRun;

    /** Text runs of one page, in content-stream order. Reused across pages. */
    typedef struct EePdfPageText
    {
        EePdfTextRun *runs;
        uint32_t nruns;
        uint32_t cap_runs;
        char *text; /* UTF-8 text pool referenced by runs */
        size_t text_len;
        size_t text_cap;
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

#ifdef __cplusplus
}
#endif
