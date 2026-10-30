/**
 * @file ee_results_ocr.h
 * @brief Election results from a scanned results report (page images only), read by OCR:
 *        ES&S Electionware summary and precinct reports, and the older ES&S monospaced
 *        reports ("SUMMRY REPT-GROUP DETAIL", "PREC REPORT-GROUP DETAIL", report EL30A).
 *
 * Each page image (pdf_reader.c) goes through Windows OCR (ocr_win.c) in the orientation that
 * reads best. The words are grouped into rows; numbers go to the columns the table headings
 * place ("TOTAL VOTES  EV  MAIL  ED  PROV", "TOTAL  VOTE %  Mail  EV  ED"). Names read a little
 * differently on each page, so contests, choices and precincts are matched by similarity.
 * Every row's total must equal the sum of its vote types and every column the contest's Total
 * row: a count OCR left out is filled from them (most are zeros), a misread one corrected when
 * one change fixes both, and rows that still do not add up are listed for review.
 *
 * The result is the intermediate model ee_results.c turns into EeResults.
 *
 * See docs/scanned-results-survey.md.
 */
#pragma once

#include <windows.h>
#include <stdint.h>

#include "ee_results_pdf.h"
#include "pdf_reader.h"

#ifdef __cplusplus
extern "C"
{
#endif

    /** TRUE when page @p page of an open PDF has no text but draws an image (a scan). */
    BOOL EeRocr_IsScannedPage(EePdf *pdf, uint32_t page);

    /**
     * Read a scanned results report by OCR into @p out (zeroed first; free with EeRpdf_Free).
     *
     * @param pdf       The open document.
     * @param page_fn   Optional; called after each page with pages done / total. Returns
     *                  FALSE to cancel (the read then fails with "Load cancelled.").
     * @param user      Passed to @p page_fn.
     * @param err       Receives a message on failure: no OCR engine, not a report this reader
     *                  knows, or cancelled.
     * @param errcch    Capacity of @p err.
     * @return TRUE on success.
     */
    BOOL EeRocr_Parse(EePdf *pdf,
                      EeRpdfDoc *out,
                      BOOL (*page_fn)(uint32_t done, uint32_t total, void *user),
                      void *user,
                      wchar_t *err,
                      size_t errcch);

#ifdef __cplusplus
}
#endif
