/**
 * @file ee_results_dom.h
 * @brief Election results from Dominion Democracy Suite reports, as Results Tally and
 *        Reporting renders them through Microsoft Reporting Services: the "Election Summary
 *        Report" (ElectionSummaryReportRPT) saved as XML, XLSX or PDF, and the "Statement of
 *        Votes Cast" (StatementOfVotesCastRPT) PDF, which gives every precinct by vote type.
 *
 * Each report is read into the intermediate model the ES&S and Hart PDF readers use
 * (EeRpdfDoc), which ee_results.c turns into EeResults. The summary has county-wide totals
 * only; its counting groups ("Election Day", "Vote by Mail") give ballots cast by vote type.
 *
 * See docs/election-results-design.md (Dominion).
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

    /** TRUE when @p data is an Election Summary Report saved as XML. */
    BOOL EeRdom_IsSummaryXml(const void *data, size_t size);

    /** TRUE when @p data is an .xlsx workbook of an Election Summary Report. */
    BOOL EeRdom_IsSummaryXlsx(const void *data, size_t size);

    /** Read an Election Summary Report XML (EE_RPDF_DOM_SUMMARY). */
    BOOL EeRdom_ParseSummaryXml(const void *data, size_t size, EeRpdfDoc *out, wchar_t *err, size_t errcch);

    /** Read an Election Summary Report workbook (EE_RPDF_DOM_SUMMARY). */
    BOOL EeRdom_ParseSummaryXlsx(const void *data, size_t size, EeRpdfDoc *out, wchar_t *err, size_t errcch);

    /**
     * The Dominion report an open PDF is, from its Title: EE_RPDF_DOM_SUMMARY,
     * EE_RPDF_DOM_SOVC, or 0 for any other PDF.
     */
    int EeRdom_PdfKind(EePdf *pdf);

    /** Read a Dominion report PDF of @p kind (from EeRdom_PdfKind). */
    BOOL EeRdom_ParsePdf(EePdf *pdf, int kind, EeRpdfDoc *out, wchar_t *err, size_t errcch);

#ifdef __cplusplus
}
#endif
