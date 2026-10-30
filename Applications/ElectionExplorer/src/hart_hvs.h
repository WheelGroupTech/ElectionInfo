/**
 * @file hart_hvs.h
 * @brief Reads the "Votes by Precinct" Cast Vote Record report of Hart's older voting system
 *        (Hart Voting System / HVS 6.x, used by Texas counties before Verity).
 *
 * The report prints one ballot per page: a title ("Votes by Precinct", then the election and
 * its voting type, e.g. "November 2024 Early Voting"), "Precinct:" and "Party:" lines, and a
 * "Contest Title" / "Candidate/Option" table with one row per contest. An unmarked contest
 * reads "No Selection"; a write-in reads "Write-in" with the written name beside it. There is
 * no ballot id, device or batch: a record is identified by its file and page.
 *
 * Copies printed through PostScript (Acrobat Distiller) draw words in pieces and pad cells
 * with spaces, and one copy draws each header value at its label's own position: lines are
 * rebuilt from the run positions, so all of these read alike.
 *
 * Some print runs cut long contest titles short ("JUSTICE, 3RD COURT OF APPEALS DIST" for
 * four different Places). After the whole file is read, a title that is the start of longer
 * titles printed for the same precincts, and never on the same ballot as them, is completed:
 * from the only such title, or, when several (Places 2, 3, 5 and 6), by ballot order -- the
 * k-th cut title on a ballot is the k-th of the full titles in the order they are printed.
 *
 * Records go into a HartOcrStore (hart_ocr.h) so hart_cvr.c feeds them through the same
 * passes as a scanned report; their status is always HOCR_OK.
 */
#pragma once

#include <windows.h>
#include <stdint.h>

#include "hart_ocr.h"
#include "pdf_reader.h"

#ifdef __cplusplus
extern "C"
{
#endif

    /** What a read did, for the load note. */
    typedef struct HartHvsStats
    {
        uint32_t pages;
        uint32_t ballots;
        uint32_t continued;       /* pages without a "Precinct:" line, read as more of the ballot before */
        uint32_t cut_titles;      /* rows whose cut-short contest title was completed */
        uint32_t cut_unresolved;  /* rows whose cut-short title could not be told apart (kept as printed) */
        uint32_t stray_lines;     /* table lines with a title but no choice (not read) */
    } HartHvsStats;

    /** TRUE if @p pt (a page's extracted text) is a page of the HVS "Votes by Precinct" report. */
    BOOL HartHvs_IsReportPage(const EePdfPageText *pt);

    /**
     * Read every page of @p pdf into @p store (cleared first): one record per ballot, the
     * record id ("Report Page") "<@p name> p.<page>". @p tick is called per page.
     * Returns FALSE on error or cancel (*cancelled set when cancelled).
     */
    BOOL HartHvs_ReadPdf(EePdf *pdf,
                         const wchar_t *name,
                         HartOcrStore *store,
                         HartOcrTickFn tick,
                         void *tick_ctx,
                         BOOL *cancelled,
                         HartHvsStats *stats,
                         wchar_t *err,
                         size_t errcch);

#ifdef __cplusplus
}
#endif
