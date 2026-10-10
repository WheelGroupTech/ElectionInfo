/**
 * @file hart_ocr.h
 * @brief Reads an image-only (scanned / flattened) Hart CVR Report PDF through OCR.
 *
 * Some counties publish the Hart "CVR Report" with every page flattened to a picture
 * (e.g. Sierra County, CA: a redacted copy, one JPEG per page). This module OCRs each
 * page (ocr_win.c), rebuilds the report layout from the word positions, and produces
 * ballot-sheet records -- header fields plus (Contest Title, Option) rows -- that the
 * Hart loader (hart_cvr.c) turns into CVR rows exactly as it does for text PDFs.
 *
 * Layout handled: one or more records per page, each a header block (Precinct, Party,
 * Polling Place, Voting Type, Central Batch Id | Device Type, Device Serial, Device Data
 * Id, Cvr Id) above a Contest Title / Option table; a record that runs past the page
 * bottom continues on the next page under a repeated header block.
 *
 * OCR errors are repaired and flagged rather than hidden:
 *  - header labels are matched loosely ("Cvr ld", "Cw ld"); the Cvr Id is whichever
 *    header value repairs into a valid GUID (I/l -> 1, O -> 0, spaces dropped, ...);
 *  - a record continues across pages when the Cvr Ids differ by at most 3 hex digits;
 *  - after the whole file is read, a rare spelling of a contest, option, precinct or
 *    other field is replaced by a common spelling only when it is within a small edit
 *    distance, has the same digits, and the match is unambiguous;
 *  - every record gets a status: OK, Corrected (something was repaired), Review (an
 *    unreadable Cvr Id, a contest row without an option, or a rare unmatched value), or
 *    No Votes Read (a header with no readable contest rows -- e.g. Sierra County blacked
 *    out the whole table of its 4-ballot Long Valley precinct).
 */
#pragma once

#include <windows.h>
#include <stdint.h>

#include "ocr_win.h"
#include "pdf_reader.h"

#ifdef __cplusplus
extern "C"
{
#endif

    /** Header fields of a record (offsets into HartOcrStore.pool; 0 = empty). */
    enum
    {
        HOF_GUID = 0,
        HOF_PRECINCT,
        HOF_PARTY,
        HOF_PPLACE,
        HOF_VTYPE,
        HOF_BATCH,
        HOF_DTYPE,
        HOF_DSERIAL,
        HOF_DDATA,
        HOF_COUNT
    };

    /** Record status (also the "OCR Status" column value). */
    enum
    {
        HOCR_OK = 0,
        HOCR_CORRECTED = 1,
        HOCR_REVIEW = 2,
        HOCR_NOVOTES = 3 /* header but no readable contest rows (e.g. redacted votes) */
    };

    typedef struct HartOcrRow
    {
        uint32_t title;  /* pool offsets */
        uint32_t option; /* 0 = no option read */
    } HartOcrRow;

    typedef struct HartOcrRecord
    {
        uint32_t f[HOF_COUNT];
        uint32_t first_row;
        uint32_t nrows;
        uint32_t page;  /* first page (0-based) */
        uint8_t status; /* HOCR_* */
    } HartOcrRecord;

    /** All records read from one PDF. Strings are NUL-terminated in @c pool; offset 0
     *  is the empty string. */
    typedef struct HartOcrStore
    {
        char *pool;
        size_t pool_len;
        size_t pool_cap;
        HartOcrRecord *recs;
        uint32_t nrecs;
        uint32_t cap_recs;
        HartOcrRow *rows;
        uint32_t nrows;
        uint32_t cap_rows;
        uint32_t pages;
        /* summary */
        uint32_t corrected_values; /* individual values repaired */
        uint32_t corrected_records;
        uint32_t review_records;
        uint32_t novote_records;
    } HartOcrStore;

    /** Called once per page during HartOcr_ReadPdf; return FALSE to cancel. */
    typedef BOOL (*HartOcrTickFn)(void *ctx);

    /**
     * TRUE if page @p page of @p pdf has no text but draws an image (a scanned or
     * flattened page that needs OCR).
     */
    BOOL HartOcr_PageIsImageOnly(EePdf *pdf, uint32_t page);

    /**
     * OCR page @p page and decide whether it is a Hart CVR Report page (a "CVR Report"
     * title, a Contest Title table header and a Cvr Id that repairs into a GUID).
     */
    BOOL HartOcr_ProbeHart(EeOcr *ocr, EePdf *pdf, uint32_t page, wchar_t *err, size_t errcch);

    /**
     * OCR every page of @p pdf into @p store (cleared first), then apply the
     * cross-record corrections and set each record's status. @p tick is called per page.
     * Returns FALSE on error or cancel (*cancelled set when cancelled).
     */
    BOOL HartOcr_ReadPdf(EeOcr *ocr,
                         EePdf *pdf,
                         HartOcrStore *store,
                         HartOcrTickFn tick,
                         void *tick_ctx,
                         BOOL *cancelled,
                         wchar_t *err,
                         size_t errcch);

    /** String at pool offset @p off. */
    const char *HartOcr_Str(const HartOcrStore *store, uint32_t off);

    /** Release a store. */
    void HartOcr_StoreFree(HartOcrStore *store);

#ifdef __cplusplus
}
#endif
