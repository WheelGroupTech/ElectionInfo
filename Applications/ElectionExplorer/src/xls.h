/**
 * @file xls.h
 * @brief Read tabular data from legacy Excel 97-2003 .xls workbooks.
 *
 * An .xls is a BIFF8 "Workbook" stream inside an OLE2 compound file. This module
 * reads both layers (no COM, no Excel) and streams a worksheet's rows to the same
 * row sink the .xlsx reader uses (xlsx.h), with the same cell text: text as-is,
 * numbers formatted by the cell's number format (a date format yields YYYY-MM-DD,
 * a zero-pad format keeps leading zeros, an integer has no ".0"), booleans as
 * TRUE/FALSE and error cells as their error text. EeXlsx_ListSheetsMem and
 * EeXlsx_ReadSheetMem hand a compound file here, so callers of the .xlsx API read
 * .xls files too. Excel 5.0/95 (BIFF5) and password-protected workbooks are
 * reported as unsupported.
 */
#pragma once

#include <windows.h>
#include <stdint.h>

#include "xlsx.h" /* EeXlsxRowSink, EE_XLSX_SHEET_NAME_CCH */

#ifdef __cplusplus
extern "C"
{
#endif

    /**
     * Whether @p data starts with the OLE2 compound-file signature (an .xls workbook,
     * or another Office 97-2003 document).
     * @param data Buffer to test (may be NULL).
     * @param size Its size in bytes.
     * @return TRUE when the first 8 bytes are D0 CF 11 E0 A1 B1 1A E1.
     */
    BOOL EeXls_IsCompoundFile(const void *data, size_t size);

    /**
     * Enumerate the worksheet names of an .xls workbook in memory, in workbook order
     * (chart and macro sheets are skipped).
     * @param data          The whole file. Only read; must stay valid for the call.
     * @param size          Its size in bytes.
     * @param names         Receives up to @p max_sheets UTF-16 names.
     * @param max_sheets    Capacity of @p names (<= EE_XLSX_MAX_SHEETS).
     * @param out_count     Receives the number of sheets written. Must be non-NULL.
     * @param error_message Optional; receives a message on failure.
     * @param error_cch     Capacity of @p error_message (incl. NUL).
     * @return EeLoadStatus_Ok on success, EeLoadStatus_Error for a damaged, encrypted
     *         or unsupported (pre-Excel 97) workbook.
     */
    EeLoadStatus EeXls_ListSheetsMem(const void *data,
                                     size_t size,
                                     wchar_t names[][EE_XLSX_SHEET_NAME_CCH],
                                     int max_sheets,
                                     int *out_count,
                                     wchar_t *error_message,
                                     size_t error_cch);

    /**
     * Read one worksheet of an .xls workbook in memory and stream its rows to @p sink
     * (rows in sheet order; a row without cells is skipped; gaps between cells are "").
     * @param data          The whole file. Only read; must stay valid for the call.
     * @param size          Its size in bytes.
     * @param sheet_index   0-based index among the worksheets (as EeXls_ListSheetsMem).
     * @param sink          Row callback; see EeXlsxRowSink.
     * @param sink_ctx      Passed to @p sink.
     * @param cancel_flag   Optional; non-zero requests cancel (checked periodically).
     * @param progress_fn   Optional; invoked during the read. Return FALSE to cancel.
     * @param progress_user Passed to @p progress_fn.
     * @param error_message Optional; receives a message on failure.
     * @param error_cch     Capacity of @p error_message (incl. NUL).
     * @return EeLoadStatus_Ok, _Cancelled (sink/flag/progress aborted), or _Error.
     */
    EeLoadStatus EeXls_ReadSheetMem(const void *data,
                                    size_t size,
                                    int sheet_index,
                                    EeXlsxRowSink sink,
                                    void *sink_ctx,
                                    volatile LONG *cancel_flag,
                                    EeLoadProgressFn progress_fn,
                                    void *progress_user,
                                    wchar_t *error_message,
                                    size_t error_cch);

#ifdef __cplusplus
}
#endif
