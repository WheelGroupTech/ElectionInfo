/**
 * @file xlsx.h
 * @brief Read tabular data from Microsoft Excel .xlsx workbooks.
 *
 * An .xlsx is a ZIP of XML parts (Open Packaging Convention). This module
 * unzips the needed parts (via vendored miniz) and streams a worksheet's rows
 * to a caller-supplied sink as UTF-8 strings, matching the shape the CSV/TSV
 * tokenizer produces so both feed the same table-ingestion pipeline.
 *
 * Design and phasing: docs/xlsx-import-design.md.
 * v1 (Phases 1-2) emits cell values as stored: text as-is, numbers as their
 * stored decimal, dates as serials, booleans as TRUE/FALSE, error cells as their
 * error text. Style-driven date/number formatting is a Phase 3 follow-up.
 */
#pragma once

#include <windows.h>
#include <stdint.h>

#include "voter_table.h" /* EeLoadStatus, EeLoadProgressFn */

#ifdef __cplusplus
extern "C"
{
#endif

    /** Maximum worksheets reported by EeXlsx_ListSheets. */
#define EE_XLSX_MAX_SHEETS 64
    /** Capacity (in wchar_t, incl. NUL) of each sheet name buffer. */
#define EE_XLSX_SHEET_NAME_CCH 96

    /**
     * Row sink invoked once per worksheet row (header row included).
     * @param ctx    Caller context.
     * @param cells  Array of @p ncells NUL-terminated UTF-8 strings; a blank
     *               cell is "" and gaps are filled so the row width is stable.
     *               The array and its strings are valid only for this call.
     * @param ncells Number of cells in the row.
     * @return FALSE to abort the read (reported as EeLoadStatus_Cancelled).
     */
    typedef BOOL (*EeXlsxRowSink)(void *ctx, const char *const *cells, uint32_t ncells);

    /**
     * Enumerate worksheet names in workbook order (for a sheet picker).
     * Reads only workbook.xml / its rels.
     * @param path          Path to the .xlsx file.
     * @param names         Receives up to @p max_sheets UTF-16 names.
     * @param max_sheets    Capacity of @p names (<= EE_XLSX_MAX_SHEETS).
     * @param out_count     Receives the number of sheets written. Must be non-NULL.
     * @param error_message Optional; receives a message on failure.
     * @param error_cch     Capacity of @p error_message (incl. NUL).
     * @return EeLoadStatus_Ok on success.
     */
    EeLoadStatus EeXlsx_ListSheets(const wchar_t *path,
                                   wchar_t names[][EE_XLSX_SHEET_NAME_CCH],
                                   int max_sheets,
                                   int *out_count,
                                   wchar_t *error_message,
                                   size_t error_cch);

    /**
     * Read one worksheet and stream its rows to @p sink.
     * @param path          Path to the .xlsx file.
     * @param sheet_index   0-based index in workbook order (from EeXlsx_ListSheets).
     * @param sink          Row callback; see EeXlsxRowSink.
     * @param sink_ctx      Passed to @p sink.
     * @param cancel_flag   Optional; non-zero requests cancel (checked periodically).
     * @param progress_fn   Optional; invoked during the read. Return FALSE to cancel.
     * @param progress_user Passed to @p progress_fn.
     * @param error_message Optional; receives a message on failure.
     * @param error_cch     Capacity of @p error_message (incl. NUL).
     * @return EeLoadStatus_Ok, _Cancelled (sink/flag/progress aborted), or _Error.
     */
    EeLoadStatus EeXlsx_ReadSheet(const wchar_t *path,
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
