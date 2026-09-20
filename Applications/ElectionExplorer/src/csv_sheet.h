/**
 * @file csv_sheet.h
 * @brief Delimited-text (CSV / TSV) sheet reader with the same row-sink contract
 *        as the XLSX reader, so tabular loaders (e.g. the CVR table) can accept
 *        comma- or tab-separated exports as well as Excel workbooks.
 *
 * Handles a UTF-8 (with or without BOM), UTF-16LE, or UTF-16BE file, RFC 4180
 * quoting (double-quoted fields, "" escapes, delimiters and newlines inside
 * quotes), and CRLF/LF/CR line endings. Cells are delivered to the sink as
 * UTF-8, exactly as EeXlsx_ReadSheet delivers them.
 */
#pragma once

#include <windows.h>
#include <stdint.h>

#include "voter_table.h" /* EeLoadStatus, EeLoadProgressFn */

#ifdef __cplusplus
extern "C"
{
#endif

    /** Same shape as EeXlsxRowSink: return FALSE to abort the read. */
    typedef BOOL (*EeCsvRowSink)(void *ctx, const char *const *cells, uint32_t ncells);

    /**
     * Read a delimited-text file, invoking @p sink once per record.
     *
     * The delimiter is chosen from the extension (".tsv" -> tab, ".csv" -> comma)
     * and otherwise sniffed from the first line (tab if it has more tabs than
     * commas). Blank trailing lines are ignored; a record's trailing empty cells
     * are preserved so a fixed-width header/row schema round-trips.
     *
     * @param path           Path to the .csv / .tsv / .txt file.
     * @param sink           Row callback (UTF-8 cells).
     * @param sink_ctx       Passed to @p sink.
     * @param cancel_flag    Optional; non-zero requests cancel.
     * @param progress_fn    Optional progress callback (by bytes consumed).
     * @param progress_user  Passed to @p progress_fn.
     * @param error_message  Optional; receives a message on failure.
     * @param error_cch      Capacity of @p error_message (incl. NUL).
     * @return EeLoadStatus_Ok on success.
     */
    EeLoadStatus EeCsv_ReadSheet(const wchar_t *path,
                                 EeCsvRowSink sink,
                                 void *sink_ctx,
                                 volatile LONG *cancel_flag,
                                 EeLoadProgressFn progress_fn,
                                 void *progress_user,
                                 wchar_t *error_message,
                                 size_t error_cch);

#ifdef __cplusplus
}
#endif
