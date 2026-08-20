/**
 * @file voter_table.h
 * @brief In-memory voter list table and background file load.
 *
 * Stores cell text in a UTF-8 pool with uint32_t offsets so multi-million-row
 * lists remain practical. Display uses a sort index over physical rows.
 */
#pragma once

#include <windows.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C"
{
#endif

/**
 * Maximum display columns (synthesized Voter ID and Name plus source fields).
 * Historical county exports can exceed 380 source columns (VOTED/PLACE/PARTY
 * per election).
 */
#define EE_MAX_COLUMNS 1024

/** First two display columns are always Voter ID and Name (frozen in UI). */
#define EE_FROZEN_COLUMN_COUNT 2

    typedef enum EeLoadStatus
    {
        EeLoadStatus_Ok = 0,
        EeLoadStatus_Cancelled,
        EeLoadStatus_Error
    } EeLoadStatus;

    typedef struct EeVoterTable
    {
        wchar_t *column_titles[EE_MAX_COLUMNS];
        uint32_t column_count;
        char delimiter; /* ',' or '\t' from the loaded file */

        char *pool;
        size_t pool_len;
        size_t pool_cap;

        /* cells[row * column_count + col] -> pool offset; 0 is empty string. */
        uint32_t *cells;
        uint32_t row_count;
        uint32_t row_cap;

        /* view_index[i] = physical row for visual order (sorting). */
        uint32_t *view_index;

        int sort_column; /* -1 if unsorted original order */
        BOOL sort_ascending;
    } EeVoterTable;

    typedef struct EeLoadProgress
    {
        uint32_t percent; /* 0..100 */
        uint32_t rows_loaded;
        uint64_t bytes_read;
        uint64_t bytes_total;
    } EeLoadProgress;

    /**
 * @brief Callback invoked on the worker thread during load (throttle yourself).
 * @return FALSE to request cancel.
 */
    typedef BOOL (*EeLoadProgressFn)(const EeLoadProgress *progress, void *user);

    /**
 * @brief Initialize an empty table.
 */
    void EeVoterTable_Init(EeVoterTable *table);

    /**
 * @brief Free all memory owned by the table and re-initialize.
 */
    void EeVoterTable_Clear(EeVoterTable *table);

    /**
 * @brief Get UTF-8 cell text for a physical row/column (never NULL; may be "").
 */
    const char *EeVoterTable_GetCellUtf8(const EeVoterTable *table,
                                         uint32_t physical_row,
                                         uint32_t column);

    /**
 * @brief Get UTF-8 cell for a visual row (after sorting).
 */
    const char *EeVoterTable_GetViewCellUtf8(const EeVoterTable *table,
                                             uint32_t view_row,
                                             uint32_t column);

    /**
 * @brief Convert UTF-8 cell to UTF-16 into caller buffer (empty on failure).
 */
    void EeVoterTable_GetViewCellW(const EeVoterTable *table,
                                   uint32_t view_row,
                                   uint32_t column,
                                   wchar_t *buffer,
                                   size_t buffer_cch);

    /**
 * @brief Sort by display column; toggles direction if same column.
 */
    BOOL EeVoterTable_SortByColumn(EeVoterTable *table, uint32_t column);

    /**
 * @brief Format selected view rows as delimited text for the clipboard.
 *
 * @param table               Loaded table.
 * @param view_rows           Visual row indices (sort order).
 * @param n_rows              Number of entries in @p view_rows.
 * @param prepend_normalized  If TRUE, emit Voter ID and Name before source fields.
 * @param out_text            Receives heap UTF-8 (caller frees). Never NULL on
 *                            success (empty string if @p n_rows is 0).
 * @param out_len             Optional; byte length excluding the terminating NUL.
 *
 * @return FALSE on invalid arguments or out of memory.
 */
    BOOL EeVoterTable_FormatCopyUtf8(const EeVoterTable *table,
                                     const uint32_t *view_rows,
                                     uint32_t n_rows,
                                     BOOL prepend_normalized,
                                     char **out_text,
                                     size_t *out_len);

    /**
 * @brief Load a CSV (comma) or TXT (tab) voter roster into @p out_table.
 *
 * @param path           Wide path to file.
 * @param out_table      Destination; cleared on entry.
 * @param cancel_flag    Optional; non-zero means cancel (checked often).
 * @param progress_fn    Optional progress callback (worker thread).
 * @param progress_user  Passed to progress_fn.
 * @param error_message  Optional; receives message on failure (cch includes NUL).
 * @param error_cch      Capacity of error_message.
 */
    EeLoadStatus EeVoterTable_LoadFromFile(const wchar_t *path,
                                           EeVoterTable *out_table,
                                           volatile LONG *cancel_flag,
                                           EeLoadProgressFn progress_fn,
                                           void *progress_user,
                                           wchar_t *error_message,
                                           size_t error_cch);

#ifdef __cplusplus
}
#endif
