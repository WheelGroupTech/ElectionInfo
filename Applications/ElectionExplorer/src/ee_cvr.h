/**
 * @file ee_cvr.h
 * @brief Cast Vote Record (CVR) table: sparse storage for wide, tall ballot data
 *        loaded from one or more identical-schema Excel workbooks.
 *
 * Each row is one cast ballot; leading "key" columns (Cast Vote Record, Batch,
 * Ballot Status, Precinct, Ballot Style, as present) are frozen, and every other
 * column is a contest whose cell holds the selection (candidate, "undervote",
 * "Yes", …) or is blank. Ballots are sparse (only contests on the ballot style
 * are filled), so cells are stored sparsely with interned values.
 *
 * See docs/cvr-design.md.
 */
#pragma once

#include <windows.h>
#include <stdint.h>

#include "voter_table.h" /* EeLoadStatus, EeLoadProgressFn */

#ifdef __cplusplus
extern "C"
{
#endif

    typedef struct EeCvrTable
    {
        wchar_t **col_titles;  /* ncols owned wide strings */
        uint32_t ncols;
        uint32_t frozen_count; /* leading key columns (>= 1 once loaded) */
        uint32_t nrows;
        uint32_t *view_index;  /* nrows; display order (permuted by sort) */

        /* Sparse cells (CSR): per row, non-blank entries in ascending column
         * order. row_start has nrows+1 offsets into ent_col / ent_val. */
        uint32_t *row_start;
        uint32_t *ent_col;
        uint32_t *ent_val; /* interned value id */
        size_t nent;
        size_t cap_ent;
        size_t cap_rows;

        /* Interned selection values (UTF-8), stored once each. */
        char *val_pool;
        size_t val_len;
        size_t val_cap;
        uint32_t *val_off; /* val_off[id] -> offset into val_pool */
        uint32_t val_count;
        uint32_t val_cap_ids;
        uint32_t *val_hash; /* open addressing: slot -> id + 1 (0 = empty) */
        uint32_t val_hash_cap;
    } EeCvrTable;

    /** Initialize an empty table. */
    void EeCvr_Init(EeCvrTable *t);

    /** Release all memory and reset to empty. */
    void EeCvr_Clear(EeCvrTable *t);

    /**
     * Load one or more `.xlsx` files (first worksheet of each) into @p out.
     * All files must share an identical header row; the first file sets the
     * schema and any mismatch aborts with EeLoadStatus_Error (out is cleared and
     * @p error_message names the offending file). Same-schema files are
     * concatenated in the given order.
     *
     * @param paths          Array of @p count wide file paths.
     * @param count          Number of files (>= 1).
     * @param out            Destination; cleared on entry.
     * @param cancel_flag    Optional; non-zero requests cancel.
     * @param progress_fn    Optional progress callback.
     * @param progress_user  Passed to @p progress_fn.
     * @param error_message  Optional; receives a message on failure.
     * @param error_cch      Capacity of @p error_message.
     */
    EeLoadStatus EeCvr_LoadFromFiles(const wchar_t *const *paths,
                                     int count,
                                     EeCvrTable *out,
                                     volatile LONG *cancel_flag,
                                     EeLoadProgressFn progress_fn,
                                     void *progress_user,
                                     wchar_t *error_message,
                                     size_t error_cch);

    /**
     * Copy the cell at display row @p view_row, column @p col into @p buf as a
     * wide string ("" for a blank cell). Returns FALSE on invalid indices.
     */
    BOOL EeCvr_GetViewCellW(const EeCvrTable *t,
                            uint32_t view_row,
                            uint32_t col,
                            wchar_t *buf,
                            size_t cch);

    /** Reorder view_index by @p col (numeric-aware); @p ascending toggles order. */
    void EeCvr_SortByColumn(EeCvrTable *t, uint32_t col, BOOL ascending);

#ifdef __cplusplus
}
#endif
