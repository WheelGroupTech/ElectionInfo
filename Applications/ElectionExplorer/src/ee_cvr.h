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
        uint32_t *col_group;   /* ncols: index of the contest's title column that
                                * owns this column. For a titled column (and each
                                * frozen key column) col_group[i] == i; for a blank
                                * "vote for N" continuation column it is the index
                                * of the preceding titled column, so Phase 2 can
                                * tabulate a contest across all its columns without
                                * parsing the "(2)"/"(3)" display suffix. */
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

    /**
     * One tabulated (contest, selection, count) triple. @p contest and
     * @p selection are owned wide strings; free via EeCvr_FreeTally.
     */
    typedef struct EeCvrTally
    {
        wchar_t *contest;   /* contest name (a contest's title-column header) */
        wchar_t *selection; /* the selection: candidate, Yes/No, undervote, … */
        uint32_t count;     /* number of ballots with that selection          */
    } EeCvrTally;

    /**
     * Tabulate every contest in @p t: for each contest (all columns sharing a
     * col_group beyond the frozen key columns), count how many ballots carry each
     * non-blank selection across the contest's columns. Blank cells (contest not on
     * the ballot) are not counted.
     *
     * Results are grouped by contest (in column order). Within a contest, real
     * candidates come first (by count descending, then selection text), followed by
     * the non-candidate outcomes in this fixed order: write-in, overvote, undervote.
     *
     * When @p merge_writeins is TRUE, all write-in variants in a contest (the scanned
     * `[write-in]` image marker, any literal "Write-in" text, and ES&S's
     * "No image found" placeholder for a write-in whose image was not retrieved) are
     * combined into a single "write-in" tally row; when FALSE each distinct write-in
     * value is its own row.
     *
     * On success *out_items / *out_count receive a heap array the caller owns (free
     * with EeCvr_FreeTally). Returns FALSE on OOM or bad args.
     */
    BOOL EeCvr_Tabulate(const EeCvrTable *t,
                        BOOL merge_writeins,
                        EeCvrTally **out_items,
                        uint32_t *out_count);

    /** Free an array returned by EeCvr_Tabulate. */
    void EeCvr_FreeTally(EeCvrTally *items, uint32_t count);

    /**
     * Heuristic: does the CVR look like it holds multi-card/multi-page ballots
     * (one row per ballot sheet rather than per ballot)?
     *
     * ES&S per-sheet CVRs record each additional page/card as its own row that is
     * blank in the top-of-ballot statewide contests -- President, Governor (not
     * Lieutenant Governor), or U.S. Senator -- which appear on page 1 of every ballot
     * style in every U.S. county.
     * So the CVR is treated as multi-card when it contains such a reference contest
     * AND a MINORITY of rows carry none of them (those rows are continuation sheets).
     *
     * This is informational only (it never affects tallies). It deliberately does
     * not fire on: combined-party primary CVRs (every ballot carries its own party's
     * top race, so no row lacks a reference contest); runoff/local CVRs that lack
     * these statewide races (no reference contest to gate on). Because it keys on the
     * presence/absence of the reference contest rather than a fixed fill threshold,
     * it stays correct even when many ballot styles are multi-page.
     */
    BOOL EeCvr_HasMultiCard(const EeCvrTable *t);

#ifdef __cplusplus
}
#endif
