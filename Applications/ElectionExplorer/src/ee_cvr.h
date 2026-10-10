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

        /* Optional owned note from the loader for the user (e.g. the OCR summary of a
         * scanned Hart PDF); NULL when there is nothing to report. */
        wchar_t *load_note;
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
     * Load Hart Cast Vote Records from any mix of `.zip` exports (one XML per ballot
     * sheet) and `.pdf` "CVR Report" exports into @p out. See hart_cvr.c.
     *  - ZIPs only: votes and keys from the XML.
     *  - PDFs only: votes and keys from the PDF records (adds Voting Type, Polling
     *    Place, Device Type, Device Serial, Device Data Id; no Sheet Number, Batch
     *    Sequence or Is Blank).
     *  - ZIPs + PDFs: votes from the XML, decorated with the PDF fields matched by
     *    Cvr Id (error if no Cvr Id is shared).
     * Every PDF must be a Hart CVR Report (see EeCvr_IsHartCvrPdf); another vendor's
     * PDF, or any file that is not .zip/.pdf, aborts the load with @p error_message
     * naming the file. Same status/cancel/progress contract as EeCvr_LoadFromFiles.
     */
    EeLoadStatus EeCvr_LoadFromHartFiles(const wchar_t *const *paths,
                                         int count,
                                         EeCvrTable *out,
                                         volatile LONG *cancel_flag,
                                         EeLoadProgressFn progress_fn,
                                         void *progress_user,
                                         wchar_t *error_message,
                                         size_t error_cch);

    /**
     * TRUE if @p path is a Hart "CVR Report" PDF: its first page carries the Hart
     * header labels (Cvr Id, Device Serial, Device Data Id, Central Batch Id) and the
     * Contest Title / Option table header. On FALSE, @p error_message (optional)
     * explains why (unreadable PDF, or a PDF from another source).
     */
    BOOL EeCvr_IsHartCvrPdf(const wchar_t *path, wchar_t *error_message, size_t error_cch);

    /**
     * Load one or more Hart CVR `.zip` files (each holds one XML per ballot sheet)
     * into @p out. Equivalent to EeCvr_LoadFromHartFiles with only zip paths; kept
     * for existing callers. Same status/cancel/progress contract as
     * EeCvr_LoadFromFiles.
     */
    EeLoadStatus EeCvr_LoadFromHartZips(const wchar_t *const *paths,
                                        int count,
                                        EeCvrTable *out,
                                        volatile LONG *cancel_flag,
                                        EeLoadProgressFn progress_fn,
                                        void *progress_user,
                                        wchar_t *error_message,
                                        size_t error_cch);

    /**
     * Load Dominion (Democracy Suite / Liberty Vote) Cast Vote Record exports into
     * @p out. Each path is a `.zip` CVR export holding the JSON manifests
     * (ContestManifest.json, CandidateManifest.json, ...) and one or more
     * CvrExport*.json files. One row per tabulation session (normally one ballot card);
     * the adjudicated version of a session is used when present and only marks Dominion
     * counts as votes (IsVote) are recorded. A ranked-choice contest becomes one column
     * per rank, titled "<contest> (Rank N)". Several zips load together only when their
     * contest layouts are identical; otherwise the load fails naming the file. Same
     * status/cancel/progress contract as EeCvr_LoadFromFiles. See dominion_cvr.c.
     */
    EeLoadStatus EeCvr_LoadFromDominionZips(const wchar_t *const *paths,
                                            int count,
                                            EeCvrTable *out,
                                            volatile LONG *cancel_flag,
                                            EeLoadProgressFn progress_fn,
                                            void *progress_user,
                                            wchar_t *error_message,
                                            size_t error_cch);

    /** TRUE if @p path is a Dominion CVR export zip (it holds ContestManifest.json and at
     *  least one CvrExport*.json). Never fails loudly: an unreadable file is FALSE. */
    BOOL EeCvr_IsDominionZip(const wchar_t *path);

    /* --- Table builder (used by the Hart loader; layout is caller-computed) ------ */

    /** Clear @p t and establish a column layout from UTF-8 @p header_cells (blank ""
     *  cells become "vote for N" continuation columns) with an explicit frozen count.
     *  Returns FALSE on OOM/bad args. */
    BOOL EeCvr_BuildBegin(EeCvrTable *t,
                          const char *const *header_cells,
                          uint32_t ncells,
                          uint32_t frozen_count);

    /** Append one physical row from UTF-8 @p cells (indexed by column; "" = blank).
     *  Unlike the file loader this keeps every row (a blank ballot sheet is a real
     *  record). Returns FALSE only on OOM. */
    BOOL EeCvr_BuildAppendRow(EeCvrTable *t, const char *const *cells, uint32_t ncells);

    /**
     * Copy the cell at display row @p view_row, column @p col into @p buf as a
     * wide string ("" for a blank cell). Returns FALSE on invalid indices.
     */
    BOOL EeCvr_GetViewCellW(const EeCvrTable *t,
                            uint32_t view_row,
                            uint32_t col,
                            wchar_t *buf,
                            size_t cch);

    /**
     * Copy the cell at PHYSICAL row @p row, column @p col into @p buf ("" if blank).
     * Unlike EeCvr_GetViewCellW this does not go through the sort/view index, so it
     * is suitable for filtering and for a caller-maintained display map.
     */
    BOOL EeCvr_GetCellW(const EeCvrTable *t,
                        uint32_t row,
                        uint32_t col,
                        wchar_t *buf,
                        size_t cch);

    /**
     * Collect the distinct selection values that appear in column @p col (candidate
     * names, "undervote", "overvote", write-in variants, …), case-insensitively
     * sorted, stopping after @p max_values uniques. On success *out_values is a heap
     * array of @p *out_count owned wide strings (caller frees each string then the
     * array). Blank cells are not included.
     */
    BOOL EeCvr_CollectColumnValues(const EeCvrTable *t,
                                   uint32_t col,
                                   uint32_t max_values,
                                   wchar_t ***out_values,
                                   uint32_t *out_count);

    /** Reorder view_index by @p col (numeric-aware); @p ascending toggles order. */
    void EeCvr_SortByColumn(EeCvrTable *t, uint32_t col, BOOL ascending);

    /**
     * Format the given PHYSICAL rows (in the order provided) as delimited UTF-8 text
     * for file export: every column, @p delim between fields ('\t' or ','), CR/LF
     * between rows, RFC-4180 quoting. When @p include_header is TRUE a leading row of
     * column titles is emitted. *out_text is a heap buffer the caller frees (never
     * NULL on success). Returns FALSE on OOM/bad args.
     */
    BOOL EeCvr_FormatDelimitedUtf8(const EeCvrTable *t,
                                   const uint32_t *rows,
                                   uint32_t n_rows,
                                   char delim,
                                   BOOL include_header,
                                   char **out_text,
                                   size_t *out_len);

    /**
     * Find the column whose header equals @p title (trimmed, case-insensitive), e.g.
     * L"Batch", L"Precinct", L"Ballot Style". On success sets *out_col and returns
     * TRUE; returns FALSE if no such column exists.
     */
    BOOL EeCvr_FindColumnByTitle(const EeCvrTable *t, const wchar_t *title, uint32_t *out_col);

    /**
     * TRUE if column @p col holds at least one non-blank value that is not a
     * redaction placeholder (e.g. "<Redacted>"). Used to decide whether a per-column
     * value report is meaningful: a column that is entirely blank or entirely
     * redacted has nothing to report.
     */
    BOOL EeCvr_ColumnHasReportableData(const EeCvrTable *t, uint32_t col);

    /** One (value, ballot-record count) pair; @p value is an owned wide string. */
    typedef struct EeCvrValueCount
    {
        wchar_t *value;
        uint32_t count;
    } EeCvrValueCount;

    /**
     * Count the ballot records carrying each distinct value in column @p col. Blank
     * cells are tallied into *out_blank (not included in the returned array). On
     * success *out_items is a heap array of *out_count owned entries (free with
     * EeCvr_FreeColumnCounts). The array is unsorted. Returns FALSE on OOM/bad args.
     */
    BOOL EeCvr_CollectColumnCounts(const EeCvrTable *t,
                                   uint32_t col,
                                   EeCvrValueCount **out_items,
                                   uint32_t *out_count,
                                   uint32_t *out_blank);

    /** Free an array returned by EeCvr_CollectColumnCounts. */
    void EeCvr_FreeColumnCounts(EeCvrValueCount *items, uint32_t count);

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

    /** Like EeCvr_Tabulate but counts only the @p nrows physical rows in @p rows
     *  (used to tabulate the currently-filtered subset). */
    BOOL EeCvr_TabulateRows(const EeCvrTable *t,
                            const uint32_t *rows,
                            uint32_t nrows,
                            BOOL merge_writeins,
                            EeCvrTally **out_items,
                            uint32_t *out_count);

    /** Reorder tabulation output so a primary's contests group by party: the
     *  display-first party (@p party_first: 0 = Republican, 1 = Democratic) first,
     *  then the other party, then non-partisan contests; category order is preserved
     *  within each block. Partisan contests are recognized by a "REP "/"DEM " title
     *  prefix. No-op for a general election (no prefixes). */
    void EeCvr_ReorderTallyByParty(EeCvrTally *items, uint32_t count, int party_first);

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

    /* --- Ranked-choice (instant-runoff) tabulation -- see ee_rcv.c ------------- */

    /**
     * Find the ranked-choice contests in @p t. A ranked-choice contest is a run of
     * consecutive contest columns titled "<contest> (Rank 1)", "<contest> (Rank 2)", ...
     * (the layout the Dominion loader produces, preserved by CSV/TSV export). Writes up
     * to @p cap first-column ("Rank 1") indices to @p first_cols (may be NULL) and
     * returns the total number found.
     */
    uint32_t EeCvr_FindRcvContests(const EeCvrTable *t, uint32_t *first_cols, uint32_t cap);

    /** Round-by-round result of one ranked-choice contest. */
    typedef struct EeRcvResult
    {
        wchar_t *contest;        /* contest title without the " (Rank N)" suffix */
        uint32_t nranks;         /* rank columns in the contest */
        uint32_t ncand;          /* candidates (every name ranked at least once) */
        wchar_t **cand;          /* names in finishing order: winner, runner-up, then
                              * the remaining candidates latest-eliminated first */
        uint32_t nrounds;
        uint32_t *votes;         /* [round * ncand + cand]; 0 once eliminated */
        uint32_t *continuing;    /* per round: ballots counting for a candidate */
        uint32_t *blanks;        /* per round: ballots with the contest but no ranking */
        uint32_t *exhausted;     /* per round: ballots with no continuing candidate left */
        uint32_t *overvotes;     /* per round: ballots stopped by an overvoted ranking */
        int32_t *eliminated;     /* per round: candidate eliminated after it, or -1 */
        BOOL *elim_tie;          /* per round: that elimination broke a tie for last */
        int32_t winner;          /* candidate index (0 when decided), -1 if none */
        uint32_t majority_round; /* first round (1-based) in which the leader held a
                                  * majority of continuing ballots; 0 = never */
    } EeRcvResult;

    /**
     * Tabulate the ranked-choice contest whose "Rank 1" column is @p first_col by
     * single-winner instant runoff, using the rules of San Francisco's official Dominion
     * tabulation: each round a ballot counts for its highest-ranked continuing candidate;
     * skipped rankings ("undervote") are passed over; reaching an overvoted ranking
     * stops the ballot (counted under overvotes); unresolved write-ins are excluded (the
     * ranking is passed over); a ballot with no candidate ranking at all is a blank.
     * After each round the single candidate with the fewest votes is eliminated (a tie
     * for last is broken by the lower total in the most recent earlier round that
     * differs, then by name, and flagged in elim_tie, since officials resolve true ties
     * by lot). Rounds continue until two candidates remain; the winner has the most votes
     * in the final round.
     *
     * Counts only the @p nrows physical rows in @p rows, or every row when @p rows is
     * NULL. Rows that do not carry the contest are ignored. On success @p out is filled
     * (free with EeCvr_FreeRcvResult). Returns FALSE on OOM or bad args.
     */
    BOOL EeCvr_TabulateRcv(const EeCvrTable *t,
                           uint32_t first_col,
                           const uint32_t *rows,
                           uint32_t nrows,
                           EeRcvResult *out);

    /** Release an EeRcvResult filled by EeCvr_TabulateRcv (safe on a zeroed struct). */
    void EeCvr_FreeRcvResult(EeRcvResult *r);

#ifdef __cplusplus
}
#endif
