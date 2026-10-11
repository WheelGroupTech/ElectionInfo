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
 * Maximum display columns (synthesized Voter ID, Precinct, Name, Address plus
 * source fields).
 * Historical county exports can exceed 380 source columns (VOTED/PLACE/PARTY
 * per election).
 */
#define EE_MAX_COLUMNS 1024

/** Frozen display columns: Voter ID, Precinct, Name, Address. */
#define EE_FROZEN_COLUMN_COUNT 4
#define EE_COL_VOTER_ID        0
#define EE_COL_PRECINCT        1
#define EE_COL_NAME            2
#define EE_COL_ADDRESS         3

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

        /* Per display column: 0 unknown, 1 numeric/date only, 2 text. */
        uint8_t column_value_kind[EE_MAX_COLUMNS];
        /* TRUE when the header is a date field (contains "Date", EDR, DOB, …). */
        uint8_t column_is_date[EE_MAX_COLUMNS];

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

        /* Display-column indices for name parts (-1 if absent). */
        int name_full_col;
        int name_prefix_col;
        int name_first_col;
        int name_middle_col;
        int name_last_col;
        int name_suffix_col;
        BOOL name_surname_first;

        /* Display-column indices for residence address parts (-1 if absent). */
        int addr_full_col;
        int addr_number_col;
        int addr_number_suffix_col; /* e.g. "1/2", "A" after the house number */
        int addr_predir_col;
        int addr_street_col;
        int addr_type_col;
        int addr_postdir_col;
        int addr_unit_type_col;
        int addr_unit_col;
        int addr_city_col;
        int addr_state_col;
        int addr_zip_col;
        int addr_zip4_col;
    } EeVoterTable;

    typedef struct EeLoadProgress
    {
        uint32_t percent; /* 0..100 */
        uint32_t rows_loaded;
        uint64_t bytes_read;
        uint64_t bytes_total;
        int scanning; /* TRUE during a preliminary discovery pass that has not yet
                       * produced any rows (e.g. the Hart loader's pass 1); rows_loaded
                       * is 0 while this holds. Producers that have no scan pass leave
                       * it 0. The value 2 marks the OCR pass of a scanned Hart PDF:
                       * bytes_read / bytes_total then hold pages read / pages total. */
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
     * @brief TRUE if this display column is known to hold only numbers or dates.
     *
     * Confirmed from loaded cell text. If every value is empty, known headers
     * such as Voter ID, DOB, Age, and Precinct still count as numeric/date.
     */
    BOOL EeVoterTable_ColumnIsNumericOrDate(const EeVoterTable *table, uint32_t column);

    /**
     * @brief TRUE if this display column is a date field (header contains Date, EDR, …).
     */
    BOOL EeVoterTable_ColumnIsDate(const EeVoterTable *table, uint32_t column);

    /**
     * @brief Parse a date to YYYYMMDD. Accepts YYYYMMDD, YYYY-MM-DD, and M/D/YYYY.
     * @return FALSE if @p s is not a date. @p out_ymd may be NULL.
     */
    BOOL EeVoterTable_ParseDateYmdW(const wchar_t *s, uint32_t *out_ymd);

    /**
     * @brief Distinct normalized Voter ID values that occur on two or more rows.
     *        Empty IDs are ignored. Caller frees *out_ids and each string.
     */
    BOOL EeVoterTable_CollectDuplicateVoterIds(const EeVoterTable *table,
                                               wchar_t ***out_ids,
                                               uint32_t *out_count);

    /**
     * @brief Index of a date-of-birth column, or -1 if none (Birth Date, DOB, …).
     */
    int EeVoterTable_FindBirthdateColumn(const EeVoterTable *table);

    /**
     * @brief Voter IDs of rows that share the same normalized name and DOB
     *        with at least one other row. Caller frees *out_ids and each string.
     */
    BOOL EeVoterTable_CollectDuplicateVotersByNameDob(const EeVoterTable *table,
                                                      wchar_t ***out_ids,
                                                      uint32_t *out_count);

    /**
     * @brief Flag physical rows that share their Voter ID with another row.
     *
     * O(n) hash grouping. Empty IDs are ignored. @p marks must point to at least
     * @p table->row_count bytes; the function sets a byte to 1 for each duplicate
     * row and leaves the rest untouched (callers normally pass a zeroed buffer).
     *
     * @param marks         Per physical row; byte set to 1 when the row is a duplicate.
     * @param out_count     Receives the number of marked rows (0 if none).
     * @param cancel_flag   Optional; non-zero requests cancel (checked periodically).
     * @param progress_fn   Optional; invoked on the calling thread. Return FALSE to cancel.
     * @param progress_user Passed to @p progress_fn.
     *
     * @return FALSE on invalid arguments or out of memory. On cancel it returns
     *         TRUE with partial marks; distinguish via @p cancel_flag.
     */
    BOOL EeVoterTable_MarkDuplicateVoterIds(const EeVoterTable *table,
                                            uint8_t *marks,
                                            uint32_t *out_count,
                                            volatile LONG *cancel_flag,
                                            EeLoadProgressFn progress_fn,
                                            void *progress_user);

    /**
     * @brief Flag physical rows sharing a normalized name and DOB with another row.
     *
     * Same match rule as EeVoterTable_CollectDuplicateVotersByNameDob but O(n) and
     * marks rows directly. Rows with an empty name or DOB are ignored. @p marks
     * must point to at least @p table->row_count bytes (see MarkDuplicateVoterIds).
     *
     * @return FALSE on invalid arguments or out of memory. TRUE (0 marked) when the
     *         table has no birth-date column. On cancel, TRUE with partial marks.
     */
    BOOL EeVoterTable_MarkDuplicateVotersByNameDob(const EeVoterTable *table,
                                                   uint8_t *marks,
                                                   uint32_t *out_count,
                                                   volatile LONG *cancel_flag,
                                                   EeLoadProgressFn progress_fn,
                                                   void *progress_user);

    /** A distinct display-column value and how many rows carry it. */
    typedef struct EeValueCount
    {
        wchar_t *value; /* heap UTF-16; caller frees (or EeVoterTable_FreeValueCounts). */
        uint32_t count;
    } EeValueCount;

    /**
     * @brief Count rows per distinct (case-insensitive) value in a display column.
     *
     * Empty cells are excluded from @p out_items but tallied into @p out_blank_count
     * so callers can surface incomplete records separately. Returns an unsorted heap
     * array; the displayed value is the first spelling seen. O(n) hash aggregation.
     *
     * @param out_items        Receives the array (NULL when no non-empty values exist).
     * @param out_count        Receives the number of distinct non-empty values.
     * @param out_blank_count  Optional; receives the number of rows with an empty cell.
     * @return FALSE on invalid arguments or out of memory.
     */
    BOOL EeVoterTable_CollectValueCounts(const EeVoterTable *table,
                                         uint32_t column,
                                         EeValueCount **out_items,
                                         uint32_t *out_count,
                                         uint32_t *out_blank_count);

    /** @brief Free an array returned by EeVoterTable_CollectValueCounts. */
    void EeVoterTable_FreeValueCounts(EeValueCount *items, uint32_t count);

    /* -------------------------------------------------------------------------- */
    /* Two-file compare (by Voter ID)                                             */
    /* -------------------------------------------------------------------------- */

    /**
     * Per physical row classification produced by EeVoterTable_CompareByVoterId,
     * as a bit set. EE_CMP_ONLY_HERE is exclusive of the matched bits; a matched
     * row with none of the change bits set is "identical". Name and Address each
     * grade to a minor or major change (normalized case-insensitive edit distance);
     * Precinct is a binary changed flag (a precinct is a code, not free text).
     */
    enum
    {
        EE_CMP_NONE = 0x00,       /* untouched (not part of the compare) */
        EE_CMP_ONLY_HERE = 0x01,  /* Voter ID absent from the other file (incl. blank ID) */
        EE_CMP_MATCHED = 0x02,    /* Voter ID present in both files */
        EE_CMP_NAME_MINOR = 0x04, /* Name differs slightly */
        EE_CMP_NAME_MAJOR = 0x08, /* Name differs substantially */
        EE_CMP_ADDR_MINOR = 0x10, /* Address differs slightly */
        EE_CMP_ADDR_MAJOR = 0x20, /* Address differs substantially */
        EE_CMP_PCT_CHANGED = 0x40,    /* Precinct differs */
        EE_CMP_METHOD_CHANGED = 0x80, /* Voting Method differs (voter roster compare) */
        EE_CMP_DATE_CHANGED = 0x100,  /* Date Voted differs (voter roster compare) */
        EE_CMP_REPEATED = 0x200       /* Voter ID in both files, but on more rows here than
                                       * there: this row was left unpaired (rows pair one
                                       * to one). Exclusive like EE_CMP_ONLY_HERE */
    };

    /* All "something changed" bits (matched rows without any of these are identical). */
#define EE_CMP_CHANGE_BITS                                                                         \
    (EE_CMP_NAME_MINOR | EE_CMP_NAME_MAJOR | EE_CMP_ADDR_MINOR | EE_CMP_ADDR_MAJOR |               \
     EE_CMP_PCT_CHANGED | EE_CMP_METHOD_CHANGED | EE_CMP_DATE_CHANGED)

    /** Row-count tallies from a compare (A = first table, B = second). */
    typedef struct EeCompareResult
    {
        uint32_t only_a, identical_a;
        uint32_t name_minor_a, name_major_a;
        uint32_t addr_minor_a, addr_major_a;
        uint32_t pct_changed_a;
        uint32_t only_b, identical_b;
        uint32_t name_minor_b, name_major_b;
        uint32_t addr_minor_b, addr_major_b;
        uint32_t pct_changed_b;
        uint32_t method_changed_a, date_changed_a; /* voter roster compares only */
        uint32_t method_changed_b, date_changed_b;
        uint32_t repeated_a, repeated_b; /* EE_CMP_REPEATED rows */
    } EeCompareResult;

    /** How names are compared (see EeCompareOptions). */
    typedef enum EeCompareNames
    {
        EE_CMP_NAMES_FULL = 0,  /* the normalized Name column */
        EE_CMP_NAMES_PARTS,     /* last + first + middle + suffix parts (independent of the
                                 * surname-first display option); FULL if a table has no
                                 * first / last name columns */
        EE_CMP_NAMES_FIRST_LAST /* equal when first and last names match (ignoring middle
                                 * names, case and punctuation); otherwise graded as PARTS */
    } EeCompareNames;

    /** What a compare looks at. NULL options = a voter-list compare: Name (FULL),
     *  Address, and Precinct when the address is unchanged. */
    typedef struct EeCompareOptions
    {
        BOOL compare_address; /* grade Address; FALSE never compares it, and Precinct is
                               * then always compared */
        BOOL loose_precinct;  /* "0400" equals "400" (all-digit precincts) */
        int names;            /* EeCompareNames */
        BOOL compare_vote;    /* compare the "Voting Method" and "Date Voted" columns (both
                               * tables are voter rosters); a Voter ID on several rows
                               * matches the other file's row with the same method and date
                               * when there is one */
    } EeCompareOptions;

    /**
     * @brief Classify every row of two tables by matching normalized Voter ID.
     *
     * Rows with a blank Voter ID are "only here" (unmatchable). For a matched row,
     * Name and Address are each graded unchanged / minor / major by normalized
     * case-insensitive edit distance, and Precinct is flagged if it differs. A
     * matched row with no change is "identical". O(rows_a + rows_b) hash join.
     *
     * @param class_a  Receives one EE_CMP_* bit set per physical row of @p a
     *                 (must hold @p a->row_count bytes). Every row is written.
     * @param class_b  Same for @p b (@p b->row_count bytes).
     * @param out      Receives the per-side counts.
     * @param cancel_flag   Optional; non-zero requests cancel (checked periodically).
     * @param progress_fn   Optional; invoked on the calling thread. Return FALSE to cancel.
     * @param progress_user Passed to @p progress_fn.
     *
     * @return FALSE on invalid arguments or out of memory. On cancel it returns
     *         TRUE with partial classifications; distinguish via @p cancel_flag.
     */
    BOOL EeVoterTable_CompareByVoterId(const EeVoterTable *a,
                                       const EeVoterTable *b,
                                       uint8_t *class_a,
                                       uint8_t *class_b,
                                       EeCompareResult *out,
                                       volatile LONG *cancel_flag,
                                       EeLoadProgressFn progress_fn,
                                       void *progress_user);

    /** One matched voter that differs, paired across the two tables. */
    typedef struct EeCompareDiff
    {
        uint32_t row_a; /* physical row in table A */
        uint32_t row_b; /* physical row in table B */
        uint16_t bits;  /* EE_CMP_* change bits */
    } EeCompareDiff;

    /**
     * @brief List matched voters (by Voter ID) whose Name, Address, or Precinct
     *        differ, pairing each A row with its B row. Same grading as
     *        EeVoterTable_CompareByVoterId; identical voters are omitted.
     *
     * @param out        Receives a heap array (caller frees with free()); NULL if none.
     * @param out_count  Receives the number of entries.
     * @return FALSE on invalid arguments or out of memory. On cancel, TRUE with a
     *         partial list; distinguish via @p cancel_flag.
     */
    BOOL EeVoterTable_CollectDifferences(const EeVoterTable *a,
                                         const EeVoterTable *b,
                                         EeCompareDiff **out,
                                         uint32_t *out_count,
                                         volatile LONG *cancel_flag,
                                         EeLoadProgressFn progress_fn,
                                         void *progress_user);

    /**
     * @brief EeVoterTable_CompareByVoterId / EeVoterTable_CollectDifferences with
     *        options (NULL = voter-list defaults) and 16-bit classes, which can hold
     *        EE_CMP_METHOD_CHANGED / EE_CMP_DATE_CHANGED / EE_CMP_REPEATED.
     *
     * Rows pair one to one: a Voter ID on several rows pairs each row with a different
     * row of the other file -- identical rows first, then the closest -- and rows left
     * over are EE_CMP_REPEATED, so every matched category counts the same on both sides.
     * (The 8-bit API reports EE_CMP_REPEATED rows as EE_CMP_ONLY_HERE.)
     */
    BOOL EeVoterTable_CompareByVoterIdEx(const EeVoterTable *a,
                                         const EeVoterTable *b,
                                         const EeCompareOptions *options,
                                         uint16_t *class_a,
                                         uint16_t *class_b,
                                         EeCompareResult *out,
                                         volatile LONG *cancel_flag,
                                         EeLoadProgressFn progress_fn,
                                         void *progress_user);

    BOOL EeVoterTable_CollectDifferencesEx(const EeVoterTable *a,
                                           const EeVoterTable *b,
                                           const EeCompareOptions *options,
                                           EeCompareDiff **out,
                                           uint32_t *out_count,
                                           volatile LONG *cancel_flag,
                                           EeLoadProgressFn progress_fn,
                                           void *progress_user);

    /**
     * @brief Flag voters of @p list who voted -- whose Voter ID appears in @p roster -- and
     *        share a normalized name and DOB with another voter who voted under a
     *        different Voter ID (e.g. two people with the same name and birth date, told
     *        apart only by driver license numbers that lists do not publish).
     *
     * Name + DOB match as in EeVoterTable_MarkDuplicateVotersByNameDob. A group needs two
     * or more distinct Voter IDs among its voters who voted; only those voters are
     * marked. @p marks must hold @p list->row_count bytes (zeroed by the caller).
     *
     * @param out_count   Rows marked.
     * @param out_groups  Optional; name + DOB groups found.
     * @return FALSE on invalid arguments or out of memory. TRUE (0 marked) when @p list
     *         has no birth-date column. On cancel, TRUE with no marks.
     */
    BOOL EeVoterTable_MarkDuplicateVotersVoting(const EeVoterTable *list,
                                                const EeVoterTable *roster,
                                                uint8_t *marks,
                                                uint32_t *out_count,
                                                uint32_t *out_groups,
                                                volatile LONG *cancel_flag,
                                                EeLoadProgressFn progress_fn,
                                                void *progress_user);

    /** Source column titled @p title (case-insensitive), or -1. */
    int EeVoterTable_FindColumnByTitle(const EeVoterTable *table, const wchar_t *title);

    /**
 * @brief Sort by display column; toggles direction if same column.
 */
    BOOL EeVoterTable_SortByColumn(EeVoterTable *table, uint32_t column);

    /**
 * @brief Rebuild the synthesized Name column in surname-first or given-first form.
 *
 * @param table          Loaded table.
 * @param surname_first  TRUE for "Last, First Middle"; FALSE for "First Middle Last".
 * @param progress_fn    Optional; invoked from the calling thread.
 * @param progress_user  Passed to progress_fn.
 *
 * @return FALSE on invalid arguments or out of memory.
 */
    BOOL EeVoterTable_SetNameSurnameFirst(EeVoterTable *table,
                                          BOOL surname_first,
                                          EeLoadProgressFn progress_fn,
                                          void *progress_user);

    /**
 * @brief Format selected view rows as delimited text for the clipboard.
 *
 * @param table               Loaded table.
 * @param view_rows           Visual row indices (sort order).
 * @param n_rows              Number of entries in @p view_rows.
 * @param prepend_normalized  If TRUE, emit Voter ID, Precinct, Name, and Address before
 *                            source fields.
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
 * @brief Format selected view rows as delimited UTF-8 text for file export.
 *
 * Like EeVoterTable_FormatCopyUtf8 but with an explicit @p delim (',' or '\t')
 * and an optional header row of column titles. Rows are emitted in the order of
 * @p view_rows; fields are RFC-4180 quoted when they contain the delimiter, a
 * quote, or a newline.
 *
 * @param delim           Field delimiter (',' for CSV, '\t' for TSV).
 * @param include_header  If TRUE, emit a leading row of column titles.
 */
    BOOL EeVoterTable_FormatDelimitedUtf8(const EeVoterTable *table,
                                          const uint32_t *view_rows,
                                          uint32_t n_rows,
                                          BOOL prepend_normalized,
                                          char delim,
                                          BOOL include_header,
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

    /**
 * @brief Load one worksheet of an Excel .xlsx workbook into @p out_table.
 *
 * Rows are fed through the same ingestion pipeline as the CSV/TSV loader, so
 * column-role detection and Voter ID / Precinct / Name / Address composition
 * behave identically. Use EeXlsx_ListSheets (xlsx.h) to present a sheet picker.
 * EeVoterTable_LoadFromFile dispatches a ".xlsx" path here with @p sheet_index 0.
 *
 * @param path           Wide path to the .xlsx file.
 * @param sheet_index    0-based worksheet index in workbook order.
 * @param out_table      Destination; cleared on entry.
 * @param cancel_flag    Optional; non-zero means cancel (checked often).
 * @param progress_fn    Optional progress callback.
 * @param progress_user  Passed to progress_fn.
 * @param error_message  Optional; receives message on failure (cch includes NUL).
 * @param error_cch      Capacity of error_message.
 */
    EeLoadStatus EeVoterTable_LoadXlsxSheet(const wchar_t *path,
                                            int sheet_index,
                                            EeVoterTable *out_table,
                                            volatile LONG *cancel_flag,
                                            EeLoadProgressFn progress_fn,
                                            void *progress_user,
                                            wchar_t *error_message,
                                            size_t error_cch);

    /**
     * Incremental table builder: fill @p out from an in-memory header + rows (UTF-8)
     * whose columns are classified by header name exactly as a file load classifies
     * them (so the result works with filters, duplicate scans, export, reports and
     * compare). Used by loaders that assemble rows themselves (voter rosters).
     *
     * BuilderBegin clears @p out (keeping its surname-first preference) and returns a
     * builder, or NULL on failure (message in @p err). Append one row per call; cells
     * are only read during the call. BuilderEnd finalizes the table when @p keep is
     * TRUE (else clears it) and frees the builder.
     */
    typedef struct EeVoterTableBuilder EeVoterTableBuilder;
    EeVoterTableBuilder *EeVoterTable_BuilderBegin(EeVoterTable *out,
                                                   const char *const *header,
                                                   uint32_t ncols,
                                                   wchar_t *err,
                                                   size_t errcch);
    BOOL EeVoterTable_BuilderAppend(EeVoterTableBuilder *b,
                                    const char *const *cells,
                                    uint32_t ncells,
                                    wchar_t *err,
                                    size_t errcch);
    void EeVoterTable_BuilderEnd(EeVoterTableBuilder *b, BOOL keep);

#ifdef __cplusplus
}
#endif
