/**
 * @file ee_ui_voters.h
 * @brief A voter list for the macOS and Linux windows (AppKit, GTK), over the engine's
 *        EeVoterTable (voter_table.h) and filters (filter.h), in plain C types only.
 *
 * The windows' code never includes the engine's headers: they bring the stand-in
 * <windows.h>, whose BOOL clashes with Objective-C's. Text is UTF-8.
 *
 * Rows are *shown rows*: the rows that pass the filter, in the current sort order. The
 * order and the filter together are a *view*. A new view is computed off the UI thread
 * (EeUiVoters_ComputeView: a sort of 1.5 million rows takes seconds) while the window keeps
 * showing the old one, then swapped in on the UI thread (EeUiVoters_SetView, which is cheap).
 * Only one ComputeView may run at a time, and SetView must not run while one does.
 */
#pragma once

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C"
{
#endif

    typedef struct EeUiVoters EeUiVoters;
    typedef struct EeUiView EeUiView;

    /** Load progress (any thread): percent 0..100 and rows so far. Return 0 to cancel. */
    typedef int (*EeUiProgressFn)(void *user, unsigned percent, unsigned rows);

    /** Filter relations, as in filter.h's EeFilterRelation (and the Windows Filter window). */
    enum
    {
        EE_UI_REL_IS = 0,
        EE_UI_REL_IS_NOT,
        EE_UI_REL_LESS_THAN,
        EE_UI_REL_MORE_THAN,
        EE_UI_REL_BEGINS_WITH,
        EE_UI_REL_ENDS_WITH,
        EE_UI_REL_CONTAINS,
        EE_UI_REL_EXCLUDES,
        EE_UI_REL_COUNT
    };

#define EE_UI_RULE_VALUE_BYTES 768 /* UTF-8: the engine keeps 255 UTF-16 units */

    /**
     * One filter rule, as in the Windows Filter window: "<column> <relation> <value> then
     * Include/Exclude". Includes on the same column are ORed, different columns ANDed, and
     * any matching exclude hides the row.
     */
    typedef struct EeUiRule
    {
        uint32_t column;
        int relation;                       /* EE_UI_REL_* */
        int exclude;                        /* 0 include, 1 exclude */
        int enabled;
        char value[EE_UI_RULE_VALUE_BYTES]; /* UTF-8 */
    } EeUiRule;

    /**
     * Load a voter list (CSV, TSV, TXT, XLSX: what Election Explorer opens on Windows).
     * Blocks; call it off the UI thread. @p cancel (optional) set non-zero cancels.
     * Returns NULL with a message in @p err (UTF-8) on failure or cancel.
     */
    EeUiVoters *EeUiVoters_Load(const char *path_utf8,
                                EeUiProgressFn fn,
                                void *user,
                                volatile int32_t *cancel,
                                char *err,
                                size_t errcap);

    void EeUiVoters_Free(EeUiVoters *v);

    /** Shown rows (after the filter), and all rows in the list. */
    uint32_t EeUiVoters_Rows(const EeUiVoters *v);
    uint32_t EeUiVoters_TotalRows(const EeUiVoters *v);
    uint32_t EeUiVoters_Columns(const EeUiVoters *v);

    /** Column @p c's title (UTF-8; valid until the list is freed). */
    const char *EeUiVoters_Title(const EeUiVoters *v, uint32_t c);

    /** The cell at shown row @p row, column @p c (UTF-8, never NULL). */
    const char *EeUiVoters_Cell(const EeUiVoters *v, uint32_t row, uint32_t c);

    /** 1 when column @p c holds only numbers or dates (shown right-aligned). */
    int EeUiVoters_IsNumeric(const EeUiVoters *v, uint32_t c);

    /** The sorted column (-1 for file order) and whether it is ascending. */
    int EeUiVoters_SortColumn(const EeUiVoters *v, int *ascending);

    /** The current filter's rules (enabled or not). */
    uint32_t EeUiVoters_RuleCount(const EeUiVoters *v);
    int EeUiVoters_GetRule(const EeUiVoters *v, uint32_t i, EeUiRule *out);

    /** 1 when the current filter hides rows (some rule is enabled). */
    int EeUiVoters_IsFiltered(const EeUiVoters *v);

    /**
     * 1 when @p rule can be added; otherwise 0 with the reason in @p why (UTF-8). A date
     * column compared by less than / more than needs a date.
     */
    int EeUiVoters_RuleIsValid(const EeUiVoters *v, const EeUiRule *rule, char *why, size_t whycap);

    /** "is", "is not", ... (never NULL). */
    const char *EeUiVoters_RelationName(int relation);

    /**
     * Compute a view: the order by @p sort_column (-1 for file order) and the filter
     * @p rules. Any thread; reads the list but changes nothing. Returns NULL when out of
     * memory.
     */
    EeUiView *EeUiVoters_ComputeView(const EeUiVoters *v,
                                     int sort_column,
                                     int ascending,
                                     const EeUiRule *rules,
                                     uint32_t nrules);

    /** Show @p view (UI thread; takes ownership). */
    void EeUiVoters_SetView(EeUiVoters *v, EeUiView *view);

    /** Free a view that was not shown. */
    void EeUiView_Free(EeUiView *view);

    /** Sort (on the calling thread) by column @p c; sorting the same column again reverses it. */
    void EeUiVoters_Sort(EeUiVoters *v, uint32_t c);

    /** Sort (on the calling thread) by column @p c in the given direction. */
    void EeUiVoters_SortTo(EeUiVoters *v, uint32_t c, int ascending);

    /**
     * The windows' `--selftest <file>`: load @p path, sort it by column 1 both ways, filter it
     * and print (stdout) what the window would show: the row and column counts, the first
     * column titles and the first rows' column-1 cells. Returns 0, or 1 when the load fails.
     */
    int EeUiVoters_SelfTest(const char *path_utf8);

#ifdef __cplusplus
}
#endif
