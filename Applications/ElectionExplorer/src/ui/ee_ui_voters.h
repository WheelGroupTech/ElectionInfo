/**
 * @file ee_ui_voters.h
 * @brief A voter list for the macOS and Linux windows (AppKit, GTK), over the engine's
 *        EeVoterTable (voter_table.h), in plain C types only.
 *
 * The windows' code never includes the engine's headers: they bring the stand-in
 * <windows.h>, whose BOOL clashes with Objective-C's. Text is UTF-8; rows are display rows
 * (in the current sort order).
 */
#pragma once

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C"
{
#endif

    typedef struct EeUiVoters EeUiVoters;

    /** Load progress (any thread): percent 0..100 and rows so far. Return 0 to cancel. */
    typedef int (*EeUiProgressFn)(void *user, unsigned percent, unsigned rows);

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

    uint32_t EeUiVoters_Rows(const EeUiVoters *v);
    uint32_t EeUiVoters_Columns(const EeUiVoters *v);

    /** Column @p c's title (UTF-8; valid until the list is freed). */
    const char *EeUiVoters_Title(const EeUiVoters *v, uint32_t c);

    /** The cell at display row @p row, column @p c (UTF-8, never NULL). */
    const char *EeUiVoters_Cell(const EeUiVoters *v, uint32_t row, uint32_t c);

    /** 1 when column @p c holds only numbers or dates (shown right-aligned). */
    int EeUiVoters_IsNumeric(const EeUiVoters *v, uint32_t c);

    /** Sort by column @p c; sorting the same column again reverses it. */
    void EeUiVoters_Sort(EeUiVoters *v, uint32_t c);

    /** Sort by column @p c in the given direction (GTK's column view asks for one). */
    void EeUiVoters_SortTo(EeUiVoters *v, uint32_t c, int ascending);

    /** The sorted column (-1 for file order) and whether it is ascending. */
    int EeUiVoters_SortColumn(const EeUiVoters *v, int *ascending);

    /**
     * The windows' `--selftest <file>`: load @p path, sort it by column 1 both ways and print
     * (stdout) what the window would show: the row and column counts, the first column titles
     * and the first rows' column-1 cells. Returns 0, or 1 when the load fails.
     */
    int EeUiVoters_SelfTest(const char *path_utf8);

#ifdef __cplusplus
}
#endif
