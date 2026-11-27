/**
 * @file ee_ui_results.h
 * @brief Election results for the macOS and Linux windows (AppKit, GTK), over the engine's
 *        EeResults (ee_results.h), in plain C types only: Windows' Election Results window
 *        (main.c, "Election Results window (1.5)").
 *
 * The windows' code never includes the engine's headers (see ee_ui_voters.h). Text is UTF-8.
 *
 * A result is one election: the files of one election load as one result, the richest file
 * shown and the others checked against it (EeResults_LoadFiles). Its *rows* are Windows'
 * list rows: a row per contest and choice (and the contest's Over Votes and Under Votes when
 * the files give them), or, in the By Precinct view, those rows for each precinct in the
 * contest's district; primaries list the party chosen in Options first. Its *columns* are
 * Windows' columns for the view: Contest, Vote For, Choice, Party, (Precinct), a column per
 * vote type when Options asks for them, Total Votes and Percent. Rows, columns, cells and
 * sorting all run on the UI thread, as on Windows; only loading blocks.
 */
#pragma once

#include "ee_ui_voters.h" /* EeUiProgressFn, EE_UI_ZOOM_* */

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C"
{
#endif

    typedef struct EeUiResults EeUiResults;

    /** EeUiResults_Load's outcome. */
    enum
    {
        EE_UI_RESULTS_OK = 0,
        EE_UI_RESULTS_ERROR,     /* a message in err */
        EE_UI_RESULTS_CANCELLED, /* (no message: Windows shows none) */
    };

    /**
     * Load election results from @p n files (UTF-8 paths), as Windows' File > Load Election
     * Results... does: Clarity ZIPs and their files, ES&S, Hart and Dominion results reports,
     * and Election Explorer's results export. Blocks; call it off the UI thread. @p fn gets
     * the percent over the files (rows 0). On success @p out receives one result per election
     * (free each with EeUiResults_Free, the array with free()) and @p nout their number.
     */
    int EeUiResults_Load(const char *const *paths_utf8,
                         uint32_t n,
                         EeUiProgressFn fn,
                         void *user,
                         volatile int32_t *cancel,
                         EeUiResults ***out,
                         uint32_t *nout,
                         char *err,
                         size_t errcap);

    void EeUiResults_Free(EeUiResults *r);

    /** "<election> — Election Results" (the first file's name when the files name none). */
    const char *EeUiResults_WindowTitle(const EeUiResults *r);

    /** The first file loaded (the window's file). */
    const char *EeUiResults_Path(const EeUiResults *r);

    /** 1 when a contest has precinct detail: View > By Precinct is offered. */
    int EeUiResults_HasPrecinctDetail(const EeUiResults *r);

    /** 1 when a file other than the shown one disagrees with it: the Load Summary opens. */
    int EeUiResults_HasDifferences(const EeUiResults *r);

    /**
     * Reports > Display Load Summary...: the files, the one shown, and every note and
     * difference found when the others were checked against it (Windows' text; lines end in
     * "\n"). Owned by @p r.
     */
    const char *EeUiResults_LoadSummary(EeUiResults *r);

    /** Values for EeUiResultsOptions.party_first (settings.h's EE_TAB_PARTY_*). */
    enum
    {
        EE_UI_PARTY_REP = 0,
        EE_UI_PARTY_DEM = 1
    };

    /** Windows' Election Results Options. */
    typedef struct EeUiResultsOptions
    {
        int zoom_percent;    /* the list's text size (shared with the other windows) */
        int show_vote_types; /* a column for each vote type */
        int party_first;     /* EE_UI_PARTY_*: in a primary, these contests come first
                              * (shared with the CVR tabulation) */
    } EeUiResultsOptions;

    /** The options saved last (Windows' value names), or Windows' defaults (100%, vote types
     * shown, Republican first). */
    void EeUiResultsOptions_Load(EeUiResultsOptions *out);

    /** Save the options, keeping the other saved settings. Returns 0 when it fails. */
    int EeUiResultsOptions_Save(const EeUiResultsOptions *in);

    /**
     * Build the rows and columns for a view: Totals or By Precinct (when there is precinct
     * detail), with or without the vote-type columns, primaries' @p party_first contests
     * first. A sort whose column is still shown is kept (Windows' Res_Rebuild). A new result
     * shows Totals, the vote types and Republicans first until this is called. Returns 0
     * when out of memory (no rows then).
     */
    int EeUiResults_SetView(EeUiResults *r, int by_precinct, int show_vote_types, int party_first);

    int EeUiResults_ByPrecinct(const EeUiResults *r);
    int EeUiResults_ShowVoteTypes(const EeUiResults *r);
    int EeUiResults_PartyFirst(const EeUiResults *r);

    uint32_t EeUiResults_Rows(const EeUiResults *r);
    int EeUiResults_Columns(const EeUiResults *r);

    /** A column's heading ("Contest", ..., a vote type's name, "Total Votes", "Percent"). */
    const char *EeUiResults_ColumnTitle(const EeUiResults *r, int col);

    /** Windows' width of a column, in pixels at 100%. */
    int EeUiResults_ColumnWidth(const EeUiResults *r, int col);

    /** 1 for a count or percent column: right-aligned (but never the first column), and a
     * first heading click sorts it largest first. */
    int EeUiResults_ColumnNumeric(const EeUiResults *r, int col);

    /** One cell's text (UTF-8) into @p buf; returns @p buf. */
    const char *EeUiResults_Cell(const EeUiResults *r,
                                 uint32_t row,
                                 int col,
                                 char *buf,
                                 size_t cap);

    /**
     * Sort by @p col (-1: the order the rows had when loaded, Windows' Reset View), ascending
     * or not. Text sorts ignoring case, whole numbers as numbers (precinct 99 before 100);
     * counts and percents as numbers, a blank below 0; ties keep the loaded order.
     */
    void EeUiResults_Sort(EeUiResults *r, int col, int ascending);

    /** A heading click, as on Windows: the sorted column again turns the order around; a new
     * column sorts text A to Z and counts largest first. */
    void EeUiResults_ClickHeading(EeUiResults *r, int col);

    /** The sorted column (-1 for none) and its direction. */
    int EeUiResults_SortColumn(const EeUiResults *r, int *ascending);

    /** The status line, as on Windows: the election and date, contests, precincts, rows (By
     * Precinct), the files' formats, "as of" the files' time, and the differences or notes. */
    const char *EeUiResults_StatusText(EeUiResults *r);

    /**
     * Edit > Copy: the @p n rows given (shown positions, in that order), every column,
     * tab-separated, a line each ("\n"; the window puts them on the clipboard). Heap UTF-8
     * (free()), or NULL when out of memory.
     */
    char *EeUiResults_CopyText(const EeUiResults *r, const uint32_t *rows, uint32_t n);

    /**
     * The windows' `--selftest-results <files...>`: load the files, then print for each
     * election its title, status line, columns, cells, a sort, the By Precinct view, the view
     * without vote types, the other party first, Copy and the load summary, as the Election
     * Results window would show them. Returns 0, or 1 when the load fails.
     */
    int EeUiResults_SelfTest(const char *const *paths_utf8, uint32_t n);

#ifdef __cplusplus
}
#endif
