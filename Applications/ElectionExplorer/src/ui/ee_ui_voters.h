/**
 * @file ee_ui_voters.h
 * @brief A voter list for the macOS and Linux windows (AppKit, GTK), over the engine's
 *        EeVoterTable (voter_table.h) and filters (filter.h), in plain C types only.
 *
 * The windows' code never includes the engine's headers: they bring the stand-in
 * <windows.h>, whose BOOL clashes with Objective-C's. Text is UTF-8.
 *
 * Rows are *shown rows*: the rows that pass the filter (and, in a duplicates view, are
 * duplicates), in the current sort order. The order, the filter and the duplicates together
 * are a *view*. A new view is computed off the UI thread
 * (EeUiVoters_ComputeView: a sort of 1.5 million rows takes seconds) while the window keeps
 * showing the old one, then swapped in on the UI thread (EeUiVoters_SetView, which is cheap).
 * Only one ComputeView may run at a time, and SetView must not run while one does.
 *
 * Columns are *shown columns*: every column of a voter list; for a voter roster
 * (EeUiVoters_LoadRoster), Windows' roster columns (Voter ID, Precinct, Name, then Voting
 * Method, Date Voted and the roster's other columns, not the VUID and name parts behind them).
 * Rules, sorting and titles all use them.
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

    /** What a view shows besides the filter: all rows, or only duplicates (Windows' Filter menu). */
    enum
    {
        EE_UI_MARKS_NONE = 0,
        EE_UI_MARKS_DUP_IDS,    /* rows sharing a Voter ID with another row */
        EE_UI_MARKS_DUP_VOTERS, /* rows sharing first, middle and last name and birth date */
        EE_UI_MARKS_DUP_VOTING, /* those of them who voted (in a roster) under different IDs */
        EE_UI_MARKS_COMPARE,    /* a compare's category (Show these rows in ...) */
        EE_UI_MARKS_COUNT
    };

    /** The normalized columns the windows use: the duplicates views sort by Voter ID or Name
     * (as on Windows), and an Address cell has Show in Maps... */
    enum
    {
        EE_UI_COLUMN_VOTER_ID = 0,
        EE_UI_COLUMN_NAME = 2,
        EE_UI_COLUMN_ADDRESS = 3
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
     * Returns NULL with a message in @p err (UTF-8) on failure or cancel. @p surname_first:
     * the Name column as "Last, First Middle" (the Options default), else "First Middle Last".
     */
    EeUiVoters *EeUiVoters_Load(const char *path_utf8,
                                int surname_first,
                                EeUiProgressFn fn,
                                void *user,
                                volatile int32_t *cancel,
                                char *err,
                                size_t errcap);

    /**
     * Load a voter roster from @p n files, as Windows' File > Load Voter Roster... does
     * (EeRoster_LoadFiles: ZIPs of workbooks, .xlsx, .xls, .csv, .tsv, .txt, .pdf; corrections
     * replace their originals). Blocks; call it off the UI thread. Returns NULL with a message
     * in @p err on failure or cancel.
     */
    EeUiVoters *EeUiVoters_LoadRoster(const char *const *paths_utf8,
                                      uint32_t n,
                                      int surname_first,
                                      EeUiProgressFn fn,
                                      void *user,
                                      volatile int32_t *cancel,
                                      char *err,
                                      size_t errcap);

    /** 1 for a voter roster: Windows' Voter Roster window, with its own menus and words. */
    int EeUiVoters_IsRoster(const EeUiVoters *v);

    /**
     * A roster's load summary (Reports > Display Load Summary...: what was read, per voting
     * method, and what was skipped; UTF-8, lines ending in "\n"), or NULL. @p notable (optional)
     * is 1 when it says more than the counts: Windows opens it after loading then.
     */
    const char *EeUiVoters_LoadSummary(const EeUiVoters *v, int *notable);

    /** The shown Address column (Show in Maps..., the Address report), or -1 (a roster). */
    int EeUiVoters_AddressColumn(const EeUiVoters *v);

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

    /** The shown view's duplicates (EE_UI_MARKS_*), and how many rows are duplicates. */
    int EeUiVoters_Marks(const EeUiVoters *v, uint32_t *marked);

    /** 1 when the list has a birth date column (Show Duplicate Voters needs one). */
    int EeUiVoters_HasBirthDate(const EeUiVoters *v);

    /** For EE_UI_MARKS_*: "duplicate Voter IDs", ... and "No duplicate Voter IDs.", ... (the
     * status line of a Duplicate Voters Voting view also names its roster) */
    const char *EeUiVoters_MarksLabel(int marks);
    const char *EeUiVoters_MarksNoneText(int marks);

    /**
     * The status line's count, in the Windows words: "5 voters", "Filtered: 3 of 5 voters",
     * "Showing 4 duplicate Voter IDs of 8 voters", "duplicate Voter IDs within filter: 2 of 8
     * voters"; a roster counts "voter records".
     */
    void EeUiVoters_CountText(const EeUiVoters *v, char *out, size_t cap);

    /**
     * Compute a view: the order by @p sort_column (-1 for file order), the filter @p rules and
     * the duplicates @p marks (EE_UI_MARKS_*: those of the shown view are reused, others are
     * found). Any thread; reads the list but changes nothing. Returns NULL when out of memory.
     */
    EeUiView *EeUiVoters_ComputeView(const EeUiVoters *v,
                                     int sort_column,
                                     int ascending,
                                     const EeUiRule *rules,
                                     uint32_t nrules,
                                     int marks);

    /** Duplicates found in advance (Show Duplicate Voters Voting's), with their status words. */
    typedef struct EeUiMarks EeUiMarks;

    /**
     * Show Duplicate Voters Voting (Windows' App_ShowDuplicateVotersVoting): the rows of voter
     * list @p list whose first, middle and last name and birth date match another voter's, and
     * who voted (their Voter ID is in @p roster) under two or more different Voter IDs.
     * @p roster_name (its first file's name) goes in the status line. UI thread (it reads the
     * roster, which its window may free). Returns NULL with @p msg in Windows' words when there
     * is nothing to show: no list, no birth dates, no roster, none found or out of memory.
     */
    EeUiMarks *EeUiVoters_DuplicatesVoting(const EeUiVoters *list,
                                           const EeUiVoters *roster,
                                           const char *roster_name,
                                           char *msg,
                                           size_t cap);

    /** How many rows @p m marks. */
    uint32_t EeUiMarks_Count(const EeUiMarks *m);

    void EeUiMarks_Free(EeUiMarks *m);

    /**
     * EeUiVoters_ComputeView with rows found in advance: with @p marks EE_UI_MARKS_DUP_VOTING
     * or EE_UI_MARKS_COMPARE, @p given (read, not taken) is the view's rows. Without it the
     * shown view's are reused, as for the other kinds.
     */
    EeUiView *EeUiVoters_ComputeViewWith(const EeUiVoters *v,
                                         int sort_column,
                                         int ascending,
                                         const EeUiRule *rules,
                                         uint32_t nrules,
                                         int marks,
                                         const EeUiMarks *given);

    /**
     * A computed view's duplicates (EE_UI_MARKS_*) and how many rows are duplicates. When a
     * duplicates view found none, Windows says so (EeUiVoters_MarksNoneText) and keeps the
     * shown view.
     */
    int EeUiView_Marks(const EeUiView *view, uint32_t *marked);

    /** Show @p view (UI thread; takes ownership). */
    void EeUiVoters_SetView(EeUiVoters *v, EeUiView *view);

    /** Free a view that was not shown. */
    void EeUiView_Free(EeUiView *view);

    /**
     * Shown rows @p rows (in that order) as text for the clipboard, as Windows' Copy: a line a
     * row, fields separated as in the list's file (commas for a CSV, tabs for a TSV).
     * @p normalized: with the Voter ID, Precinct, Name and Address columns first (Windows'
     * default; never for a roster, as on Windows). Returns heap UTF-8 (free()), or NULL when out
     * of memory.
     */
    char *EeUiVoters_CopyText(const EeUiVoters *v,
                              const uint32_t *rows,
                              uint32_t n,
                              int normalized,
                              size_t *len);

    /**
     * Export shown rows @p rows (NULL: all shown rows, in order) to @p path_utf8 as Windows'
     * Export does: UTF-8 with a byte order mark, a heading row, @p delim ',' (CSV) or '	'
     * (TSV). Returns the rows written, or -1 with a message in @p err.
     */
    long EeUiVoters_Export(const EeUiVoters *v,
                           const char *path_utf8,
                           const uint32_t *rows,
                           uint32_t n,
                           int normalized,
                           char delim,
                           char *err,
                           size_t errcap);

    /**
     * Windows' suggested export name for the list at @p list_path: "<name>-All_Voters.csv",
     * "-Filtered_Voters" (filtered or a duplicates view) or "-Selected_Voters"; for a roster
     * "-Roster", "-Filtered_Roster" or "-Selected_Roster".
     */
    void EeUiVoters_ExportName(const EeUiVoters *v,
                               const char *list_path,
                               int selection,
                               char *out,
                               size_t cap);

    /* ---------------------------------------------------------------------- */
    /* Saved filters (Windows' Export Filter... / Load Filter... in the Filter */
    /* window): the rules in a UTF-8 CSV or TSV that names columns by title    */
    /* ---------------------------------------------------------------------- */

    /** Windows' suggested name: "<list name> Filter.filter.csv" ("Voter List" without a list). */
    void EeUiFilterFile_Name(const char *list_path, char *out, size_t cap);

    /**
     * The file a chosen name saves to, as on Windows: a name ending in .csv or .tsv is kept and
     * decides the format; another gets the chosen type's ".filter.csv" (@p type 0) or
     * ".filter.tsv" (1). Returns the field separator (',' or '\t'), or 0 when @p out is too
     * small. When @p out differs from @p chosen, Windows asks before replacing that file.
     */
    char EeUiFilterFile_Path(const char *chosen, int type, char *out, size_t cap);

    /**
     * Save @p rules (all of them, as listed in the Filter window, on or off) to @p path_utf8.
     * Returns 1, or 0 with Windows' message in @p err ("There are no filter rules to export."
     * when @p n is 0).
     */
    int EeUiVoters_SaveFilter(const EeUiVoters *v,
                              const char *path_utf8,
                              const EeUiRule *rules,
                              uint32_t n,
                              char delim,
                              char *err,
                              size_t errcap);

    /**
     * Load a saved filter for this list: columns matched by title, as on Windows. Returns:
     * - -1: the file could not be read or is not a saved filter (an error, in @p msg);
     * - 0: it holds no rules ("The file holds no filter rules.", in @p msg);
     * - 1: @p *rules (malloc(), free() it; NULL when every rule was skipped) and @p *n replace
     *   the Filter window's rules. When some could not be used, @p msg says how many ("Loaded 2
     *   rules. 1 rule could not be used here:") and @p detail lists them, a line each;
     *   otherwise both are empty.
     */
    int EeUiVoters_LoadFilter(const EeUiVoters *v,
                              const char *path_utf8,
                              EeUiRule **rules,
                              uint32_t *n,
                              char *msg,
                              size_t msgcap,
                              char *detail,
                              size_t detailcap);

    /* ---------------------------------------------------------------------- */
    /* Reports (Windows' Reports menu): a row per distinct Precinct or Address */
    /* ---------------------------------------------------------------------- */

    typedef struct EeUiReport EeUiReport;

    enum
    {
        EE_UI_REPORT_PRECINCT = 0,
        EE_UI_REPORT_ADDRESS,
        EE_UI_REPORT_COUNT
    };

    /** A report's two columns: the value, then "Number of Voters". */
    enum
    {
        EE_UI_REPORT_COLUMN_VALUE = 0,
        EE_UI_REPORT_COLUMN_COUNT = 1
    };

    /**
     * Display Precinct / Address Report, as on Windows: each distinct value (case-insensitive;
     * shown as first spelled) with its number of voters, over ALL loaded rows (the filter and a
     * duplicates view are left out). Empty cells make a "(blank)" row when other values exist.
     * Sorted by the value, ascending (precincts as numbers). Returns NULL with the Windows
     * message in @p err when the list has no values ("No precinct information available") or
     * memory runs out. The report keeps its own copy: it outlives the list.
     */
    EeUiReport *EeUiVoters_Report(const EeUiVoters *v, int kind, char *err, size_t errcap);

    void EeUiReport_Free(EeUiReport *r);

    int EeUiReport_Kind(const EeUiReport *r);

    /** "Precinct" or "Address": the first column's heading. */
    const char *EeUiReport_Heading(int kind);

    uint32_t EeUiReport_Rows(const EeUiReport *r);

    /** Row @p row's value as shown and copied ("(blank)" for empty cells; never NULL). */
    const char *EeUiReport_Value(const EeUiReport *r, uint32_t row);

    /** 1 for the "(blank)" row. */
    int EeUiReport_IsBlank(const EeUiReport *r, uint32_t row);

    uint32_t EeUiReport_Count(const EeUiReport *r, uint32_t row);

    /**
     * Include / Exclude on a report row, as on Windows: the rule "<the list's Precinct or
     * Address column> is <value>" ("" for the blank row, matching the empty cells).
     */
    void EeUiReport_Rule(const EeUiReport *r, uint32_t row, int exclude, EeUiRule *out);

    /** Sort by EE_UI_REPORT_COLUMN_* (a click on a heading: the same one again reverses it). */
    void EeUiReport_Sort(EeUiReport *r, int column, int ascending);
    int EeUiReport_SortColumn(const EeUiReport *r, int *ascending);

    /** The status line: "308 unique precincts", "... (includes blank)". */
    void EeUiReport_StatusText(const EeUiReport *r, char *out, size_t cap);

    /** The window's title: "Precinct Report - <file name>". */
    void EeUiReport_WindowTitle(const EeUiReport *r, const char *list_path, char *out, size_t cap);

    /** Rows @p rows as Copy puts them on the clipboard: "<value>\t<count>\r\n" each. */
    char *EeUiReport_CopyText(const EeUiReport *r, const uint32_t *rows, uint32_t n, size_t *len);

    /**
     * Export rows @p rows (NULL: all, in the shown order) as Windows does: UTF-8 with a byte
     * order mark, the headings "Precinct" (or "Address") and "Number of Voters". Returns the
     * rows written, or -1 with a message in @p err.
     */
    long EeUiReport_Export(const EeUiReport *r,
                           const char *path_utf8,
                           const uint32_t *rows,
                           uint32_t n,
                           char delim,
                           char *err,
                           size_t errcap);

    /** "<list name>-All_Precincts.csv", "-Selected_Addresses.csv", ... */
    void EeUiReport_ExportName(const EeUiReport *r,
                               const char *list_path,
                               int selection,
                               char *out,
                               size_t cap);

    /**
     * Show in Maps (Address report): a search link for row @p row in map @p engine (EE_UI_MAP_*,
     * the Options' Map engine). Heap UTF-8 (free()), or NULL for the blank row.
     */
    char *EeUiReport_MapUrl(const EeUiReport *r, uint32_t row, int engine);

    /* ---------------------------------------------------------------------- */
    /* Voting Totals (a roster's Reports > Display Voting Totals...)            */
    /* ---------------------------------------------------------------------- */

    typedef struct EeUiTotals EeUiTotals;

    /**
     * Display Voting Totals..., as on Windows (EeRoster_ComputeTotals): voters per Date Voted
     * (and Party, when the roster has one) and Voting Method over ALL loaded records (the
     * filter is left out), each Voter ID counted once, on its earliest record. A row per date
     * (per party, with an "All parties" row when several voted that day), then the Total rows.
     * Columns: Date Voted, Party (only with parties), Mail Ballot, Early Vote In-Person,
     * Election Day In-Person, Provisional, Limited, Other (only for other Voting Method values),
     * Total. A method with no file reads "not available"; one whose files could not be read
     * (PDF) "not loaded". Not sortable, as on Windows. Returns NULL with Windows' message in
     * @p err when the list is not a roster or memory runs out. Keeps its own copy.
     */
    EeUiTotals *EeUiVoters_Totals(const EeUiVoters *v, char *err, size_t errcap);

    void EeUiTotals_Free(EeUiTotals *t);

    uint32_t EeUiTotals_Rows(const EeUiTotals *t);
    uint32_t EeUiTotals_Columns(const EeUiTotals *t);
    const char *EeUiTotals_Title(const EeUiTotals *t, uint32_t c);

    /** 1 for Date Voted and Party (shown at the left); the counts are at the right. */
    int EeUiTotals_IsTextColumn(const EeUiTotals *t, uint32_t c);

    /** Windows' column width at 100% (pixels). */
    int EeUiTotals_ColumnWidth(const EeUiTotals *t, uint32_t c);

    /**
     * The cell at row @p row, column @p c: "11/03/2026", "(no date)", "Total", a party,
     * "(blank)", "All parties", a count ("1,234" when @p display, "1234" for Copy and Export),
     * "not available" or "not loaded".
     */
    void EeUiTotals_Cell(const EeUiTotals *t,
                         uint32_t row,
                         uint32_t c,
                         int display,
                         char *out,
                         size_t cap);

    /** 1 for a Total row and, with parties, an "All parties" row (Windows draws them bold). */
    int EeUiTotals_IsBold(const EeUiTotals *t, uint32_t row);

    /**
     * The status line: "11 voters, each counted once (1 later record of 1 Voter ID not
     * counted) — right-click to copy or export".
     */
    void EeUiTotals_StatusText(const EeUiTotals *t, char *out, size_t cap);

    /** "Voting Totals - <file name>". */
    void EeUiTotals_WindowTitle(const EeUiTotals *t, const char *list_path, char *out, size_t cap);

    /** Copy: a heading line, then rows @p rows, tab-separated, plain numbers, "\r\n" lines. */
    char *EeUiTotals_CopyText(const EeUiTotals *t, const uint32_t *rows, uint32_t n, size_t *len);

    /**
     * Export rows @p rows (NULL: all) as Windows does: UTF-8 with a byte order mark, the
     * headings, plain numbers. Returns the rows written, or -1 with a message in @p err.
     */
    long EeUiTotals_Export(const EeUiTotals *t,
                           const char *path_utf8,
                           const uint32_t *rows,
                           uint32_t n,
                           char delim,
                           char *err,
                           size_t errcap);

    /** "<list name>-Voting_Totals.csv", "-Selected_Voting_Totals.csv". */
    void EeUiTotals_ExportName(const EeUiTotals *t,
                               const char *list_path,
                               int selection,
                               char *out,
                               size_t cap);

    /* ---------------------------------------------------------------------- */
    /* Compare (Windows' Compare menu and Compare Summary window)              */
    /* ---------------------------------------------------------------------- */

    /** Two files matched by Voter ID, and the counts of each category on each side. */
    typedef struct EeUiCompare EeUiCompare;

    /**
     * The Compare menu's item for another window (Windows' App_BuildCompareMenu): "Compare
     * with <file>", naming the kind when it is a roster or differs from this window's
     * ("(voter roster)", "(voter list)"). With @p other_path NULL, the grayed item Windows
     * shows when there is no other window.
     */
    void EeUiCompare_MenuLabel(const char *other_path,
                               int other_roster,
                               int this_roster,
                               char *out,
                               size_t cap);

    /**
     * Compare list or roster @p a (this window's) with @p b (Windows' App_StartCompare): rows
     * matched by Voter ID, with what Windows compares for the pair (two lists: Name, Address,
     * Precinct; with a roster: no Address, first + last names; two rosters: also Voting
     * Method and Date Voted). @p path_a and @p path_b are the files (a roster's first).
     * Any thread; reads both lists and changes nothing; non-zero @p *cancel stops it. Returns
     * NULL with @p err in Windows' words, or "" when cancelled.
     */
    EeUiCompare *EeUiVoters_Compare(const EeUiVoters *a,
                                    const EeUiVoters *b,
                                    const char *path_a,
                                    const char *path_b,
                                    volatile int32_t *cancel,
                                    char *err,
                                    size_t errcap);

    void EeUiCompare_Free(EeUiCompare *c);

    /** The summary's rows (the categories that apply to the pair) and their words. */
    uint32_t EeUiCompare_Rows(const EeUiCompare *c);
    const char *EeUiCompare_Category(const EeUiCompare *c, uint32_t row);

    /** A row's count in file A (@p side 0) or B (1). */
    uint32_t EeUiCompare_Count(const EeUiCompare *c, uint32_t row, int side);

    /** File A's or B's name, for the headings and menu ("file A" / "file B" without one). */
    const char *EeUiCompare_Name(const EeUiCompare *c, int side);

    /** "Compare - <A> vs <B>". */
    void EeUiCompare_WindowTitle(const EeUiCompare *c, char *out, size_t cap);

    /** Windows' status line: "Matched by Voter ID. Right-click a row to show it in a file." */
    const char *EeUiCompare_StatusText(void);

    /**
     * Show these rows in <file> (Windows' App_ShowCompareCategory): the rows of @p row's
     * category in side @p side's list @p v, for EeUiVoters_ComputeViewWith with
     * EE_UI_MARKS_COMPARE (grouped by Voter ID), with Windows' status words ("voters only
     * here vs <other file>"). NULL with @p msg when there are none ("No voters in that
     * category."), when the file changed since ("... Run Compare again.") or out of memory.
     */
    EeUiMarks *EeUiCompare_Marks(const EeUiCompare *c,
                                 uint32_t row,
                                 int side,
                                 const EeUiVoters *v,
                                 char *msg,
                                 size_t cap);

    /**
     * Export Summary... (Category, <A>, <B>) and Export All Records... (every record of both
     * files: File, Voter ID, Precinct, Name, Address for two lists, Voting Method and Date
     * Voted with a roster, Result, Changed Fields), as Windows writes them: UTF-8 with a BOM,
     * @p delim ',' or '\t'. Records need the two lists compared (unchanged since: else "A file
     * changed since the comparison was run. Run Compare again."). Returns the rows written,
     * or -1 with @p err.
     */
    long EeUiCompare_ExportSummary(const EeUiCompare *c,
                                   const char *path_utf8,
                                   char delim,
                                   char *err,
                                   size_t errcap);
    long EeUiCompare_ExportRecords(const EeUiCompare *c,
                                   const EeUiVoters *a,
                                   const EeUiVoters *b,
                                   const char *path_utf8,
                                   char delim,
                                   char *err,
                                   size_t errcap);

    /** "<A>-vs-<B>-Compare_Summary.csv" (@p records 0) or "-Compare_Records.csv". */
    void EeUiCompare_ExportName(const EeUiCompare *c, int records, char *out, size_t cap);

    /** Show Differences...: a row per differing field of each voter matched in both files. */
    typedef struct EeUiDiff EeUiDiff;

    /**
     * Show Differences... (Windows' App_ShowDifferences): for compare @p c of lists @p a and
     * @p b (unchanged since), a row per field that differs for a voter in both files (Name,
     * Address, Precinct, Voting Method, Date Voted, as compared), in the files' order. The
     * rows read the two lists' cells, so they must outlive it (Windows closes the window
     * with the Compare window, and it with either file's). UI thread. NULL with @p err in
     * Windows' words: "A file changed since the comparison was run. Run Compare again.",
     * "Could not collect the differences.", "No differing voters found."
     */
    EeUiDiff *EeUiCompare_Differences(const EeUiCompare *c,
                                      const EeUiVoters *a,
                                      const EeUiVoters *b,
                                      char *err,
                                      size_t errcap);

    void EeUiDiff_Free(EeUiDiff *d);

    /** The rows, and a row's cell: 0 Voter ID, 1 Field, 2 the value in A, 3 the value in B. */
    uint32_t EeUiDiff_Rows(const EeUiDiff *d);
    const char *EeUiDiff_Cell(const EeUiDiff *d, uint32_t row, uint32_t column);

    /** The headings ("Voter ID", "Field", A's and B's file names, "file A" / "file B" without
     * one) and Windows' widths at 100% (pixels). */
    const char *EeUiDiff_Title(const EeUiDiff *d, uint32_t column);
    int EeUiDiff_ColumnWidth(uint32_t column);

    /** "Differences - <A> vs <B>". */
    void EeUiDiff_WindowTitle(const EeUiDiff *d, char *out, size_t cap);

    /** "12 differing field(s) — click a header to sort, Ctrl+C to copy" (Windows' words; the
     * Mac's say Cmd-C). */
    void EeUiDiff_StatusText(const EeUiDiff *d, int mac, char *out, size_t cap);

    /**
     * A heading click, as on Windows (Diff_Sort): by @p column ascending, or the other way when
     * it is already the sort column. Voter IDs of digits sort as numbers; the others ignore
     * case; ties go by Voter ID, so a voter's fields stay together.
     */
    void EeUiDiff_Sort(EeUiDiff *d, uint32_t column);

    /** The sort column (-1: the files' order), and whether it is ascending. */
    int EeUiDiff_SortColumn(const EeUiDiff *d, int *ascending);

    /** Copy: the rows @p rows, tab-separated (Voter ID, Field, A, B), no heading, as on
     * Windows. NULL when there are none or out of memory; free() it. */
    char *EeUiDiff_CopyText(const EeUiDiff *d, const uint32_t *rows, uint32_t n, size_t *len);

    /**
     * Export Selected... (@p rows) or Export All... (@p rows NULL), in the order shown, with
     * the headings, UTF-8 with a BOM, @p delim ',' or '\t', as on Windows. Returns the rows
     * written, or -1 with @p err.
     */
    long EeUiDiff_Export(const EeUiDiff *d,
                         const char *path_utf8,
                         const uint32_t *rows,
                         uint32_t n,
                         char delim,
                         char *err,
                         size_t errcap);

    /** "<A>-vs-<B>-Differences.csv", "-Selected_Differences.csv". */
    void EeUiDiff_ExportName(const EeUiDiff *d, int selection, char *out, size_t cap);

    /* ---------------------------------------------------------------------- */
    /* Options (Windows' Edit > Options...), kept between runs                 */
    /* ---------------------------------------------------------------------- */

    /** Map engines, in the Options' order (Windows' EeMapEngine values, as saved). */
    enum
    {
        EE_UI_MAP_GOOGLE = 0,
        EE_UI_MAP_BING,
        EE_UI_MAP_APPLE,
        EE_UI_MAP_OPENSTREETMAP,
        EE_UI_MAP_COUNT
    };

#define EE_UI_ZOOM_MIN     50
#define EE_UI_ZOOM_MAX     250
#define EE_UI_ZOOM_DEFAULT 100

    /** The voter list's options, as in Windows' Options window. */
    typedef struct EeUiOptions
    {
        int copy_normalized; /* Copy puts the Voter ID, Precinct, Name and Address first */
        int surname_first;   /* the Name column as "Last, First Middle" */
        int zoom_percent;    /* the list's text size, EE_UI_ZOOM_MIN..MAX */
        int map_engine;      /* EE_UI_MAP_*: what Show in Maps... opens */
    } EeUiOptions;

    /** Windows' defaults: copy normalized, surname first, 100%, Google Maps. */
    void EeUiOptions_Defaults(EeUiOptions *out);

    /**
     * The options saved last (Windows' value names, in a settings file: see settings_posix.c),
     * or the defaults for those never saved; out-of-range values are brought into range.
     */
    void EeUiOptions_Load(EeUiOptions *out);

    /** Save the options, keeping the other saved settings. Returns 0 when it fails. */
    int EeUiOptions_Save(const EeUiOptions *in);

    /** @p zoom brought into EE_UI_ZOOM_MIN..MAX. */
    int EeUiOptions_ClampZoom(int zoom);

    /** "Google Maps", "Bing Maps", "Apple Maps", "Open Street Maps" (Windows' names). */
    const char *EeUiOptions_MapName(int engine);

    /**
     * Show in Maps...: the search link for @p address (UTF-8) in map @p engine, as on Windows.
     * Heap UTF-8 (free()), or NULL for an empty address.
     */
    char *EeUiOptions_MapUrl(int engine, const char *address);

    /**
     * Open link...: the first "https://" address (any case) in @p text, a cell of shown column
     * @p column, as on Windows (App_ExtractHttpsUrl): in a voter list only in the file's own
     * columns (not Voter ID, Precinct, Name or Address), in a roster in any column (@p v NULL:
     * a voter list's column); it ends at a space, tab, line break, < > " ' ( or ), without a
     * trailing . , ; : or ]; at most 2047 bytes. Heap UTF-8 (free()), or NULL when the cell has
     * no link.
     */
    char *EeUiVoters_CellLink(const EeUiVoters *v, uint32_t column, const char *text);

    /**
     * Rebuild the Name column surname first or given name first (on the UI thread, while no
     * EeUiVoters_ComputeView runs: it rewrites the cells). Returns 1 when the names changed and
     * the shown view used them (sorted by Name, or a rule on it: ask for a new view), 0 when
     * nothing more is needed, -1 when memory runs out.
     */
    int EeUiVoters_SetSurnameFirst(EeUiVoters *v, int surname_first);

    /** 1 when the Name column is surname first. */
    int EeUiVoters_SurnameFirst(const EeUiVoters *v);

    /** Sort (on the calling thread) by column @p c; sorting the same column again reverses it. */
    void EeUiVoters_Sort(EeUiVoters *v, uint32_t c);

    /** Sort (on the calling thread) by column @p c in the given direction. */
    void EeUiVoters_SortTo(EeUiVoters *v, uint32_t c, int ascending);

    /**
     * The windows' `--selftest <file>`: load @p path, sort it by column 1 both ways, filter it,
     * copy rows, look for duplicates, make the Precinct and Address reports and try the Options
     * (the names both ways, Copy without the normalized fields, the map links; the default
     * options, no settings file), and print (stdout) what the window would show: the row and
     * column counts, the first column titles, the first rows' column-1 cells and counts. Returns
     * 0, or 1 when the load fails.
     */
    int EeUiVoters_SelfTest(const char *path_utf8);

    /**
     * The windows' `--selftest-roster <files...>`: load the roster, then print its columns, the
     * status line, a sort, a filter on Voting Method, Copy, the duplicate Voter IDs, the export
     * names, the Precinct report, a saved filter and the load summary, as the Voter Roster
     * window would show them. Returns 0, or 1 when the load fails.
     */
    int EeUiVoters_RosterSelfTest(const char *const *paths_utf8, uint32_t n);

    /**
     * The windows' `--selftest-voting <list> <roster files...>`: load the voter list and the
     * roster, then print Show Duplicate Voters Voting as the voter list window would show it:
     * the status line and the duplicates' Voter IDs (no names), then a sort and a filter that
     * keep them, and the export name. Returns 0, or 1 when a load fails.
     */
    int EeUiVoters_VotingSelfTest(const char *list_utf8,
                                  const char *const *roster_utf8,
                                  uint32_t n);

    /**
     * The windows' `--selftest-compare list|roster <file A> list|roster <file B>`: load the two
     * files, then print the Compare menu's item, the summary window (title, rows, counts),
     * Show these rows in each file, the exports and Show Differences... (its rows, sorted,
     * copied and exported), as the windows would show them. With @p cells 0 it prints counts
     * and Voter IDs only (a real file's cells are voters' data). Returns 0, or 1 when a load or the compare fails.
     */
    int EeUiVoters_CompareSelfTest(int roster_a,
                                   const char *a_utf8,
                                   int roster_b,
                                   const char *b_utf8,
                                   int cells);

#ifdef __cplusplus
}
#endif
