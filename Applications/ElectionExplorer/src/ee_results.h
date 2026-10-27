/**
 * @file ee_results.h
 * @brief Election results: contest × choice totals, optionally broken down by precinct
 *        and vote type, loaded from Clarity Elections exports (summary.csv, detail.txt,
 *        detail.xls / detail.xlsx, detail.xml, or their ZIPs) and from Election
 *        Explorer's own results export.
 *
 * Several files of one election are loaded together: the richest file supplies the data
 * (XML > XLS/XLSX > TXT > summary) and every other file is cross-checked against it,
 * with each disagreement recorded as an issue. Files of different elections are grouped
 * into separate results.
 *
 * See docs/election-results-design.md.
 */
#pragma once

#include <windows.h>
#include <stdint.h>

#include "voter_table.h" /* EeLoadStatus, EeLoadProgressFn */

#ifdef __cplusplus
extern "C"
{
#endif

/** Source formats (bit flags in EeResults.formats). */
#define EE_RES_FMT_CLARITY_SUMMARY 0x01u /* summary.csv */
#define EE_RES_FMT_CLARITY_TXT     0x02u /* detail.txt */
#define EE_RES_FMT_CLARITY_XLS     0x04u /* detail.xls / detail.xlsx */
#define EE_RES_FMT_CLARITY_XML     0x08u /* detail.xml */
#define EE_RES_FMT_EE_EXPORT       0x10u /* Election Explorer results export (CSV/TSV) */
#define EE_RES_FMT_EE_VIEW         0x20u /* Election Explorer displayed-view export */

/** Unknown count (registered voters, ballots cast, precinct counts). */
#define EE_RES_UNKNOWN (-1)

/** The vote-type dimension of the count arrays: nvt, or 1 when the source reports
 *  precinct detail without a vote-type breakdown. */
#define EE_RESULTS_NVT(r) ((r)->nvt != 0 ? (r)->nvt : 1u)

    typedef struct EeResultsPrecinct
    {
        wchar_t *name;
        int64_t registered; /* EE_RES_UNKNOWN when not reported */
        int64_t ballots;    /* EE_RES_UNKNOWN when not reported */
    } EeResultsPrecinct;

    typedef struct EeResultsChoice
    {
        wchar_t *name;
        wchar_t *party; /* NULL when not reported */
        int64_t total;  /* reported total over all precincts and vote types, or
                         * EE_RES_UNKNOWN when every loaded file withholds it */
    } EeResultsChoice;

    typedef struct EeResultsContest
    {
        wchar_t *title;        /* without the "(Vote For N)" / "( N)" suffix */
        uint32_t vote_for;     /* 0 when not reported */
        int8_t is_question;    /* 1 yes, 0 no, -1 not reported */
        wchar_t *party;        /* primary party (DEM, REP, …) from the title prefix, or NULL */
        uint32_t first_choice; /* index of the first choice in EeResults.choices */
        uint32_t nchoices;

        /* Precinct detail (NULL / 0 for a summary-only contest). */
        uint32_t npre;            /* precincts in the contest's district */
        uint32_t *pre;            /* npre indexes into EeResults.precincts */
        int32_t *counts;          /* [nchoices][npre][EE_RESULTS_NVT] votes */
        uint32_t nprotected;      /* cells the source withheld ("protected"); counted as 0 */
        uint8_t *protected_cells; /* same shape as counts, 1 = withheld; NULL when none */

        /* Over/under votes, when reported. Per precinct when the source has them. */
        BOOL has_over_under;
        int64_t over_total;
        int64_t under_total;
        int32_t *over;  /* npre, or NULL */
        int32_t *under; /* npre, or NULL */

        /* Contest-level figures from the summary (EE_RES_UNKNOWN when absent). */
        int64_t registered;
        int64_t ballots;
        int32_t precincts_total;
        int32_t precincts_reporting;
    } EeResultsContest;

    /** One file (or ZIP entry) that went into a result. */
    typedef struct EeResultsSource
    {
        wchar_t *path;   /* the selected file */
        wchar_t *member; /* the ZIP entry name, or NULL */
        uint32_t format; /* one EE_RES_FMT_* */
        BOOL primary;    /* TRUE for the file whose data is shown */
        uint32_t ncontests;
        uint32_t nchoices;
        uint32_t nprecincts;
    } EeResultsSource;

    /** A cross-check disagreement or other loader note. */
    typedef struct EeResultsIssue
    {
        uint32_t source; /* index into EeResults.sources */
        wchar_t *text;
    } EeResultsIssue;

    typedef struct EeResults
    {
        wchar_t *election_name; /* NULL when the files do not say */
        wchar_t *election_date;
        wchar_t *region;
        wchar_t *timestamp;
        uint32_t formats;     /* EE_RES_FMT_* of every source */

        wchar_t **vote_types; /* nvt names, in file order */
        uint32_t nvt;

        EeResultsPrecinct *precincts;
        uint32_t nprecincts;
        int32_t *prec_ballots_vt; /* [nprecincts][nvt] ballots cast by vote type, or NULL */
        int64_t *ballots_vt;      /* nvt ballots cast by vote type (all precincts), or NULL */

        int64_t registered_total; /* EE_RES_UNKNOWN when not reported */
        int64_t ballots_total;

        EeResultsContest *contests;
        uint32_t ncontests;
        EeResultsChoice *choices;
        uint32_t nchoices;

        EeResultsSource *sources;
        uint32_t nsources;
        EeResultsIssue *issues;
        uint32_t nissues;
    } EeResults;

    /** Initialize an empty result. */
    void EeResults_Init(EeResults *r);

    /** Release all memory and reset to empty. */
    void EeResults_Clear(EeResults *r);

    /** Free an array returned by EeResults_LoadFiles (each element is cleared). */
    void EeResults_FreeArray(EeResults *arr, uint32_t count);

    /** TRUE when at least one contest has precinct detail. */
    BOOL EeResults_HasPrecinctDetail(const EeResults *r);

    /**
     * Load one or more results files. A ZIP contributes each of its entries. Files are
     * grouped into elections: files with the same contest list belong together, except
     * that a group holds one file of each format (a second summary.csv of the same
     * election, such as an earlier snapshot, starts another group). Within a group the
     * richest file supplies the data and the others are cross-checked against it.
     *
     * @param paths         Array of @p count wide file paths.
     * @param count         Number of files (>= 1).
     * @param out           Receives a malloc'd array of results, one per election
     *                      (free with EeResults_FreeArray). NULL on failure.
     * @param out_count     Receives the number of results.
     * @param cancel_flag   Optional; non-zero requests cancel.
     * @param progress_fn   Optional progress callback (percent over the files).
     * @param progress_user Passed to @p progress_fn.
     * @param error_message Optional; receives a message naming the file on failure.
     * @param error_cch     Capacity of @p error_message (incl. NUL).
     * @return EeLoadStatus_Ok, _Cancelled, or _Error (unreadable or unrecognized file).
     */
    EeLoadStatus EeResults_LoadFiles(const wchar_t *const *paths,
                                     uint32_t count,
                                     EeResults **out,
                                     uint32_t *out_count,
                                     volatile LONG *cancel_flag,
                                     EeLoadProgressFn progress_fn,
                                     void *progress_user,
                                     wchar_t *error_message,
                                     size_t error_cch);

    /**
     * Total each choice over the included precincts and vote types.
     *
     * @param r             The results.
     * @param precinct_mask nprecincts flags (non-zero = included), or NULL for all.
     * @param vt_mask       nvt flags (non-zero = included), or NULL for all.
     * @param choice_totals Receives nchoices totals.
     * @param choice_vt     Optional; receives [nchoices][nvt] totals by vote type.
     * @param over          Optional; receives ncontests over-vote totals (0 if unreported).
     * @param under         Optional; receives ncontests under-vote totals.
     * @return TRUE when every total is exact. FALSE when a mask excludes something but a
     *         contest has no precinct (or vote type) detail, so that contest's totals are
     *         its reported totals, or when a contest has withheld ("protected") cells.
     */
    BOOL EeResults_Tabulate(const EeResults *r,
                            const uint8_t *precinct_mask,
                            const uint8_t *vt_mask,
                            int64_t *choice_totals,
                            int64_t *choice_vt,
                            int64_t *over,
                            int64_t *under);

    /**
     * Format the results in Election Explorer's reloadable long form ("All detail"): a
     * header, then one record per turnout figure, contest figure and choice × precinct ×
     * vote type. UTF-8 with RFC-4180 quoting; no BOM.
     *
     * @param r             The results.
     * @param delim         ',' or '\t'.
     * @param precinct_mask nprecincts flags, or NULL for all.
     * @param vt_mask       nvt flags, or NULL for all.
     * @param contest_mask  ncontests flags, or NULL for all.
     * @param out           Receives a malloc'd buffer (free with free()).
     * @param out_len       Receives its length in bytes.
     * @return TRUE on success, FALSE when out of memory.
     */
    BOOL EeResults_FormatDetailUtf8(const EeResults *r,
                                    char delim,
                                    const uint8_t *precinct_mask,
                                    const uint8_t *vt_mask,
                                    const uint8_t *contest_mask,
                                    char **out,
                                    size_t *out_len);

    /**
     * Compare two names the way the results cross-check does: case-insensitive, with
     * whitespace runs collapsed, a '?' on either side matching any one character (a
     * Windows-1252 summary.csv turns characters it lacks into '?'), and any quote mark
     * matching any other (Clarity writes a nickname 'Lulu', an ES&S CVR "Lulu").
     */
    BOOL EeResults_NamesMatch(const wchar_t *a, const wchar_t *b);

    /** A display label for one EE_RES_FMT_* value ("Clarity detail (XML)", …). */
    const wchar_t *EeResults_FormatLabel(uint32_t format);

    /** One tabulated selection from another source, e.g. a Cast Vote Record tabulation. */
    typedef struct EeResultsTallyItem
    {
        uint32_t contest_key;    /* tells two contests with the same name apart */
        const wchar_t *contest;  /* CVR header ("Mayor (45918)"); the "(digits)" is dropped */
        const wchar_t *choice;   /* "Kara King (EC12)" -> "Kara King"; "overvote" and
                                  * "undervote" become the contest's Over/Under Votes */
        const wchar_t *precinct; /* NULL for a total over all precincts */
        int64_t votes;
    } EeResultsTallyItem;

    /**
     * Build results from a tabulation so they can be compared with loaded results. Contests
     * and choices keep their first-seen order. Items with a precinct give precinct detail
     * (no vote types); choice totals are then the precinct sums.
     *
     * @param items   Tabulated selections (all totals, or all per precinct).
     * @param n       Number of items.
     * @param ballots Ballots tabulated (EE_RES_UNKNOWN if not known).
     * @param out     Receives the results (initialized first; free with EeResults_Clear).
     * @return TRUE on success, FALSE when out of memory.
     */
    BOOL EeResults_FromTally(const EeResultsTallyItem *items,
                             uint32_t n,
                             int64_t ballots,
                             EeResults *out);

/** EeResultsDiff.status */
#define EE_DIFF_MATCH        0u
#define EE_DIFF_DIFFERS      1u
#define EE_DIFF_ONLY_A       2u /* only the first results have it (or a non-zero count) */
#define EE_DIFF_ONLY_B       3u
#define EE_DIFF_NOT_REPORTED 4u /* one side does not report this figure at all */

/** EeResultsDiff.kind */
#define EE_DIFFROW_CHOICE     0u
#define EE_DIFFROW_OVER       1u
#define EE_DIFFROW_UNDER      2u
#define EE_DIFFROW_BALLOTS    3u
#define EE_DIFFROW_REGISTERED 4u

    /** One compared figure. */
    typedef struct EeResultsDiff
    {
        wchar_t *contest;   /* NULL for an election-wide figure (ballots, registered) */
        wchar_t *choice;    /* the choice, "Over Votes", "Under Votes", "Ballots Cast", … */
        wchar_t *precinct;  /* NULL for a total over the included precincts */
        wchar_t *vote_type; /* NULL unless compared within one vote type */
        uint8_t kind;       /* EE_DIFFROW_* */
        uint8_t status;     /* EE_DIFF_* */
        int64_t a;          /* EE_RES_UNKNOWN when the first results have no figure */
        int64_t b;
    } EeResultsDiff;

    /**
     * Compare two results: contests by name (repeated names in order), choices by name
     * (EeResults_NamesMatch), over/under votes, ballots cast and registered voters. When
     * both sides have precinct detail, each choice is also compared precinct by precinct
     * and, when both have the same vote types, by vote type; only differing precinct and
     * vote-type rows are listed. A choice with no votes and a missing choice are equal.
     *
     * @param a, b           The results.
     * @param a_pmask, b_pmask  Counted precincts (nprecincts flags) of each side, or NULL.
     * @param a_vmask, b_vmask  Counted vote types (nvt flags) of each side, or NULL.
     * @param out            Receives a malloc'd array (free with EeResults_FreeDiff).
     * @param out_count      Receives its length.
     * @return TRUE on success, FALSE when out of memory.
     */
    BOOL EeResults_Compare(const EeResults *a,
                           const uint8_t *a_pmask,
                           const uint8_t *a_vmask,
                           const EeResults *b,
                           const uint8_t *b_pmask,
                           const uint8_t *b_vmask,
                           EeResultsDiff **out,
                           uint32_t *out_count);

    /** Free an array returned by EeResults_Compare. */
    void EeResults_FreeDiff(EeResultsDiff *d, uint32_t count);

#ifdef __cplusplus
}
#endif
