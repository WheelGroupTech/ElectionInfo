/**
 * @file ee_results_pdf.h
 * @brief Election results read from a vendor's results report PDF: ES&S Electionware
 *        "Summary Results Report" (cumulative) and precinct reports ("Precinct Summary
 *        Results Report", "Precinct Results", "Canvass"), and Hart Verity "Cumulative
 *        Results Report" and "Canvass Results Report" (Microsoft Reporting Services).
 *
 * The report is read through pdf_reader.c's positioned text: contest titles, "Vote For"
 * lines, column headings and rows are told apart by font size, and each number goes to
 * the column whose right edge it lines up with. The result is a plain intermediate model
 * that ee_results.c turns into EeResults.
 *
 * See docs/election-results-design.md (step 8).
 */
#pragma once

#include <windows.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C"
{
#endif

/** EeRpdfDoc.kind */
#define EE_RPDF_ESS_SUMMARY   1 /* cumulative totals (by vote type) */
#define EE_RPDF_ESS_PRECINCT  2 /* the same tables repeated for each precinct */
#define EE_RPDF_HART_SUMMARY  3 /* Hart cumulative report */
#define EE_RPDF_HART_PRECINCT 4 /* Hart canvass: precinct x choice grids, no vote types */
#define EE_RPDF_HART_PCT_REPORT 5 /* Hart precinct report (before Verity): the cumulative
                                   * tables repeated for each precinct, by vote type */
#define EE_RPDF_DOM_SUMMARY 6 /* Dominion Election Summary Report (ee_results_dom.c) */
#define EE_RPDF_DOM_SOVC    7 /* Dominion Statement of Votes Cast: precinct x vote type */

/** EeRpdfCount.votes for a count the report withholds ("****", too few voters to keep the
 *  vote secret): the same value as ee_results.c's RES_PROTECTED. */
#define EE_RPDF_PROTECTED ((int64_t)INT32_MIN)

/** Unknown figure (the same value as EE_RES_UNKNOWN). */
#define EE_RPDF_UNKNOWN (-1)

/** EeRpdfCount.choice for a contest's over and under votes. */
#define EE_RPDF_OVER  0xFFFFFFFEu
#define EE_RPDF_UNDER 0xFFFFFFFDu
/** EeRpdfCount.vt when the row has no vote-type columns. */
#define EE_RPDF_NO_VT 0xFFFFFFFFu

    typedef struct EeRpdfChoice
    {
        wchar_t *name;
        wchar_t *party; /* "REP", "DEM", … the code leading the name (kept there too), or NULL */
        int64_t total;  /* reported total (summary report), else EE_RPDF_UNKNOWN */
    } EeRpdfChoice;

    typedef struct EeRpdfContest
    {
        wchar_t *title;
        uint32_t vote_for; /* 0 when not printed */
        EeRpdfChoice *choices;
        uint32_t nchoices, cap_choices;
        int64_t over, under; /* summary report totals, else EE_RPDF_UNKNOWN */
        int64_t ballots;     /* "Contest Totals" (summary report), else EE_RPDF_UNKNOWN */
        BOOL has_over_under; /* the report prints Overvotes / Undervotes rows */
    } EeRpdfContest;

    /** One count of a precinct report (or a precinct's ballots cast by vote type). */
    typedef struct EeRpdfCount
    {
        uint32_t contest;
        uint32_t choice; /* index in the contest, EE_RPDF_OVER or EE_RPDF_UNDER */
        uint32_t pre;
        uint32_t vt; /* index in vote_types, or EE_RPDF_NO_VT */
        int64_t votes;
    } EeRpdfCount;

    typedef struct EeRpdfPrecinct
    {
        wchar_t *name;
        int64_t registered; /* EE_RPDF_UNKNOWN when not printed */
        int64_t ballots;
    } EeRpdfPrecinct;

    typedef struct EeRpdfDoc
    {
        int kind; /* EE_RPDF_* */
        wchar_t *election_name;
        wchar_t *election_date;
        wchar_t *region;
        wchar_t *timestamp; /* when the report was run, from its footer */

        wchar_t **vote_types; /* column names in first-seen order */
        uint32_t nvt, cap_vt;

        EeRpdfPrecinct *precincts;
        uint32_t nprecincts, cap_pre;

        EeRpdfContest *contests;
        uint32_t ncontests, cap_contests;

        EeRpdfCount *counts; /* precinct report */
        size_t ncounts, cap_counts;
        EeRpdfCount *turnout; /* precinct report: ballots cast by vote type (contest = 0) */
        size_t nturnout, cap_turnout;

        int64_t registered_total; /* summary report statistics, else EE_RPDF_UNKNOWN */
        int64_t ballots_total;
        int64_t *ballots_vt_total; /* nvt (summary report), entries EE_RPDF_UNKNOWN if absent */

        wchar_t **notes; /* things read but not kept, for the load summary */
        uint32_t nnotes, cap_notes;
    } EeRpdfDoc;

    /** TRUE when @p data starts like a PDF file. */
    BOOL EeRpdf_LooksLikePdf(const void *data, size_t size);

    /**
     * Read a results report PDF held in memory.
     *
     * @param data, size  The PDF bytes (must stay valid during the call).
     * @param out         Receives the report (free with EeRpdf_Free); zeroed first.
     * @param err         Receives a message on failure: not a PDF, unreadable, or not a
     *                    results report this reader knows.
     * @param errcch      Capacity of @p err.
     * @return TRUE on success.
     */
    BOOL EeRpdf_Parse(const void *data, size_t size, EeRpdfDoc *out, wchar_t *err, size_t errcch);

    /** Release a report from EeRpdf_Parse. */
    void EeRpdf_Free(EeRpdfDoc *doc);

#ifdef __cplusplus
}
#endif
