/**
 * @file voter_roster.h
 * @brief Voter roster loader (who has voted in an election, by day and method).
 *
 * Texas counties publish rosters during an election in no standard format. This
 * module reads a county's roster files -- ZIPs of Excel workbooks, loose .xlsx files,
 * or CSV/TSV (including Election Explorer's own roster export) -- and builds an
 * EeVoterTable with canonical source columns:
 *
 *   VUID, Precinct, Last Name, First Name, [Middle Name], [Name Suffix],
 *   Voting Method, Date Voted, [Party], [extra columns...], Source File
 *
 * so the table's normalized Voter ID / Precinct / Name columns, filters, duplicate
 * scans, export, reports and compare all work as for a voter list. Bracketed columns
 * appear only when some row has a value. Travis County, TX is the first format
 * supported. See docs/voter-roster-design.md.
 */
#pragma once

#include <windows.h>
#include <stdint.h>

#include "voter_table.h"

#ifdef __cplusplus
extern "C"
{
#endif

    /** How a voter voted (the roster's "Voting Method" column). */
    typedef enum EeVotingMethod
    {
        EE_VM_NONE = 0, /* not determinable from the file */
        EE_VM_MAIL,
        EE_VM_EARLY,
        EE_VM_ELECTION_DAY,
        EE_VM_PROVISIONAL,
        EE_VM_LIMITED,
        EE_VM_COUNT
    } EeVotingMethod;

/* Canonical roster column titles (source columns of the built table). */
#define EE_ROSTER_COL_VUID   "VUID"
#define EE_ROSTER_COL_PCT    "Precinct"
#define EE_ROSTER_COL_LAST   "Last Name"
#define EE_ROSTER_COL_FIRST  "First Name"
#define EE_ROSTER_COL_MIDDLE "Middle Name"
#define EE_ROSTER_COL_SUFFIX "Name Suffix"
#define EE_ROSTER_COL_METHOD "Voting Method"
#define EE_ROSTER_COL_DATE   "Date Voted"
#define EE_ROSTER_COL_PARTY  "Party"
#define EE_ROSTER_COL_SOURCE "Source File"

    /** Display label for @p m ("Mail Ballot", "Early Vote In-Person", "Election Day
     *  In-Person", "Provisional", "Limited"); "" for EE_VM_NONE. */
    const char *EeRoster_MethodLabel(EeVotingMethod m);

    /** Method for a label (case-insensitive exact match), else EE_VM_NONE. */
    EeVotingMethod EeRoster_MethodFromLabel(const char *label);

    /** What a load found and did; filled by EeRoster_LoadFiles. */
    typedef struct EeRosterLoadInfo
    {
        uint32_t voters;                        /* rows in the table */
        uint32_t files_read;                    /* spreadsheets / text files read */
        uint32_t sheets_read;                   /* worksheets read */
        uint32_t method_rows[EE_VM_COUNT];      /* rows per voting method */
        BOOL method_source[EE_VM_COUNT];        /* a file of that method was present
                                                 * (even if empty or not readable) */
        BOOL method_read[EE_VM_COUNT];          /* a sheet of that method was read (even
                                                 * if it had no voters) */
        uint32_t superseded_files;              /* originals replaced by an _Updated file */
        uint32_t repeated_files;                /* same file name in several inputs */
        uint32_t copy_files;                    /* whole-file copies of another method */
        uint32_t unsupported_files;             /* PDFs and other files not read */
        uint32_t recovered_sheets;              /* sheets missing a header row, recovered */
        uint32_t skipped_sheets;                /* sheets that could not be read */
        uint32_t rows_vuid_only;                /* Voter IDs with no name or precinct */
        uint32_t rows_non_voter;                /* totals, notes, placeholders */
        uint32_t vuids_malformed;               /* kept rows whose Voter ID is not 10 digits */
        uint32_t vuids_missing;                 /* kept named rows with a blank Voter ID (in a
                                                 * sheet that has a VUID column) */
        uint32_t vuids_duplicated;              /* distinct Voter IDs on more than one row */
        uint32_t rows_redacted;                 /* protected voters ("--Redacted--"): kept with
                                                 * no Voter ID or name */
        uint32_t rows_no_date;                  /* records whose file gives no voting date */
        BOOL has_party;                         /* a Party column was produced */
        wchar_t *note;                          /* human-readable summary (owned) */
    } EeRosterLoadInfo;

    /**
     * Load one or more roster files into @p out (cleared first; its surname-first
     * preference is kept). Accepts .zip (of .xlsx; other entries such as PDFs are
     * counted and listed but not read), .xlsx, .csv, .tsv and .txt. A correction
     * "<name>_Updated" replaces "<name>"; a file whose voters all appear in another
     * file of a different voting method is skipped; duplicate Voter IDs are kept.
     *
     * @param info  Optional; receives counts and a summary note (free with
     *              EeRoster_FreeInfo, also on failure).
     * @return EeLoadStatus_Ok, _Cancelled, or _Error (message in @p error_message).
     */
    EeLoadStatus EeRoster_LoadFiles(const wchar_t *const *paths,
                                    int count,
                                    EeVoterTable *out,
                                    EeRosterLoadInfo *info,
                                    volatile LONG *cancel_flag,
                                    EeLoadProgressFn progress_fn,
                                    void *progress_user,
                                    wchar_t *error_message,
                                    size_t error_cch);

    /** Free the note in @p info and zero it. */
    void EeRoster_FreeInfo(EeRosterLoadInfo *info);

    /**
     * Voting totals of a roster table: voters per Date Voted x Party x Voting Method.
     * Each Voter ID is counted once, on its earliest record (by date, then method
     * order); rows with a blank Voter ID are each counted. Cells are
     * counts[(date * nparties + party) * EE_VM_COUNT + method]; method EE_VM_NONE holds
     * rows whose Voting Method is not one of the five labels.
     */
    typedef struct EeRosterTotals
    {
        uint32_t ndates;
        uint32_t *dates;   /* yyyymmdd, ascending; 0 (first) = no or unreadable date */
        uint32_t nparties; /* >= 1 */
        char **parties;    /* UTF-8, sorted; "" = blank (sorted last). One "" entry
                            * when the table has no party values */
        BOOL has_party;    /* some row has a non-blank Party */
        uint32_t *counts;
        uint32_t records;  /* rows in the table */
        uint32_t counted;  /* voters counted */
        uint32_t repeats;  /* later records of an already-counted Voter ID */
        uint32_t repeated_ids;            /* Voter IDs with repeat records */
        BOOL method_present[EE_VM_COUNT]; /* some row has the method */
    } EeRosterTotals;

    /** Compute @p out from a roster table (built by EeRoster_LoadFiles). FALSE on
     *  out-of-memory, or when the table has no "Voting Method" column. */
    BOOL EeRoster_ComputeTotals(const EeVoterTable *table, EeRosterTotals *out);

    /** Free @p t and zero it. */
    void EeRoster_FreeTotals(EeRosterTotals *t);

#ifdef __cplusplus
}
#endif
