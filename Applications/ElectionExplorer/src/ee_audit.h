/**
 * @file ee_audit.h
 * @brief Audit logs read into events: ES&S Electionware's "Election Audit Events Report" and
 *        "Admin Audit Events Report" PDFs, and Hart Verity's "Audit Log Report" PDFs.
 *
 * Each PDF is one printed copy of a log: a JasperReports table (Event | Description |
 * Timestamp | User Id | Module) printed on landscape pages, newest event first. Text is
 * placed by column position through pdf_reader.c; a row's description, or its module's
 * second line, can run onto the top of the next page and is joined back to its event.
 *
 * A Hart Verity "Audit Log Report" (Microsoft Reporting Services) is one component's log:
 * VerityCount (tabulation), VerityCentral (central scanning) or VerityData (the election
 * definition). Its rows are numbered and oldest first; they are read into the same events
 * (Event -> level, Event Data -> description, Device ID -> module) and kept newest first.
 * The reader is ee_audit_hart.c.
 *
 * See docs/audit-log-design.md.
 */
#pragma once

#include <windows.h>
#include <stdint.h>

#include "voter_table.h" /* EeLoadStatus, EeLoadProgressFn */

#ifdef __cplusplus
extern "C"
{
#endif

/** EeAuditCopy.kind */
#define EE_AUDIT_ELECTION 1 /* "Election Audit Events Report": one election's database */
#define EE_AUDIT_ADMIN    2 /* "Admin Audit Events Report": the whole system */
#define EE_AUDIT_HART_COUNT   3 /* Hart Verity Audit Log Report, Component VerityCount: tabulation */
#define EE_AUDIT_HART_CENTRAL 4 /* Component VerityCentral: central scanning */
#define EE_AUDIT_HART_DATA    5 /* Component VerityData (or VerityBuild): the election definition */

/** The logs an election can have: EeAuditElection.logs[kind - 1]. */
#define EE_AUDIT_NLOGS 5

/** TRUE for a Hart Verity log kind. */
#define EE_AUDIT_IS_HART(kind) ((kind) >= EE_AUDIT_HART_COUNT && (kind) <= EE_AUDIT_HART_DATA)

    /** One row of the report. Strings are UTF-8 in the copy's pools. */
    typedef struct EeAuditEvent
    {
        int64_t ts;    /* the Timestamp, seconds since 1970-01-01 in local time (no zone) */
        uint32_t desc; /* offset of the NUL-terminated description in EeAuditCopy.pool */
        uint32_t page; /* the page the row starts on (1-based) */
        uint32_t level;  /* Event column ("Info", "Warning", ...): index in EeAuditCopy.strs */
        uint32_t user;   /* User Id: index in strs */
        uint32_t module; /* Module: index in strs */
    } EeAuditEvent;

    /** One printed copy of a log (one PDF). */
    typedef struct EeAuditCopy
    {
        wchar_t *path;
        int kind;            /* EE_AUDIT_* */
        char *customer;      /* header: "Travis County" */
        char *election;      /* header: the open election's name, cut at the field width */
        int election_date;   /* header election date as yyyymmdd, 0 when unreadable */
        int64_t printed;     /* print time (seconds, minute precision), 0 when unreadable */
        uint32_t pages;
        char *component;       /* Hart: the header's Component ("VerityCount 2.7.5"); NULL for ES&S */
        uint32_t missing_rows; /* Hart: row numbers missing from the report's 1..N */
        EeAuditEvent *events;  /* in report order: newest first */
        uint32_t nevents;
        uint32_t cap_events;
        char *pool; /* descriptions */
        size_t pool_len;
        size_t pool_cap;
        char **strs; /* interned levels, users and modules */
        uint32_t nstrs;
        uint32_t cap_strs;
        uint32_t *str_hash; /* open-addressing table of indexes into strs (+1; 0 = empty) */
        uint32_t hash_cap;
    } EeAuditCopy;

    /** Initialize an empty copy. */
    void EeAudit_InitCopy(EeAuditCopy *c);

    /** Release a copy's memory (the struct itself is not freed). */
    void EeAudit_FreeCopy(EeAuditCopy *c);

    /** An interned string of @p c ("" for an index out of range). */
    const char *EeAudit_Str(const EeAuditCopy *c, uint32_t id);

    /** An event's description. */
    const char *EeAudit_Desc(const EeAuditCopy *c, const EeAuditEvent *e);

    /**
     * TRUE when the PDF's Creator or first page shows an Electionware audit events report or
     * a Hart Verity audit log report. Does not read the whole file.
     */
    BOOL EeAudit_IsAuditPdf(const wchar_t *path);

    /**
     * Read one audit log PDF (Electionware or Hart Verity).
     *
     * @param path          The PDF.
     * @param out           Receives the copy (initialized first; free with EeAudit_FreeCopy).
     * @param cancel_flag   Optional; non-zero requests cancel.
     * @param progress_fn   Optional progress callback (percent of the pages).
     * @param progress_user Passed to @p progress_fn.
     * @param err           Optional; receives a message on failure.
     * @param errcch        Capacity of @p err.
     * @return EeLoadStatus_Ok, _Cancelled, or _Error (unreadable, or not an audit log).
     */
    EeLoadStatus EeAudit_LoadCopy(const wchar_t *path,
                                  EeAuditCopy *out,
                                  volatile LONG *cancel_flag,
                                  EeLoadProgressFn progress_fn,
                                  void *progress_user,
                                  wchar_t *err,
                                  size_t errcch);

    /* ---------------------------------------------------------------------- */
    /* One election's logs: copies grouped and merged                          */
    /* ---------------------------------------------------------------------- */

    /** Most copies of one log the merge keeps apart (EeAuditMerged.copies is a mask). */
#define EE_AUDIT_MAX_COPIES 64

    /** An event of the merged log: held by one or more copies. */
    typedef struct EeAuditMerged
    {
        uint32_t copy;   /* the first copy (in print order) that holds it: its strings */
        uint32_t index;  /* the event in that copy */
        uint64_t copies; /* bit i: copy i holds the event */
    } EeAuditMerged;

    /** How one copy compares with the copies printed before it. */
    typedef struct EeAuditCopyCheck
    {
        int64_t first_ts;       /* its oldest and newest events */
        int64_t last_ts;
        uint32_t events;        /* its events */
        uint32_t new_events;    /* events no earlier copy holds */
        uint32_t lacks_outside; /* earlier copies' events it lacks, older than its oldest event
                                 * (printed with a start date) or newer than its newest */
        uint32_t lacks_missing; /* earlier copies' events it lacks inside its own time span */
    } EeAuditCopyCheck;

    /** One log of an election (Election, Admin or a Hart component): all its copies, merged. */
    typedef struct EeAuditLog
    {
        int kind; /* EE_AUDIT_*; 0 = no copy loaded */
        EeAuditCopy *copies; /* in print order, oldest first */
        uint32_t ncopies;
        EeAuditCopyCheck *checks; /* one per copy */
        EeAuditMerged *events;    /* every event of every copy once, newest first */
        uint32_t nevents;
        BOOL names_differ; /* the copies' headers name more than one election */
    } EeAuditLog;

    /** The logs of one election (one county, one election date). */
    typedef struct EeAuditElection
    {
        char *customer;    /* from the first copy's header */
        char *election;    /* the election name of the most copies */
        int election_date; /* yyyymmdd */
        EeAuditLog logs[EE_AUDIT_NLOGS]; /* [kind - 1]: [0] Election, [1] Admin, [2] VerityCount,
                                          * [3] VerityCentral, [4] VerityData */
    } EeAuditElection;

    /** The log of @p kind in @p el. */
    const EeAuditLog *EeAudit_Log(const EeAuditElection *el, int kind);

    /** TRUE when @p el's logs are Hart Verity logs. */
    BOOL EeAudit_IsHart(const EeAuditElection *el);

    /** A log's name: "Election", "Admin", "VerityCount", "VerityCentral", "VerityData". */
    const wchar_t *EeAudit_LogName(int kind);

    /** EeAudit_LogName as UTF-8. */
    const char *EeAudit_LogNameA(int kind);

    /** The kind a log name names (EeAudit_LogNameA, any case), or 0. */
    int EeAudit_LogKindOf(const char *name);

    /** A merged event's copy and its event there. */
    const EeAuditEvent *EeAudit_MergedEvent(const EeAuditLog *log, uint32_t i, const EeAuditCopy **copy);

    /**
     * Load audit log PDFs and group them into elections: copies with the same customer and
     * election date belong to one election, each kind of log apart. Each log's copies are
     * ordered by print time and merged. A .zip is read for the audit log PDFs it holds (Hays
     * County posts its Hart log zipped with an XML signature, which is left out).
     *
     * @param paths         Array of @p count wide file paths.
     * @param count         Number of files (>= 1).
     * @param out           Receives a malloc'd array of elections (free with
     *                      EeAudit_FreeElections), ordered by election date.
     * @param out_count     Receives the number of elections.
     * @param cancel_flag   Optional; non-zero requests cancel.
     * @param progress_fn   Optional progress callback (percent over the files' pages).
     * @param progress_user Passed to @p progress_fn.
     * @param err           Optional; receives a message naming the file on failure.
     * @param errcch        Capacity of @p err.
     */
    EeLoadStatus EeAudit_LoadFiles(const wchar_t *const *paths,
                                   uint32_t count,
                                   EeAuditElection **out,
                                   uint32_t *out_count,
                                   volatile LONG *cancel_flag,
                                   EeLoadProgressFn progress_fn,
                                   void *progress_user,
                                   wchar_t *err,
                                   size_t errcch);

    /**
     * Group already loaded copies (taken over: on success and on failure the caller no
     * longer owns them) into elections, as EeAudit_LoadFiles does after reading.
     */
    BOOL EeAudit_GroupCopies(EeAuditCopy *copies, uint32_t ncopies, EeAuditElection **out, uint32_t *out_count);

    /** Free an array from EeAudit_LoadFiles / EeAudit_GroupCopies. */
    void EeAudit_FreeElections(EeAuditElection *els, uint32_t count);

    /* ---------------------------------------------------------------------- */
    /* Ballot accounting: the batches loaded into the EMS                      */
    /* ---------------------------------------------------------------------- */

/** EeAuditBatch.status */
#define EE_AUDIT_BATCH_COUNTED  0 /* still loaded when its session ended */
#define EE_AUDIT_BATCH_CLEARED  1 /* removed by a media clear */
#define EE_AUDIT_BATCH_REPLACED 2 /* its batch ID was committed again later in the session */

/** Vote types told from a batch's poll place name (EeAuditBatch.vote_type). */
#define EE_AUDIT_VT_OTHER        0
#define EE_AUDIT_VT_EARLY        1 /* early voting in person */
#define EE_AUDIT_VT_ELECTION_DAY 2
#define EE_AUDIT_VT_MAIL         3
#define EE_AUDIT_VT_PROVISIONAL  4
#define EE_AUDIT_VT_COUNT        5

    /** One batch commit ("Committing new Batch <name> ID <id> Number of Ballots <n>") or
     *  manual-entry batch ("Updating Batch ... Revision <r>"). From a Hart VerityCount log, one
     *  vDrive read into a task ("vDrive read with values: Polling Place- '<place>', Ballot count
     *  = n, ..., State- Read, ..., vDrive ID- <id>"). */
    typedef struct EeAuditBatch
    {
        uint32_t event;  /* merged event of its commit (its last update for a revised batch) */
        uint32_t id;     /* EMS batch ID */
        char *name;      /* "A0013-EV Vote Center - 33"; Hart: the polling place */
        char *place;     /* poll place: the name without the "A0013-" prefix and " - 33" media */
        char *device;    /* Hart: the vDrive ID; NULL for ES&S */
        int media;       /* media ID from the name, -1 when the name has none */
        BOOL central;    /* a central count batch: the name starts "A0013-" (a poll place's
                          * own tabulator media are numbered apart) */
        int64_t ballots; /* Number of Ballots: ballots, not sheets */
        int64_t ts;
        uint32_t session;
        int status;    /* EE_AUDIT_BATCH_* */
        uint32_t clear; /* EE_AUDIT_BATCH_CLEARED: the media clear that removed it (in clears) */
        int vote_type;  /* EE_AUDIT_VT_* */
        BOOL test;     /* a logic and accuracy test deck ("Logic EV", "LA-Mail") */
        uint32_t revision;
    } EeAuditBatch;

    /** A media clear: "Electionware election results for Central Count Media-<m> will be
     *  cleared" confirmed by "Results successfully cleared". */
    typedef struct EeAuditMediaClear
    {
        uint32_t event; /* merged event of "Results successfully cleared" */
        int64_t ts;
        int media;
        int device;       /* 1 "Central Count Media-n", 2 "Poll Place Media-n", 0 not named */
        char *place;      /* the poll place whose batches it removed; NULL when ambiguous */
        uint32_t removed; /* batches it removed */
        int64_t removed_ballots;
        uint32_t places;      /* poll places that had batches on this media number */
        int64_t all_ballots;  /* the ballots of every batch on this media number (any place) */
        uint32_t session;
    } EeAuditMediaClear;

    /** The batches between two full clears ("All Electionware election results will be
     *  cleared" + "Results successfully cleared"), or before the first. Hart: one task (a
     *  results container that keeps its vDrives each time it is opened again). */
    typedef struct EeAuditSession
    {
        char *name;    /* Hart: the task's name; NULL for ES&S */
        int64_t start; /* the clear that started it, or the log's first event; Hart: the task's
                        * first opening */
        int64_t end;   /* the last event before the next full clear */
        BOOL after_clear;
        uint32_t first_batch; /* its batches: batches[first_batch .. first_batch + nbatches) */
        uint32_t nbatches;
        uint32_t counted; /* batches still loaded at its end */
        int64_t ballots;  /* their ballots */
        int64_t ballots_vt[EE_AUDIT_VT_COUNT];
        uint32_t first_clear; /* its media clears: clears[first_clear .. + nclears) */
        uint32_t nclears;
        BOOL test; /* every batch is an L&A test deck (or it has none); Hart: a test task */
    } EeAuditSession;

/** EeAuditTransfer.status */
#define EE_AUDIT_XFER_MATCHED  0 /* VerityCount read the vDrive with the ballots written to it */
#define EE_AUDIT_XFER_DIFFERS  1 /* read with another ballot count */
#define EE_AUDIT_XFER_UNREAD   2 /* VerityCount's log shows no read of the vDrive */
#define EE_AUDIT_XFER_NO_COUNT 3 /* no VerityCount log was loaded */

    /** Hart: CVRs VerityCentral wrote to a vDrive ("CVRs written to vDrive" + "vDrive closed"),
     *  and VerityCount's read of that vDrive. */
    typedef struct EeAuditTransfer
    {
        uint32_t event;        /* merged VerityCentral event of "CVRs written to vDrive" */
        int64_t ts;
        char *device;          /* the vDrive ID (the "vDrive closed" after it); NULL when none */
        char *batches;         /* the batch list as logged: "1, 2, 3, 25" */
        int64_t ballots;       /* Total ballots written */
        int read_batch;        /* the VerityCount read (index in batches), -1 */
        BOOL by_count;         /* no vDrive ID was logged: matched to a read of the same ballot count
                                * within 2 days after it */
        int64_t read_ballots;
        int status;            /* EE_AUDIT_XFER_* */
    } EeAuditTransfer;

    typedef struct EeAuditAccounting
    {
        EeAuditBatch *batches; /* in load order */
        uint32_t nbatches;
        EeAuditSession *sessions;
        uint32_t nsessions;
        EeAuditMediaClear *clears;
        uint32_t nclears;
        int election_session;       /* the last session that loaded non-test batches; -1 none */
        uint32_t previously_loaded; /* "This batch was previously loaded ..." (no change) */
        uint32_t sheet_errors;      /* "An error occurred while loading Ballot Sheet ID n" */
        uint32_t sheet_error_ids;   /* distinct sheet IDs among them */
        int log;                    /* the log the batches' events are in (index in EeAuditElection.logs) */
        BOOL hart;                  /* from Hart Verity logs: batches are vDrive reads, sessions tasks */
        uint32_t rejected;          /* Hart: vDrives rejected */
        EeAuditTransfer *transfers; /* Hart: VerityCentral's vDrives, oldest first */
        uint32_t ntransfers;
    } EeAuditAccounting;

    /**
     * Replay an Election log's batch loads and clears, oldest first. A full clear starts a
     * session. A media clear names a device type ("Central Count") and a media number,
     * which each poll place numbers on its own: it removes that device type's batches on
     * that media of the poll place whose batches are loaded next (or of the only poll place
     * with that media), otherwise nothing (ambiguous).
     */
    BOOL EeAudit_Accounting(const EeAuditLog *log, EeAuditAccounting *out);

    /**
     * The ballot accounting of an election: EeAudit_Accounting of its Election log, or for Hart
     * Verity logs, EeAudit_HartAccounting.
     */
    BOOL EeAudit_ElectionAccounting(const EeAuditElection *el, EeAuditAccounting *out);

    /**
     * Hart: the VerityCount log's vDrive reads (State Read) by task. Each task is a session,
     * in the order first opened, its reads oldest first; a vDrive read again into the same
     * task replaces its earlier read. A read belongs to the task last opened before it. A task
     * is a test task when its name holds a test word (EeAudit_SetTestTaskWords); the election
     * session is the last official task with reads. VerityCentral's vDrives are matched to
     * VerityCount's reads by vDrive ID.
     */
    BOOL EeAudit_HartAccounting(const EeAuditElection *el, EeAuditAccounting *out);

    /**
     * Set the words that make a Hart task a test task, comma separated ("LAT, L&A, Test,
     * Training, Logic"). A word matches in any case when not followed by a letter, at the
     * start of the name, after a non-letter, or after a prefix of up to 3 letters ("PreLAT",
     * "PLAT"). NULL or "" sets the default.
     */
    void EeAudit_SetTestTaskWords(const wchar_t *words);

    /** The default test words. */
    const wchar_t *EeAudit_DefaultTestTaskWords(void);

    /** TRUE for a Hart test task's name. */
    BOOL EeAudit_IsTestTask(const char *name);

    /** Release an accounting (the struct itself is not freed). */
    void EeAudit_FreeAccounting(EeAuditAccounting *a);

    /** The vote type a poll place name points to (EE_AUDIT_VT_*). */
    int EeAudit_VoteTypeOf(const char *place);

    /** "Early voting", "Election Day", "Mail", "Provisional", "Other". */
    const wchar_t *EeAudit_VoteTypeName(int vote_type);

    /** TRUE for a logic and accuracy test deck's poll place or batch name. */
    BOOL EeAudit_IsTestDeck(const char *name);

    /* ---------------------------------------------------------------------- */
    /* Notable events                                                          */
    /* ---------------------------------------------------------------------- */

/** Notable event categories (EeAuditNote.category), most serious first. */
#define EE_AUDIT_N_RESULTS_EARLY   0  /* a results report printed, saved or exported before Election Day
                                       * while ballots (not test decks; more than 0) were loaded (Hart:
                                       * read into the official task open at the time) */
#define EE_AUDIT_N_LOADED_EARLY    1  /* ballots (not test decks; more than 0) committed more than 3 days
                                       * before Election Day */
#define EE_AUDIT_N_TRANSFER        2  /* Hart: VerityCentral wrote a vDrive VerityCount read with another
                                       * ballot count, or did not read */
#define EE_AUDIT_N_DELETED         3  /* an election deleted (its audit log goes with it); Hart: a task */
#define EE_AUDIT_N_RESTORE_FAILED  4  /* a restore or backup that failed */
#define EE_AUDIT_N_VALIDATION      5  /* a results or media validation error */
#define EE_AUDIT_N_ERRORS          6  /* other Error and Fatal events; Hart: errors and exceptions */
#define EE_AUDIT_N_SHEET_ERRORS    7  /* "An error occurred while loading Ballot Sheet ID n" */
#define EE_AUDIT_N_MANUAL_VOTES    8  /* Hart: a manual vote record created or deleted */
#define EE_AUDIT_N_OVERRIDES       9  /* Hart: a reporting status set by hand, or an Open vDrive override */
#define EE_AUDIT_N_MEDIA_CLEARS    10 /* a media clear */
#define EE_AUDIT_N_CLOCK           11 /* a time later than the event printed above it: a clock off; Hart: a
                                       * vDrive event logged at a time other than its Time read */
#define EE_AUDIT_N_VDRIVE_REJECTED 12 /* Hart: a vDrive rejected */
#define EE_AUDIT_N_FULL_CLEARS     13 /* a full clear */
#define EE_AUDIT_N_ZERO_BATCHES    14 /* a batch (not a test deck) of 0 ballots */
#define EE_AUDIT_N_PREV_LOADED     15 /* "This batch was previously loaded" */
#define EE_AUDIT_N_RESULTS_TEST    16 /* a results report before Election Day with only test decks or
                                       * no ballots loaded (L&A tests, zero reports) */
#define EE_AUDIT_N_REG_VOTERS      17 /* Hart: registered voters of a precinct changed */
#define EE_AUDIT_N_RESTORES        18 /* an election restored */
#define EE_AUDIT_N_USERS           19 /* a user account created or changed, or passwords viewed */
#define EE_AUDIT_N_COUNT           20

/** Severity of a category (EeAudit_NoteSeverity). */
#define EE_AUDIT_SEV_INFO   0
#define EE_AUDIT_SEV_REVIEW 1
#define EE_AUDIT_SEV_RED    2

/** Tabulation may start this many days before Election Day (Texas, counties of 100,000 or more). */
#define EE_AUDIT_EARLY_COUNT_DAYS 3

    /** One notable event. */
    typedef struct EeAuditNote
    {
        int category;  /* EE_AUDIT_N_* */
        int log;       /* the log: index in EeAuditElection.logs (0 = Election, 1 = Admin, ...) */
        uint32_t event; /* merged event of that log */
        uint32_t batches; /* results categories: batches loaded at the time (not test decks for
                           * RESULTS_EARLY, test decks for RESULTS_TEST) */
        int64_t value;    /* RESULTS_EARLY, LOADED_EARLY: their ballots; CLOCK: seconds ahead of the
                           * event printed above it (Hart: of the Time read); MANUAL_VOTES: the
                           * ballots; TRANSFER: the ballots read less those written */
    } EeAuditNote;

    typedef struct EeAuditNotes
    {
        EeAuditNote *notes; /* by log, then oldest first */
        uint32_t count;
        uint32_t per_category[EE_AUDIT_N_COUNT];
        uint32_t other_events; /* Election log events while another election was open (a database
                                * copied from an earlier election keeps its log): not checked */
        char *other_election;  /* the first such election's name, or NULL */
    } EeAuditNotes;

    /**
     * Find the notable events of an election's logs. Each event is given at most one category
     * (the first that applies, in the order above). Results reports before Election Day are
     * told apart by what the Election log shows loaded at the time since the last full clear.
     * Election log events while the log shows another election open ("Election Opened: March
     * 3, 2026 Joint Primary Election" in a copy of that database) are not checked; events in
     * a test copy of the election are. Hart logs are checked by EeAudit_HartNotable.
     */
    BOOL EeAudit_Notable(const EeAuditElection *el, EeAuditNotes *out);

    void EeAudit_FreeNotes(EeAuditNotes *n);

    /** Hart: the notable events of an election's Verity logs (from EeAudit_Notable). */
    BOOL EeAudit_HartNotable(const EeAuditElection *el, EeAuditNotes *out);

    /** "Results printed before Election Day", ... */
    const wchar_t *EeAudit_NoteName(int category);

    /** What a category means, a sentence. */
    const wchar_t *EeAudit_NoteMeaning(int category);

    /** EE_AUDIT_SEV_* */
    int EeAudit_NoteSeverity(int category);

    /* ---------------------------------------------------------------------- */
    /* Logic and accuracy testing timeline                                     */
    /* ---------------------------------------------------------------------- */

/** EeAuditTest.kind */
#define EE_AUDIT_TEST_SESSION  1 /* an Election log session of test decks only */
#define EE_AUDIT_TEST_OPENED   2 /* a test copy of the election opened (Admin log) */
#define EE_AUDIT_TEST_RESTORED 3 /* a test copy restored (and opened) (Admin log) */

/** A gap between loads this long or longer is a pause. */
#define EE_AUDIT_PAUSE_SECS 7200

    typedef struct EeAuditTest
    {
        int64_t start;
        int64_t end;
        int kind;       /* EE_AUDIT_TEST_* */
        int log;        /* index in EeAuditElection.logs */
        uint32_t event; /* its first event: the first test batch, or the restore / open */
        char *name;     /* the test copy's name; NULL for a session */
        uint32_t session; /* EE_AUDIT_TEST_SESSION: the accounting session (Hart: the test task;
                           * a task read into on several days gives a test per day) */
        uint32_t batches; /* EE_AUDIT_TEST_SESSION: its test batches and their ballots */
        int64_t ballots;
    } EeAuditTest;

    typedef struct EeAuditPause
    {
        int64_t from; /* the last load before it and the first after */
        int64_t to;
        BOOL closed; /* the Admin log shows the election closed during it */
    } EeAuditPause;

    /** Loads of ballots (not test decks) between two tests. */
    typedef struct EeAuditPeriod
    {
        int64_t first;
        int64_t last;
        uint32_t first_event; /* merged Election log events of its first and last load */
        uint32_t last_event;
        uint32_t batches;
        int64_t ballots;
        int test_before; /* the last test that started before its first load (index in tests), -1 */
        int test_after;  /* the first test that started after its last load, -1 */
        uint32_t first_pause; /* its pauses: pauses[first_pause .. + npauses) */
        uint32_t npauses;
    } EeAuditPeriod;

    typedef struct EeAuditTimeline
    {
        EeAuditTest *tests; /* by start time */
        uint32_t ntests;
        EeAuditPeriod *periods;
        uint32_t nperiods;
        EeAuditPause *pauses;
        uint32_t npauses;
        uint32_t other_copies; /* Admin log test copies left out as another election's */
        char *other_copy;      /* one of their names, or NULL */
        uint32_t other_loads;  /* loads while the Election log shows another election open */
    } EeAuditTimeline;

    /**
     * The tests and counting periods the logs show: the Election log's test sessions, the
     * Admin log's test copies of this election restored or opened (EeAudit_IsTestCopyName), and
     * the loads of ballots (not test decks), split into periods at each test. The Admin log
     * covers the whole system, so a test copy counts as this election's when one of its
     * restores follows a backup of this election (the backup's path names it) within two hours.
     * Loads and test sessions while the Election log shows another election open are left out.
     */
    BOOL EeAudit_Timeline(const EeAuditElection *el, const EeAuditAccounting *acct, EeAuditTimeline *out);

    void EeAudit_FreeTimeline(EeAuditTimeline *t);

    /** TRUE for a test copy's name: "PR26 EV CC 3rd Tab Test", "... Tabulation Test", "L&A", "Logic". */
    BOOL EeAudit_IsTestCopyName(const char *name);

    /**
     * Append an event to @p c (events are kept in report order: add the newest first). For
     * copies read from somewhere other than a PDF, such as a reloaded export.
     */
    BOOL EeAudit_AddEvent(EeAuditCopy *c, int64_t ts, const char *level, const char *desc, const char *user,
                          const char *module, uint32_t page);

    /** Seconds since 1970-01-01 for a civil date and time (no zone). */
    int64_t EeAudit_MakeTime(int year, int month, int day, int hour, int minute, int second);

    /** Format @p ts as "MM/DD/YYYY hh:mm:ssAM", the report's own form. */
    void EeAudit_FormatTime(int64_t ts, char *buf, size_t cap);

    /* ---------------------------------------------------------------------- */
    /* The reloadable Events export                                            */
    /* ---------------------------------------------------------------------- */

    /*
     * A delimited file with a header row:
     *   Record Type, Timestamp, Log, Event, Module, User Id, Description, Copies, First Copy,
     *   Page, County, Election Date
     * "Copy" records come first, one per copy of each log exported: its print time, log, the
     * election its header names (Description), its number ("2 of 5"), file name, pages,
     * county and election date. "Event" records follow in printed order (newest first), each
     * with the copies that hold it ("1-3,5 of 5"), the file name of the first of them, and its
     * page there. Reloading builds each copy back from the events it holds, so the merge, the
     * Copies Check and the ballot accounting come out as before.
     */

    /** An event to export: a merged event of a log, or an event of the one copy exported. */
    typedef struct EeAuditExportRow
    {
        uint32_t log;   /* index in EeAuditElection.logs */
        uint32_t index; /* merged event, or the event in the copy exported */
    } EeAuditExportRow;

    /**
     * Build the reloadable export of @p rows (in printed order) as UTF-8 without a BOM.
     *
     * @param el       The election.
     * @param copy_log With @p copy >= 0: the log of the one copy exported.
     * @param copy     A copy of log @p copy_log exported as printed (rows index its events), or
     *                 -1 for merged events (rows index EeAuditLog.events).
     * @param rows     The events, in printed order.
     * @param nrows    Their number.
     * @param delim    ',' or '\t'.
     * @param out      Receives a malloc'd buffer (free with free()).
     * @param out_len  Receives its length.
     * @return FALSE out of memory.
     */
    BOOL EeAudit_FormatExportUtf8(const EeAuditElection *el,
                                  int copy_log,
                                  int copy,
                                  const EeAuditExportRow *rows,
                                  uint32_t nrows,
                                  char delim,
                                  char **out,
                                  size_t *out_len);

    /** TRUE when @p path names a delimited-text file (.csv, .tsv, .txt): a possible export. */
    BOOL EeAudit_IsExportPath(const wchar_t *path);

    /**
     * Read a reloadable export back into copies (newest event first in each).
     *
     * @param path          The .csv / .tsv file.
     * @param out           Receives a malloc'd array of copies (each freed with
     *                      EeAudit_FreeCopy, then the array with free()).
     * @param out_count     Receives the number of copies.
     * @param cancel_flag   Optional; non-zero requests cancel.
     * @param progress_fn   Optional progress callback.
     * @param progress_user Passed to @p progress_fn.
     * @param err           Optional; receives a message naming the file on failure.
     * @param errcch        Capacity of @p err.
     */
    EeLoadStatus EeAudit_LoadExport(const wchar_t *path,
                                    EeAuditCopy **out,
                                    uint32_t *out_count,
                                    volatile LONG *cancel_flag,
                                    EeLoadProgressFn progress_fn,
                                    void *progress_user,
                                    wchar_t *err,
                                    size_t errcch);

#ifdef __cplusplus
}
#endif
