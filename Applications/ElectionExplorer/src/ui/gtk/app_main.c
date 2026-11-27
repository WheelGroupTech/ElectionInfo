/**
 * @file app_main.c
 * @brief Election Explorer for Linux, prototype: the voter list window in GTK 4.
 *
 * The same window as the AppKit prototype (src/ui/macos/app_main.m): File > Open... (or a file
 * on the command line), the load in the background with a progress bar, sorting by a column's
 * heading (again to reverse), numbers and dates right-aligned, the Filter window (Ctrl+L, as on
 * Windows, with Export Filter... and Load Filter...), Reset Filter, right-click Include /
 * Exclude on a cell, Copy (Ctrl+C), Export Voter
 * List... and Export Selected..., Show Duplicate Voter IDs / Voters / Voters Voting (with a
 * roster open), Reset View, the Precinct and Address reports (a window each), Edit > Options... (kept between runs, as on Windows),
 * Show in Maps... on an Address cell, Open link... on a cell with an https:// link, and a status
 * line. The list comes from the engine through
 * ee_ui_voters.h (plain C).
 *
 * File > Open Voter Roster... opens a roster's files in a Voter Roster window of its own, as on
 * Windows: the same window with Windows' roster columns, menus and words (no duplicate voters,
 * Address report, Copy prefix or map; Display Voting Totals... and Display Load Summary...).
 * Open Voter List... loads into
 * the window while it is an empty voter list window, else into a new one, as on Windows.
 *
 * The rows are a GListModel of our own that makes a small row object only for the rows on
 * screen, so a list of 1.5 million voters costs no more than one of five. The column view's
 * sorters only show the heading's arrow. Sorting and filtering are the engine's, computed on a
 * worker thread (EeUiVoters_ComputeView) while the window keeps showing the old order, then
 * swapped in (EeUiVoters_SetView); the model then reports its rows changed.
 *
 * For checking without looking:
 * - `--selftest <file>` loads, sorts and filters the list without a window and prints what the
 *   window would show.
 * - `--snapshot <png> [--sort <column>[:desc]] [--include|--exclude <column>=<value>]... <file>`
 *   opens the window, loads the list, sorts it as a heading click would and filters it as the
 *   right-click menu would, writes the window's content as a PNG and quits.
 *   With `--filter-window` the PNG is of the Filter window, opened on those rules.
 *   `--load-filter <file>` then does what its Load Filter... does, and `--export-filter <file>`
 *   its Export Filter... (without the file dialogs; either opens the Filter window).
 *   `--duplicates ids|voters|voting` shows a duplicates view first (as the Filter menu does;
 *   voting needs `--voting-roster <file>`, repeatable, each opened as a roster window), and
 *   `--export <file>` exports the shown rows before the PNG (as Export Voter List... does,
 *   without the dialogs).
 *   `--report precinct|address` opens that report (Reports > Display ... Report...) and the PNG
 *   is of its window; then `--sort 0|1[:desc]` clicks the report's heading and `--export`
 *   exports the report (Export All...).
 *   `--zoom <percent>` and `--names given|surname` set those options once the list is loaded,
 *   as the Options window's OK does (without saving them), and `--options-window` makes the
 *   PNG of the Options window.
 *   `--roster` opens the files named as one voter roster (Open Voter Roster...), and the PNG is
 *   of its window; `--load-summary` makes it of its Load Summary, and `--voting-totals` of its
 *   Voting Totals (then `--export` exports them, as Export All... does).
 * - `--selftest-roster <files...>` loads the files as a roster without a window and prints what
 *   the Voter Roster window would show.
 * - `--selftest-voting <list> <roster files...>` prints Show Duplicate Voters Voting of the list
 *   in that roster, as the voter list window would show it.
 *
 * Needs GTK 4.12 or later (Ubuntu 24.04 has 4.14).
 */

#include <gtk/gtk.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "ee_ui_voters.h"

/* -------------------------------------------------------------------------- */
/* Rows: a list model that makes row objects on demand                        */
/* -------------------------------------------------------------------------- */

#define EE_TYPE_ROW (ee_row_get_type())
G_DECLARE_FINAL_TYPE(EeRow, ee_row, EE, ROW, GObject)

struct _EeRow
{
    GObject parent_instance;
    guint index; /* display row */
};

G_DEFINE_TYPE(EeRow, ee_row, G_TYPE_OBJECT)

static void ee_row_class_init(EeRowClass *klass)
{
    (void)klass;
}

static void ee_row_init(EeRow *self)
{
    self->index = 0;
}

#define EE_TYPE_ROWS (ee_rows_get_type())
G_DECLARE_FINAL_TYPE(EeRows, ee_rows, EE, ROWS, GObject)

struct _EeRows
{
    GObject parent_instance;
    guint count;
};

static GType ee_rows_item_type(GListModel *list)
{
    (void)list;
    return EE_TYPE_ROW;
}

static guint ee_rows_n_items(GListModel *list)
{
    return EE_ROWS(list)->count;
}

static gpointer ee_rows_item(GListModel *list, guint position)
{
    EeRow *row;
    if (position >= EE_ROWS(list)->count)
    {
        return NULL;
    }
    row = g_object_new(EE_TYPE_ROW, NULL);
    row->index = position;
    return row;
}

static void ee_rows_model_init(GListModelInterface *iface)
{
    iface->get_item_type = ee_rows_item_type;
    iface->get_n_items = ee_rows_n_items;
    iface->get_item = ee_rows_item;
}

G_DEFINE_TYPE_WITH_CODE(EeRows,
                        ee_rows,
                        G_TYPE_OBJECT,
                        G_IMPLEMENT_INTERFACE(G_TYPE_LIST_MODEL, ee_rows_model_init))

static void ee_rows_class_init(EeRowsClass *klass)
{
    (void)klass;
}

static void ee_rows_init(EeRows *self)
{
    self->count = 0;
}

static EeRows *ee_rows_new(guint count)
{
    EeRows *rows = g_object_new(EE_TYPE_ROWS, NULL);
    rows->count = count;
    return rows;
}

/* -------------------------------------------------------------------------- */
/* The voter list window                                                      */
/* -------------------------------------------------------------------------- */

typedef struct FilterDialog FilterDialog;
typedef struct ReportWindow ReportWindow;
typedef struct TotalsWindow TotalsWindow;
typedef struct OptionsDialog OptionsDialog;

typedef struct VoterWindow
{
    GtkApplication *app;
    GtkWindow *window;
    gboolean roster; /* a Voter Roster window (Windows' roster mode of the voter viewer) */
    struct VoterWindow *opener; /* the window whose Open Voter Roster... made this one */
    GtkWindow *summary;         /* the roster's Load Summary window while it is open */
    GtkColumnView *view;
    GtkLabel *status;
    GtkProgressBar *progress;
    GtkWidget
        *cell_menu; /* the right-click menu while it is open (a GtkPopoverMenu on the table) */
    EeUiVoters *voters;
    EeRows *rows;
    char *name;          /* the file's name, for the title and status line */
    char *path;          /* the list's path, for the export's name */
    gboolean rebuilding; /* columns being replaced: ignore the sorter's changes */
    gboolean closed;     /* the window is gone (the struct stays for queued idle calls) */

    /* the load in progress (the worker writes these, the UI timer reads them) */
    GThread *worker;
    char *loading_path;
    GPtrArray *loading_roster; /* a roster's files (char *), or NULL for a voter list */
    volatile gint percent;
    volatile gint rows_so_far;
    volatile int32_t cancel;
    guint progress_timer;

    /* the view the window asks for (sort and filter); a job computes it in the background */
    int want_sort;
    gboolean want_ascending;
    GArray *rules;  /* EeUiRule */
    int want_marks; /* EE_UI_MARKS_*: a duplicates view */
    /* Show Duplicate Voters Voting's duplicates, for the next job (or NULL) */
    EeUiMarks *voting_marks;
    guint want_gen;
    GThread *job;
    const char *job_label; /* the running job's "Sorting" or "Filtering" */

    /* the right-clicked cell, for Include / Exclude, and its link for Open link... (or NULL) */
    guint quick_column;
    char *quick_value;
    char *quick_link;

    FilterDialog *filter_dialog;
    ReportWindow *reports[EE_UI_REPORT_COUNT]; /* the open Precinct / Address reports */
    TotalsWindow *totals;                      /* a roster's open Voting Totals */
    OptionsDialog *options_dialog;
    EeUiOptions opts;                /* Edit > Options... (loaded at start, saved on OK) */
    GtkCssProvider *zoom_css;        /* the list's text size */
    int zoom_shown;                  /* the zoom the columns are sized for */
    gboolean names_pending;          /* the Name column to rebuild once a sort or filter is done */

    char *snapshot;                  /* --snapshot: write the content here once loaded, then quit */
    int snapshot_sort;               /* --sort: click this column's heading first (-1: none) */
    gboolean snapshot_ascending;
    GPtrArray *snapshot_quick;       /* --include / --exclude: "+column=value" / "-column=value" */
    gboolean snapshot_filter_window; /* --filter-window: the PNG is of the Filter window */
    int snapshot_marks;              /* --duplicates: EE_UI_MARKS_* (voting: once rosters load) */
    char *snapshot_export;           /* --export: export the shown rows here first */
    int snapshot_report;             /* --report: EE_UI_REPORT_* (-1: none) */
    int snapshot_zoom;               /* --zoom: 0 for none */
    int snapshot_names;              /* --names: 1 surname first, 0 given name first, -1 none */
    gboolean snapshot_options_window;   /* --options-window: the PNG is of the Options window */
    const char *snapshot_load_filter;   /* --load-filter: Load Filter... in the Filter window */
    const char *snapshot_export_filter; /* --export-filter: then Export Filter... */
    gboolean snapshot_summary;          /* --load-summary: the PNG is of the Load Summary */
    gboolean snapshot_totals;           /* --voting-totals: the PNG is of the Voting Totals */
    int snapshot_tries;
} VoterWindow;

typedef struct LoadDone
{
    VoterWindow *w;
    int surname_first; /* the names as the options had them when the load began */
    EeUiVoters *voters;
    char err[1024];
    gint64 started;
} LoadDone;

typedef struct ViewJob
{
    VoterWindow *w;
    int sort;
    gboolean ascending;
    EeUiRule *rules;
    guint nrules;
    int marks;
    EeUiMarks *given; /* Show Duplicate Voters Voting's (found in advance), or NULL */
    guint gen;
    EeUiView *view;
    gint64 started;
} ViewJob;

static void set_status(VoterWindow *w)
{
    GString *s;
    int asc = 0;
    int sc = EeUiVoters_SortColumn(w->voters, &asc);
    if (w->voters == NULL)
    {
        gtk_label_set_text(w->status,
                           w->roster ? "No voter roster loaded." /* (Windows' words) */
                                     : "Open a voter list (File > Open Voter List...).");
        return;
    }
    s = g_string_new(NULL);
    {
        char count[256];
        EeUiVoters_CountText(w->voters, count, sizeof(count)); /* (the Windows words) */
        g_string_append(s, count);
    }
    g_string_append_printf(s,
                           ", %u columns \xE2\x80\x94 %s",
                           EeUiVoters_Columns(w->voters),
                           w->name != NULL ? w->name : "");
    if (sc >= 0)
    {
        g_string_append_printf(s,
                               " \xE2\x80\x94 sorted by %s, %s",
                               EeUiVoters_Title(w->voters, (uint32_t)sc),
                               asc ? "ascending" : "descending");
    }
    gtk_label_set_text(w->status, s->str);
    g_string_free(s, TRUE);
}

/* -------------------------------------------------------------------------- */
/* Sorting and filtering in the background                                    */
/* -------------------------------------------------------------------------- */

static void start_view_job(VoterWindow *w);
static char *roster_name(VoterWindow *w);
static void apply_names(VoterWindow *w);
static void alert(VoterWindow *w, const char *message, const char *detail);
static void show_sort_arrow(VoterWindow *w, int column, gboolean ascending);

static gboolean pulse_progress(gpointer user)
{
    VoterWindow *w = (VoterWindow *)user;
    gtk_progress_bar_pulse(w->progress);
    return G_SOURCE_CONTINUE;
}

static void stop_progress(VoterWindow *w)
{
    if (w->progress_timer != 0)
    {
        g_source_remove(w->progress_timer);
        w->progress_timer = 0;
    }
    gtk_widget_set_visible(GTK_WIDGET(w->progress), FALSE);
}

static void view_job_free(ViewJob *job)
{
    EeUiView_Free(job->view);
    EeUiMarks_Free(job->given);
    g_free(job->rules);
    g_free(job);
}

static gboolean view_done(gpointer user)
{
    ViewJob *job = (ViewJob *)user;
    VoterWindow *w = job->w;
    guint before, after;
    if (w->closed)
    {
        view_job_free(job);
        return G_SOURCE_REMOVE;
    }
    g_thread_join(w->job);
    w->job = NULL;
    if (job->view == NULL)
    {
        stop_progress(w);
        set_status(w);
        gtk_label_set_text(w->status, "Not enough memory to sort or filter the list.");
        view_job_free(job);
        apply_names(w); /* (if the Options changed them meanwhile) */
        return G_SOURCE_REMOVE;
    }
    fprintf(stderr,
            "%s took %.2f s\n",
            w->job_label,
            (g_get_monotonic_time() - job->started) / 1e6);
    {
        uint32_t marked = 0;
        int kind = EeUiView_Marks(job->view, &marked);
        if (kind != EE_UI_MARKS_NONE && marked == 0)
        {
            /* (as on Windows: no duplicates is said, and the shown view stays) */
            int asc = 0;
            int sc = EeUiVoters_SortColumn(w->voters, &asc);
            w->want_marks = EeUiVoters_Marks(w->voters, NULL);
            w->want_sort = sc;
            w->want_ascending = asc != 0;
            show_sort_arrow(w, sc, asc != 0);
            stop_progress(w);
            set_status(w);
            alert(w, EeUiVoters_MarksNoneText(kind), NULL);
            view_job_free(job);
            apply_names(w);
            return G_SOURCE_REMOVE;
        }
    }
    before = w->rows->count;
    EeUiVoters_SetView(w->voters, job->view);
    job->view = NULL;
    after = EeUiVoters_Rows(w->voters);
    w->rows->count = after;
    g_list_model_items_changed(G_LIST_MODEL(w->rows), 0, before, after);
    set_status(w);
    if (job->gen != w->want_gen)
    {
        start_view_job(w); /* (asked for something else meanwhile) */
    }
    else
    {
        stop_progress(w);
        apply_names(w);
    }
    view_job_free(job);
    return G_SOURCE_REMOVE;
}

static gpointer view_worker(gpointer user)
{
    ViewJob *job = (ViewJob *)user;
    job->view = EeUiVoters_ComputeViewWith(job->w->voters,
                                           job->sort,
                                           job->ascending,
                                           job->rules,
                                           job->nrules,
                                           job->marks,
                                           job->given);
    g_idle_add(view_done, job);
    return NULL;
}

static void start_view_job(VoterWindow *w)
{
    ViewJob *job = g_new0(ViewJob, 1);
    char text[128];
    job->w = w;
    job->sort = w->want_sort;
    job->ascending = w->want_ascending;
    job->nrules = w->rules->len;
    job->rules = job->nrules > 0
                     ? (EeUiRule *)g_memdup2(w->rules->data, (gsize)job->nrules * sizeof(EeUiRule))
                     : NULL;
    job->marks = w->want_marks;
    if (job->marks == EE_UI_MARKS_DUP_VOTING)
    {
        job->given = w->voting_marks; /* (NULL: the shown view's are kept) */
    }
    else
    {
        EeUiMarks_Free(w->voting_marks); /* (asked for something else meanwhile) */
    }
    w->voting_marks = NULL;
    job->gen = w->want_gen;
    job->started = g_get_monotonic_time();
    {
        int asc = 0;
        int sc = EeUiVoters_SortColumn(w->voters, &asc);
        /* (named for what changes: duplicates to find, a new order, else the filter) */
        if (job->marks != EE_UI_MARKS_NONE && job->marks != EeUiVoters_Marks(w->voters, NULL))
        {
            w->job_label = "Finding duplicates";
        }
        else if (sc != job->sort || (sc >= 0 && (asc != 0) != (job->ascending != 0)))
        {
            w->job_label = "Sorting";
        }
        else
        {
            w->job_label = "Filtering";
        }
    }
    g_snprintf(text,
               sizeof(text),
               "%s%s %u voters\xE2\x80\xA6",
               w->job_label,
               strcmp(w->job_label, "Finding duplicates") == 0 ? " in" : "",
               EeUiVoters_TotalRows(w->voters));
    gtk_label_set_text(w->status, text);
    if (w->progress_timer == 0)
    {
        gtk_widget_set_visible(GTK_WIDGET(w->progress), TRUE);
        w->progress_timer = g_timeout_add(100, pulse_progress, w);
    }
    w->job = g_thread_new("ee-view", view_worker, job);
}

/* ask for the view in want_sort / rules; the latest request wins */
static void request_view(VoterWindow *w)
{
    if (w->voters == NULL)
    {
        return;
    }
    w->want_gen++;
    if (w->job == NULL)
    {
        start_view_job(w);
    }
}

/* a heading clicked: the column view's sorter changed; sort with the engine */
static void on_sorter_changed(GtkSorter *sorter, GtkSorterChange change, gpointer user)
{
    VoterWindow *w = (VoterWindow *)user;
    GtkColumnViewSorter *cvs = GTK_COLUMN_VIEW_SORTER(sorter);
    GtkColumnViewColumn *col;
    (void)change;
    if (w->voters == NULL || w->rebuilding)
    {
        return;
    }
    col = gtk_column_view_sorter_get_primary_sort_column(cvs);
    w->want_sort =
        col != NULL ? (int)GPOINTER_TO_UINT(g_object_get_data(G_OBJECT(col), "column")) : -1;
    w->want_ascending = gtk_column_view_sorter_get_primary_sort_order(cvs) == GTK_SORT_ASCENDING;
    if (gtk_column_view_sorter_get_n_sort_columns(cvs) > 1)
    {
        /* (the sorter keeps earlier columns as secondary sorts, with their arrows: the engine
         * sorts by one column, so keep only the clicked one) */
        show_sort_arrow(w, w->want_sort, w->want_ascending);
    }
    request_view(w);
}

static void set_rules(VoterWindow *w, const EeUiRule *rules, guint n)
{
    g_array_set_size(w->rules, 0);
    if (n > 0)
    {
        g_array_append_vals(w->rules, rules, n);
    }
    request_view(w);
}

/* Include / Exclude a value: a rule "<column> is <value>", as the Windows right-click menu adds */
static void add_quick_rule(VoterWindow *w, guint column, const char *value, gboolean exclude)
{
    EeUiRule r;
    if (w->voters == NULL || column >= EeUiVoters_Columns(w->voters))
    {
        return;
    }
    memset(&r, 0, sizeof(r));
    r.column = column;
    r.relation = EE_UI_REL_IS;
    r.exclude = exclude ? 1 : 0;
    r.enabled = 1;
    g_strlcpy(r.value, value != NULL ? value : "", sizeof(r.value));
    g_array_append_val(w->rules, r);
    request_view(w);
}

/* the heading's arrow without a new sort (-1: none) */
static void show_sort_arrow(VoterWindow *w, int column, gboolean ascending)
{
    GListModel *cols = gtk_column_view_get_columns(w->view);
    GtkColumnViewColumn *col = NULL;
    if (column >= 0 && (guint)column < g_list_model_get_n_items(cols))
    {
        col = g_list_model_get_item(cols, (guint)column);
    }
    w->rebuilding = TRUE; /* (on_sorter_changed: not a heading click) */
    gtk_column_view_sort_by_column(w->view, NULL, GTK_SORT_ASCENDING); /* (one arrow only) */
    gtk_column_view_sort_by_column(w->view,
                                   col,
                                   ascending ? GTK_SORT_ASCENDING : GTK_SORT_DESCENDING);
    w->rebuilding = FALSE;
    if (col != NULL)
    {
        g_object_unref(col);
    }
}

/* Show Duplicate Voter IDs / Voters: only the duplicates, grouped by Voter ID or Name (as on
 * Windows), within the filter */
static void show_duplicates(VoterWindow *w, int marks)
{
    int column = marks == EE_UI_MARKS_DUP_VOTERS ? EE_UI_COLUMN_NAME : EE_UI_COLUMN_VOTER_ID;
    if (w->voters == NULL || w->worker != NULL)
    {
        alert(w,
              marks == EE_UI_MARKS_DUP_VOTERS
                  ? "Load a voter list before checking for duplicate voters."
                  : "Load a voter list before checking for duplicate Voter IDs.",
              NULL);
        return;
    }
    if (marks == EE_UI_MARKS_DUP_VOTERS && !EeUiVoters_HasBirthDate(w->voters))
    {
        alert(w, "No birth date data is available", NULL);
        return;
    }
    if ((guint)column >= EeUiVoters_Columns(w->voters))
    {
        column = -1;
    }
    w->want_marks = marks;
    w->want_sort = column;
    w->want_ascending = TRUE;
    show_sort_arrow(w, column, TRUE);
    request_view(w);
}

/* the roster windows with a roster loaded (as Windows looks through its open windows) */
static GPtrArray *open_rosters(VoterWindow *w)
{
    GPtrArray *a = g_ptr_array_new();
    GList *l;
    for (l = gtk_application_get_windows(w->app); l != NULL; l = l->next)
    {
        VoterWindow *o = (VoterWindow *)g_object_get_data(G_OBJECT(l->data), "ee-voter-window");
        if (o != NULL && !o->closed && o->roster && o->voters != NULL && o->worker == NULL &&
            EeUiVoters_TotalRows(o->voters) > 0)
        {
            g_ptr_array_add(a, o);
        }
    }
    return a;
}

/* Show Duplicate Voters Voting with roster window @p r: found here (it reads the roster, which
 * its window may free) and shown as the other duplicates views, grouped by Name */
static void show_voting_with(VoterWindow *w, VoterWindow *r)
{
    char msg[512], *base;
    EeUiMarks *m;
    int column = EE_UI_COLUMN_NAME;
    if (w->closed || r->closed || r->voters == NULL || r->worker != NULL)
    {
        return; /* (closed or reloaded meanwhile) */
    }
    base = g_path_get_basename(r->path != NULL ? r->path : "");
    m = EeUiVoters_DuplicatesVoting(w->worker == NULL ? w->voters : NULL,
                                    r->voters,
                                    base,
                                    msg,
                                    sizeof(msg));
    g_free(base);
    if (m == NULL)
    {
        alert(w, msg, NULL);
        return;
    }
    EeUiMarks_Free(w->voting_marks);
    w->voting_marks = m;
    if ((guint)column >= EeUiVoters_Columns(w->voters))
    {
        column = -1;
    }
    w->want_marks = EE_UI_MARKS_DUP_VOTING;
    w->want_sort = column;
    w->want_ascending = TRUE;
    show_sort_arrow(w, column, TRUE);
    request_view(w);
}

/* Select Voter Roster: with several rosters open, the one to check (Windows' App_PickItem) */
typedef struct RosterPick
{
    VoterWindow *w;
    GtkWindow *win;
    GtkDropDown *list;
    GPtrArray *rosters; /* VoterWindow * */
} RosterPick;

static void on_pick_destroy(GtkWidget *widget, gpointer user)
{
    RosterPick *p = (RosterPick *)user;
    (void)widget;
    g_ptr_array_unref(p->rosters);
    g_free(p);
}

static void on_pick_ok(GtkButton *button, gpointer user)
{
    RosterPick *p = (RosterPick *)user;
    guint i = gtk_drop_down_get_selected(p->list);
    VoterWindow *w = p->w, *r = i < p->rosters->len ? g_ptr_array_index(p->rosters, i) : NULL;
    (void)button;
    gtk_window_destroy(p->win); /* (frees p) */
    if (r != NULL)
    {
        show_voting_with(w, r);
    }
}

static void on_pick_cancel(GtkButton *button, gpointer user)
{
    (void)button;
    gtk_window_destroy(((RosterPick *)user)->win);
}

static void pick_roster(VoterWindow *w, GPtrArray *rosters)
{
    RosterPick *p = g_new0(RosterPick, 1);
    GtkWidget *box = gtk_box_new(GTK_ORIENTATION_VERTICAL, 12);
    GtkWidget *buttons = gtk_box_new(GTK_ORIENTATION_HORIZONTAL, 8);
    GtkWidget *prompt = gtk_label_new("Several voter rosters are open. Choose the one to check:");
    GtkWidget *cancel = gtk_button_new_with_label("Cancel");
    GtkWidget *ok = gtk_button_new_with_label("OK");
    GtkStringList *titles = gtk_string_list_new(NULL);
    GtkEventController *keys = gtk_shortcut_controller_new();
    guint i;
    p->w = w;
    p->rosters = rosters;
    for (i = 0; i < rosters->len; i++)
    {
        VoterWindow *r = (VoterWindow *)g_ptr_array_index(rosters, i);
        gtk_string_list_append(titles, gtk_window_get_title(r->window));
    }
    p->list = GTK_DROP_DOWN(gtk_drop_down_new(G_LIST_MODEL(titles), NULL)); /* (takes titles) */
    p->win = GTK_WINDOW(gtk_window_new());
    gtk_window_set_title(p->win, "Select Voter Roster");
    gtk_window_set_transient_for(p->win, w->window);
    gtk_window_set_modal(p->win, TRUE);
    gtk_window_set_resizable(p->win, FALSE);
    gtk_label_set_xalign(GTK_LABEL(prompt), 0.0f);
    gtk_widget_set_halign(buttons, GTK_ALIGN_END);
    gtk_widget_add_css_class(ok, "suggested-action");
    gtk_box_append(GTK_BOX(buttons), cancel);
    gtk_box_append(GTK_BOX(buttons), ok);
    gtk_widget_set_margin_start(box, 16);
    gtk_widget_set_margin_end(box, 16);
    gtk_widget_set_margin_top(box, 16);
    gtk_widget_set_margin_bottom(box, 16);
    gtk_box_append(GTK_BOX(box), prompt);
    gtk_box_append(GTK_BOX(box), GTK_WIDGET(p->list));
    gtk_box_append(GTK_BOX(box), buttons);
    gtk_window_set_child(p->win, box);
    gtk_window_set_default_widget(p->win, ok);
    gtk_shortcut_controller_add_shortcut(GTK_SHORTCUT_CONTROLLER(keys),
                                         gtk_shortcut_new(gtk_keyval_trigger_new(GDK_KEY_Escape, 0),
                                                          gtk_named_action_new("window.close")));
    gtk_widget_add_controller(GTK_WIDGET(p->win), keys);
    g_signal_connect(ok, "clicked", G_CALLBACK(on_pick_ok), p);
    g_signal_connect(cancel, "clicked", G_CALLBACK(on_pick_cancel), p);
    g_signal_connect(p->win, "destroy", G_CALLBACK(on_pick_destroy), p);
    gtk_window_present(p->win);
}

/* Filter > Show Duplicate Voters Voting...: the list's voters with the same name and birth date
 * who voted (Voter ID in a loaded roster) under different Voter IDs, as on Windows. With
 * several rosters open, the user picks one. */
static void show_duplicates_voting(VoterWindow *w)
{
    GPtrArray *rosters;
    char msg[512];
    if (w->roster)
    {
        return; /* (as on Windows: not in a roster window) */
    }
    if (w->voters == NULL || w->worker != NULL || EeUiVoters_TotalRows(w->voters) == 0)
    {
        alert(w, "Load a voter list before checking for duplicate voters.", NULL);
        return;
    }
    if (!EeUiVoters_HasBirthDate(w->voters))
    {
        alert(w, "No birth date data is available", NULL);
        return;
    }
    rosters = open_rosters(w);
    if (rosters->len == 0)
    {
        EeUiMarks_Free(EeUiVoters_DuplicatesVoting(w->voters, NULL, NULL, msg, sizeof(msg)));
        alert(w, msg, NULL);
        g_ptr_array_unref(rosters);
    }
    else if (rosters->len == 1)
    {
        show_voting_with(w, (VoterWindow *)g_ptr_array_index(rosters, 0));
        g_ptr_array_unref(rosters);
    }
    else
    {
        pick_roster(w, rosters); /* (takes rosters) */
    }
}

/* Reset Filter / Reset View: all rows (no rules, no duplicates view), as on Windows */
static void reset_view(VoterWindow *w)
{
    if (w->voters != NULL && (w->rules->len > 0 || w->want_marks != EE_UI_MARKS_NONE))
    {
        w->want_marks = EE_UI_MARKS_NONE;
        set_rules(w, NULL, 0);
    }
}

/* a column view's selected rows (below @p count), in order */
static uint32_t *view_selection(GtkColumnView *view, guint count, guint *n)
{
    GtkSelectionModel *model = gtk_column_view_get_model(view);
    GtkBitset *set;
    GtkBitsetIter it;
    guint pos, k = 0;
    uint32_t *rows;
    *n = 0;
    if (model == NULL)
    {
        return NULL;
    }
    set = gtk_selection_model_get_selection(model);
    rows = g_new(uint32_t, gtk_bitset_get_size(set) > 0 ? gtk_bitset_get_size(set) : 1);
    if (gtk_bitset_iter_init_first(&it, set, &pos))
    {
        do
        {
            if (pos < count)
            {
                rows[k++] = pos;
            }
        } while (gtk_bitset_iter_next(&it, &pos));
    }
    gtk_bitset_unref(set);
    *n = k;
    return rows;
}

/* the selected shown rows, in order */
static uint32_t *selected_rows(VoterWindow *w, guint *n)
{
    return view_selection(w->view, w->rows != NULL ? w->rows->count : 0, n);
}

/* an export's path: as on Windows, a name without .csv or .tsv is a CSV (g_free() it) */
static char *export_named(const char *path)
{
    size_t len = strlen(path);
    if (len >= 4 && (g_ascii_strcasecmp(path + len - 4, ".tsv") == 0 ||
                     g_ascii_strcasecmp(path + len - 4, ".csv") == 0))
    {
        return g_strdup(path);
    }
    return g_strconcat(path, ".csv", NULL);
}

/* the export's field separator, from its extension: tabs for .tsv, else commas */
static char export_delim(const char *path)
{
    size_t len = strlen(path);
    return len >= 4 && g_ascii_strcasecmp(path + len - 4, ".tsv") == 0 ? '\t' : ',';
}

/* the Save dialog's file types, as on Windows (g_object_unref() it) */
static GListModel *export_filters(void)
{
    GtkFileFilter *csv = gtk_file_filter_new(), *tsv = gtk_file_filter_new();
    GListStore *filters = g_list_store_new(GTK_TYPE_FILE_FILTER);
    gtk_file_filter_set_name(csv, "UTF-8 CSV (*.csv)");
    gtk_file_filter_add_suffix(csv, "csv");
    gtk_file_filter_set_name(tsv, "UTF-8 TSV (*.tsv)");
    gtk_file_filter_add_suffix(tsv, "tsv");
    g_list_store_append(filters, csv);
    g_list_store_append(filters, tsv);
    g_object_unref(csv);
    g_object_unref(tsv);
    return G_LIST_MODEL(filters);
}

/* Copy: the selected rows, separated as in the file, with the Voter ID / Precinct / Name /
 * Address columns first unless the Options say not to (Windows' default is first) */
static void copy_selection(VoterWindow *w)
{
    guint n = 0;
    uint32_t *rows;
    char *text, msg[96];
    if (w->voters == NULL || w->worker != NULL || w->job != NULL)
    {
        return;
    }
    rows = selected_rows(w, &n);
    if (n == 0)
    {
        gtk_label_set_text(w->status, "Nothing selected to copy.");
        g_free(rows);
        return;
    }
    text = EeUiVoters_CopyText(w->voters, rows, n, w->opts.copy_normalized, NULL);
    g_free(rows);
    if (text == NULL)
    {
        alert(w, "Not enough memory to copy the rows.", NULL);
        return;
    }
    gdk_clipboard_set_text(gtk_widget_get_clipboard(GTK_WIDGET(w->window)), text);
    free(text);
    g_snprintf(msg, sizeof(msg), "Copied %u row%s to the clipboard.", n, n == 1 ? "" : "s");
    gtk_label_set_text(w->status, msg);
}

/* Export: Windows' Export Options (include the normalized fields?), then where to save it */
typedef struct ExportJob
{
    VoterWindow *w;
    GtkWindow *options;
    GtkCheckButton *normalized;
    uint32_t *rows; /* shown rows, or NULL for all of them */
    guint n;
    gboolean selection;
} ExportJob;

static void export_job_free(ExportJob *e)
{
    g_free(e->rows);
    g_free(e);
}

static void export_to(VoterWindow *w,
                      const char *path,
                      const uint32_t *rows,
                      guint n,
                      gboolean normalized)
{
    char err[256], msg[96];
    char delim = export_delim(path);
    gint64 started = g_get_monotonic_time();
    long wrote = EeUiVoters_Export(w->voters, path, rows, n, normalized, delim, err, sizeof(err));
    if (wrote < 0)
    {
        alert(w, err, NULL);
        return;
    }
    fprintf(stderr,
            "exported %ld rows in %.2f s\n",
            wrote,
            (g_get_monotonic_time() - started) / 1e6);
    g_snprintf(msg, sizeof(msg), "Exported %ld row%s.", wrote, wrote == 1 ? "" : "s");
    gtk_label_set_text(w->status, msg);
}

static void on_export_chosen(GObject *source, GAsyncResult *result, gpointer user)
{
    ExportJob *e = (ExportJob *)user;
    GFile *file = gtk_file_dialog_save_finish(GTK_FILE_DIALOG(source), result, NULL);
    if (file != NULL && !e->w->closed && e->w->voters != NULL)
    {
        char *path = g_file_get_path(file);
        if (path != NULL)
        {
            char *named = export_named(path);
            export_to(e->w,
                      named,
                      e->rows,
                      e->n,
                      GPOINTER_TO_INT(g_object_get_data(source, "normalized")));
            g_free(named);
            g_free(path);
        }
    }
    if (file != NULL)
    {
        g_object_unref(file);
    }
    export_job_free(e);
}

/* where to save it (the job is freed once chosen or cancelled) */
static void export_choose_file(ExportJob *e, gboolean normalized)
{
    GtkFileDialog *d = gtk_file_dialog_new();
    GListModel *filters = export_filters();
    char name[512];
    g_object_set_data(G_OBJECT(d), "normalized", GINT_TO_POINTER(normalized ? 1 : 0));
    EeUiVoters_ExportName(e->w->voters, e->w->path, e->selection, name, sizeof(name));
    gtk_file_dialog_set_title(d, "Export");
    gtk_file_dialog_set_filters(d, filters);
    gtk_file_dialog_set_initial_name(d, name);
    gtk_file_dialog_save(d, e->w->window, NULL, on_export_chosen, e);
    g_object_unref(filters);
    g_object_unref(d);
}

static void on_export_options_ok(GtkWidget *button, gpointer user)
{
    ExportJob *e = (ExportJob *)user;
    gboolean normalized = gtk_check_button_get_active(e->normalized);
    (void)button;
    {
        GtkWindow *options = e->options;
        e->options = NULL; /* (the job lives on: on_export_options_destroy keeps it) */
        gtk_window_destroy(options);
    }
    export_choose_file(e, normalized);
}

static void on_export_options_cancel(GtkWidget *button, gpointer user)
{
    ExportJob *e = (ExportJob *)user;
    (void)button;
    gtk_window_destroy(e->options); /* (on_export_options_destroy frees the job) */
}

static void on_export_options_destroy(GtkWidget *widget, gpointer user)
{
    ExportJob *e = (ExportJob *)user;
    (void)widget;
    if (e->options != NULL)
    {
        export_job_free(e); /* (closed without OK) */
    }
}

static void export_rows(VoterWindow *w, gboolean selection)
{
    ExportJob *e;
    GtkWidget *box, *buttons, *ok, *cancel;
    if (w->voters == NULL || w->worker != NULL || w->job != NULL)
    {
        return;
    }
    e = g_new0(ExportJob, 1);
    e->w = w;
    e->selection = selection;
    if (selection)
    {
        e->rows = selected_rows(w, &e->n);
        if (e->n == 0)
        {
            gtk_label_set_text(w->status, "Nothing selected to export.");
            export_job_free(e);
            return;
        }
    }
    else if (EeUiVoters_Rows(w->voters) == 0)
    {
        gtk_label_set_text(w->status, "Nothing to export.");
        export_job_free(e);
        return;
    }
    if (w->roster)
    {
        /* (as on Windows: a roster exports its own columns, which reload into a roster) */
        export_choose_file(e, FALSE);
        return;
    }

    /* Export Options, as on Windows: one check box, off at first */
    e->options = GTK_WINDOW(gtk_window_new());
    gtk_window_set_title(e->options, "Export Options");
    gtk_window_set_transient_for(e->options, w->window);
    gtk_window_set_modal(e->options, TRUE);
    gtk_window_set_resizable(e->options, FALSE);
    gtk_window_set_destroy_with_parent(e->options, TRUE);
    box = gtk_box_new(GTK_ORIENTATION_VERTICAL, 12);
    gtk_widget_set_margin_start(box, 16);
    gtk_widget_set_margin_end(box, 16);
    gtk_widget_set_margin_top(box, 16);
    gtk_widget_set_margin_bottom(box, 12);
    e->normalized =
        GTK_CHECK_BUTTON(gtk_check_button_new_with_mnemonic("Include _normalized data fields"));
    gtk_box_append(GTK_BOX(box), GTK_WIDGET(e->normalized));
    buttons = gtk_box_new(GTK_ORIENTATION_HORIZONTAL, 6);
    gtk_widget_set_halign(buttons, GTK_ALIGN_END);
    ok = gtk_button_new_with_mnemonic("_OK");
    gtk_widget_add_css_class(ok, "suggested-action");
    cancel = gtk_button_new_with_mnemonic("_Cancel");
    g_signal_connect(ok, "clicked", G_CALLBACK(on_export_options_ok), e);
    g_signal_connect(cancel, "clicked", G_CALLBACK(on_export_options_cancel), e);
    gtk_box_append(GTK_BOX(buttons), ok);
    gtk_box_append(GTK_BOX(buttons), cancel);
    gtk_box_append(GTK_BOX(box), buttons);
    gtk_window_set_child(e->options, box);
    gtk_window_set_default_widget(e->options, ok);
    g_signal_connect(e->options, "destroy", G_CALLBACK(on_export_options_destroy), e);
    gtk_window_present(e->options);
}

/* -------------------------------------------------------------------------- */
/* The table                                                                  */
/* -------------------------------------------------------------------------- */

static void on_cell_right_click(GtkGestureClick *g, int n_press, double x, double y, gpointer user);

/* cells: a label per visible cell, set from the engine when bound to a row */
static void cell_setup(GtkSignalListItemFactory *f, GtkListItem *item, gpointer user)
{
    GtkWidget *label = gtk_label_new(NULL);
    GtkGesture *click = gtk_gesture_click_new();
    gboolean numeric = GPOINTER_TO_INT(g_object_get_data(G_OBJECT(f), "numeric"));
    gtk_label_set_xalign(GTK_LABEL(label), numeric ? 1.0f : 0.0f);
    gtk_label_set_ellipsize(GTK_LABEL(label), PANGO_ELLIPSIZE_END);
    gtk_label_set_single_line_mode(GTK_LABEL(label), TRUE);
    g_object_set_data(G_OBJECT(label), "column", g_object_get_data(G_OBJECT(f), "column"));
    gtk_gesture_single_set_button(GTK_GESTURE_SINGLE(click), GDK_BUTTON_SECONDARY);
    g_signal_connect(click, "pressed", G_CALLBACK(on_cell_right_click), user);
    gtk_widget_add_controller(label, GTK_EVENT_CONTROLLER(click));
    gtk_list_item_set_child(item, label);
}

static void cell_bind(GtkSignalListItemFactory *f, GtkListItem *item, gpointer user)
{
    VoterWindow *w = (VoterWindow *)user;
    EeRow *row = EE_ROW(gtk_list_item_get_item(item));
    GtkWidget *label = gtk_list_item_get_child(item);
    guint c = GPOINTER_TO_UINT(g_object_get_data(G_OBJECT(f), "column"));
    g_object_set_data(G_OBJECT(label), "row", GUINT_TO_POINTER(row->index));
    gtk_label_set_text(GTK_LABEL(label), EeUiVoters_Cell(w->voters, row->index, c));
}

/* a menu label for a cell value: at most 40 characters (as on Windows), underscores kept */
static char *menu_value(const char *value)
{
    char *shown, *escaped;
    GString *s;
    const char *p;
    if (g_utf8_strlen(value, -1) >= 40)
    {
        char *head = g_utf8_substring(value, 0, 36);
        shown = g_strconcat(head, "...", NULL);
        g_free(head);
    }
    else
    {
        shown = g_strdup(value);
    }
    s = g_string_new(NULL);
    for (p = shown; *p != '\0'; p++)
    {
        g_string_append_c(s, *p);
        if (*p == '_')
        {
            g_string_append_c(s, '_'); /* (a single underscore is a mnemonic) */
        }
    }
    g_free(shown);
    escaped = g_string_free(s, FALSE);
    return escaped;
}

static gboolean drop_cell_menu(gpointer user)
{
    VoterWindow *w = (VoterWindow *)user;
    if (!w->closed && w->cell_menu != NULL && !gtk_widget_get_visible(w->cell_menu))
    {
        gtk_widget_unparent(w->cell_menu);
        w->cell_menu = NULL;
    }
    return G_SOURCE_REMOVE;
}

static void on_cell_menu_closed(GtkPopover *popover, gpointer user)
{
    (void)popover;
    /* (later: the menu's action runs after "closed") */
    g_idle_add(drop_cell_menu, user);
}

static void on_cell_right_click(GtkGestureClick *g, int n_press, double x, double y, gpointer user)
{
    VoterWindow *w = (VoterWindow *)user;
    GtkWidget *label = gtk_event_controller_get_widget(GTK_EVENT_CONTROLLER(g));
    graphene_point_t at;
    GMenu *menu, *section;
    char *shown, *text;
    (void)n_press;
    if (w->voters == NULL || w->job != NULL)
    {
        return;
    }
    w->quick_column = GPOINTER_TO_UINT(g_object_get_data(G_OBJECT(label), "column"));
    g_free(w->quick_value);
    w->quick_value = g_strdup(gtk_label_get_text(GTK_LABEL(label)));
    {
        /* (as on Windows: a right-click outside the selection selects that row) */
        GtkSelectionModel *model = gtk_column_view_get_model(w->view);
        guint row = GPOINTER_TO_UINT(g_object_get_data(G_OBJECT(label), "row"));
        if (model != NULL && row < w->rows->count && !gtk_selection_model_is_selected(model, row))
        {
            gtk_selection_model_select_item(model, row, TRUE);
        }
    }

    /* Windows' order: Copy; Include, Exclude; Show in Maps... (an address); Open link... (a
     * file column's https:// link); Filter...; Export Selected... */
    menu = g_menu_new();
    section = g_menu_new();
    g_menu_append(section, "Copy", "win.copy");
    g_menu_append_section(menu, NULL, G_MENU_MODEL(section));
    g_object_unref(section);
    section = g_menu_new();
    shown = menu_value(w->quick_value);
    text = g_strdup_printf("Include \"%s\"", shown);
    g_menu_append(section, text, "win.quick-include");
    g_free(text);
    text = g_strdup_printf("Exclude \"%s\"", shown);
    g_menu_append(section, text, "win.quick-exclude");
    g_free(text);
    g_free(shown);
    g_menu_append_section(menu, NULL, G_MENU_MODEL(section));
    g_object_unref(section);
    if ((int)w->quick_column == EeUiVoters_AddressColumn(w->voters) && w->quick_value[0] != '\0')
    {
        section = g_menu_new();
        g_menu_append(section, "Show in Maps\xE2\x80\xA6", "win.show-in-maps");
        g_menu_append_section(menu, NULL, G_MENU_MODEL(section));
        g_object_unref(section);
    }
    free(w->quick_link);
    w->quick_link = EeUiVoters_CellLink(w->voters, w->quick_column, w->quick_value);
    if (w->quick_link != NULL)
    {
        section = g_menu_new();
        g_menu_append(section, "Open link\xE2\x80\xA6", "win.open-link");
        g_menu_append_section(menu, NULL, G_MENU_MODEL(section));
        g_object_unref(section);
    }
    section = g_menu_new();
    g_menu_append(section, "Filter\xE2\x80\xA6", "win.filter");
    g_menu_append_section(menu, NULL, G_MENU_MODEL(section));
    g_object_unref(section);
    section = g_menu_new();
    g_menu_append(section, "Export Selected\xE2\x80\xA6", "win.export-selected");
    g_menu_append_section(menu, NULL, G_MENU_MODEL(section));
    g_object_unref(section);
    /* (made for each click and removed once closed: the column view does not lay out
     * children it does not know, so none is left on it) */
    if (w->cell_menu != NULL)
    {
        gtk_widget_unparent(w->cell_menu);
    }
    w->cell_menu = gtk_popover_menu_new_from_model(G_MENU_MODEL(menu));
    gtk_widget_set_parent(w->cell_menu, GTK_WIDGET(w->view));
    gtk_popover_set_has_arrow(GTK_POPOVER(w->cell_menu), FALSE);
    gtk_widget_set_halign(w->cell_menu, GTK_ALIGN_START);
    g_signal_connect(w->cell_menu, "closed", G_CALLBACK(on_cell_menu_closed), w);
    g_object_unref(menu);

    if (gtk_widget_compute_point(label,
                                 GTK_WIDGET(w->view),
                                 &GRAPHENE_POINT_INIT((float)x, (float)y),
                                 &at))
    {
        GdkRectangle r = {(int)at.x, (int)at.y, 1, 1};
        gtk_popover_set_pointing_to(GTK_POPOVER(w->cell_menu), &r);
    }
    gtk_popover_popup(GTK_POPOVER(w->cell_menu));
    gtk_gesture_set_state(GTK_GESTURE(g), GTK_EVENT_SEQUENCE_CLAIMED);
}

/* the column sorters only carry the heading's arrow: the model is never sorted by them */
static int no_compare(gconstpointer a, gconstpointer b, gpointer user)
{
    (void)a;
    (void)b;
    (void)user;
    return 0;
}

/* (measured with the tables' digits of one width: see add_table_css) */
static int text_width(GtkWidget *widget, const char *text)
{
    PangoLayout *layout = gtk_widget_create_pango_layout(widget, text);
    PangoAttrList *attrs = pango_attr_list_new();
    int width = 0, height = 0;
    pango_attr_list_insert(attrs, pango_attr_font_features_new("tnum 1"));
    pango_layout_set_attributes(layout, attrs);
    pango_attr_list_unref(attrs);
    pango_layout_get_pixel_size(layout, &width, &height);
    g_object_unref(layout);
    return width;
}

static void remove_columns(VoterWindow *w)
{
    GListModel *cols = gtk_column_view_get_columns(w->view);
    while (g_list_model_get_n_items(cols) > 0)
    {
        GtkColumnViewColumn *col = g_list_model_get_item(cols, 0);
        gtk_column_view_remove_column(w->view, col);
        g_object_unref(col);
    }
}

static void rebuild_columns(VoterWindow *w)
{
    uint32_t c, r, nc = EeUiVoters_Columns(w->voters), nr = EeUiVoters_Rows(w->voters);
    GtkSelectionModel *sel;
    w->rebuilding = TRUE;
    remove_columns(w);
    for (c = 0; c < nc; c++)
    {
        const char *title = EeUiVoters_Title(w->voters, c);
        GtkListItemFactory *f = gtk_signal_list_item_factory_new();
        GtkColumnViewColumn *col;
        GtkSorter *sorter;
        int width = text_width(GTK_WIDGET(w->view), title) + 28; /* (room for the arrow) */
        for (r = 0; r < nr && r < 200; r++)
        {
            int cw = text_width(GTK_WIDGET(w->view), EeUiVoters_Cell(w->voters, r, c)) +
                     24; /* (cell padding) */
            width = cw > width ? cw : width;
        }
        width = width < 40 ? 40 : width > 320 ? 320 : width;
        g_object_set_data(G_OBJECT(f), "column", GUINT_TO_POINTER(c));
        g_object_set_data(G_OBJECT(f),
                          "numeric",
                          GINT_TO_POINTER(EeUiVoters_IsNumeric(w->voters, c)));
        g_signal_connect(f, "setup", G_CALLBACK(cell_setup), w);
        g_signal_connect(f, "bind", G_CALLBACK(cell_bind), w);
        col = gtk_column_view_column_new(title, f);
        g_object_set_data(G_OBJECT(col), "column", GUINT_TO_POINTER(c));
        gtk_column_view_column_set_resizable(col, TRUE);
        gtk_column_view_column_set_fixed_width(col, width);
        sorter = GTK_SORTER(gtk_custom_sorter_new(no_compare, NULL, NULL));
        gtk_column_view_column_set_sorter(col, sorter);
        g_object_unref(sorter);
        gtk_column_view_append_column(w->view, col);
        g_object_unref(col);
    }
    g_clear_object(&w->rows);
    w->rows = ee_rows_new(nr);
    sel = GTK_SELECTION_MODEL(gtk_multi_selection_new(G_LIST_MODEL(g_object_ref(w->rows))));
    gtk_column_view_set_model(w->view, sel);
    g_object_unref(sel);
    w->rebuilding = FALSE;
}

/* -------------------------------------------------------------------------- */
/* The Filter window (as on Windows: "Display entries matching these          */
/* conditions:", column / relation / value / then Include or Exclude)        */
/* -------------------------------------------------------------------------- */

struct FilterDialog
{
    VoterWindow *w;
    GtkWindow *win;
    GtkDropDown *column;
    GtkDropDown *relation;
    GtkDropDown *action;
    GtkEntry *value;
    GtkListBox *list;
    GArray *draft;         /* EeUiRule */
    GCancellable *pending; /* an Export / Load Filter file dialog or question that is open */
};

static void fd_alert_detail(FilterDialog *d, const char *message, const char *detail)
{
    GtkAlertDialog *a = gtk_alert_dialog_new("%s", message);
    if (detail != NULL && detail[0] != '\0')
    {
        gtk_alert_dialog_set_detail(a, detail);
    }
    gtk_alert_dialog_show(a, d->win);
    g_object_unref(a);
}

static void fd_alert(FilterDialog *d, const char *message)
{
    fd_alert_detail(d, message, NULL);
}

static void fd_refresh(FilterDialog *d, int select);

/* -------------------------------------------------------------------------- */
/* Export Filter... / Load Filter... (as on Windows: the listed rules, on or  */
/* off, to a UTF-8 CSV or TSV that names the columns by title)                */
/* -------------------------------------------------------------------------- */

static char *g_filter_folder; /* the last exported or loaded filter's folder (this run) */

static void fd_remember_folder(const char *path)
{
    g_free(g_filter_folder);
    g_filter_folder = g_path_get_dirname(path);
}

static void fd_dialog_folder(GtkFileDialog *dlg)
{
    if (g_filter_folder != NULL)
    {
        GFile *f = g_file_new_for_path(g_filter_folder);
        gtk_file_dialog_set_initial_folder(dlg, f);
        g_object_unref(f);
    }
}

static void fd_add_filter(GListStore *filters, const char *name, const char *const *suffixes)
{
    GtkFileFilter *f = gtk_file_filter_new();
    gtk_file_filter_set_name(f, name);
    for (; *suffixes != NULL; suffixes++)
    {
        if (strcmp(*suffixes, "*") == 0)
        {
            gtk_file_filter_add_pattern(f, "*");
        }
        else
        {
            gtk_file_filter_add_suffix(f, *suffixes);
        }
    }
    g_list_store_append(filters, f);
    g_object_unref(f);
}

/* the file dialog's answer: NULL when dismissed, or when the Filter window has closed (its
 * cancellable cancelled: @p d is gone then, so it is not touched) */
static char *fd_finish_path(GFile *file, GError *error, FilterDialog *d, gboolean *gone)
{
    char *path = NULL;
    *gone = error != NULL && g_error_matches(error, G_IO_ERROR, G_IO_ERROR_CANCELLED);
    if (error != NULL)
    {
        g_error_free(error);
    }
    if (!*gone)
    {
        g_clear_object(&d->pending);
    }
    if (file != NULL)
    {
        path = g_file_get_path(file);
        g_object_unref(file);
    }
    return path;
}

/* Save the listed rules to @p path (the name already made whole by EeUiFilterFile_Path) */
static gboolean fd_save(FilterDialog *d, const char *path, char delim)
{
    char err[256];
    if (!EeUiVoters_SaveFilter(d->w->voters,
                               path,
                               (const EeUiRule *)(void *)d->draft->data,
                               d->draft->len,
                               delim,
                               err,
                               sizeof(err)))
    {
        fd_alert(d, err);
        return FALSE;
    }
    fd_remember_folder(path);
    return TRUE;
}

typedef struct FdReplace
{
    FilterDialog *d;
    char *path;
    char delim;
} FdReplace;

static void fd_on_replace_answer(GObject *source, GAsyncResult *result, gpointer user)
{
    FdReplace *q = (FdReplace *)user;
    GError *error = NULL;
    int button = gtk_alert_dialog_choose_finish(GTK_ALERT_DIALOG(source), result, &error);
    gboolean gone = error != NULL && g_error_matches(error, G_IO_ERROR, G_IO_ERROR_CANCELLED);
    if (error != NULL)
    {
        g_error_free(error);
    }
    if (!gone)
    {
        g_clear_object(&q->d->pending);
        if (button == 1)
        {
            fd_save(q->d, q->path, q->delim);
        }
    }
    g_free(q->path);
    g_free(q);
}

/* Export Filter's file: a name without .csv / .tsv gets ".filter.csv", and Windows then asks
 * before replacing a file of that name (the save dialog asked only about the name typed) */
static void fd_save_chosen(FilterDialog *d, const char *chosen)
{
    char path[4096];
    char delim = EeUiFilterFile_Path(chosen, 0, path, sizeof(path));
    if (delim == 0)
    {
        fd_alert(d, "Could not create the filter file.");
        return;
    }
    if (strcmp(path, chosen) != 0 && g_file_test(path, G_FILE_TEST_EXISTS))
    {
        const char *buttons[] = {"_Cancel", "_Replace", NULL};
        char *base = g_path_get_basename(path);
        GtkAlertDialog *a = gtk_alert_dialog_new("%s already exists.", base);
        FdReplace *q = g_new0(FdReplace, 1);
        g_free(base);
        gtk_alert_dialog_set_detail(a, "Do you want to replace it?");
        gtk_alert_dialog_set_buttons(a, buttons);
        gtk_alert_dialog_set_cancel_button(a, 0);
        gtk_alert_dialog_set_default_button(a, 0);
        q->d = d;
        q->path = g_strdup(path);
        q->delim = delim;
        d->pending = g_cancellable_new();
        gtk_alert_dialog_choose(a, d->win, d->pending, fd_on_replace_answer, q);
        g_object_unref(a);
        return;
    }
    fd_save(d, path, delim);
}

static void fd_on_export_chosen(GObject *source, GAsyncResult *result, gpointer user)
{
    GError *error = NULL;
    GFile *file = gtk_file_dialog_save_finish(GTK_FILE_DIALOG(source), result, &error);
    gboolean gone;
    char *path = fd_finish_path(file, error, (FilterDialog *)user, &gone);
    if (!gone && path != NULL)
    {
        fd_save_chosen((FilterDialog *)user, path);
    }
    g_free(path);
}

static void fd_on_export(GtkWidget *button, gpointer user)
{
    FilterDialog *d = (FilterDialog *)user;
    GtkFileDialog *dlg;
    GListStore *filters;
    char name[512];
    static const char *const csv[] = {"filter.csv", NULL}, *const tsv[] = {"filter.tsv", NULL};
    (void)button;
    if (d->pending != NULL)
    {
        return;
    }
    if (d->draft->len == 0)
    {
        fd_alert(d, "There are no filter rules to export."); /* (Windows' words) */
        return;
    }
    filters = g_list_store_new(GTK_TYPE_FILE_FILTER);
    fd_add_filter(filters, "Election Explorer filter, CSV (*.filter.csv)", csv);
    fd_add_filter(filters, "Election Explorer filter, tab-delimited (*.filter.tsv)", tsv);
    EeUiFilterFile_Name(d->w->path, name, sizeof(name));
    dlg = gtk_file_dialog_new();
    gtk_file_dialog_set_title(dlg, "Export Filter");
    gtk_file_dialog_set_filters(dlg, G_LIST_MODEL(filters));
    gtk_file_dialog_set_initial_name(dlg, name);
    fd_dialog_folder(dlg);
    d->pending = g_cancellable_new();
    gtk_file_dialog_save(dlg, d->win, d->pending, fd_on_export_chosen, d);
    g_object_unref(dlg);
    g_object_unref(filters);
}

/* Load Filter's file: its rules replace the listed ones (they take effect on Apply or OK); the
 * rules this list cannot use are listed, as on Windows */
static void fd_load(FilterDialog *d, const char *path)
{
    EeUiRule *rules = NULL;
    uint32_t n = 0;
    char msg[256], detail[2048];
    int r = EeUiVoters_LoadFilter(d->w->voters,
                                  path,
                                  &rules,
                                  &n,
                                  msg,
                                  sizeof(msg),
                                  detail,
                                  sizeof(detail));
    fd_remember_folder(path);
    if (r <= 0)
    {
        fd_alert(d, msg);
        return;
    }
    g_array_set_size(d->draft, 0);
    if (n > 0)
    {
        g_array_append_vals(d->draft, rules, n);
    }
    free(rules);
    fd_refresh(d, -1);
    if (msg[0] != '\0')
    {
        fd_alert_detail(d, msg, detail);
    }
}

static void fd_on_load_chosen(GObject *source, GAsyncResult *result, gpointer user)
{
    GError *error = NULL;
    GFile *file = gtk_file_dialog_open_finish(GTK_FILE_DIALOG(source), result, &error);
    gboolean gone;
    char *path = fd_finish_path(file, error, (FilterDialog *)user, &gone);
    if (!gone && path != NULL)
    {
        fd_load((FilterDialog *)user, path);
    }
    g_free(path);
}

static void fd_on_load(GtkWidget *button, gpointer user)
{
    FilterDialog *d = (FilterDialog *)user;
    GtkFileDialog *dlg;
    GListStore *filters;
    static const char *const saved[] = {"filter.csv", "filter.tsv", NULL},
                             *const delimited[] = {"csv", "tsv", "txt", NULL},
                             *const all[] = {"*", NULL};
    (void)button;
    if (d->pending != NULL)
    {
        return;
    }
    filters = g_list_store_new(GTK_TYPE_FILE_FILTER);
    fd_add_filter(filters, "Election Explorer filters (*.filter.csv, *.filter.tsv)", saved);
    fd_add_filter(filters, "CSV or tab-delimited files (*.csv, *.tsv, *.txt)", delimited);
    fd_add_filter(filters, "All files", all);
    dlg = gtk_file_dialog_new();
    gtk_file_dialog_set_title(dlg, "Load Filter");
    gtk_file_dialog_set_filters(dlg, G_LIST_MODEL(filters));
    fd_dialog_folder(dlg);
    d->pending = g_cancellable_new();
    gtk_file_dialog_open(dlg, d->win, d->pending, fd_on_load_chosen, d);
    g_object_unref(dlg);
    g_object_unref(filters);
}

static void fd_on_toggled(GtkCheckButton *check, gpointer user)
{
    FilterDialog *d = (FilterDialog *)user;
    guint i = GPOINTER_TO_UINT(g_object_get_data(G_OBJECT(check), "rule"));
    if (i < d->draft->len)
    {
        g_array_index(d->draft, EeUiRule, i).enabled = gtk_check_button_get_active(check) ? 1 : 0;
        /* (the check box takes its click, so select its row here too) */
        gtk_list_box_select_row(d->list, gtk_list_box_get_row_at_index(d->list, (int)i));
    }
}

/* Rows are a check box (on/off) and a separate label: a check button's own label would take
   the click as a toggle, so a rule could never be selected for Remove. */
static void fd_refresh(FilterDialog *d, int select)
{
    GtkWidget *child;
    guint i;
    while ((child = gtk_widget_get_first_child(GTK_WIDGET(d->list))) != NULL)
    {
        gtk_list_box_remove(d->list, child);
    }
    for (i = 0; i < d->draft->len; i++)
    {
        const EeUiRule *r = &g_array_index(d->draft, EeUiRule, i);
        char *text = g_strdup_printf("%s %s \"%s\" then %s",
                                     EeUiVoters_Title(d->w->voters, r->column),
                                     EeUiVoters_RelationName(r->relation),
                                     r->value,
                                     r->exclude ? "Exclude" : "Include");
        GtkWidget *row = gtk_box_new(GTK_ORIENTATION_HORIZONTAL, 6);
        GtkWidget *check = gtk_check_button_new();
        GtkWidget *label = gtk_label_new(text);
        g_free(text);
        gtk_check_button_set_active(GTK_CHECK_BUTTON(check), r->enabled != 0);
        g_object_set_data(G_OBJECT(check), "rule", GUINT_TO_POINTER(i));
        g_signal_connect(check, "toggled", G_CALLBACK(fd_on_toggled), d);
        gtk_label_set_xalign(GTK_LABEL(label), 0.0f);
        gtk_widget_set_hexpand(label, TRUE);
        gtk_box_append(GTK_BOX(row), check);
        gtk_box_append(GTK_BOX(row), label);
        gtk_list_box_append(d->list, row);
    }
    if (select >= 0 && d->draft->len > 0)
    {
        if ((guint)select >= d->draft->len)
        {
            select = (int)d->draft->len - 1;
        }
        gtk_list_box_select_row(d->list, gtk_list_box_get_row_at_index(d->list, select));
    }
}

static void fd_on_row_selected(GtkListBox *list, GtkListBoxRow *row, gpointer user)
{
    FilterDialog *d = (FilterDialog *)user;
    const EeUiRule *r;
    (void)list;
    if (row == NULL || (guint)gtk_list_box_row_get_index(row) >= d->draft->len)
    {
        return;
    }
    /* (as on Windows: a selected rule fills the fields, to change it and Add again) */
    r = &g_array_index(d->draft, EeUiRule, gtk_list_box_row_get_index(row));
    gtk_drop_down_set_selected(d->column, r->column);
    gtk_drop_down_set_selected(d->relation, (guint)r->relation);
    gtk_drop_down_set_selected(d->action, r->exclude ? 1 : 0);
    gtk_editable_set_text(GTK_EDITABLE(d->value), r->value);
}

static void fd_on_add(GtkWidget *button, gpointer user)
{
    FilterDialog *d = (FilterDialog *)user;
    EeUiRule r;
    char why[256];
    guint i;
    (void)button;
    memset(&r, 0, sizeof(r));
    r.column = gtk_drop_down_get_selected(d->column);
    r.relation = (int)gtk_drop_down_get_selected(d->relation);
    r.exclude = gtk_drop_down_get_selected(d->action) == 1;
    r.enabled = 1;
    g_strlcpy(r.value, gtk_editable_get_text(GTK_EDITABLE(d->value)), sizeof(r.value));
    if (!EeUiVoters_RuleIsValid(d->w->voters, &r, why, sizeof(why)))
    {
        fd_alert(d, why);
        return;
    }
    for (i = 0; i < d->draft->len; i++)
    {
        EeUiRule *e = &g_array_index(d->draft, EeUiRule, i);
        if (e->column == r.column && e->relation == r.relation && e->exclude == r.exclude &&
            strcmp(e->value, r.value) == 0)
        {
            e->enabled = 1; /* (already listed: just turn it on) */
            fd_refresh(d, -1);
            return;
        }
    }
    g_array_append_val(d->draft, r);
    fd_refresh(d, -1);
}

static void fd_on_remove(GtkWidget *button, gpointer user)
{
    FilterDialog *d = (FilterDialog *)user;
    GtkListBoxRow *row = gtk_list_box_get_selected_row(d->list);
    int i;
    (void)button;
    if (row == NULL)
    {
        return; /* (as on Windows and the Mac: nothing selected, nothing removed) */
    }
    i = gtk_list_box_row_get_index(row);
    if (i >= 0 && (guint)i < d->draft->len)
    {
        g_array_remove_index(d->draft, (guint)i);
        fd_refresh(d, i); /* (the next rule stays selected, to Remove again) */
    }
}

static void fd_on_reset(GtkWidget *button, gpointer user)
{
    FilterDialog *d = (FilterDialog *)user;
    (void)button;
    g_array_set_size(d->draft, 0);
    fd_refresh(d, -1);
}

static void fd_on_apply(GtkWidget *button, gpointer user)
{
    FilterDialog *d = (FilterDialog *)user;
    (void)button;
    set_rules(d->w, (const EeUiRule *)(void *)d->draft->data, d->draft->len);
}

static void fd_on_ok(GtkWidget *button, gpointer user)
{
    FilterDialog *d = (FilterDialog *)user;
    fd_on_apply(button, user);
    gtk_window_destroy(d->win);
}

static void fd_on_cancel(GtkWidget *button, gpointer user)
{
    FilterDialog *d = (FilterDialog *)user;
    (void)button;
    gtk_window_destroy(d->win);
}

static void fd_on_destroy(GtkWidget *widget, gpointer user)
{
    FilterDialog *d = (FilterDialog *)user;
    (void)widget;
    if (!d->w->closed)
    {
        d->w->filter_dialog = NULL;
    }
    if (d->pending != NULL)
    {
        /* (an open file dialog or question: its answer finds this window gone) */
        g_cancellable_cancel(d->pending);
        g_clear_object(&d->pending);
    }
    g_array_unref(d->draft);
    g_free(d);
}

static GtkWidget *fd_button(const char *label, GCallback cb, FilterDialog *d)
{
    GtkWidget *b = gtk_button_new_with_mnemonic(label);
    g_signal_connect(b, "clicked", cb, d);
    return b;
}

static void show_filter_dialog(VoterWindow *w)
{
    FilterDialog *d;
    GtkWidget *box, *fields, *buttons, *scroll, *bottom, *prompt, *then, *ok, *spacer;
    GtkStringList *titles;
    GtkExpression *expr;
    const char *relations[EE_UI_REL_COUNT + 1];
    const char *actions[] = {"Include", "Exclude", NULL};
    uint32_t c;
    int i;
    if (w->voters == NULL)
    {
        return;
    }
    if (w->filter_dialog != NULL)
    {
        gtk_window_present(w->filter_dialog->win);
        return;
    }
    d = g_new0(FilterDialog, 1);
    d->w = w;
    d->draft = g_array_new(FALSE, TRUE, sizeof(EeUiRule));
    g_array_append_vals(d->draft, w->rules->data, w->rules->len);
    w->filter_dialog = d;

    d->win = GTK_WINDOW(gtk_window_new());
    gtk_window_set_title(d->win, "Filter");
    gtk_window_set_transient_for(d->win, w->window);
    gtk_window_set_modal(d->win, TRUE);
    gtk_window_set_default_size(d->win, 760, 420);
    gtk_window_set_destroy_with_parent(d->win, TRUE);

    box = gtk_box_new(GTK_ORIENTATION_VERTICAL, 8);
    gtk_widget_set_margin_start(box, 12);
    gtk_widget_set_margin_end(box, 12);
    gtk_widget_set_margin_top(box, 12);
    gtk_widget_set_margin_bottom(box, 12);
    prompt = gtk_label_new("Display entries matching these conditions:");
    gtk_label_set_xalign(GTK_LABEL(prompt), 0.0f);
    gtk_box_append(GTK_BOX(box), prompt);

    /* column / relation / value / then / action */
    fields = gtk_box_new(GTK_ORIENTATION_HORIZONTAL, 6);
    titles = gtk_string_list_new(NULL);
    for (c = 0; c < EeUiVoters_Columns(w->voters); c++)
    {
        gtk_string_list_append(titles, EeUiVoters_Title(w->voters, c));
    }
    d->column = GTK_DROP_DOWN(gtk_drop_down_new(G_LIST_MODEL(titles), NULL));
    expr = gtk_property_expression_new(GTK_TYPE_STRING_OBJECT, NULL, "string");
    gtk_drop_down_set_expression(d->column, expr);
    gtk_expression_unref(expr);
    gtk_drop_down_set_enable_search(d->column, TRUE); /* (216 columns in some lists) */
    for (i = 0; i < EE_UI_REL_COUNT; i++)
    {
        relations[i] = EeUiVoters_RelationName(i);
    }
    relations[EE_UI_REL_COUNT] = NULL;
    d->relation = GTK_DROP_DOWN(gtk_drop_down_new_from_strings(relations));
    d->value = GTK_ENTRY(gtk_entry_new());
    gtk_widget_set_hexpand(GTK_WIDGET(d->value), TRUE);
    g_signal_connect(d->value, "activate", G_CALLBACK(fd_on_add), d);
    then = gtk_label_new("then");
    d->action = GTK_DROP_DOWN(gtk_drop_down_new_from_strings(actions));
    gtk_box_append(GTK_BOX(fields), GTK_WIDGET(d->column));
    gtk_box_append(GTK_BOX(fields), GTK_WIDGET(d->relation));
    gtk_box_append(GTK_BOX(fields), GTK_WIDGET(d->value));
    gtk_box_append(GTK_BOX(fields), then);
    gtk_box_append(GTK_BOX(fields), GTK_WIDGET(d->action));
    gtk_box_append(GTK_BOX(box), fields);

    buttons = gtk_box_new(GTK_ORIENTATION_HORIZONTAL, 6);
    gtk_widget_set_halign(buttons, GTK_ALIGN_END);
    gtk_box_append(GTK_BOX(buttons), fd_button("R_eset", G_CALLBACK(fd_on_reset), d));
    gtk_box_append(GTK_BOX(buttons), fd_button("_Add", G_CALLBACK(fd_on_add), d));
    gtk_box_append(GTK_BOX(buttons), fd_button("_Remove", G_CALLBACK(fd_on_remove), d));
    gtk_box_append(GTK_BOX(box), buttons);

    d->list = GTK_LIST_BOX(gtk_list_box_new());
    gtk_list_box_set_selection_mode(d->list, GTK_SELECTION_SINGLE);
    g_signal_connect(d->list, "row-selected", G_CALLBACK(fd_on_row_selected), d);
    scroll = gtk_scrolled_window_new();
    gtk_scrolled_window_set_has_frame(GTK_SCROLLED_WINDOW(scroll), TRUE);
    gtk_scrolled_window_set_child(GTK_SCROLLED_WINDOW(scroll), GTK_WIDGET(d->list));
    gtk_widget_set_vexpand(scroll, TRUE);
    gtk_box_append(GTK_BOX(box), scroll);

    /* (Windows' bottom row: Export Filter... and Load Filter... at the left, OK, Cancel and
     * Apply at the right) */
    bottom = gtk_box_new(GTK_ORIENTATION_HORIZONTAL, 6);
    gtk_box_append(GTK_BOX(bottom),
                   fd_button("E_xport Filter\342\200\246", G_CALLBACK(fd_on_export), d));
    gtk_box_append(GTK_BOX(bottom),
                   fd_button("_Load Filter\342\200\246", G_CALLBACK(fd_on_load), d));
    spacer = gtk_label_new(NULL);
    gtk_widget_set_hexpand(spacer, TRUE);
    gtk_box_append(GTK_BOX(bottom), spacer);
    ok = fd_button("_OK", G_CALLBACK(fd_on_ok), d);
    gtk_widget_add_css_class(ok, "suggested-action");
    gtk_box_append(GTK_BOX(bottom), ok);
    gtk_box_append(GTK_BOX(bottom), fd_button("_Cancel", G_CALLBACK(fd_on_cancel), d));
    gtk_box_append(GTK_BOX(bottom), fd_button("A_pply", G_CALLBACK(fd_on_apply), d));
    gtk_box_append(GTK_BOX(box), bottom);

    gtk_window_set_child(d->win, box);
    g_signal_connect(d->win, "destroy", G_CALLBACK(fd_on_destroy), d);
    fd_refresh(d, -1);
    gtk_window_present(d->win);
}

/* -------------------------------------------------------------------------- */
/* Reports (as on Windows: Display Precinct / Address Report..., a window     */
/* each, a row per value with its Number of Voters, over all loaded rows)     */
/* -------------------------------------------------------------------------- */

struct ReportWindow
{
    VoterWindow *w;
    int kind; /* EE_UI_REPORT_* */
    int refs; /* the window, and each export dialog or menu still to finish */
    gboolean closed;
    GtkWindow *win;
    GtkColumnView *view;
    GtkLabel *status;
    GtkWidget *menu; /* the right-click menu while it is open */
    EeUiReport *report;
    EeRows *rows;
    char *list_path;
    guint menu_row; /* the right-clicked row */
    gboolean rebuilding;
};

static void report_unref(ReportWindow *rw)
{
    if (--rw->refs == 0)
    {
        EeUiReport_Free(rw->report);
        g_clear_object(&rw->rows);
        g_free(rw->list_path);
        g_free(rw);
    }
}

static void report_alert(ReportWindow *rw, const char *message)
{
    GtkAlertDialog *d = gtk_alert_dialog_new("%s", message);
    gtk_alert_dialog_show(d, rw->win);
    g_object_unref(d);
}

static uint32_t *report_selection(ReportWindow *rw, guint *n)
{
    return view_selection(rw->view, rw->rows->count, n);
}

/* Copy (Ctrl+C or the right-click menu): "<value>\t<count>" a line, as on Windows */
static void report_copy(ReportWindow *rw)
{
    guint n = 0;
    uint32_t *rows = report_selection(rw, &n);
    char *text = n > 0 ? EeUiReport_CopyText(rw->report, rows, n, NULL) : NULL;
    if (text != NULL)
    {
        gdk_clipboard_set_text(gtk_widget_get_clipboard(GTK_WIDGET(rw->win)), text);
        free(text);
    }
    g_free(rows);
}

/* Include / Exclude: a rule "<Precinct or Address> is <value>" in the voter list's filter for
 * each selected row (the blank row matches the empty cells) */
static void report_filter(ReportWindow *rw, gboolean exclude)
{
    VoterWindow *w = rw->w;
    guint n = 0, i;
    uint32_t *rows;
    if (w->closed || w->voters == NULL || w->worker != NULL)
    {
        return;
    }
    rows = report_selection(rw, &n);
    for (i = 0; i < n; i++)
    {
        EeUiRule r;
        EeUiReport_Rule(rw->report, rows[i], exclude ? 1 : 0, &r);
        g_array_append_val(w->rules, r);
    }
    g_free(rows);
    if (n > 0)
    {
        request_view(w);
    }
}

static void report_export_to(ReportWindow *rw, const char *path, const uint32_t *rows, guint n)
{
    char err[256];
    long wrote = EeUiReport_Export(rw->report, path, rows, n, export_delim(path), err, sizeof(err));
    if (wrote < 0)
    {
        fprintf(stderr, "report export: %s\n", err);
        report_alert(rw, err);
        return;
    }
    fprintf(stderr, "exported %ld report rows\n", wrote);
}

typedef struct ReportExport
{
    ReportWindow *rw;
    uint32_t *rows; /* NULL: all */
    guint n;
} ReportExport;

static void on_report_export_chosen(GObject *source, GAsyncResult *result, gpointer user)
{
    ReportExport *e = (ReportExport *)user;
    GFile *file = gtk_file_dialog_save_finish(GTK_FILE_DIALOG(source), result, NULL);
    if (file != NULL)
    {
        char *path = g_file_get_path(file);
        if (path != NULL && !e->rw->closed)
        {
            char *named = export_named(path);
            report_export_to(e->rw, named, e->rows, e->n);
            g_free(named);
        }
        g_free(path);
        g_object_unref(file);
    }
    report_unref(e->rw);
    g_free(e->rows);
    g_free(e);
}

/* Export Selected... / Export All...: where to save it, then the file (no options: a report
 * has no normalized fields) */
static void report_export(ReportWindow *rw, gboolean selection)
{
    ReportExport *e = g_new0(ReportExport, 1);
    GtkFileDialog *d;
    GListModel *filters;
    char name[512];
    if (selection)
    {
        e->rows = report_selection(rw, &e->n);
        if (e->n == 0)
        {
            g_free(e->rows);
            g_free(e);
            return;
        }
    }
    e->rw = rw;
    rw->refs++;
    d = gtk_file_dialog_new();
    filters = export_filters();
    EeUiReport_ExportName(rw->report, rw->list_path, selection, name, sizeof(name));
    gtk_file_dialog_set_title(d, "Export");
    gtk_file_dialog_set_filters(d, filters);
    gtk_file_dialog_set_initial_name(d, name);
    gtk_file_dialog_save(d, rw->win, NULL, on_report_export_chosen, e);
    g_object_unref(filters);
    g_object_unref(d);
}

static void on_maps_launched(GObject *source, GAsyncResult *result, gpointer user)
{
    ReportWindow *rw = (ReportWindow *)user;
    if (!gtk_uri_launcher_launch_finish(GTK_URI_LAUNCHER(source), result, NULL) && !rw->closed)
    {
        report_alert(rw, "Could not open the map in a browser.");
    }
    report_unref(rw);
}

/* Show in Maps... (Address report): the address in a browser's map */
static void report_maps(ReportWindow *rw, guint row)
{
    char *url = EeUiReport_MapUrl(rw->report, row, rw->w->opts.map_engine);
    GtkUriLauncher *launcher;
    if (url == NULL)
    {
        report_alert(rw, "Could not build a map link for this address.");
        return;
    }
    launcher = gtk_uri_launcher_new(url);
    rw->refs++;
    gtk_uri_launcher_launch(launcher, rw->win, NULL, on_maps_launched, rw);
    g_object_unref(launcher);
    free(url);
}

static void report_action(GSimpleAction *action, GVariant *param, gpointer user)
{
    ReportWindow *rw = (ReportWindow *)user;
    const char *name = g_action_get_name(G_ACTION(action));
    (void)param;
    if (rw->closed)
    {
        return;
    }
    if (strcmp(name, "copy") == 0)
    {
        report_copy(rw);
    }
    else if (strcmp(name, "include") == 0 || strcmp(name, "exclude") == 0)
    {
        report_filter(rw, name[0] == 'e');
    }
    else if (strcmp(name, "maps") == 0)
    {
        report_maps(rw, rw->menu_row);
    }
    else if (strcmp(name, "export-selected") == 0 || strcmp(name, "export-all") == 0)
    {
        report_export(rw, name[7] == 's');
    }
}

static gboolean report_drop_menu(gpointer user)
{
    ReportWindow *rw = (ReportWindow *)user;
    if (!rw->closed && rw->menu != NULL && !gtk_widget_get_visible(rw->menu))
    {
        gtk_widget_unparent(rw->menu);
        rw->menu = NULL;
    }
    report_unref(rw);
    return G_SOURCE_REMOVE;
}

static void on_report_menu_closed(GtkPopover *popover, gpointer user)
{
    ReportWindow *rw = (ReportWindow *)user;
    (void)popover;
    rw->refs++;
    g_idle_add(report_drop_menu, rw); /* (later: the menu's action runs after "closed") */
}

static void menu_section(GMenu *menu, GMenu *section)
{
    g_menu_append_section(menu, NULL, G_MENU_MODEL(section));
    g_object_unref(section);
}

/* the right-click menu, in Windows' order: Copy; Include, Exclude (on the value column); Show
 * in Maps... (an address); Export Selected..., Export All... */
static void on_report_right_click(GtkGestureClick *g,
                                  int n_press,
                                  double x,
                                  double y,
                                  gpointer user)
{
    ReportWindow *rw = (ReportWindow *)user;
    GtkWidget *label = gtk_event_controller_get_widget(GTK_EVENT_CONTROLLER(g));
    GtkSelectionModel *model = gtk_column_view_get_model(rw->view);
    guint row = GPOINTER_TO_UINT(g_object_get_data(G_OBJECT(label), "row"));
    guint column = GPOINTER_TO_UINT(g_object_get_data(G_OBJECT(label), "column"));
    GMenu *menu, *section;
    graphene_point_t at;
    (void)n_press;
    if (rw->closed || row >= rw->rows->count)
    {
        return;
    }
    rw->menu_row = row;
    if (!gtk_selection_model_is_selected(model, row))
    {
        gtk_selection_model_select_item(model, row, TRUE); /* (as on Windows) */
    }
    menu = g_menu_new();
    section = g_menu_new();
    g_menu_append(section, "Copy", "report.copy");
    menu_section(menu, section);
    if (column == EE_UI_REPORT_COLUMN_VALUE)
    {
        char *shown = menu_value(EeUiReport_Value(rw->report, row)), *text;
        section = g_menu_new();
        text = g_strdup_printf("Include \"%s\"", shown);
        g_menu_append(section, text, "report.include");
        g_free(text);
        text = g_strdup_printf("Exclude \"%s\"", shown);
        g_menu_append(section, text, "report.exclude");
        g_free(text);
        g_free(shown);
        menu_section(menu, section);
        if (rw->kind == EE_UI_REPORT_ADDRESS && !EeUiReport_IsBlank(rw->report, row))
        {
            section = g_menu_new();
            g_menu_append(section, "Show in Maps\xE2\x80\xA6", "report.maps");
            menu_section(menu, section);
        }
    }
    section = g_menu_new();
    g_menu_append(section, "Export Selected\xE2\x80\xA6", "report.export-selected");
    g_menu_append(section, "Export All\xE2\x80\xA6", "report.export-all");
    menu_section(menu, section);
    if (rw->menu != NULL)
    {
        gtk_widget_unparent(rw->menu);
    }
    rw->menu = gtk_popover_menu_new_from_model(G_MENU_MODEL(menu));
    gtk_widget_set_parent(rw->menu, GTK_WIDGET(rw->view));
    gtk_popover_set_has_arrow(GTK_POPOVER(rw->menu), FALSE);
    gtk_widget_set_halign(rw->menu, GTK_ALIGN_START);
    g_signal_connect(rw->menu, "closed", G_CALLBACK(on_report_menu_closed), rw);
    g_object_unref(menu);
    if (gtk_widget_compute_point(label,
                                 GTK_WIDGET(rw->view),
                                 &GRAPHENE_POINT_INIT((float)x, (float)y),
                                 &at))
    {
        GdkRectangle r = {(int)at.x, (int)at.y, 1, 1};
        gtk_popover_set_pointing_to(GTK_POPOVER(rw->menu), &r);
    }
    gtk_popover_popup(GTK_POPOVER(rw->menu));
    gtk_gesture_set_state(GTK_GESTURE(g), GTK_EVENT_SEQUENCE_CLAIMED);
}

static void report_cell_setup(GtkSignalListItemFactory *f, GtkListItem *item, gpointer user)
{
    ReportWindow *rw = (ReportWindow *)user;
    GtkWidget *label = gtk_label_new(NULL);
    GtkGesture *click = gtk_gesture_click_new();
    guint c = GPOINTER_TO_UINT(g_object_get_data(G_OBJECT(f), "column"));
    /* (as on Windows: Precinct and Number of Voters centered, Address at the left) */
    gtk_label_set_xalign(GTK_LABEL(label),
                         c == EE_UI_REPORT_COLUMN_VALUE && rw->kind == EE_UI_REPORT_ADDRESS ? 0.0f
                                                                                            : 0.5f);
    gtk_label_set_ellipsize(GTK_LABEL(label), PANGO_ELLIPSIZE_END);
    gtk_label_set_single_line_mode(GTK_LABEL(label), TRUE);
    g_object_set_data(G_OBJECT(label), "column", GUINT_TO_POINTER(c));
    gtk_gesture_single_set_button(GTK_GESTURE_SINGLE(click), GDK_BUTTON_SECONDARY);
    g_signal_connect(click, "pressed", G_CALLBACK(on_report_right_click), rw);
    gtk_widget_add_controller(label, GTK_EVENT_CONTROLLER(click));
    gtk_list_item_set_child(item, label);
}

static void report_cell_bind(GtkSignalListItemFactory *f, GtkListItem *item, gpointer user)
{
    ReportWindow *rw = (ReportWindow *)user;
    EeRow *row = EE_ROW(gtk_list_item_get_item(item));
    GtkWidget *label = gtk_list_item_get_child(item);
    guint c = GPOINTER_TO_UINT(g_object_get_data(G_OBJECT(f), "column"));
    g_object_set_data(G_OBJECT(label), "row", GUINT_TO_POINTER(row->index));
    if (c == EE_UI_REPORT_COLUMN_COUNT)
    {
        char num[16];
        g_snprintf(num, sizeof(num), "%u", EeUiReport_Count(rw->report, row->index));
        gtk_label_set_text(GTK_LABEL(label), num);
    }
    else
    {
        gtk_label_set_text(GTK_LABEL(label), EeUiReport_Value(rw->report, row->index));
    }
}

/* the report's arrow, or a heading click (--sort with --report) */
static void report_sort_by(ReportWindow *rw, guint column, gboolean ascending, gboolean quietly)
{
    GtkColumnViewColumn *col = g_list_model_get_item(gtk_column_view_get_columns(rw->view), column);
    if (col == NULL)
    {
        return;
    }
    rw->rebuilding = TRUE;
    gtk_column_view_sort_by_column(rw->view, NULL, GTK_SORT_ASCENDING); /* (one arrow only) */
    rw->rebuilding = quietly;
    gtk_column_view_sort_by_column(rw->view,
                                   col,
                                   ascending ? GTK_SORT_ASCENDING : GTK_SORT_DESCENDING);
    rw->rebuilding = FALSE;
    g_object_unref(col);
}

/* a heading clicked: sort the report (it is small: on the UI thread, as on Windows) */
static void on_report_sorter_changed(GtkSorter *sorter, GtkSorterChange change, gpointer user)
{
    ReportWindow *rw = (ReportWindow *)user;
    GtkColumnViewSorter *cvs = GTK_COLUMN_VIEW_SORTER(sorter);
    GtkColumnViewColumn *col;
    guint n = rw->rows->count, column;
    gboolean ascending;
    (void)change;
    if (rw->rebuilding || rw->closed)
    {
        return;
    }
    col = gtk_column_view_sorter_get_primary_sort_column(cvs);
    if (col == NULL)
    {
        return;
    }
    column = GPOINTER_TO_UINT(g_object_get_data(G_OBJECT(col), "column"));
    ascending = gtk_column_view_sorter_get_primary_sort_order(cvs) == GTK_SORT_ASCENDING;
    if (gtk_column_view_sorter_get_n_sort_columns(cvs) > 1)
    {
        report_sort_by(rw, column, ascending, TRUE); /* (one arrow only) */
    }
    EeUiReport_Sort(rw->report, (int)column, ascending);
    gtk_selection_model_unselect_all(gtk_column_view_get_model(rw->view));
    g_list_model_items_changed(G_LIST_MODEL(rw->rows), 0, n, n);
}

static void on_report_destroy(GtkWidget *widget, gpointer user)
{
    ReportWindow *rw = (ReportWindow *)user;
    (void)widget;
    rw->closed = TRUE;
    if (rw->menu != NULL)
    {
        gtk_widget_unparent(rw->menu);
        rw->menu = NULL;
    }
    if (rw->w->reports[rw->kind] == rw)
    {
        rw->w->reports[rw->kind] = NULL;
    }
    report_unref(rw);
}

/* Reports > Display Precinct / Address Report...: a window of its own (the open one is brought
 * forward); it closes when the list is reloaded or its voter list window closes */
static void show_report(VoterWindow *w, int kind)
{
    static const GActionEntry actions[] = {
        {"copy", report_action, NULL, NULL, NULL, {0}},
        {"include", report_action, NULL, NULL, NULL, {0}},
        {"exclude", report_action, NULL, NULL, NULL, {0}},
        {"maps", report_action, NULL, NULL, NULL, {0}},
        {"export-selected", report_action, NULL, NULL, NULL, {0}},
        {"export-all", report_action, NULL, NULL, NULL, {0}},
    };
    char err[256], text[600];
    EeUiReport *report;
    ReportWindow *rw;
    GSimpleActionGroup *group;
    GtkEventController *keys;
    GtkWidget *box, *scroll;
    GtkSelectionModel *sel;
    guint c;
    if (w->voters == NULL || w->worker != NULL || kind < 0 || kind >= EE_UI_REPORT_COUNT)
    {
        return;
    }
    if (w->reports[kind] != NULL)
    {
        gtk_window_present(w->reports[kind]->win);
        return;
    }
    {
        gint64 started = g_get_monotonic_time();
        report = EeUiVoters_Report(w->voters, kind, err, sizeof(err));
        if (report == NULL)
        {
            alert(w, err, NULL); /* ("No precinct information available", as on Windows) */
            return;
        }
        fprintf(stderr,
                "%s report: %u rows in %.2f s\n",
                EeUiReport_Heading(kind),
                EeUiReport_Rows(report),
                (g_get_monotonic_time() - started) / 1e6);
    }
    rw = g_new0(ReportWindow, 1);
    rw->w = w;
    rw->kind = kind;
    rw->refs = 1;
    rw->report = report;
    rw->list_path = g_strdup(w->path);
    rw->rows = ee_rows_new(EeUiReport_Rows(report));
    w->reports[kind] = rw;

    /* (as on Windows: a window of its own, not kept above the voter list) */
    rw->win = GTK_WINDOW(gtk_window_new());
    EeUiReport_WindowTitle(report, w->path, text, sizeof(text));
    gtk_window_set_title(rw->win, text);
    gtk_window_set_default_size(rw->win, kind == EE_UI_REPORT_ADDRESS ? 560 : 400, 600);
    group = g_simple_action_group_new();
    g_action_map_add_action_entries(G_ACTION_MAP(group), actions, G_N_ELEMENTS(actions), rw);
    gtk_widget_insert_action_group(GTK_WIDGET(rw->win), "report", G_ACTION_GROUP(group));
    g_object_unref(group);
    keys = gtk_shortcut_controller_new();
    gtk_shortcut_controller_add_shortcut(
        GTK_SHORTCUT_CONTROLLER(keys),
        gtk_shortcut_new(gtk_keyval_trigger_new(GDK_KEY_c, GDK_CONTROL_MASK),
                         gtk_named_action_new("report.copy")));
    gtk_shortcut_controller_set_scope(GTK_SHORTCUT_CONTROLLER(keys), GTK_SHORTCUT_SCOPE_GLOBAL);
    gtk_widget_add_controller(GTK_WIDGET(rw->win), keys);

    rw->view = GTK_COLUMN_VIEW(gtk_column_view_new(NULL));
    gtk_column_view_set_show_column_separators(rw->view, TRUE);
    gtk_column_view_set_show_row_separators(rw->view, TRUE); /* (Windows' grid lines) */
    gtk_column_view_set_reorderable(rw->view, FALSE);
    gtk_widget_add_css_class(GTK_WIDGET(rw->view), "data-table");
    for (c = 0; c < 2; c++)
    {
        GtkListItemFactory *f = gtk_signal_list_item_factory_new();
        GtkColumnViewColumn *col;
        GtkSorter *sorter;
        g_object_set_data(G_OBJECT(f), "column", GUINT_TO_POINTER(c));
        g_signal_connect(f, "setup", G_CALLBACK(report_cell_setup), rw);
        g_signal_connect(f, "bind", G_CALLBACK(report_cell_bind), rw);
        col = gtk_column_view_column_new(c == EE_UI_REPORT_COLUMN_VALUE ? EeUiReport_Heading(kind)
                                                                        : "Number of Voters",
                                         f);
        g_object_set_data(G_OBJECT(col), "column", GUINT_TO_POINTER(c));
        gtk_column_view_column_set_resizable(col, TRUE);
        if (c == EE_UI_REPORT_COLUMN_VALUE)
        {
            gtk_column_view_column_set_expand(col, TRUE);
        }
        else
        {
            /* (the heading, with room for the arrow) */
            gtk_column_view_column_set_fixed_width(
                col,
                text_width(GTK_WIDGET(rw->view), "Number of Voters") + 40);
        }
        sorter = GTK_SORTER(gtk_custom_sorter_new(no_compare, NULL, NULL));
        gtk_column_view_column_set_sorter(col, sorter);
        g_object_unref(sorter);
        gtk_column_view_append_column(rw->view, col);
        g_object_unref(col);
    }
    sel = GTK_SELECTION_MODEL(gtk_multi_selection_new(G_LIST_MODEL(g_object_ref(rw->rows))));
    gtk_column_view_set_model(rw->view, sel);
    g_object_unref(sel);
    report_sort_by(rw, EE_UI_REPORT_COLUMN_VALUE, TRUE, TRUE); /* (sorted by the value) */
    g_signal_connect(gtk_column_view_get_sorter(rw->view),
                     "changed",
                     G_CALLBACK(on_report_sorter_changed),
                     rw);
    scroll = gtk_scrolled_window_new();
    gtk_scrolled_window_set_child(GTK_SCROLLED_WINDOW(scroll), GTK_WIDGET(rw->view));
    gtk_widget_set_vexpand(scroll, TRUE);

    EeUiReport_StatusText(report, text, sizeof(text));
    rw->status = GTK_LABEL(gtk_label_new(text));
    gtk_label_set_xalign(rw->status, 0.0f);
    gtk_widget_set_margin_start(GTK_WIDGET(rw->status), 8);
    gtk_widget_set_margin_end(GTK_WIDGET(rw->status), 8);
    gtk_widget_set_margin_top(GTK_WIDGET(rw->status), 4);
    gtk_widget_set_margin_bottom(GTK_WIDGET(rw->status), 4);
    box = gtk_box_new(GTK_ORIENTATION_VERTICAL, 0);
    gtk_box_append(GTK_BOX(box), scroll);
    gtk_box_append(GTK_BOX(box), GTK_WIDGET(rw->status));
    gtk_window_set_child(rw->win, box);
    g_signal_connect(rw->win, "destroy", G_CALLBACK(on_report_destroy), rw);
    gtk_window_present(rw->win);
}

/* -------------------------------------------------------------------------- */
/* Voting Totals (a roster's Reports > Display Voting Totals..., as on Windows) */
/* -------------------------------------------------------------------------- */

struct TotalsWindow
{
    VoterWindow *w;
    int refs; /* the window, and each export dialog or menu still to finish */
    gboolean closed;
    GtkWindow *win;
    GtkColumnView *view;
    GtkWidget *menu; /* the right-click menu while it is open */
    EeUiTotals *totals;
    EeRows *rows;
    char *list_path;
};

static void totals_unref(TotalsWindow *tw)
{
    if (--tw->refs == 0)
    {
        EeUiTotals_Free(tw->totals);
        g_clear_object(&tw->rows);
        g_free(tw->list_path);
        g_free(tw);
    }
}

/* Copy (Ctrl+C or the right-click menu): a heading line, then the selected rows, as on
 * Windows */
static void totals_copy(TotalsWindow *tw)
{
    guint n = 0;
    uint32_t *rows = view_selection(tw->view, tw->rows->count, &n);
    char *text = n > 0 ? EeUiTotals_CopyText(tw->totals, rows, n, NULL) : NULL;
    if (text != NULL)
    {
        gdk_clipboard_set_text(gtk_widget_get_clipboard(GTK_WIDGET(tw->win)), text);
        free(text);
    }
    g_free(rows);
}

static void totals_export_to(TotalsWindow *tw, const char *path, const uint32_t *rows, guint n)
{
    char err[256];
    long wrote = EeUiTotals_Export(tw->totals, path, rows, n, export_delim(path), err, sizeof(err));
    if (wrote < 0)
    {
        GtkAlertDialog *d = gtk_alert_dialog_new("%s", err);
        fprintf(stderr, "voting totals export: %s\n", err);
        gtk_alert_dialog_show(d, tw->win);
        g_object_unref(d);
        return;
    }
    fprintf(stderr, "exported %ld voting totals rows\n", wrote);
}

typedef struct TotalsExport
{
    TotalsWindow *tw;
    uint32_t *rows; /* NULL: all */
    guint n;
} TotalsExport;

static void on_totals_export_chosen(GObject *source, GAsyncResult *result, gpointer user)
{
    TotalsExport *e = (TotalsExport *)user;
    GFile *file = gtk_file_dialog_save_finish(GTK_FILE_DIALOG(source), result, NULL);
    if (file != NULL)
    {
        char *path = g_file_get_path(file);
        if (path != NULL && !e->tw->closed)
        {
            char *named = export_named(path);
            totals_export_to(e->tw, named, e->rows, e->n);
            g_free(named);
        }
        g_free(path);
        g_object_unref(file);
    }
    totals_unref(e->tw);
    g_free(e->rows);
    g_free(e);
}

/* Export Selected... / Export All...: where to save it, then the file */
static void totals_export(TotalsWindow *tw, gboolean selection)
{
    TotalsExport *e = g_new0(TotalsExport, 1);
    GtkFileDialog *d;
    GListModel *filters;
    char name[512];
    if (selection)
    {
        e->rows = view_selection(tw->view, tw->rows->count, &e->n);
        if (e->n == 0)
        {
            g_free(e->rows);
            g_free(e);
            return;
        }
    }
    e->tw = tw;
    tw->refs++;
    d = gtk_file_dialog_new();
    filters = export_filters();
    EeUiTotals_ExportName(tw->totals, tw->list_path, selection, name, sizeof(name));
    gtk_file_dialog_set_title(d, "Export");
    gtk_file_dialog_set_filters(d, filters);
    gtk_file_dialog_set_initial_name(d, name);
    gtk_file_dialog_save(d, tw->win, NULL, on_totals_export_chosen, e);
    g_object_unref(filters);
    g_object_unref(d);
}

static void totals_action(GSimpleAction *action, GVariant *param, gpointer user)
{
    TotalsWindow *tw = (TotalsWindow *)user;
    const char *name = g_action_get_name(G_ACTION(action));
    (void)param;
    if (tw->closed)
    {
        return;
    }
    if (strcmp(name, "copy") == 0)
    {
        totals_copy(tw);
    }
    else if (strcmp(name, "export-selected") == 0 || strcmp(name, "export-all") == 0)
    {
        totals_export(tw, name[7] == 's');
    }
}

static gboolean totals_drop_menu(gpointer user)
{
    TotalsWindow *tw = (TotalsWindow *)user;
    if (!tw->closed && tw->menu != NULL && !gtk_widget_get_visible(tw->menu))
    {
        gtk_widget_unparent(tw->menu);
        tw->menu = NULL;
    }
    totals_unref(tw);
    return G_SOURCE_REMOVE;
}

static void on_totals_menu_closed(GtkPopover *popover, gpointer user)
{
    TotalsWindow *tw = (TotalsWindow *)user;
    (void)popover;
    tw->refs++;
    g_idle_add(totals_drop_menu, tw); /* (later: the menu's action runs after "closed") */
}

/* the right-click menu, as on Windows: Copy; Export Selected..., Export All... */
static void on_totals_right_click(GtkGestureClick *g,
                                  int n_press,
                                  double x,
                                  double y,
                                  gpointer user)
{
    TotalsWindow *tw = (TotalsWindow *)user;
    GtkWidget *label = gtk_event_controller_get_widget(GTK_EVENT_CONTROLLER(g));
    GtkSelectionModel *model = gtk_column_view_get_model(tw->view);
    guint row = GPOINTER_TO_UINT(g_object_get_data(G_OBJECT(label), "row"));
    GMenu *menu, *section;
    graphene_point_t at;
    (void)n_press;
    if (tw->closed || row >= tw->rows->count)
    {
        return;
    }
    if (!gtk_selection_model_is_selected(model, row))
    {
        gtk_selection_model_select_item(model, row, TRUE); /* (as on Windows) */
    }
    menu = g_menu_new();
    section = g_menu_new();
    g_menu_append(section, "Copy", "totals.copy");
    menu_section(menu, section);
    section = g_menu_new();
    g_menu_append(section, "Export Selected\xE2\x80\xA6", "totals.export-selected");
    g_menu_append(section, "Export All\xE2\x80\xA6", "totals.export-all");
    menu_section(menu, section);
    if (tw->menu != NULL)
    {
        gtk_widget_unparent(tw->menu);
    }
    tw->menu = gtk_popover_menu_new_from_model(G_MENU_MODEL(menu));
    gtk_widget_set_parent(tw->menu, GTK_WIDGET(tw->view));
    gtk_popover_set_has_arrow(GTK_POPOVER(tw->menu), FALSE);
    gtk_widget_set_halign(tw->menu, GTK_ALIGN_START);
    g_signal_connect(tw->menu, "closed", G_CALLBACK(on_totals_menu_closed), tw);
    g_object_unref(menu);
    if (gtk_widget_compute_point(label,
                                 GTK_WIDGET(tw->view),
                                 &GRAPHENE_POINT_INIT((float)x, (float)y),
                                 &at))
    {
        GdkRectangle r = {(int)at.x, (int)at.y, 1, 1};
        gtk_popover_set_pointing_to(GTK_POPOVER(tw->menu), &r);
    }
    gtk_popover_popup(GTK_POPOVER(tw->menu));
    gtk_gesture_set_state(GTK_GESTURE(g), GTK_EVENT_SEQUENCE_CLAIMED);
}

static void totals_cell_setup(GtkSignalListItemFactory *f, GtkListItem *item, gpointer user)
{
    TotalsWindow *tw = (TotalsWindow *)user;
    GtkWidget *label = gtk_label_new(NULL);
    GtkGesture *click = gtk_gesture_click_new();
    guint c = GPOINTER_TO_UINT(g_object_get_data(G_OBJECT(f), "column"));
    /* (as on Windows: the date and party at the left, the counts at the right) */
    gtk_label_set_xalign(GTK_LABEL(label), EeUiTotals_IsTextColumn(tw->totals, c) ? 0.0f : 1.0f);
    gtk_label_set_ellipsize(GTK_LABEL(label), PANGO_ELLIPSIZE_END);
    gtk_label_set_single_line_mode(GTK_LABEL(label), TRUE);
    gtk_gesture_single_set_button(GTK_GESTURE_SINGLE(click), GDK_BUTTON_SECONDARY);
    g_signal_connect(click, "pressed", G_CALLBACK(on_totals_right_click), tw);
    gtk_widget_add_controller(label, GTK_EVENT_CONTROLLER(click));
    gtk_list_item_set_child(item, label);
}

static void totals_cell_bind(GtkSignalListItemFactory *f, GtkListItem *item, gpointer user)
{
    TotalsWindow *tw = (TotalsWindow *)user;
    EeRow *row = EE_ROW(gtk_list_item_get_item(item));
    GtkWidget *label = gtk_list_item_get_child(item);
    guint c = GPOINTER_TO_UINT(g_object_get_data(G_OBJECT(f), "column"));
    char text[256];
    g_object_set_data(G_OBJECT(label), "row", GUINT_TO_POINTER(row->index));
    EeUiTotals_Cell(tw->totals, row->index, c, 1, text, sizeof(text));
    gtk_label_set_text(GTK_LABEL(label), text);
    if (EeUiTotals_IsBold(tw->totals, row->index))
    {
        /* (the Total and All parties rows in bold, as on Windows) */
        PangoAttrList *bold = pango_attr_list_new();
        pango_attr_list_insert(bold, pango_attr_weight_new(PANGO_WEIGHT_BOLD));
        gtk_label_set_attributes(GTK_LABEL(label), bold);
        pango_attr_list_unref(bold);
    }
    else
    {
        gtk_label_set_attributes(GTK_LABEL(label), NULL);
    }
}

static void on_totals_destroy(GtkWidget *widget, gpointer user)
{
    TotalsWindow *tw = (TotalsWindow *)user;
    (void)widget;
    tw->closed = TRUE;
    if (tw->menu != NULL)
    {
        gtk_widget_unparent(tw->menu);
        tw->menu = NULL;
    }
    if (tw->w->totals == tw)
    {
        tw->w->totals = NULL;
    }
    totals_unref(tw);
}

/* Reports > Display Voting Totals... (a roster): a window of its own (the open one is brought
 * forward), not sortable; it closes when the roster window closes, as on Windows */
static void show_totals(VoterWindow *w)
{
    static const GActionEntry actions[] = {
        {"copy", totals_action, NULL, NULL, NULL, {0}},
        {"export-selected", totals_action, NULL, NULL, NULL, {0}},
        {"export-all", totals_action, NULL, NULL, NULL, {0}},
    };
    char err[256], text[600];
    EeUiTotals *totals;
    TotalsWindow *tw;
    GSimpleActionGroup *group;
    GtkEventController *keys;
    GtkWidget *box, *scroll;
    GtkLabel *status;
    GtkSelectionModel *sel;
    guint c;
    int total_width = 24; /* (the window: the columns and the scroll bar) */
    gint64 started = g_get_monotonic_time();
    if (w->voters == NULL || w->worker != NULL || !EeUiVoters_IsRoster(w->voters))
    {
        return;
    }
    if (w->totals != NULL)
    {
        gtk_window_present(w->totals->win);
        return;
    }
    totals = EeUiVoters_Totals(w->voters, err, sizeof(err));
    if (totals == NULL)
    {
        alert(w, err, NULL);
        return;
    }
    fprintf(stderr,
            "voting totals: %u rows in %.2f s\n",
            EeUiTotals_Rows(totals),
            (g_get_monotonic_time() - started) / 1e6);
    tw = g_new0(TotalsWindow, 1);
    tw->w = w;
    tw->refs = 1;
    tw->totals = totals;
    tw->list_path = g_strdup(w->path);
    tw->rows = ee_rows_new(EeUiTotals_Rows(totals));
    w->totals = tw;

    tw->win = GTK_WINDOW(gtk_window_new());
    EeUiTotals_WindowTitle(totals, w->path, text, sizeof(text));
    gtk_window_set_title(tw->win, text);
    group = g_simple_action_group_new();
    g_action_map_add_action_entries(G_ACTION_MAP(group), actions, G_N_ELEMENTS(actions), tw);
    gtk_widget_insert_action_group(GTK_WIDGET(tw->win), "totals", G_ACTION_GROUP(group));
    g_object_unref(group);
    keys = gtk_shortcut_controller_new();
    gtk_shortcut_controller_add_shortcut(
        GTK_SHORTCUT_CONTROLLER(keys),
        gtk_shortcut_new(gtk_keyval_trigger_new(GDK_KEY_c, GDK_CONTROL_MASK),
                         gtk_named_action_new("totals.copy")));
    gtk_shortcut_controller_set_scope(GTK_SHORTCUT_CONTROLLER(keys), GTK_SHORTCUT_SCOPE_GLOBAL);
    gtk_widget_add_controller(GTK_WIDGET(tw->win), keys);

    tw->view = GTK_COLUMN_VIEW(gtk_column_view_new(NULL));
    gtk_column_view_set_show_column_separators(tw->view, TRUE);
    gtk_column_view_set_show_row_separators(tw->view, TRUE); /* (Windows' grid lines) */
    gtk_column_view_set_reorderable(tw->view, FALSE);
    gtk_widget_add_css_class(GTK_WIDGET(tw->view), "data-table");
    for (c = 0; c < EeUiTotals_Columns(totals); c++)
    {
        GtkListItemFactory *f = gtk_signal_list_item_factory_new();
        GtkColumnViewColumn *col;
        const char *title = EeUiTotals_Title(totals, c);
        int width = EeUiTotals_ColumnWidth(totals, c), need;
        g_object_set_data(G_OBJECT(f), "column", GUINT_TO_POINTER(c));
        g_signal_connect(f, "setup", G_CALLBACK(totals_cell_setup), tw);
        g_signal_connect(f, "bind", G_CALLBACK(totals_cell_bind), tw);
        col = gtk_column_view_column_new(title, f);
        gtk_column_view_column_set_resizable(col, TRUE);
        /* (the heading fits: it is bold, with the header's padding) */
        need = text_width(GTK_WIDGET(tw->view), title) * 11 / 10 + 32;
        width = need > width ? need : width;
        total_width += width;
        gtk_column_view_column_set_fixed_width(col, width);
        gtk_column_view_append_column(tw->view, col);
        g_object_unref(col);
    }
    gtk_window_set_default_size(tw->win, total_width, 600);
    sel = GTK_SELECTION_MODEL(gtk_multi_selection_new(G_LIST_MODEL(g_object_ref(tw->rows))));
    gtk_column_view_set_model(tw->view, sel);
    g_object_unref(sel);
    scroll = gtk_scrolled_window_new();
    gtk_scrolled_window_set_child(GTK_SCROLLED_WINDOW(scroll), GTK_WIDGET(tw->view));
    gtk_widget_set_vexpand(scroll, TRUE);

    EeUiTotals_StatusText(totals, text, sizeof(text));
    status = GTK_LABEL(gtk_label_new(text));
    gtk_label_set_xalign(status, 0.0f);
    gtk_label_set_ellipsize(status, PANGO_ELLIPSIZE_END);
    gtk_widget_set_margin_start(GTK_WIDGET(status), 8);
    gtk_widget_set_margin_end(GTK_WIDGET(status), 8);
    gtk_widget_set_margin_top(GTK_WIDGET(status), 4);
    gtk_widget_set_margin_bottom(GTK_WIDGET(status), 4);
    box = gtk_box_new(GTK_ORIENTATION_VERTICAL, 0);
    gtk_box_append(GTK_BOX(box), scroll);
    gtk_box_append(GTK_BOX(box), GTK_WIDGET(status));
    gtk_window_set_child(tw->win, box);
    g_signal_connect(tw->win, "destroy", G_CALLBACK(on_totals_destroy), tw);
    gtk_window_present(tw->win);
}

static void close_reports(VoterWindow *w)
{
    int k;
    for (k = 0; k < EE_UI_REPORT_COUNT; k++)
    {
        if (w->reports[k] != NULL)
        {
            gtk_window_destroy(w->reports[k]->win); /* (on_report_destroy clears the slot) */
        }
    }
    if (w->totals != NULL)
    {
        gtk_window_destroy(w->totals->win); /* (on_totals_destroy clears the slot) */
    }
}

/* the Reports items: only with a list loaded (Windows grays them) */
static void enable_reports(VoterWindow *w, gboolean on)
{
    static const char *const names[] = {"report-precinct",
                                        "report-address",
                                        "report-totals",
                                        "report-summary"};
    size_t i;
    for (i = 0; i < G_N_ELEMENTS(names); i++)
    {
        GAction *a = g_action_map_lookup_action(G_ACTION_MAP(w->window), names[i]);
        if (a != NULL)
        {
            g_simple_action_set_enabled(G_SIMPLE_ACTION(a), on);
        }
    }
}

/* -------------------------------------------------------------------------- */
/* Options (as on Windows: Edit > Options..., kept between runs)              */
/* -------------------------------------------------------------------------- */

struct OptionsDialog
{
    VoterWindow *w;
    GtkWindow *win;
    GtkCheckButton *prepend;
    GtkCheckButton *surname;
    GtkSpinButton *zoom;
    GtkDropDown *map;
};

/* The tables' digits all of one width ("tnum"), as Windows' Segoe UI has them, so IDs and
 * counts line up whatever the font: Ubuntu's own has them so, but not every font does. Once
 * for the display, for the voter list and the report windows. */
static void add_table_css(GdkDisplay *display)
{
    static GdkDisplay *added;
    GtkCssProvider *css;
    if (added == display)
    {
        return;
    }
    css = gtk_css_provider_new();
    gtk_css_provider_load_from_string(css, "columnview { font-feature-settings: \"tnum\"; }");
    gtk_style_context_add_provider_for_display(display,
                                               GTK_STYLE_PROVIDER(css),
                                               GTK_STYLE_PROVIDER_PRIORITY_APPLICATION);
    g_object_unref(css);
    added = display;
}

/* Zoom: the list's text size (its table only, as on Windows), and the columns' widths with it */
static void apply_zoom(VoterWindow *w, int zoom)
{
    char css[96];
    int old = w->zoom_shown > 0 ? w->zoom_shown : EE_UI_ZOOM_DEFAULT;
    GListModel *cols = gtk_column_view_get_columns(w->view);
    guint i;
    zoom = EeUiOptions_ClampZoom(zoom);
    if (w->zoom_css == NULL)
    {
        w->zoom_css = gtk_css_provider_new();
        gtk_style_context_add_provider_for_display(gtk_widget_get_display(GTK_WIDGET(w->window)),
                                                   GTK_STYLE_PROVIDER(w->zoom_css),
                                                   GTK_STYLE_PROVIDER_PRIORITY_APPLICATION);
    }
    g_snprintf(css, sizeof(css), "columnview.voter-list { font-size: %d%%; }", zoom);
    gtk_css_provider_load_from_string(w->zoom_css, css);
    w->zoom_shown = zoom;
    for (i = 0; old != zoom && i < g_list_model_get_n_items(cols); i++)
    {
        GtkColumnViewColumn *col = g_list_model_get_item(cols, i);
        /* (the text scales, the cell's padding of 24 does not: see rebuild_columns) */
        int width = gtk_column_view_column_get_fixed_width(col) - 24;
        width = (width > 0 ? width : 0) * zoom / old + 24;
        gtk_column_view_column_set_fixed_width(col, width < 40 ? 40 : width);
        g_object_unref(col);
    }
}

/* Display name in surname-first format: rebuild the Name column on the UI thread, as on
 * Windows (not while a sort or filter reads the cells: once it is done; a load in progress
 * composes them as the options had them, and load_done brings them up to date) */
static void apply_names(VoterWindow *w)
{
    gint64 started;
    int r;
    if (w->voters == NULL || w->worker != NULL ||
        EeUiVoters_SurnameFirst(w->voters) == (w->opts.surname_first != 0))
    {
        w->names_pending = FALSE;
        return;
    }
    if (w->job != NULL)
    {
        w->names_pending = TRUE;
        return;
    }
    w->names_pending = FALSE;
    started = g_get_monotonic_time();
    r = EeUiVoters_SetSurnameFirst(w->voters, w->opts.surname_first);
    if (r < 0)
    {
        alert(w, "Could not update the Name column.", NULL);
        return;
    }
    fprintf(stderr, "names took %.2f s\n", (g_get_monotonic_time() - started) / 1e6);
    g_list_model_items_changed(G_LIST_MODEL(w->rows), 0, w->rows->count, w->rows->count);
    gtk_label_set_text(w->status,
                       w->opts.surname_first ? "Names shown surname-first."
                                             : "Names shown given-name first.");
    if (r > 0)
    {
        request_view(w); /* (sorted by Name, or a rule on it) */
    }
}

/* the options as the Options window's OK sets them; saved for the next run when @p save */
static void set_options(VoterWindow *w, const EeUiOptions *o, gboolean save)
{
    w->opts = *o;
    w->opts.zoom_percent = EeUiOptions_ClampZoom(o->zoom_percent);
    apply_zoom(w, w->opts.zoom_percent);
    apply_names(w);
    if (save && !EeUiOptions_Save(&w->opts))
    {
        fprintf(stderr, "could not save the options\n");
    }
}

static void od_on_ok(GtkButton *button, gpointer user)
{
    OptionsDialog *d = (OptionsDialog *)user;
    VoterWindow *w = d->w;
    EeUiOptions o = w->opts;
    (void)button;
    if (w->roster)
    {
        /* (the two it does not show are kept as saved, perhaps by another window since) */
        EeUiOptions saved;
        EeUiOptions_Load(&saved);
        gtk_check_button_set_active(d->prepend, saved.copy_normalized != 0);
        gtk_drop_down_set_selected(d->map, (guint)saved.map_engine);
    }
    o.copy_normalized = gtk_check_button_get_active(d->prepend) ? 1 : 0;
    o.surname_first = gtk_check_button_get_active(d->surname) ? 1 : 0;
    gtk_spin_button_update(d->zoom); /* (a number typed but not yet taken) */
    o.zoom_percent = gtk_spin_button_get_value_as_int(d->zoom);
    o.map_engine = (int)gtk_drop_down_get_selected(d->map);
    gtk_window_destroy(d->win);
    set_options(w, &o, TRUE);
}

static void od_on_cancel(GtkButton *button, gpointer user)
{
    OptionsDialog *d = (OptionsDialog *)user;
    (void)button;
    gtk_window_destroy(d->win);
}

static void od_on_destroy(GtkWidget *widget, gpointer user)
{
    OptionsDialog *d = (OptionsDialog *)user;
    (void)widget;
    if (d->w->options_dialog == d)
    {
        d->w->options_dialog = NULL;
    }
    g_free(d);
}

/* Edit > Options...: Windows' four options, OK and Cancel (Esc) */
static void show_options_dialog(VoterWindow *w)
{
    OptionsDialog *d;
    GtkWidget *box, *row, *label, *bottom, *ok, *cancel;
    GtkShortcutController *keys;
    const char *maps[EE_UI_MAP_COUNT + 1];
    int i;
    if (w->options_dialog != NULL)
    {
        gtk_window_present(w->options_dialog->win);
        return;
    }
    d = g_new0(OptionsDialog, 1);
    d->w = w;
    w->options_dialog = d;
    d->win = GTK_WINDOW(gtk_window_new());
    gtk_window_set_title(d->win, "Options");
    gtk_window_set_transient_for(d->win, w->window);
    gtk_window_set_modal(d->win, TRUE);
    gtk_window_set_resizable(d->win, FALSE);
    gtk_window_set_destroy_with_parent(d->win, TRUE);

    box = gtk_box_new(GTK_ORIENTATION_VERTICAL, 10);
    gtk_widget_set_margin_start(box, 16);
    gtk_widget_set_margin_end(box, 16);
    gtk_widget_set_margin_top(box, 16);
    gtk_widget_set_margin_bottom(box, 16);
    d->prepend = GTK_CHECK_BUTTON(gtk_check_button_new_with_label(
        "Pre-pend normalized data for copies (ID, Precinct, Name, Address)"));
    gtk_check_button_set_active(d->prepend, w->opts.copy_normalized != 0);
    gtk_box_append(GTK_BOX(box), GTK_WIDGET(d->prepend));
    d->surname =
        GTK_CHECK_BUTTON(gtk_check_button_new_with_label("Display name in surname-first format"));
    gtk_check_button_set_active(d->surname, w->opts.surname_first != 0);
    gtk_box_append(GTK_BOX(box), GTK_WIDGET(d->surname));

    row = gtk_box_new(GTK_ORIENTATION_HORIZONTAL, 6);
    label = gtk_label_new("Zoom:");
    gtk_box_append(GTK_BOX(row), label);
    d->zoom = GTK_SPIN_BUTTON(gtk_spin_button_new_with_range(EE_UI_ZOOM_MIN, EE_UI_ZOOM_MAX, 1));
    gtk_spin_button_set_numeric(d->zoom, TRUE);
    gtk_spin_button_set_value(d->zoom, EeUiOptions_ClampZoom(w->opts.zoom_percent));
    gtk_editable_set_width_chars(GTK_EDITABLE(d->zoom), 4);
    gtk_box_append(GTK_BOX(row), GTK_WIDGET(d->zoom));
    gtk_box_append(GTK_BOX(row), gtk_label_new("%  (50\342\200\223250)")); /* (an en dash) */
    gtk_box_append(GTK_BOX(box), row);

    row = gtk_box_new(GTK_ORIENTATION_HORIZONTAL, 6);
    gtk_box_append(GTK_BOX(row), gtk_label_new("Map engine:"));
    for (i = 0; i < EE_UI_MAP_COUNT; i++)
    {
        maps[i] = EeUiOptions_MapName(i);
    }
    maps[EE_UI_MAP_COUNT] = NULL;
    d->map = GTK_DROP_DOWN(gtk_drop_down_new_from_strings(maps));
    gtk_drop_down_set_selected(d->map, (guint)w->opts.map_engine);
    gtk_box_append(GTK_BOX(row), GTK_WIDGET(d->map));
    gtk_box_append(GTK_BOX(box), row);
    if (w->roster)
    {
        /* (Windows' roster Options: no Copy prefix, since a roster copies its own columns, and
         * no map, since it has no addresses) */
        gtk_widget_set_visible(GTK_WIDGET(d->prepend), FALSE);
        gtk_widget_set_visible(row, FALSE);
    }

    bottom = gtk_box_new(GTK_ORIENTATION_HORIZONTAL, 6);
    gtk_widget_set_halign(bottom, GTK_ALIGN_END);
    gtk_widget_set_margin_top(bottom, 8);
    ok = gtk_button_new_with_mnemonic("_OK");
    gtk_widget_add_css_class(ok, "suggested-action");
    g_signal_connect(ok, "clicked", G_CALLBACK(od_on_ok), d);
    cancel = gtk_button_new_with_mnemonic("_Cancel");
    g_signal_connect(cancel, "clicked", G_CALLBACK(od_on_cancel), d);
    gtk_box_append(GTK_BOX(bottom), ok);
    gtk_box_append(GTK_BOX(bottom), cancel);
    gtk_box_append(GTK_BOX(box), bottom);

    /* Enter is OK (also in the zoom box), Esc is Cancel, as in a Windows dialog */
    gtk_window_set_default_widget(d->win, ok);
    {
        /* (a spin button's text is a GtkText; gtk_spin_button_set_activates_default is 4.14) */
        GtkEditable *text = gtk_editable_get_delegate(GTK_EDITABLE(d->zoom));
        if (GTK_IS_TEXT(text))
        {
            gtk_text_set_activates_default(GTK_TEXT(text), TRUE);
        }
    }
    keys = GTK_SHORTCUT_CONTROLLER(gtk_shortcut_controller_new());
    gtk_shortcut_controller_add_shortcut(
        keys,
        gtk_shortcut_new(gtk_shortcut_trigger_parse_string("Escape"),
                         gtk_named_action_new("window.close")));
    gtk_widget_add_controller(GTK_WIDGET(d->win), GTK_EVENT_CONTROLLER(keys));

    gtk_window_set_child(d->win, box);
    g_signal_connect(d->win, "destroy", G_CALLBACK(od_on_destroy), d);
    gtk_window_present(d->win);
}

/* Show in Maps... on an Address cell: the address in the Options' map, in a browser */
static void on_cell_maps_launched(GObject *source, GAsyncResult *result, gpointer user)
{
    VoterWindow *w = (VoterWindow *)user;
    if (!gtk_uri_launcher_launch_finish(GTK_URI_LAUNCHER(source), result, NULL) && !w->closed)
    {
        alert(w, "Could not open the map in a browser.", NULL);
    }
}

static void show_in_maps(VoterWindow *w)
{
    char *url = EeUiOptions_MapUrl(w->opts.map_engine, w->quick_value);
    GtkUriLauncher *launcher;
    if (url == NULL)
    {
        alert(w, "Could not build a map link for this address.", NULL);
        return;
    }
    launcher = gtk_uri_launcher_new(url);
    gtk_uri_launcher_launch(launcher, w->window, NULL, on_cell_maps_launched, w);
    g_object_unref(launcher);
    free(url);
}

/* Open link... on a cell of the file's own columns: its https:// link, in a browser */
static void on_link_launched(GObject *source, GAsyncResult *result, gpointer user)
{
    VoterWindow *w = (VoterWindow *)user;
    if (!gtk_uri_launcher_launch_finish(GTK_URI_LAUNCHER(source), result, NULL) && !w->closed)
    {
        alert(w, "Could not open the link in a browser.", NULL);
    }
}

static void open_link(VoterWindow *w)
{
    GtkUriLauncher *launcher;
    if (w->quick_link == NULL)
    {
        return;
    }
    launcher = gtk_uri_launcher_new(w->quick_link);
    gtk_uri_launcher_launch(launcher, w->window, NULL, on_link_launched, w);
    g_object_unref(launcher);
}

/* -------------------------------------------------------------------------- */
/* A roster's Load Summary (Windows' "Voter Roster Load Summary")              */
/* -------------------------------------------------------------------------- */

static void on_summary_destroy(GtkWidget *widget, gpointer user)
{
    VoterWindow *w = (VoterWindow *)user;
    (void)widget;
    w->summary = NULL;
}

static void on_summary_ok(GtkButton *button, gpointer user)
{
    (void)button;
    gtk_window_destroy(GTK_WINDOW(user));
}

/* Reports > Display Load Summary...: what the load read and skipped, in a text window with
 * OK, as Windows shows it (modal, over the roster) */
static void show_load_summary(VoterWindow *w)
{
    const char *text = EeUiVoters_LoadSummary(w->voters, NULL);
    GtkWidget *box, *scroll, *view, *bottom, *ok;
    GtkShortcutController *keys;
    if (text == NULL)
    {
        return;
    }
    if (w->summary != NULL)
    {
        gtk_window_present(w->summary);
        return;
    }
    w->summary = GTK_WINDOW(gtk_window_new());
    gtk_window_set_title(w->summary, "Voter Roster Load Summary");
    gtk_window_set_transient_for(w->summary, w->window);
    gtk_window_set_modal(w->summary, TRUE);
    gtk_window_set_destroy_with_parent(w->summary, TRUE);
    gtk_window_set_default_size(w->summary, 560, 360);
    box = gtk_box_new(GTK_ORIENTATION_VERTICAL, 8);
    gtk_widget_set_margin_start(box, 12);
    gtk_widget_set_margin_end(box, 12);
    gtk_widget_set_margin_top(box, 12);
    gtk_widget_set_margin_bottom(box, 12);
    view = gtk_text_view_new();
    gtk_text_view_set_editable(GTK_TEXT_VIEW(view), FALSE);
    gtk_text_view_set_cursor_visible(GTK_TEXT_VIEW(view), FALSE);
    gtk_text_view_set_wrap_mode(GTK_TEXT_VIEW(view), GTK_WRAP_WORD_CHAR);
    gtk_text_view_set_left_margin(GTK_TEXT_VIEW(view), 6);
    gtk_text_view_set_right_margin(GTK_TEXT_VIEW(view), 6);
    gtk_text_view_set_top_margin(GTK_TEXT_VIEW(view), 6);
    gtk_text_buffer_set_text(gtk_text_view_get_buffer(GTK_TEXT_VIEW(view)), text, -1);
    scroll = gtk_scrolled_window_new();
    gtk_scrolled_window_set_child(GTK_SCROLLED_WINDOW(scroll), view);
    gtk_scrolled_window_set_has_frame(GTK_SCROLLED_WINDOW(scroll), TRUE);
    gtk_widget_set_vexpand(scroll, TRUE);
    gtk_box_append(GTK_BOX(box), scroll);
    bottom = gtk_box_new(GTK_ORIENTATION_HORIZONTAL, 6);
    gtk_widget_set_halign(bottom, GTK_ALIGN_END);
    ok = gtk_button_new_with_mnemonic("_OK");
    gtk_widget_add_css_class(ok, "suggested-action");
    g_signal_connect(ok, "clicked", G_CALLBACK(on_summary_ok), w->summary);
    gtk_box_append(GTK_BOX(bottom), ok);
    gtk_box_append(GTK_BOX(box), bottom);
    gtk_window_set_child(w->summary, box);
    gtk_window_set_default_widget(w->summary, ok);
    keys = GTK_SHORTCUT_CONTROLLER(gtk_shortcut_controller_new());
    gtk_shortcut_controller_add_shortcut(
        keys,
        gtk_shortcut_new(gtk_shortcut_trigger_parse_string("Escape"),
                         gtk_named_action_new("window.close")));
    gtk_widget_add_controller(GTK_WIDGET(w->summary), GTK_EVENT_CONTROLLER(keys));
    g_signal_connect(w->summary, "destroy", G_CALLBACK(on_summary_destroy), w);
    gtk_window_present(w->summary);
    gtk_widget_grab_focus(ok);
}

/* -------------------------------------------------------------------------- */
/* Loading                                                                    */
/* -------------------------------------------------------------------------- */

static gboolean take_snapshot(gpointer user)
{
    VoterWindow *w = (VoterWindow *)user;
    ReportWindow *rw = w->snapshot_report >= 0 ? w->reports[w->snapshot_report] : NULL;
    TotalsWindow *tw = w->snapshot_totals ? w->totals : NULL;
    GtkWindow *target = tw != NULL                                  ? tw->win
                        : rw != NULL                                ? rw->win
                        : w->snapshot_summary && w->summary != NULL ? w->summary
                        : w->options_dialog != NULL                 ? w->options_dialog->win
                        : w->filter_dialog != NULL                  ? w->filter_dialog->win
                                                                    : w->window;
    GtkWidget *content = gtk_window_get_child(target);
    int width = gtk_widget_get_width(content), height = gtk_widget_get_height(content);
    GdkPaintable *paint;
    GtkSnapshot *snap;
    GskRenderNode *node;
    if (w->job != NULL)
    {
        return G_SOURCE_CONTINUE; /* (a sort or filter is still running) */
    }
    if (w->snapshot_export != NULL && tw != NULL)
    {
        totals_export_to(tw, w->snapshot_export, NULL, 0); /* (Export All..., no dialog) */
        g_clear_pointer(&w->snapshot_export, g_free);
    }
    else if (w->snapshot_export != NULL && rw != NULL)
    {
        report_export_to(rw, w->snapshot_export, NULL, 0); /* (Export All..., no dialog) */
        g_clear_pointer(&w->snapshot_export, g_free);
    }
    else if (w->snapshot_export != NULL && w->voters != NULL)
    {
        export_to(w, w->snapshot_export, NULL, 0, FALSE);
        g_clear_pointer(&w->snapshot_export, g_free);
    }
    paint = gtk_widget_paintable_new(content);
    snap = gtk_snapshot_new();
    gdk_paintable_snapshot(paint, GDK_SNAPSHOT(snap), width, height);
    node = gtk_snapshot_free_to_node(snap);
    if (node != NULL)
    {
        GskRenderer *renderer = gtk_native_get_renderer(GTK_NATIVE(target));
        GdkTexture *tex =
            gsk_renderer_render_texture(renderer, node, &GRAPHENE_RECT_INIT(0, 0, width, height));
        if (!gdk_texture_save_to_png(tex, w->snapshot))
        {
            fprintf(stderr, "could not write %s\n", w->snapshot);
        }
        g_object_unref(tex);
        gsk_render_node_unref(node);
    }
    else
    {
        /* (nothing yet: a sort or filter that just finished has not been laid out; try again
         * on the next tick, for up to 12 s) */
        g_object_unref(paint);
        if (++w->snapshot_tries < 15)
        {
            return G_SOURCE_CONTINUE;
        }
        fprintf(stderr, "nothing to write: the window has not drawn (%d x %d)\n", width, height);
    }
    g_object_unref(paint);
    g_application_quit(G_APPLICATION(w->app));
    return G_SOURCE_REMOVE;
}

/* --snapshot with --sort / --include / --exclude: what a heading click and the right-click
 * menu do, then the PNG once the window has drawn the result */
static void snapshot_actions(VoterWindow *w);

static gboolean retry_snapshot_actions(gpointer user)
{
    snapshot_actions((VoterWindow *)user);
    return G_SOURCE_REMOVE;
}

static void snapshot_actions(VoterWindow *w)
{
    guint i;
    if (w->snapshot_marks == EE_UI_MARKS_DUP_VOTING)
    {
        GList *l;
        for (l = gtk_application_get_windows(w->app); l != NULL; l = l->next)
        {
            VoterWindow *o = (VoterWindow *)g_object_get_data(G_OBJECT(l->data), "ee-voter-window");
            if (o != NULL && o->roster && o->worker != NULL)
            {
                g_timeout_add(200, retry_snapshot_actions, w); /* (a roster still loading) */
                return;
            }
        }
    }
    if (w->snapshot_zoom > 0 || w->snapshot_names >= 0)
    {
        /* --zoom / --names: as the Options window's OK sets them, but not saved */
        EeUiOptions o = w->opts;
        if (w->snapshot_zoom > 0)
        {
            o.zoom_percent = w->snapshot_zoom;
        }
        if (w->snapshot_names >= 0)
        {
            o.surname_first = w->snapshot_names;
        }
        set_options(w, &o, FALSE);
    }
    if (w->voters != NULL && w->snapshot_marks == EE_UI_MARKS_DUP_VOTING)
    {
        show_duplicates_voting(w);
    }
    else if (w->voters != NULL && w->snapshot_marks != EE_UI_MARKS_NONE)
    {
        show_duplicates(w, w->snapshot_marks);
    }
    if (w->voters != NULL && w->snapshot_quick != NULL)
    {
        for (i = 0; i < w->snapshot_quick->len; i++)
        {
            const char *q = (const char *)g_ptr_array_index(w->snapshot_quick, i);
            const char *eq = strchr(q, '=');
            if (eq != NULL)
            {
                add_quick_rule(w, (guint)atoi(q + 1), eq + 1, q[0] == '-');
            }
        }
    }
    if (w->voters != NULL && w->snapshot_report >= 0)
    {
        /* --report: the report's window (and --sort clicks its heading, 0 or 1) */
        show_report(w, w->snapshot_report);
        if (w->reports[w->snapshot_report] != NULL && w->snapshot_sort >= 0 &&
            w->snapshot_sort <= EE_UI_REPORT_COLUMN_COUNT)
        {
            report_sort_by(w->reports[w->snapshot_report],
                           (guint)w->snapshot_sort,
                           w->snapshot_ascending,
                           FALSE);
        }
    }
    else if (w->voters != NULL && w->snapshot_sort >= 0 &&
             (guint)w->snapshot_sort <
                 g_list_model_get_n_items(gtk_column_view_get_columns(w->view)))
    {
        GtkColumnViewColumn *col =
            g_list_model_get_item(gtk_column_view_get_columns(w->view), (guint)w->snapshot_sort);
        /* (a heading click leaves the earlier column as a secondary sort, which
         * on_sorter_changed drops; this call replaces it, but GTK keeps its arrow on screen
         * unless the sort is cleared first) */
        w->rebuilding = TRUE;
        gtk_column_view_sort_by_column(w->view, NULL, GTK_SORT_ASCENDING);
        w->rebuilding = FALSE;
        gtk_column_view_sort_by_column(w->view,
                                       col,
                                       w->snapshot_ascending ? GTK_SORT_ASCENDING
                                                             : GTK_SORT_DESCENDING);
        g_object_unref(col);
    }
    if (w->snapshot_filter_window || w->snapshot_load_filter != NULL ||
        w->snapshot_export_filter != NULL)
    {
        show_filter_dialog(w);
        /* --load-filter / --export-filter: the Filter window's Load Filter... and Export
         * Filter..., without the file dialogs */
        if (w->filter_dialog != NULL && w->snapshot_load_filter != NULL)
        {
            fd_load(w->filter_dialog, w->snapshot_load_filter);
        }
        if (w->filter_dialog != NULL && w->snapshot_export_filter != NULL)
        {
            if (w->filter_dialog->draft->len == 0)
            {
                fd_alert(w->filter_dialog, "There are no filter rules to export.");
            }
            else
            {
                fd_save_chosen(w->filter_dialog, w->snapshot_export_filter);
            }
        }
    }
    if (w->snapshot_options_window)
    {
        show_options_dialog(w);
    }
    if (w->snapshot_summary && w->summary == NULL)
    {
        show_load_summary(w);
    }
    if (w->snapshot_totals)
    {
        show_totals(w);
    }
    g_timeout_add(800, take_snapshot, w); /* (time for the window to lay out and draw) */
}

static gboolean show_load_progress(gpointer user)
{
    VoterWindow *w = (VoterWindow *)user;
    char text[128];
    gtk_progress_bar_set_fraction(w->progress, g_atomic_int_get(&w->percent) / 100.0);
    g_snprintf(text,
               sizeof(text),
               "Loading %s... %d rows",
               w->name != NULL ? w->name : "",
               g_atomic_int_get(&w->rows_so_far));
    gtk_label_set_text(w->status, text);
    return G_SOURCE_CONTINUE;
}

static int on_progress(void *user, unsigned percent, unsigned rows)
{
    VoterWindow *w = (VoterWindow *)user;
    g_atomic_int_set(&w->percent, (gint)percent);
    g_atomic_int_set(&w->rows_so_far, (gint)rows);
    return g_atomic_int_get((volatile gint *)&w->cancel) == 0;
}

static void alert(VoterWindow *w, const char *message, const char *detail)
{
    GtkAlertDialog *d = gtk_alert_dialog_new("%s", message);
    if (detail != NULL)
    {
        gtk_alert_dialog_set_detail(d, detail);
    }
    gtk_alert_dialog_show(d, w->window);
    g_object_unref(d);
}

static gboolean load_done(gpointer user)
{
    LoadDone *done = (LoadDone *)user;
    VoterWindow *w = done->w;
    char title[512];
    if (w->closed)
    {
        EeUiVoters_Free(done->voters); /* (the window closed while loading) */
        g_free(done);
        return G_SOURCE_REMOVE;
    }
    g_thread_join(w->worker);
    w->worker = NULL;
    stop_progress(w);
    if (done->voters == NULL && w->roster)
    {
        /* (as on Windows, a roster that does not load leaves no window: say so over the window
         * it was opened from, unless loading was cancelled) */
        VoterWindow *opener = w->opener != NULL && !w->opener->closed ? w->opener : NULL;
        if (w->cancel == 0)
        {
            GtkAlertDialog *d = gtk_alert_dialog_new("Could not load the voter roster.");
            gtk_alert_dialog_set_detail(d, done->err);
            gtk_alert_dialog_show(d, opener != NULL ? opener->window : NULL);
            g_object_unref(d);
        }
        g_clear_pointer(&w->loading_path, g_free);
        g_clear_pointer(&w->loading_roster, g_ptr_array_unref);
        if (w->snapshot != NULL)
        {
            g_application_quit(G_APPLICATION(w->app));
        }
        gtk_window_destroy(w->window);
        g_free(done);
        return G_SOURCE_REMOVE;
    }
    if (done->voters == NULL)
    {
        alert(w, "The voter list could not be opened.", done->err);
        set_status(w);
    }
    else
    {
        fprintf(stderr, "loaded in %.2f s\n", (g_get_monotonic_time() - done->started) / 1e6);
        if (w->filter_dialog != NULL)
        {
            gtk_window_destroy(w->filter_dialog->win);
        }
        close_reports(w); /* (as on Windows: a report is of the list it was made from) */
        EeUiVoters_Free(w->voters);
        w->voters = done->voters;
        if (EeUiVoters_SurnameFirst(w->voters) != (w->opts.surname_first != 0))
        {
            /* (the Options changed while it loaded; nothing is sorted or filtered yet) */
            EeUiVoters_SetSurnameFirst(w->voters, w->opts.surname_first);
        }
        g_array_set_size(w->rules, 0); /* (a new list starts unfiltered, in file order) */
        w->want_marks = EE_UI_MARKS_NONE;
        g_clear_pointer(&w->voting_marks, EeUiMarks_Free);
        w->want_sort = -1;
        w->want_ascending = TRUE;
        g_free(w->name);
        w->name = roster_name(w);
        g_free(w->path);
        w->path = g_strdup(w->loading_path);
        rebuild_columns(w);
        g_snprintf(title,
                   sizeof(title),
                   w->roster ? "%s \xE2\x80\x94 Voter Roster" : "%s \xE2\x80\x94 Election Explorer",
                   w->name);
        gtk_window_set_title(w->window, title);
        set_status(w);
    }
    enable_reports(w, w->voters != NULL);
    g_clear_pointer(&w->loading_path, g_free);
    g_clear_pointer(&w->loading_roster, g_ptr_array_unref);
    if (w->snapshot != NULL)
    {
        snapshot_actions(w);
    }
    else if (w->voters != NULL && w->roster)
    {
        int notable = 0;
        EeUiVoters_LoadSummary(w->voters, &notable);
        if (notable)
        {
            show_load_summary(w); /* (as on Windows: when it says more than the counts) */
        }
    }
    g_free(done);
    return G_SOURCE_REMOVE;
}

static gpointer load_worker(gpointer user)
{
    LoadDone *done = (LoadDone *)user;
    VoterWindow *w = done->w;
    if (w->loading_roster != NULL)
    {
        done->voters = EeUiVoters_LoadRoster((const char *const *)w->loading_roster->pdata,
                                             w->loading_roster->len,
                                             done->surname_first,
                                             on_progress,
                                             w,
                                             &w->cancel,
                                             done->err,
                                             sizeof(done->err));
    }
    else
    {
        done->voters = EeUiVoters_Load(w->loading_path,
                                       done->surname_first,
                                       on_progress,
                                       w,
                                       &w->cancel,
                                       done->err,
                                       sizeof(done->err));
    }
    g_idle_add(load_done, done);
    return NULL;
}

/* the window's name for the title and status line: the file's, and for a roster of several
 * files "<first> (+2 more)", as on Windows */
static char *roster_name(VoterWindow *w)
{
    char *leaf = g_path_get_basename(w->loading_path), *name;
    if (w->loading_roster == NULL || w->loading_roster->len < 2)
    {
        return leaf;
    }
    name = g_strdup_printf("%s (+%u more)", leaf, w->loading_roster->len - 1);
    g_free(leaf);
    return name;
}

static VoterWindow *voter_window_new(GtkApplication *app, gboolean roster);

/* load into this window: a voter list, or the roster's files (@p roster, taken over) */
static void start_load(VoterWindow *w, const char *path, GPtrArray *roster)
{
    LoadDone *done = g_new0(LoadDone, 1);
    done->w = w;
    done->surname_first = w->opts.surname_first;
    done->started = g_get_monotonic_time();
    w->loading_path = g_strdup(path);
    w->loading_roster = roster;
    g_free(w->name);
    w->name = roster_name(w);
    g_atomic_int_set(&w->percent, 0);
    g_atomic_int_set(&w->rows_so_far, 0);
    w->cancel = 0;
    gtk_progress_bar_set_fraction(w->progress, 0.0);
    gtk_widget_set_visible(GTK_WIDGET(w->progress), TRUE);
    enable_reports(w, FALSE); /* (grayed while loading, as on Windows) */
    w->progress_timer = g_timeout_add(100, show_load_progress, w);
    w->worker = g_thread_new("ee-load", load_worker, done);
}

/* Open Voter List...: as on Windows, into this window while it is empty, else into a new one */
static void open_path(VoterWindow *w, const char *path)
{
    if (w->roster || w->voters != NULL || w->worker != NULL)
    {
        VoterWindow *nw = voter_window_new(w->app, FALSE);
        gtk_window_present(nw->window);
        w = nw;
    }
    else if (w->job != NULL)
    {
        return;
    }
    start_load(w, path, NULL);
}

/* Open Voter Roster...: as on Windows, always in a Voter Roster window of its own */
static VoterWindow *open_roster(VoterWindow *w, GPtrArray *paths)
{
    VoterWindow *rw = voter_window_new(w->app, TRUE);
    rw->opener = w;
    gtk_window_present(rw->window);
    start_load(rw, (const char *)g_ptr_array_index(paths, 0), paths);
    return rw;
}

/* -------------------------------------------------------------------------- */
/* Commands                                                                   */
/* -------------------------------------------------------------------------- */

static void on_open_chosen(GObject *source, GAsyncResult *result, gpointer user)
{
    VoterWindow *w = (VoterWindow *)user;
    GFile *file = gtk_file_dialog_open_finish(GTK_FILE_DIALOG(source), result, NULL);
    if (file != NULL)
    {
        char *path = g_file_get_path(file);
        if (path != NULL)
        {
            open_path(w, path);
            g_free(path);
        }
        g_object_unref(file);
    }
}

static void action_open(GSimpleAction *action, GVariant *param, gpointer user)
{
    VoterWindow *w = (VoterWindow *)user;
    GtkFileDialog *d = gtk_file_dialog_new();
    GtkFileFilter *lists = gtk_file_filter_new(), *all = gtk_file_filter_new();
    GListStore *filters = g_list_store_new(GTK_TYPE_FILE_FILTER);
    (void)action;
    (void)param;
    gtk_file_filter_set_name(lists, "Voter lists (CSV, TSV, TXT, XLSX)");
    gtk_file_filter_add_suffix(lists, "csv");
    gtk_file_filter_add_suffix(lists, "tsv");
    gtk_file_filter_add_suffix(lists, "txt");
    gtk_file_filter_add_suffix(lists, "xlsx");
    gtk_file_filter_set_name(all, "All files");
    gtk_file_filter_add_pattern(all, "*");
    g_list_store_append(filters, lists);
    g_list_store_append(filters, all);
    gtk_file_dialog_set_title(d, "Open Voter List");
    gtk_file_dialog_set_filters(d, G_LIST_MODEL(filters));
    gtk_file_dialog_open(d, w->window, NULL, on_open_chosen, w);
    g_object_unref(filters);
    g_object_unref(lists);
    g_object_unref(all);
    g_object_unref(d);
}

static void on_roster_chosen(GObject *source, GAsyncResult *result, gpointer user)
{
    VoterWindow *w = (VoterWindow *)user;
    GListModel *files = gtk_file_dialog_open_multiple_finish(GTK_FILE_DIALOG(source), result, NULL);
    GPtrArray *paths;
    guint i, n;
    if (files == NULL)
    {
        return;
    }
    paths = g_ptr_array_new_with_free_func(g_free);
    n = g_list_model_get_n_items(files);
    for (i = 0; i < n; i++)
    {
        GFile *f = g_list_model_get_item(files, i);
        char *path = g_file_get_path(f);
        if (path != NULL)
        {
            g_ptr_array_add(paths, path);
        }
        g_object_unref(f);
    }
    g_object_unref(files);
    if (paths->len == 0 || w->closed)
    {
        g_ptr_array_unref(paths);
        return;
    }
    open_roster(w, paths);
}

/* File > Open Voter Roster...: one or more files (Windows' list of types) */
static void action_open_roster(GSimpleAction *action, GVariant *param, gpointer user)
{
    VoterWindow *w = (VoterWindow *)user;
    GtkFileDialog *d = gtk_file_dialog_new();
    GtkFileFilter *rosters = gtk_file_filter_new(), *all = gtk_file_filter_new();
    GListStore *filters = g_list_store_new(GTK_TYPE_FILE_FILTER);
    static const char *const suffixes[] = {"zip", "xlsx", "xls", "csv", "tsv", "txt", "pdf"};
    size_t i;
    (void)action;
    (void)param;
    gtk_file_filter_set_name(rosters, "Voter rosters (ZIP, XLSX, XLS, CSV, TSV, TXT, PDF)");
    for (i = 0; i < G_N_ELEMENTS(suffixes); i++)
    {
        gtk_file_filter_add_suffix(rosters, suffixes[i]);
    }
    gtk_file_filter_set_name(all, "All files");
    gtk_file_filter_add_pattern(all, "*");
    g_list_store_append(filters, rosters);
    g_list_store_append(filters, all);
    gtk_file_dialog_set_title(d, "Open Voter Roster");
    gtk_file_dialog_set_filters(d, G_LIST_MODEL(filters));
    gtk_file_dialog_open_multiple(d, w->window, NULL, on_roster_chosen, w);
    g_object_unref(filters);
    g_object_unref(rosters);
    g_object_unref(all);
    g_object_unref(d);
}

static void action_close(GSimpleAction *action, GVariant *param, gpointer user)
{
    VoterWindow *w = (VoterWindow *)user;
    (void)action;
    (void)param;
    gtk_window_close(w->window);
}

static void action_quit(GSimpleAction *action, GVariant *param, gpointer user)
{
    VoterWindow *w = (VoterWindow *)user;
    (void)action;
    (void)param;
    g_application_quit(G_APPLICATION(w->app));
}

static void action_filter(GSimpleAction *action, GVariant *param, gpointer user)
{
    (void)action;
    (void)param;
    show_filter_dialog((VoterWindow *)user);
}

static void action_reset_filter(GSimpleAction *action, GVariant *param, gpointer user)
{
    (void)action;
    (void)param;
    reset_view((VoterWindow *)user); /* (as on Windows, it also leaves a duplicates view) */
}

static void action_dup_ids(GSimpleAction *action, GVariant *param, gpointer user)
{
    (void)action;
    (void)param;
    show_duplicates((VoterWindow *)user, EE_UI_MARKS_DUP_IDS);
}

static void action_dup_voters(GSimpleAction *action, GVariant *param, gpointer user)
{
    (void)action;
    (void)param;
    show_duplicates((VoterWindow *)user, EE_UI_MARKS_DUP_VOTERS);
}

static void action_dup_voting(GSimpleAction *action, GVariant *param, gpointer user)
{
    (void)action;
    (void)param;
    show_duplicates_voting((VoterWindow *)user);
}

static void action_copy(GSimpleAction *action, GVariant *param, gpointer user)
{
    (void)action;
    (void)param;
    copy_selection((VoterWindow *)user);
}

static void action_export(GSimpleAction *action, GVariant *param, gpointer user)
{
    (void)action;
    (void)param;
    export_rows((VoterWindow *)user, FALSE);
}

static void action_export_selected(GSimpleAction *action, GVariant *param, gpointer user)
{
    (void)action;
    (void)param;
    export_rows((VoterWindow *)user, TRUE);
}

static void action_report_precinct(GSimpleAction *action, GVariant *param, gpointer user)
{
    (void)action;
    (void)param;
    show_report((VoterWindow *)user, EE_UI_REPORT_PRECINCT);
}

static void action_report_address(GSimpleAction *action, GVariant *param, gpointer user)
{
    (void)action;
    (void)param;
    show_report((VoterWindow *)user, EE_UI_REPORT_ADDRESS);
}

static void action_report_summary(GSimpleAction *action, GVariant *param, gpointer user)
{
    (void)action;
    (void)param;
    show_load_summary((VoterWindow *)user);
}

static void action_report_totals(GSimpleAction *action, GVariant *param, gpointer user)
{
    (void)action;
    (void)param;
    show_totals((VoterWindow *)user);
}

static void action_quick_include(GSimpleAction *action, GVariant *param, gpointer user)
{
    VoterWindow *w = (VoterWindow *)user;
    (void)action;
    (void)param;
    add_quick_rule(w, w->quick_column, w->quick_value, FALSE);
}

static void action_quick_exclude(GSimpleAction *action, GVariant *param, gpointer user)
{
    VoterWindow *w = (VoterWindow *)user;
    (void)action;
    (void)param;
    add_quick_rule(w, w->quick_column, w->quick_value, TRUE);
}

static void action_options(GSimpleAction *action, GVariant *param, gpointer user)
{
    VoterWindow *w = (VoterWindow *)user;
    (void)action;
    (void)param;
    show_options_dialog(w);
}

static void action_show_in_maps(GSimpleAction *action, GVariant *param, gpointer user)
{
    VoterWindow *w = (VoterWindow *)user;
    (void)action;
    (void)param;
    show_in_maps(w);
}

static void action_open_link(GSimpleAction *action, GVariant *param, gpointer user)
{
    VoterWindow *w = (VoterWindow *)user;
    (void)action;
    (void)param;
    open_link(w);
}

static void action_about(GSimpleAction *action, GVariant *param, gpointer user)
{
    VoterWindow *w = (VoterWindow *)user;
    (void)action;
    (void)param;
    gtk_show_about_dialog(w->window,
                          "program-name",
                          "Election Explorer",
                          "version",
                          "0.1 (prototype)",
                          "comments",
                          "Voter lists, rosters, results and cast vote records.",
                          NULL);
}

/* the window closed: stop a load or sort in progress (its result is dropped) and free the list */
static void on_destroy(GtkWidget *widget, gpointer user)
{
    VoterWindow *w = (VoterWindow *)user;
    (void)widget;
    w->closed = TRUE;
    if (w->worker != NULL)
    {
        w->cancel = 1;
        g_thread_join(w->worker); /* (its load_done, still queued, only frees the result) */
        w->worker = NULL;
    }
    if (w->job != NULL)
    {
        g_thread_join(w->job); /* (a sort cannot be stopped: wait for it; view_done frees it) */
        w->job = NULL;
    }
    if (w->progress_timer != 0)
    {
        g_source_remove(w->progress_timer);
        w->progress_timer = 0;
    }
    if (w->cell_menu != NULL)
    {
        gtk_widget_unparent(w->cell_menu);
        w->cell_menu = NULL;
    }
    close_reports(w);
    EeUiVoters_Free(w->voters);
    w->voters = NULL;
    g_clear_pointer(&w->voting_marks, EeUiMarks_Free);
    g_free(w->quick_value);
    w->quick_value = NULL;
    free(w->quick_link);
    w->quick_link = NULL;
    g_clear_pointer(&w->loading_roster, g_ptr_array_unref);
}

static VoterWindow *voter_window_new(GtkApplication *app, gboolean roster)
{
    VoterWindow *w = g_new0(VoterWindow, 1);
    GtkWidget *box = gtk_box_new(GTK_ORIENTATION_VERTICAL, 0);
    GtkWidget *scroll = gtk_scrolled_window_new();
    GtkWidget *bar = gtk_box_new(GTK_ORIENTATION_HORIZONTAL, 8);
    GtkWidget *header = gtk_header_bar_new();
    GtkWidget *open = gtk_button_new_from_icon_name("document-open-symbolic");
    GtkWidget *filter = gtk_button_new_from_icon_name("edit-find-symbolic");
    GMenu *menu = g_menu_new(), *file = g_menu_new(), *edit = g_menu_new(), *filt = g_menu_new(),
          *dups = g_menu_new(), *reports = g_menu_new(), *help = g_menu_new();
    GtkWidget *menu_button = gtk_menu_button_new();
    const GActionEntry actions[] = {
        {"open", action_open, NULL, NULL, NULL, {0}},
        {"open-roster", action_open_roster, NULL, NULL, NULL, {0}},
        {"close", action_close, NULL, NULL, NULL, {0}},
        {"quit", action_quit, NULL, NULL, NULL, {0}},
        {"filter", action_filter, NULL, NULL, NULL, {0}},
        {"reset-filter", action_reset_filter, NULL, NULL, NULL, {0}},
        {"dup-ids", action_dup_ids, NULL, NULL, NULL, {0}},
        {"dup-voters", action_dup_voters, NULL, NULL, NULL, {0}},
        {"dup-voting", action_dup_voting, NULL, NULL, NULL, {0}},
        {"reset-view", action_reset_filter, NULL, NULL, NULL, {0}},
        {"copy", action_copy, NULL, NULL, NULL, {0}},
        {"export", action_export, NULL, NULL, NULL, {0}},
        {"export-selected", action_export_selected, NULL, NULL, NULL, {0}},
        {"report-precinct", action_report_precinct, NULL, NULL, NULL, {0}},
        {"report-address", action_report_address, NULL, NULL, NULL, {0}},
        {"report-summary", action_report_summary, NULL, NULL, NULL, {0}},
        {"report-totals", action_report_totals, NULL, NULL, NULL, {0}},
        {"quick-include", action_quick_include, NULL, NULL, NULL, {0}},
        {"quick-exclude", action_quick_exclude, NULL, NULL, NULL, {0}},
        {"options", action_options, NULL, NULL, NULL, {0}},
        {"show-in-maps", action_show_in_maps, NULL, NULL, NULL, {0}},
        {"open-link", action_open_link, NULL, NULL, NULL, {0}},
        {"about", action_about, NULL, NULL, NULL, {0}},
    };
    const char *open_accels[] = {"<Control>o", NULL}, *close_accels[] = {"<Control>w", NULL},
               *quit_accels[] = {"<Control>q", NULL}, *filter_accels[] = {"<Control>l", NULL},
               *copy_accels[] = {"<Control>c", NULL};

    w->app = app;
    w->roster = roster;
    w->rules = g_array_new(FALSE, TRUE, sizeof(EeUiRule));
    w->want_sort = -1;
    w->want_ascending = TRUE;
    w->snapshot_sort = -1;
    w->snapshot_report = -1;
    w->snapshot_names = -1;
    EeUiOptions_Load(&w->opts); /* (as saved last run) */
    w->window = GTK_WINDOW(gtk_application_window_new(app));
    /* (open_rosters finds the window's list through it) */
    g_object_set_data(G_OBJECT(w->window), "ee-voter-window", w);
    gtk_window_set_title(w->window, roster ? "Voter Roster" : "Election Explorer");
    gtk_window_set_default_size(w->window, 1100, 700);

    g_action_map_add_action_entries(G_ACTION_MAP(w->window), actions, G_N_ELEMENTS(actions), w);
    gtk_application_set_accels_for_action(app, "win.open", open_accels);
    gtk_application_set_accels_for_action(app, "win.close", close_accels);
    gtk_application_set_accels_for_action(app, "win.quit", quit_accels);
    gtk_application_set_accels_for_action(app,
                                          "win.filter",
                                          filter_accels); /* (Ctrl+L, as on Windows) */
    gtk_application_set_accels_for_action(app, "win.copy", copy_accels);

    /* header bar: Open, Filter, and a menu (File; Copy; Filter, as on Windows; About). A
     * roster window has Windows' roster menus: no duplicate voters or Address report, and the
     * Load Summary. */
    g_menu_append(file, "Open Voter List\xE2\x80\xA6", "win.open");
    g_menu_append(file, "Open Voter Roster\xE2\x80\xA6", "win.open-roster");
    g_menu_append(file,
                  roster ? "Export Voter Roster\xE2\x80\xA6" : "Export Voter List\xE2\x80\xA6",
                  "win.export");
    g_menu_append(file, "Close", "win.close");
    g_menu_append(file, "Quit", "win.quit");
    g_menu_append(edit, "Copy", "win.copy");
    g_menu_append(edit, "Options\xE2\x80\xA6", "win.options");
    g_menu_append(filt, "Filter\xE2\x80\xA6", "win.filter");
    g_menu_append(filt, "Reset Filter", "win.reset-filter");
    g_menu_append(filt, "Show Duplicate Voter IDs\xE2\x80\xA6", "win.dup-ids");
    if (!roster)
    {
        g_menu_append(filt, "Show Duplicate Voters\xE2\x80\xA6", "win.dup-voters");
        g_menu_append(filt, "Show Duplicate Voters Voting\xE2\x80\xA6", "win.dup-voting");
    }
    g_menu_append(dups, "Reset View\xE2\x80\xA6", "win.reset-view");
    g_menu_append(reports, "Display Precinct Report\xE2\x80\xA6", "win.report-precinct");
    if (roster)
    {
        g_menu_append(reports, "Display Voting Totals\xE2\x80\xA6", "win.report-totals");
        g_menu_append(reports, "Display Load Summary\xE2\x80\xA6", "win.report-summary");
    }
    else
    {
        g_menu_append(reports, "Display Address Report\xE2\x80\xA6", "win.report-address");
    }
    g_menu_append(help, "About Election Explorer", "win.about");
    g_menu_append_section(menu, NULL, G_MENU_MODEL(file));
    g_menu_append_section(menu, NULL, G_MENU_MODEL(edit));
    g_menu_append_section(menu, NULL, G_MENU_MODEL(filt));
    g_menu_append_section(menu, NULL, G_MENU_MODEL(dups));
    g_menu_append_section(menu, "Reports", G_MENU_MODEL(reports));
    g_menu_append_section(menu, NULL, G_MENU_MODEL(help));
    gtk_menu_button_set_menu_model(GTK_MENU_BUTTON(menu_button), G_MENU_MODEL(menu));
    gtk_menu_button_set_icon_name(GTK_MENU_BUTTON(menu_button), "open-menu-symbolic");
    gtk_actionable_set_action_name(GTK_ACTIONABLE(open), "win.open");
    gtk_widget_set_tooltip_text(open, "Open Voter List (Ctrl+O)");
    gtk_actionable_set_action_name(GTK_ACTIONABLE(filter), "win.filter");
    gtk_widget_set_tooltip_text(filter, "Filter (Ctrl+L)");
    gtk_header_bar_pack_start(GTK_HEADER_BAR(header), open);
    gtk_header_bar_pack_start(GTK_HEADER_BAR(header), filter);
    gtk_header_bar_pack_end(GTK_HEADER_BAR(header), menu_button);
    gtk_window_set_titlebar(w->window, header);
    g_object_unref(menu);
    g_object_unref(file);
    g_object_unref(edit);
    g_object_unref(filt);
    g_object_unref(dups);
    g_object_unref(reports);
    g_object_unref(help);
    enable_reports(w, FALSE); /* (until a list is loaded) */

    /* the table */
    w->view = GTK_COLUMN_VIEW(gtk_column_view_new(NULL));
    gtk_column_view_set_show_column_separators(w->view, TRUE);
    gtk_column_view_set_show_row_separators(w->view, FALSE);
    gtk_column_view_set_reorderable(w->view, FALSE);
    gtk_widget_add_css_class(GTK_WIDGET(w->view), "data-table"); /* (compact rows) */
    gtk_widget_add_css_class(GTK_WIDGET(w->view), "voter-list"); /* (the zoom) */
    add_table_css(gtk_widget_get_display(GTK_WIDGET(w->window)));
    g_signal_connect(gtk_column_view_get_sorter(w->view),
                     "changed",
                     G_CALLBACK(on_sorter_changed),
                     w);
    gtk_scrolled_window_set_child(GTK_SCROLLED_WINDOW(scroll), GTK_WIDGET(w->view));
    gtk_widget_set_vexpand(scroll, TRUE);

    /* status line and progress */
    w->status = GTK_LABEL(gtk_label_new(NULL));
    gtk_label_set_xalign(w->status, 0.0f);
    gtk_label_set_ellipsize(w->status, PANGO_ELLIPSIZE_END);
    gtk_widget_set_hexpand(GTK_WIDGET(w->status), TRUE);
    w->progress = GTK_PROGRESS_BAR(gtk_progress_bar_new());
    gtk_widget_set_size_request(GTK_WIDGET(w->progress), 200, -1);
    gtk_widget_set_valign(GTK_WIDGET(w->progress), GTK_ALIGN_CENTER);
    gtk_widget_set_visible(GTK_WIDGET(w->progress), FALSE);
    gtk_widget_set_margin_start(bar, 8);
    gtk_widget_set_margin_end(bar, 8);
    gtk_widget_set_margin_top(bar, 4);
    gtk_widget_set_margin_bottom(bar, 4);
    gtk_box_append(GTK_BOX(bar), GTK_WIDGET(w->status));
    gtk_box_append(GTK_BOX(bar), GTK_WIDGET(w->progress));

    gtk_box_append(GTK_BOX(box), scroll);
    gtk_box_append(GTK_BOX(box), bar);
    gtk_window_set_child(w->window, box);
    g_signal_connect(w->window, "destroy", G_CALLBACK(on_destroy), w);
    apply_zoom(w, w->opts.zoom_percent);
    set_status(w);
    return w;
}

/* -------------------------------------------------------------------------- */
/* The application                                                            */
/* -------------------------------------------------------------------------- */

typedef struct AppArgs
{
    const char *snapshot;      /* --snapshot */
    int sort_column;           /* --sort, with --snapshot: -1 for none */
    gboolean sort_ascending;
    GPtrArray *quick;          /* --include / --exclude, with --snapshot */
    gboolean filter_window;    /* --filter-window, with --snapshot */
    int marks;                 /* --duplicates ids|voters, with --snapshot */
    const char *export_to;     /* --export <file>, with --snapshot */
    int report;                /* --report precinct|address, with --snapshot: -1 for none */
    int zoom;                  /* --zoom <percent>, with --snapshot: 0 for none */
    int names;                 /* --names given|surname, with --snapshot: -1 for none */
    gboolean options_window;   /* --options-window, with --snapshot */
    const char *load_filter;   /* --load-filter <file>, with --snapshot */
    const char *export_filter; /* --export-filter <file>, with --snapshot */
    GPtrArray *files;          /* the files named (a roster's with --roster) */
    gboolean roster;           /* --roster: open the files as one voter roster */
    gboolean load_summary;     /* --load-summary, with --snapshot */
    gboolean voting_totals;    /* --voting-totals, with --snapshot */
    GPtrArray *voting_rosters; /* --voting-roster <file>: each opened as a roster first */
} AppArgs;

static void on_activate(GtkApplication *app, gpointer user)
{
    AppArgs *args = (AppArgs *)user;
    VoterWindow *w = voter_window_new(app, FALSE), *first = w;
    if (args->roster && args->files->len > 0)
    {
        /* (the roster's own window, as Open Voter Roster... opens it; the empty voter list
         * window stays, as on Windows) */
        gtk_window_present(first->window);
        w = voter_window_new(app, TRUE);
        w->opener = first;
    }
    else
    {
        guint k;
        for (k = 0; k < args->voting_rosters->len; k++)
        {
            /* (each in a roster window of its own, as Open Voter Roster... opens it) */
            VoterWindow *rw = voter_window_new(app, TRUE);
            const char *path = (const char *)g_ptr_array_index(args->voting_rosters, k);
            GPtrArray *paths = g_ptr_array_new_with_free_func(g_free);
            g_ptr_array_add(paths, g_strdup(path));
            rw->opener = first;
            gtk_window_present(rw->window);
            start_load(rw, path, paths);
        }
    }
    if (args->snapshot != NULL)
    {
        w->snapshot = g_strdup(args->snapshot);
        w->snapshot_sort = args->sort_column;
        w->snapshot_ascending = args->sort_ascending;
        w->snapshot_quick = args->quick;
        w->snapshot_filter_window = args->filter_window;
        w->snapshot_marks = args->marks;
        w->snapshot_export = g_strdup(args->export_to);
        w->snapshot_report = args->report;
        w->snapshot_zoom = args->zoom;
        w->snapshot_names = args->names;
        w->snapshot_options_window = args->options_window;
        w->snapshot_load_filter = args->load_filter;
        w->snapshot_export_filter = args->export_filter;
        w->snapshot_summary = args->load_summary;
        w->snapshot_totals = args->voting_totals;
    }
    gtk_window_present(w->window);
    if (w->roster)
    {
        GPtrArray *paths = g_ptr_array_new_with_free_func(g_free);
        guint i;
        for (i = 0; i < args->files->len; i++)
        {
            g_ptr_array_add(paths, g_strdup((const char *)g_ptr_array_index(args->files, i)));
        }
        start_load(w, (const char *)g_ptr_array_index(paths, 0), paths);
    }
    else if (args->files->len > 0)
    {
        open_path(w, (const char *)g_ptr_array_index(args->files, args->files->len - 1));
    }
}

int main(int argc, char **argv)
{
    AppArgs args = {NULL,
                    -1,
                    TRUE,
                    NULL,
                    FALSE,
                    EE_UI_MARKS_NONE,
                    NULL,
                    -1,
                    0,
                    -1,
                    FALSE,
                    NULL,
                    NULL,
                    NULL,
                    FALSE,
                    FALSE,
                    FALSE,
                    NULL};
    GtkApplication *app;
    int status, i;
    args.quick = g_ptr_array_new_with_free_func(g_free);
    args.files = g_ptr_array_new();
    args.voting_rosters = g_ptr_array_new();
    for (i = 1; i < argc; i++)
    {
        if (strcmp(argv[i], "--selftest") == 0 && i + 1 < argc)
        {
            return EeUiVoters_SelfTest(argv[i + 1]);
        }
        else if (strcmp(argv[i], "--selftest-roster") == 0 && i + 1 < argc)
        {
            return EeUiVoters_RosterSelfTest((const char *const *)&argv[i + 1],
                                             (uint32_t)(argc - i - 1));
        }
        else if (strcmp(argv[i], "--selftest-voting") == 0 && i + 2 < argc)
        {
            return EeUiVoters_VotingSelfTest(argv[i + 1],
                                             (const char *const *)&argv[i + 2],
                                             (uint32_t)(argc - i - 2));
        }
        else if (strcmp(argv[i], "--roster") == 0)
        {
            args.roster = TRUE;
        }
        else if (strcmp(argv[i], "--voting-roster") == 0 && i + 1 < argc)
        {
            g_ptr_array_add(args.voting_rosters, argv[++i]);
        }
        else if (strcmp(argv[i], "--load-summary") == 0)
        {
            args.load_summary = TRUE;
        }
        else if (strcmp(argv[i], "--voting-totals") == 0)
        {
            args.voting_totals = TRUE;
        }
        else if (strcmp(argv[i], "--snapshot") == 0 && i + 1 < argc)
        {
            args.snapshot = argv[++i];
        }
        else if (strcmp(argv[i], "--sort") == 0 && i + 1 < argc)
        {
            /* <column>[:desc] */
            i++;
            args.sort_column = atoi(argv[i]);
            args.sort_ascending = strstr(argv[i], ":desc") == NULL;
        }
        else if ((strcmp(argv[i], "--include") == 0 || strcmp(argv[i], "--exclude") == 0) &&
                 i + 1 < argc)
        {
            /* <column>=<value>: what the right-click Include / Exclude adds */
            g_ptr_array_add(args.quick,
                            g_strconcat(argv[i][2] == 'i' ? "+" : "-", argv[i + 1], NULL));
            i++;
        }
        else if (strcmp(argv[i], "--filter-window") == 0)
        {
            args.filter_window = TRUE;
        }
        else if (strcmp(argv[i], "--duplicates") == 0 && i + 1 < argc)
        {
            i++;
            args.marks = strcmp(argv[i], "voters") == 0   ? EE_UI_MARKS_DUP_VOTERS
                         : strcmp(argv[i], "voting") == 0 ? EE_UI_MARKS_DUP_VOTING
                                                          : EE_UI_MARKS_DUP_IDS;
        }
        else if (strcmp(argv[i], "--export") == 0 && i + 1 < argc)
        {
            args.export_to = argv[++i];
        }
        else if (strcmp(argv[i], "--report") == 0 && i + 1 < argc)
        {
            i++;
            args.report =
                strcmp(argv[i], "address") == 0 ? EE_UI_REPORT_ADDRESS : EE_UI_REPORT_PRECINCT;
        }
        else if (strcmp(argv[i], "--zoom") == 0 && i + 1 < argc)
        {
            args.zoom = atoi(argv[++i]);
        }
        else if (strcmp(argv[i], "--names") == 0 && i + 1 < argc)
        {
            i++;
            args.names = strcmp(argv[i], "given") == 0 ? 0 : 1;
        }
        else if (strcmp(argv[i], "--options-window") == 0)
        {
            args.options_window = TRUE;
        }
        else if (strcmp(argv[i], "--load-filter") == 0 && i + 1 < argc)
        {
            args.load_filter = argv[++i];
        }
        else if (strcmp(argv[i], "--export-filter") == 0 && i + 1 < argc)
        {
            args.export_filter = argv[++i];
        }
        else if (argv[i][0] != '-')
        {
            g_ptr_array_add(args.files, argv[i]);
        }
    }
    /* (NON_UNIQUE: each start is its own window and process, as on Windows) */
    app = gtk_application_new("com.wheelgrouptech.electionexplorer", G_APPLICATION_NON_UNIQUE);
    g_signal_connect(app, "activate", G_CALLBACK(on_activate), &args);
    status = g_application_run(G_APPLICATION(app), 1, argv); /* (our arguments are handled above) */
    g_object_unref(app);
    g_ptr_array_unref(args.quick);
    g_ptr_array_unref(args.files);
    g_ptr_array_unref(args.voting_rosters);
    return status;
}
