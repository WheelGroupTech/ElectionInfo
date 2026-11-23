/**
 * @file app_main.c
 * @brief Election Explorer for Linux, prototype: the voter list window in GTK 4.
 *
 * The same window as the AppKit prototype (src/ui/macos/app_main.m): File > Open... (or a file
 * on the command line), the load in the background with a progress bar, sorting by a column's
 * heading (again to reverse), numbers and dates right-aligned, the Filter window (Ctrl+L, as on
 * Windows), Reset Filter, right-click Include / Exclude on a cell, and a status line. The list
 * comes from the engine through ee_ui_voters.h (plain C).
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

typedef struct VoterWindow
{
    GtkApplication *app;
    GtkWindow *window;
    GtkColumnView *view;
    GtkLabel *status;
    GtkProgressBar *progress;
    GtkWidget
        *cell_menu; /* the right-click menu while it is open (a GtkPopoverMenu on the table) */
    EeUiVoters *voters;
    EeRows *rows;
    char *name;          /* the file's name, for the title and status line */
    gboolean rebuilding; /* columns being replaced: ignore the sorter's changes */
    gboolean closed;     /* the window is gone (the struct stays for queued idle calls) */

    /* the load in progress (the worker writes these, the UI timer reads them) */
    GThread *worker;
    char *loading_path;
    volatile gint percent;
    volatile gint rows_so_far;
    volatile int32_t cancel;
    guint progress_timer;

    /* the view the window asks for (sort and filter); a job computes it in the background */
    int want_sort;
    gboolean want_ascending;
    GArray *rules; /* EeUiRule */
    guint want_gen;
    GThread *job;
    const char *job_label; /* the running job's "Sorting" or "Filtering" */

    /* the right-clicked cell, for Include / Exclude */
    guint quick_column;
    char *quick_value;

    FilterDialog *filter_dialog;

    char *snapshot;                  /* --snapshot: write the content here once loaded, then quit */
    int snapshot_sort;               /* --sort: click this column's heading first (-1: none) */
    gboolean snapshot_ascending;
    GPtrArray *snapshot_quick;       /* --include / --exclude: "+column=value" / "-column=value" */
    gboolean snapshot_filter_window; /* --filter-window: the PNG is of the Filter window */
    int snapshot_tries;
} VoterWindow;

typedef struct LoadDone
{
    VoterWindow *w;
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
        gtk_label_set_text(w->status, "Open a voter list (File > Open Voter List...).");
        return;
    }
    s = g_string_new(NULL);
    if (EeUiVoters_IsFiltered(w->voters))
    {
        /* (the Windows status line's words) */
        g_string_append_printf(s,
                               "Filtered: %u of %u voters",
                               EeUiVoters_Rows(w->voters),
                               EeUiVoters_TotalRows(w->voters));
    }
    else
    {
        g_string_append_printf(s, "%u voters", EeUiVoters_Rows(w->voters));
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
        return G_SOURCE_REMOVE;
    }
    fprintf(stderr,
            "%s took %.2f s\n",
            w->job_label,
            (g_get_monotonic_time() - job->started) / 1e6);
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
    }
    view_job_free(job);
    return G_SOURCE_REMOVE;
}

static gpointer view_worker(gpointer user)
{
    ViewJob *job = (ViewJob *)user;
    job->view =
        EeUiVoters_ComputeView(job->w->voters, job->sort, job->ascending, job->rules, job->nrules);
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
    job->gen = w->want_gen;
    job->started = g_get_monotonic_time();
    {
        int asc = 0;
        int sc = EeUiVoters_SortColumn(w->voters, &asc);
        /* (named for what changes: a new order, else the filter) */
        w->job_label = (sc != job->sort || (sc >= 0 && (asc != 0) != (job->ascending != 0)))
                           ? "Sorting"
                           : "Filtering";
    }
    g_snprintf(text,
               sizeof(text),
               "%s %u voters\xE2\x80\xA6",
               w->job_label,
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

    menu = g_menu_new();
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
    section = g_menu_new();
    g_menu_append(section, "Filter\xE2\x80\xA6", "win.filter");
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

static int text_width(GtkWidget *widget, const char *text)
{
    PangoLayout *layout = gtk_widget_create_pango_layout(widget, text);
    int width = 0, height = 0;
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
    GArray *draft; /* EeUiRule */
};

static void fd_alert(FilterDialog *d, const char *message)
{
    GtkAlertDialog *a = gtk_alert_dialog_new("%s", message);
    gtk_alert_dialog_show(a, d->win);
    g_object_unref(a);
}

static void fd_on_toggled(GtkCheckButton *check, gpointer user)
{
    FilterDialog *d = (FilterDialog *)user;
    guint i = GPOINTER_TO_UINT(g_object_get_data(G_OBJECT(check), "rule"));
    if (i < d->draft->len)
    {
        g_array_index(d->draft, EeUiRule, i).enabled = gtk_check_button_get_active(check) ? 1 : 0;
    }
}

static void fd_refresh(FilterDialog *d)
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
        GtkWidget *check = gtk_check_button_new_with_label(text);
        g_free(text);
        gtk_check_button_set_active(GTK_CHECK_BUTTON(check), r->enabled != 0);
        g_object_set_data(G_OBJECT(check), "rule", GUINT_TO_POINTER(i));
        g_signal_connect(check, "toggled", G_CALLBACK(fd_on_toggled), d);
        gtk_list_box_append(d->list, check);
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
            fd_refresh(d);
            return;
        }
    }
    g_array_append_val(d->draft, r);
    fd_refresh(d);
}

static void fd_on_remove(GtkWidget *button, gpointer user)
{
    FilterDialog *d = (FilterDialog *)user;
    GtkListBoxRow *row = gtk_list_box_get_selected_row(d->list);
    (void)button;
    if (row != NULL && (guint)gtk_list_box_row_get_index(row) < d->draft->len)
    {
        g_array_remove_index(d->draft, (guint)gtk_list_box_row_get_index(row));
        fd_refresh(d);
    }
}

static void fd_on_reset(GtkWidget *button, gpointer user)
{
    FilterDialog *d = (FilterDialog *)user;
    (void)button;
    g_array_set_size(d->draft, 0);
    fd_refresh(d);
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
    GtkWidget *box, *fields, *buttons, *scroll, *bottom, *prompt, *then, *ok;
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

    bottom = gtk_box_new(GTK_ORIENTATION_HORIZONTAL, 6);
    gtk_widget_set_halign(bottom, GTK_ALIGN_END);
    ok = fd_button("_OK", G_CALLBACK(fd_on_ok), d);
    gtk_widget_add_css_class(ok, "suggested-action");
    gtk_box_append(GTK_BOX(bottom), ok);
    gtk_box_append(GTK_BOX(bottom), fd_button("_Cancel", G_CALLBACK(fd_on_cancel), d));
    gtk_box_append(GTK_BOX(bottom), fd_button("A_pply", G_CALLBACK(fd_on_apply), d));
    gtk_box_append(GTK_BOX(box), bottom);

    gtk_window_set_child(d->win, box);
    g_signal_connect(d->win, "destroy", G_CALLBACK(fd_on_destroy), d);
    fd_refresh(d);
    gtk_window_present(d->win);
}

/* -------------------------------------------------------------------------- */
/* Loading                                                                    */
/* -------------------------------------------------------------------------- */

static gboolean take_snapshot(gpointer user)
{
    VoterWindow *w = (VoterWindow *)user;
    GtkWindow *target = w->filter_dialog != NULL ? w->filter_dialog->win : w->window;
    GtkWidget *content = gtk_window_get_child(target);
    int width = gtk_widget_get_width(content), height = gtk_widget_get_height(content);
    GdkPaintable *paint;
    GtkSnapshot *snap;
    GskRenderNode *node;
    if (w->job != NULL)
    {
        return G_SOURCE_CONTINUE; /* (a sort or filter is still running) */
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
static void snapshot_actions(VoterWindow *w)
{
    guint i;
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
    if (w->voters != NULL && w->snapshot_sort >= 0 &&
        (guint)w->snapshot_sort < g_list_model_get_n_items(gtk_column_view_get_columns(w->view)))
    {
        GtkColumnViewColumn *col =
            g_list_model_get_item(gtk_column_view_get_columns(w->view), (guint)w->snapshot_sort);
        gtk_column_view_sort_by_column(w->view,
                                       col,
                                       w->snapshot_ascending ? GTK_SORT_ASCENDING
                                                             : GTK_SORT_DESCENDING);
        g_object_unref(col);
    }
    if (w->snapshot_filter_window)
    {
        show_filter_dialog(w);
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
    gtk_alert_dialog_set_detail(d, detail);
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
        EeUiVoters_Free(w->voters);
        w->voters = done->voters;
        g_array_set_size(w->rules, 0); /* (a new list starts unfiltered, in file order) */
        w->want_sort = -1;
        w->want_ascending = TRUE;
        g_free(w->name);
        w->name = g_path_get_basename(w->loading_path);
        rebuild_columns(w);
        g_snprintf(title, sizeof(title), "%s \xE2\x80\x94 Election Explorer", w->name);
        gtk_window_set_title(w->window, title);
        set_status(w);
    }
    g_clear_pointer(&w->loading_path, g_free);
    if (w->snapshot != NULL)
    {
        snapshot_actions(w);
    }
    g_free(done);
    return G_SOURCE_REMOVE;
}

static gpointer load_worker(gpointer user)
{
    LoadDone *done = (LoadDone *)user;
    done->voters = EeUiVoters_Load(done->w->loading_path,
                                   on_progress,
                                   done->w,
                                   &done->w->cancel,
                                   done->err,
                                   sizeof(done->err));
    g_idle_add(load_done, done);
    return NULL;
}

static void open_path(VoterWindow *w, const char *path)
{
    LoadDone *done;
    if (w->worker != NULL || w->job != NULL)
    {
        return; /* (one load at a time, and not while sorting) */
    }
    done = g_new0(LoadDone, 1);
    done->w = w;
    done->started = g_get_monotonic_time();
    w->loading_path = g_strdup(path);
    g_free(w->name);
    w->name = g_path_get_basename(path);
    g_atomic_int_set(&w->percent, 0);
    g_atomic_int_set(&w->rows_so_far, 0);
    w->cancel = 0;
    gtk_progress_bar_set_fraction(w->progress, 0.0);
    gtk_widget_set_visible(GTK_WIDGET(w->progress), TRUE);
    w->progress_timer = g_timeout_add(100, show_load_progress, w);
    w->worker = g_thread_new("ee-load", load_worker, done);
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
    VoterWindow *w = (VoterWindow *)user;
    (void)action;
    (void)param;
    if (w->voters != NULL && w->rules->len > 0)
    {
        set_rules(w, NULL, 0);
    }
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
    EeUiVoters_Free(w->voters);
    w->voters = NULL;
}

static VoterWindow *voter_window_new(GtkApplication *app)
{
    VoterWindow *w = g_new0(VoterWindow, 1);
    GtkWidget *box = gtk_box_new(GTK_ORIENTATION_VERTICAL, 0);
    GtkWidget *scroll = gtk_scrolled_window_new();
    GtkWidget *bar = gtk_box_new(GTK_ORIENTATION_HORIZONTAL, 8);
    GtkWidget *header = gtk_header_bar_new();
    GtkWidget *open = gtk_button_new_from_icon_name("document-open-symbolic");
    GtkWidget *filter = gtk_button_new_from_icon_name("edit-find-symbolic");
    GMenu *menu = g_menu_new(), *file = g_menu_new(), *filt = g_menu_new(), *help = g_menu_new();
    GtkWidget *menu_button = gtk_menu_button_new();
    const GActionEntry actions[] = {
        {"open", action_open, NULL, NULL, NULL, {0}},
        {"close", action_close, NULL, NULL, NULL, {0}},
        {"quit", action_quit, NULL, NULL, NULL, {0}},
        {"filter", action_filter, NULL, NULL, NULL, {0}},
        {"reset-filter", action_reset_filter, NULL, NULL, NULL, {0}},
        {"quick-include", action_quick_include, NULL, NULL, NULL, {0}},
        {"quick-exclude", action_quick_exclude, NULL, NULL, NULL, {0}},
        {"about", action_about, NULL, NULL, NULL, {0}},
    };
    const char *open_accels[] = {"<Control>o", NULL}, *close_accels[] = {"<Control>w", NULL},
               *quit_accels[] = {"<Control>q", NULL}, *filter_accels[] = {"<Control>l", NULL};

    w->app = app;
    w->rules = g_array_new(FALSE, TRUE, sizeof(EeUiRule));
    w->want_sort = -1;
    w->want_ascending = TRUE;
    w->snapshot_sort = -1;
    w->window = GTK_WINDOW(gtk_application_window_new(app));
    gtk_window_set_title(w->window, "Election Explorer");
    gtk_window_set_default_size(w->window, 1100, 700);

    g_action_map_add_action_entries(G_ACTION_MAP(w->window), actions, G_N_ELEMENTS(actions), w);
    gtk_application_set_accels_for_action(app, "win.open", open_accels);
    gtk_application_set_accels_for_action(app, "win.close", close_accels);
    gtk_application_set_accels_for_action(app, "win.quit", quit_accels);
    gtk_application_set_accels_for_action(app,
                                          "win.filter",
                                          filter_accels); /* (Ctrl+L, as on Windows) */

    /* header bar: Open, Filter, and a menu (File; Filter; About) */
    g_menu_append(file, "Open Voter List\xE2\x80\xA6", "win.open");
    g_menu_append(file, "Close", "win.close");
    g_menu_append(file, "Quit", "win.quit");
    g_menu_append(filt, "Filter\xE2\x80\xA6", "win.filter");
    g_menu_append(filt, "Reset Filter", "win.reset-filter");
    g_menu_append(help, "About Election Explorer", "win.about");
    g_menu_append_section(menu, NULL, G_MENU_MODEL(file));
    g_menu_append_section(menu, NULL, G_MENU_MODEL(filt));
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
    g_object_unref(filt);
    g_object_unref(help);

    /* the table */
    w->view = GTK_COLUMN_VIEW(gtk_column_view_new(NULL));
    gtk_column_view_set_show_column_separators(w->view, TRUE);
    gtk_column_view_set_show_row_separators(w->view, FALSE);
    gtk_column_view_set_reorderable(w->view, FALSE);
    gtk_widget_add_css_class(GTK_WIDGET(w->view), "data-table"); /* (compact rows) */
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
    set_status(w);
    return w;
}

/* -------------------------------------------------------------------------- */
/* The application                                                            */
/* -------------------------------------------------------------------------- */

typedef struct AppArgs
{
    const char *pending;    /* a file to open at start */
    const char *snapshot;   /* --snapshot */
    int sort_column;        /* --sort, with --snapshot: -1 for none */
    gboolean sort_ascending;
    GPtrArray *quick;       /* --include / --exclude, with --snapshot */
    gboolean filter_window; /* --filter-window, with --snapshot */
} AppArgs;

static void on_activate(GtkApplication *app, gpointer user)
{
    AppArgs *args = (AppArgs *)user;
    VoterWindow *w = voter_window_new(app);
    if (args->snapshot != NULL)
    {
        w->snapshot = g_strdup(args->snapshot);
        w->snapshot_sort = args->sort_column;
        w->snapshot_ascending = args->sort_ascending;
        w->snapshot_quick = args->quick;
        w->snapshot_filter_window = args->filter_window;
    }
    gtk_window_present(w->window);
    if (args->pending != NULL)
    {
        open_path(w, args->pending);
    }
}

int main(int argc, char **argv)
{
    AppArgs args = {NULL, NULL, -1, TRUE, NULL, FALSE};
    GtkApplication *app;
    int status, i;
    args.quick = g_ptr_array_new_with_free_func(g_free);
    for (i = 1; i < argc; i++)
    {
        if (strcmp(argv[i], "--selftest") == 0 && i + 1 < argc)
        {
            return EeUiVoters_SelfTest(argv[i + 1]);
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
        else if (argv[i][0] != '-')
        {
            args.pending = argv[i];
        }
    }
    /* (NON_UNIQUE: each start is its own window and process, as on Windows) */
    app = gtk_application_new("com.wheelgrouptech.electionexplorer", G_APPLICATION_NON_UNIQUE);
    g_signal_connect(app, "activate", G_CALLBACK(on_activate), &args);
    status = g_application_run(G_APPLICATION(app), 1, argv); /* (our arguments are handled above) */
    g_object_unref(app);
    g_ptr_array_unref(args.quick);
    return status;
}
