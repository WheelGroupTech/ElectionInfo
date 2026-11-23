/**
 * @file app_main.c
 * @brief Election Explorer for Linux, prototype: the voter list window in GTK 4.
 *
 * The same window as the AppKit prototype (src/ui/macos/app_main.m): File > Open... (or a file
 * on the command line), the load in the background with a progress bar, sorting by a column's
 * heading (again to reverse), numbers and dates right-aligned, and a status line. The list
 * comes from the engine through ee_ui_voters.h (plain C).
 *
 * The rows are a GListModel of our own that makes a small row object only for the rows on
 * screen, so a list of 1.5 million voters costs no more than one of five. The column view's
 * sorters only show the heading's arrow; the sorting itself is the engine's
 * (EeUiVoters_SortTo), after which the model reports every row changed.
 *
 * For checking without looking:
 * - `--selftest <file>` loads and sorts the list without a window and prints what the window
 *   would show.
 * - `--snapshot <png> [--sort <column>[:desc]] <file>` opens the window, loads the list, sorts
 *   it as a heading click would (with --sort), writes the window's content as a PNG and quits.
 *
 * Needs GTK 4.10 or later (Ubuntu 24.04 has 4.14).
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

typedef struct VoterWindow
{
    GtkApplication *app;
    GtkWindow *window;
    GtkColumnView *view;
    GtkLabel *status;
    GtkProgressBar *progress;
    EeUiVoters *voters;
    EeRows *rows;
    char *name;      /* the file's name, for the title and status line */
    gboolean sorting;
    gboolean closed; /* the window is gone (the struct stays for a queued load_done) */

    /* the load in progress (the worker writes these, the UI timer reads them) */
    GThread *worker;
    char *loading_path;
    volatile gint percent;
    volatile gint rows_so_far;
    volatile int32_t cancel;
    guint progress_timer;

    char *snapshot;    /* --snapshot: write the content here once loaded, then quit */
    int snapshot_sort; /* --sort: click this column's heading first (-1: none) */
    gboolean snapshot_ascending;
} VoterWindow;

typedef struct LoadDone
{
    VoterWindow *w;
    EeUiVoters *voters;
    char err[1024];
    gint64 started;
} LoadDone;

static void set_status(VoterWindow *w)
{
    char text[512];
    int asc = 0;
    int sc = EeUiVoters_SortColumn(w->voters, &asc);
    if (w->voters == NULL)
    {
        gtk_label_set_text(w->status, "Open a voter list (File > Open Voter List...).");
        return;
    }
    g_snprintf(text,
               sizeof(text),
               "%u voters, %u columns \xE2\x80\x94 %s",
               EeUiVoters_Rows(w->voters),
               EeUiVoters_Columns(w->voters),
               w->name != NULL ? w->name : "");
    if (sc >= 0)
    {
        size_t n = strlen(text);
        g_snprintf(text + n,
                   sizeof(text) - n,
                   " \xE2\x80\x94 sorted by %s, %s",
                   EeUiVoters_Title(w->voters, (uint32_t)sc),
                   asc ? "ascending" : "descending");
    }
    gtk_label_set_text(w->status, text);
}

/* cells: a label per visible cell, set from the engine when bound to a row */
static void cell_setup(GtkSignalListItemFactory *f, GtkListItem *item, gpointer user)
{
    GtkWidget *label = gtk_label_new(NULL);
    gboolean numeric = GPOINTER_TO_INT(g_object_get_data(G_OBJECT(f), "numeric"));
    (void)user;
    gtk_label_set_xalign(GTK_LABEL(label), numeric ? 1.0f : 0.0f);
    gtk_label_set_ellipsize(GTK_LABEL(label), PANGO_ELLIPSIZE_END);
    gtk_label_set_single_line_mode(GTK_LABEL(label), TRUE);
    gtk_list_item_set_child(item, label);
}

static void cell_bind(GtkSignalListItemFactory *f, GtkListItem *item, gpointer user)
{
    VoterWindow *w = (VoterWindow *)user;
    EeRow *row = EE_ROW(gtk_list_item_get_item(item));
    guint c = GPOINTER_TO_UINT(g_object_get_data(G_OBJECT(f), "column"));
    gtk_label_set_text(GTK_LABEL(gtk_list_item_get_child(item)),
                       EeUiVoters_Cell(w->voters, row->index, c));
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
}

/* a heading clicked: sort with the engine, then every row has changed */
static void on_sorter_changed(GtkSorter *sorter, GtkSorterChange change, gpointer user)
{
    VoterWindow *w = (VoterWindow *)user;
    GtkColumnViewSorter *cvs = GTK_COLUMN_VIEW_SORTER(sorter);
    GtkColumnViewColumn *col;
    gint64 started;
    (void)change;
    if (w->voters == NULL || w->sorting)
    {
        return;
    }
    col = gtk_column_view_sorter_get_primary_sort_column(cvs);
    if (col == NULL)
    {
        return;
    }
    started = g_get_monotonic_time();
    EeUiVoters_SortTo(w->voters,
                      GPOINTER_TO_UINT(g_object_get_data(G_OBJECT(col), "column")),
                      gtk_column_view_sorter_get_primary_sort_order(cvs) == GTK_SORT_ASCENDING);
    g_list_model_items_changed(G_LIST_MODEL(w->rows), 0, w->rows->count, w->rows->count);
    fprintf(stderr, "sorted in %.2f s\n", (g_get_monotonic_time() - started) / 1e6);
    set_status(w);
}

static gboolean take_snapshot(gpointer user)
{
    VoterWindow *w = (VoterWindow *)user;
    GtkWidget *content = gtk_window_get_child(w->window);
    int width = gtk_widget_get_width(content), height = gtk_widget_get_height(content);
    GdkPaintable *paint = gtk_widget_paintable_new(content);
    GtkSnapshot *snap = gtk_snapshot_new();
    GskRenderNode *node;
    gdk_paintable_snapshot(paint, GDK_SNAPSHOT(snap), width, height);
    node = gtk_snapshot_free_to_node(snap);
    if (node != NULL)
    {
        GskRenderer *renderer = gtk_native_get_renderer(GTK_NATIVE(w->window));
        GdkTexture *tex =
            gsk_renderer_render_texture(renderer, node, &GRAPHENE_RECT_INIT(0, 0, width, height));
        if (!gdk_texture_save_to_png(tex, w->snapshot))
        {
            fprintf(stderr, "could not write %s\n", w->snapshot);
        }
        g_object_unref(tex);
        gsk_render_node_unref(node);
    }
    g_object_unref(paint);
    g_application_quit(G_APPLICATION(w->app));
    return G_SOURCE_REMOVE;
}

static gboolean show_progress(gpointer user)
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
    if (w->progress_timer != 0)
    {
        g_source_remove(w->progress_timer);
        w->progress_timer = 0;
    }
    gtk_widget_set_visible(GTK_WIDGET(w->progress), FALSE);
    if (done->voters == NULL)
    {
        alert(w, "The voter list could not be opened.", done->err);
        set_status(w);
    }
    else
    {
        fprintf(stderr, "loaded in %.2f s\n", (g_get_monotonic_time() - done->started) / 1e6);
        w->sorting = TRUE;
        EeUiVoters_Free(w->voters);
        w->voters = done->voters;
        g_free(w->name);
        w->name = g_path_get_basename(w->loading_path);
        rebuild_columns(w);
        w->sorting = FALSE;
        g_snprintf(title, sizeof(title), "%s \xE2\x80\x94 Election Explorer", w->name);
        gtk_window_set_title(w->window, title);
        set_status(w);
    }
    g_clear_pointer(&w->loading_path, g_free);
    if (w->snapshot != NULL)
    {
        if (w->voters != NULL && w->snapshot_sort >= 0 &&
            (guint)w->snapshot_sort <
                g_list_model_get_n_items(gtk_column_view_get_columns(w->view)))
        {
            /* (what a heading click does: the column view's sorter changes, on_sorter_changed sorts) */
            GtkColumnViewColumn *col = g_list_model_get_item(gtk_column_view_get_columns(w->view),
                                                             (guint)w->snapshot_sort);
            gtk_column_view_sort_by_column(w->view,
                                           col,
                                           w->snapshot_ascending ? GTK_SORT_ASCENDING
                                                                 : GTK_SORT_DESCENDING);
            g_object_unref(col);
        }
        g_timeout_add(800, take_snapshot, w); /* (time for the window to lay out and draw) */
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
    if (w->worker != NULL)
    {
        return; /* (one load at a time) */
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
    w->progress_timer = g_timeout_add(100, show_progress, w);
    w->worker = g_thread_new("ee-load", load_worker, done);
}

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

/* the window closed: stop a load in progress (its result is dropped) and free the list */
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
    if (w->progress_timer != 0)
    {
        g_source_remove(w->progress_timer);
        w->progress_timer = 0;
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
    GMenu *menu = g_menu_new(), *file = g_menu_new(), *help = g_menu_new();
    GtkWidget *menu_button = gtk_menu_button_new();
    const GActionEntry actions[] = {
        {"open", action_open, NULL, NULL, NULL, {0}},
        {"close", action_close, NULL, NULL, NULL, {0}},
        {"quit", action_quit, NULL, NULL, NULL, {0}},
        {"about", action_about, NULL, NULL, NULL, {0}},
    };
    const char *open_accels[] = {"<Control>o", NULL}, *close_accels[] = {"<Control>w", NULL},
               *quit_accels[] = {"<Control>q", NULL};

    w->app = app;
    w->window = GTK_WINDOW(gtk_application_window_new(app));
    gtk_window_set_title(w->window, "Election Explorer");
    gtk_window_set_default_size(w->window, 1100, 700);

    g_action_map_add_action_entries(G_ACTION_MAP(w->window), actions, G_N_ELEMENTS(actions), w);
    gtk_application_set_accels_for_action(app, "win.open", open_accels);
    gtk_application_set_accels_for_action(app, "win.close", close_accels);
    gtk_application_set_accels_for_action(app, "win.quit", quit_accels);

    /* header bar: Open, and a menu (File: Open, Close, Quit; About) */
    g_menu_append(file, "Open Voter List\xE2\x80\xA6", "win.open");
    g_menu_append(file, "Close", "win.close");
    g_menu_append(file, "Quit", "win.quit");
    g_menu_append(help, "About Election Explorer", "win.about");
    g_menu_append_section(menu, NULL, G_MENU_MODEL(file));
    g_menu_append_section(menu, NULL, G_MENU_MODEL(help));
    gtk_menu_button_set_menu_model(GTK_MENU_BUTTON(menu_button), G_MENU_MODEL(menu));
    gtk_menu_button_set_icon_name(GTK_MENU_BUTTON(menu_button), "open-menu-symbolic");
    gtk_actionable_set_action_name(GTK_ACTIONABLE(open), "win.open");
    gtk_widget_set_tooltip_text(open, "Open Voter List (Ctrl+O)");
    gtk_header_bar_pack_start(GTK_HEADER_BAR(header), open);
    gtk_header_bar_pack_end(GTK_HEADER_BAR(header), menu_button);
    gtk_window_set_titlebar(w->window, header);
    g_object_unref(menu);
    g_object_unref(file);
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
    const char *pending;  /* a file to open at start */
    const char *snapshot; /* --snapshot */
    int sort_column;      /* --sort, with --snapshot: -1 for none */
    gboolean sort_ascending;
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
    }
    gtk_window_present(w->window);
    if (args->pending != NULL)
    {
        open_path(w, args->pending);
    }
}

int main(int argc, char **argv)
{
    AppArgs args = {NULL, NULL, -1, TRUE};
    GtkApplication *app;
    int status, i;
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
    return status;
}
