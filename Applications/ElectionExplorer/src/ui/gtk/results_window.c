/**
 * @file results_window.c
 * @brief The Election Results window in GTK 4, as Windows' (main.c, "Election Results window
 *        (1.5)"): File > Open Election Results... loads the files behind a progress window
 *        with Cancel, and each election opens a window of its own: a row per contest and
 *        choice (Over Votes and Under Votes too when the files give them), View > Totals or
 *        By Precinct, Reset View, a heading click to sort (again to reverse; counts largest
 *        first), Copy (Ctrl+C, or the right-click menu), Filter... (Ctrl+L; the shared Filter
 *        window, Windows' results rules) and Reset Filter, Reports > Display Load Summary...
 *        (open by itself when the files disagree), Edit > Options... (zoom, a column per vote
 *        type, the party shown first in a primary) and Windows' status line. The rows come
 *        from the engine through ee_ui_results.h (plain C).
 */

#include "app_gtk.h"
#include "ee_ui_results.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

typedef struct ResultsWindow ResultsWindow;
typedef struct ResOptions ResOptions;

struct ResultsWindow
{
    GtkApplication *app;
    GtkWindow *window;
    EeUiResults *res;
    EeRows *rows;
    GtkColumnView *view;
    GtkLabel *status;
    GtkWindow *summary;        /* the Load Summary while it is open */
    ResOptions *options;       /* the Options window while it is open */
    FilterDialog *filter;      /* the Filter window while it is open */
    GtkWidget *menu;           /* the right-click menu while it is open */
    int zoom;                  /* the list's text size (Options) */
    gboolean rebuilding;       /* columns being replaced, or the arrow set: ignore the sorter */
    gboolean closed;           /* the window is gone (the struct stays while refs remain) */
    int refs;
    EeGtkResultsSnapshot snap; /* --snapshot (snap.png NULL: none) */
    char *snap_png;
    int snap_tries;
};

static GtkCssProvider *g_zoom_css; /* the results lists' text size (one zoom for them all) */

static void results_unref(ResultsWindow *rw)
{
    if (--rw->refs == 0)
    {
        EeUiResults_Free(rw->res);
        g_clear_object(&rw->rows);
        g_free(rw->snap_png);
        g_free(rw);
    }
}

static void alert(GtkWindow *over, const char *message)
{
    GtkAlertDialog *d = gtk_alert_dialog_new("%s", message);
    gtk_alert_dialog_show(d, over);
    g_object_unref(d);
}

/* -------------------------------------------------------------------------- */
/* The table                                                                  */
/* -------------------------------------------------------------------------- */

/* the column sorters only carry the heading's arrow: the bridge sorts the rows */
static int no_compare(gconstpointer a, gconstpointer b, gpointer user)
{
    (void)a;
    (void)b;
    (void)user;
    return 0;
}

static void set_status(ResultsWindow *rw)
{
    gtk_label_set_text(rw->status, EeUiResults_StatusText(rw->res));
}

/* the heading's arrow where the bridge has sorted (none for the loaded order) */
static void show_sort_arrow(ResultsWindow *rw)
{
    int asc = 1, col = EeUiResults_SortColumn(rw->res, &asc);
    GtkColumnViewColumn *c =
        col >= 0 ? g_list_model_get_item(gtk_column_view_get_columns(rw->view), (guint)col) : NULL;
    rw->rebuilding = TRUE;
    gtk_column_view_sort_by_column(rw->view, NULL, GTK_SORT_ASCENDING);
    if (c != NULL)
    {
        gtk_column_view_sort_by_column(rw->view, c, asc ? GTK_SORT_ASCENDING : GTK_SORT_DESCENDING);
        g_object_unref(c);
    }
    rw->rebuilding = FALSE;
    g_simple_action_set_enabled(
        G_SIMPLE_ACTION(g_action_map_lookup_action(G_ACTION_MAP(rw->window), "reset-view")),
        col >= 0); /* (grayed while the rows are in the loaded order, as on Windows) */
}

/* the rows again in the bridge's (new) order */
static void rows_changed(ResultsWindow *rw)
{
    guint n = rw->rows->count;
    gtk_selection_model_unselect_all(gtk_column_view_get_model(rw->view));
    g_list_model_items_changed(G_LIST_MODEL(rw->rows), 0, n, n);
}

/* a heading clicked: Windows' order (the same column again turns it around; a new one sorts
 * text A to Z, counts largest first), sorted on the UI thread, one arrow only */
static void on_sorter_changed(GtkSorter *sorter, GtkSorterChange change, gpointer user)
{
    ResultsWindow *rw = (ResultsWindow *)user;
    GtkColumnViewColumn *col;
    (void)change;
    if (rw->rebuilding || rw->closed)
    {
        return;
    }
    col = gtk_column_view_sorter_get_primary_sort_column(GTK_COLUMN_VIEW_SORTER(sorter));
    if (col == NULL)
    {
        return;
    }
    EeUiResults_ClickHeading(rw->res,
                             (int)GPOINTER_TO_UINT(g_object_get_data(G_OBJECT(col), "column")));
    show_sort_arrow(rw);
    rows_changed(rw);
}

static void copy_selection(ResultsWindow *rw)
{
    guint n = 0;
    uint32_t *rows = view_selection(rw->view, rw->rows->count, &n);
    char *text = n > 0 ? EeUiResults_CopyText(rw->res, rows, n) : NULL;
    if (text != NULL)
    {
        gdk_clipboard_set_text(gtk_widget_get_clipboard(GTK_WIDGET(rw->window)), text);
        free(text);
    }
    g_free(rows);
}

static gboolean drop_menu(gpointer user)
{
    ResultsWindow *rw = (ResultsWindow *)user;
    if (!rw->closed && rw->menu != NULL && !gtk_widget_get_visible(rw->menu))
    {
        gtk_widget_unparent(rw->menu);
        rw->menu = NULL;
    }
    results_unref(rw);
    return G_SOURCE_REMOVE;
}

static void on_menu_closed(GtkPopover *popover, gpointer user)
{
    ResultsWindow *rw = (ResultsWindow *)user;
    (void)popover;
    rw->refs++;
    g_idle_add(drop_menu, rw); /* (later: the menu's action runs after "closed") */
}

/* the right-click menu, as on Windows: Copy (the row clicked selected first) */
static void on_right_click(GtkGestureClick *g, int n_press, double x, double y, gpointer user)
{
    ResultsWindow *rw = (ResultsWindow *)user;
    GtkWidget *label = gtk_event_controller_get_widget(GTK_EVENT_CONTROLLER(g));
    GtkSelectionModel *model = gtk_column_view_get_model(rw->view);
    guint row = GPOINTER_TO_UINT(g_object_get_data(G_OBJECT(label), "row"));
    GMenu *menu, *section;
    graphene_point_t at;
    (void)n_press;
    if (rw->closed || row >= rw->rows->count)
    {
        return;
    }
    if (!gtk_selection_model_is_selected(model, row))
    {
        gtk_selection_model_select_item(model, row, TRUE);
    }
    menu = g_menu_new();
    section = g_menu_new();
    g_menu_append(section, "Copy", "win.copy");
    menu_section(menu, section);
    if (rw->menu != NULL)
    {
        gtk_widget_unparent(rw->menu);
    }
    rw->menu = gtk_popover_menu_new_from_model(G_MENU_MODEL(menu));
    gtk_widget_set_parent(rw->menu, GTK_WIDGET(rw->view));
    gtk_popover_set_has_arrow(GTK_POPOVER(rw->menu), FALSE);
    gtk_widget_set_halign(rw->menu, GTK_ALIGN_START);
    g_signal_connect(rw->menu, "closed", G_CALLBACK(on_menu_closed), rw);
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

static void cell_setup(GtkSignalListItemFactory *f, GtkListItem *item, gpointer user)
{
    ResultsWindow *rw = (ResultsWindow *)user;
    GtkWidget *label = gtk_label_new(NULL);
    GtkGesture *click = gtk_gesture_click_new();
    gboolean numeric = GPOINTER_TO_INT(g_object_get_data(G_OBJECT(f), "numeric"));
    gtk_label_set_xalign(GTK_LABEL(label), numeric ? 1.0f : 0.0f);
    gtk_label_set_ellipsize(GTK_LABEL(label), PANGO_ELLIPSIZE_END);
    gtk_label_set_single_line_mode(GTK_LABEL(label), TRUE);
    gtk_gesture_single_set_button(GTK_GESTURE_SINGLE(click), GDK_BUTTON_SECONDARY);
    g_signal_connect(click, "pressed", G_CALLBACK(on_right_click), rw);
    gtk_widget_add_controller(label, GTK_EVENT_CONTROLLER(click));
    gtk_list_item_set_child(item, label);
}

static void cell_bind(GtkSignalListItemFactory *f, GtkListItem *item, gpointer user)
{
    ResultsWindow *rw = (ResultsWindow *)user;
    EeRow *row = EE_ROW(gtk_list_item_get_item(item));
    GtkWidget *label = gtk_list_item_get_child(item);
    int c = (int)GPOINTER_TO_UINT(g_object_get_data(G_OBJECT(f), "column"));
    char cell[1024];
    g_object_set_data(G_OBJECT(label), "row", GUINT_TO_POINTER(row->index));
    gtk_label_set_text(GTK_LABEL(label),
                       EeUiResults_Cell(rw->res, row->index, c, cell, sizeof(cell)));
}

/* the zoom: the results lists' text size, as on Windows */
static void apply_zoom_css(GtkWidget *widget, int zoom)
{
    char css[96];
    if (g_zoom_css == NULL)
    {
        g_zoom_css = gtk_css_provider_new();
        gtk_style_context_add_provider_for_display(gtk_widget_get_display(widget),
                                                   GTK_STYLE_PROVIDER(g_zoom_css),
                                                   GTK_STYLE_PROVIDER_PRIORITY_APPLICATION);
    }
    g_snprintf(css, sizeof(css), "columnview.results-list { font-size: %d%%; }", zoom);
    gtk_css_provider_load_from_string(g_zoom_css, css);
}

/* the columns of the view, at Windows' widths for the zoom (wider when the heading needs it),
 * and a new row model */
static void rebuild(ResultsWindow *rw)
{
    GListModel *cols = gtk_column_view_get_columns(rw->view);
    GtkSelectionModel *sel;
    int c, nc = EeUiResults_Columns(rw->res);
    rw->rebuilding = TRUE;
    while (g_list_model_get_n_items(cols) > 0)
    {
        GtkColumnViewColumn *col = g_list_model_get_item(cols, 0);
        gtk_column_view_remove_column(rw->view, col);
        g_object_unref(col);
    }
    for (c = 0; c < nc; c++)
    {
        const char *title = EeUiResults_ColumnTitle(rw->res, c);
        GtkListItemFactory *f = gtk_signal_list_item_factory_new();
        GtkColumnViewColumn *col;
        GtkSorter *sorter;
        int width = EeUiResults_ColumnWidth(rw->res, c) * rw->zoom / 100;
        /* (the heading in bold, at the zoom, and the arrow: measured in the window's font, as
         * a new zoom reaches the table's font only when it is next drawn) */
        int need = text_width(GTK_WIDGET(rw->window), title) * 11 / 10 * rw->zoom / 100 + 40;
        g_object_set_data(G_OBJECT(f), "column", GUINT_TO_POINTER((guint)c));
        g_object_set_data(G_OBJECT(f),
                          "numeric",
                          GINT_TO_POINTER(EeUiResults_ColumnNumeric(rw->res, c)));
        g_signal_connect(f, "setup", G_CALLBACK(cell_setup), rw);
        g_signal_connect(f, "bind", G_CALLBACK(cell_bind), rw);
        col = gtk_column_view_column_new(title, f);
        g_object_set_data(G_OBJECT(col), "column", GUINT_TO_POINTER((guint)c));
        gtk_column_view_column_set_resizable(col, TRUE);
        gtk_column_view_column_set_fixed_width(col, need > width ? need : width);
        sorter = GTK_SORTER(gtk_custom_sorter_new(no_compare, NULL, NULL));
        gtk_column_view_column_set_sorter(col, sorter);
        g_object_unref(sorter);
        gtk_column_view_append_column(rw->view, col);
        g_object_unref(col);
    }
    g_clear_object(&rw->rows);
    rw->rows = ee_rows_new(EeUiResults_Rows(rw->res));
    sel = GTK_SELECTION_MODEL(gtk_multi_selection_new(G_LIST_MODEL(g_object_ref(rw->rows))));
    gtk_column_view_set_model(rw->view, sel);
    g_object_unref(sel);
    rw->rebuilding = FALSE;
    show_sort_arrow(rw);
    set_status(rw);
}

/* View's check marks */
static void show_view_state(ResultsWindow *rw)
{
    GAction *a = g_action_map_lookup_action(G_ACTION_MAP(rw->window), "view-totals");
    g_simple_action_set_state(G_SIMPLE_ACTION(a),
                              g_variant_new_boolean(!EeUiResults_ByPrecinct(rw->res)));
    a = g_action_map_lookup_action(G_ACTION_MAP(rw->window), "view-precinct");
    g_simple_action_set_state(G_SIMPLE_ACTION(a),
                              g_variant_new_boolean(EeUiResults_ByPrecinct(rw->res)));
}

/* View > Totals / By Precinct, and Options: the rows and columns again (Windows' Res_Rebuild) */
static void set_view(ResultsWindow *rw, gboolean by_precinct, int show_vt, int party_first)
{
    if (!EeUiResults_SetView(rw->res, by_precinct, show_vt, party_first))
    {
        alert(rw->window, "Out of memory.");
    }
    rebuild(rw);
    show_view_state(rw);
}

/* -------------------------------------------------------------------------- */
/* Load Summary and Options                                                   */
/* -------------------------------------------------------------------------- */

static void on_summary_destroy(GtkWidget *widget, gpointer user)
{
    ResultsWindow *rw = (ResultsWindow *)user;
    (void)widget;
    rw->summary = NULL;
    results_unref(rw);
}

static void on_window_ok(GtkButton *button, gpointer user)
{
    (void)button;
    gtk_window_destroy(GTK_WINDOW(user));
}

/* Reports > Display Load Summary...: the files, the one shown, every note and difference, in
 * a text window with OK, modal over the results, as Windows shows it */
static void show_load_summary(ResultsWindow *rw)
{
    GtkWidget *box, *scroll, *view, *bottom, *ok;
    GtkShortcutController *keys;
    if (rw->summary != NULL)
    {
        gtk_window_present(rw->summary);
        return;
    }
    rw->summary = GTK_WINDOW(gtk_window_new());
    gtk_window_set_title(rw->summary, "Election Results Load Summary");
    gtk_window_set_transient_for(rw->summary, rw->window);
    gtk_window_set_modal(rw->summary, TRUE);
    gtk_window_set_destroy_with_parent(rw->summary, TRUE);
    gtk_window_set_default_size(rw->summary, 640, 400);
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
    gtk_text_buffer_set_text(gtk_text_view_get_buffer(GTK_TEXT_VIEW(view)),
                             EeUiResults_LoadSummary(rw->res),
                             -1);
    scroll = gtk_scrolled_window_new();
    gtk_scrolled_window_set_child(GTK_SCROLLED_WINDOW(scroll), view);
    gtk_scrolled_window_set_has_frame(GTK_SCROLLED_WINDOW(scroll), TRUE);
    gtk_widget_set_vexpand(scroll, TRUE);
    gtk_box_append(GTK_BOX(box), scroll);
    bottom = gtk_box_new(GTK_ORIENTATION_HORIZONTAL, 6);
    gtk_widget_set_halign(bottom, GTK_ALIGN_END);
    ok = gtk_button_new_with_mnemonic("_OK");
    gtk_widget_add_css_class(ok, "suggested-action");
    g_signal_connect(ok, "clicked", G_CALLBACK(on_window_ok), rw->summary);
    gtk_box_append(GTK_BOX(bottom), ok);
    gtk_box_append(GTK_BOX(box), bottom);
    gtk_window_set_child(rw->summary, box);
    gtk_window_set_default_widget(rw->summary, ok);
    keys = GTK_SHORTCUT_CONTROLLER(gtk_shortcut_controller_new());
    gtk_shortcut_controller_add_shortcut(
        keys,
        gtk_shortcut_new(gtk_shortcut_trigger_parse_string("Escape"),
                         gtk_named_action_new("window.close")));
    gtk_widget_add_controller(GTK_WIDGET(rw->summary), GTK_EVENT_CONTROLLER(keys));
    rw->refs++;
    g_signal_connect(rw->summary, "destroy", G_CALLBACK(on_summary_destroy), rw);
    gtk_window_present(rw->summary);
    gtk_widget_grab_focus(ok);
}

struct ResOptions
{
    ResultsWindow *rw;
    GtkWindow *win;
    GtkSpinButton *zoom;
    GtkCheckButton *vote_types;
    GtkCheckButton *rep;
    GtkCheckButton *dem;
};

/* the Options window's OK (and --zoom, --vote-types, --party-first): the view again when the
 * vote types, the party or the zoom changed (Windows' App_ShowResultsOptions) */
static void set_options(ResultsWindow *rw, const EeUiResultsOptions *o, gboolean save)
{
    int zoom = EeUiOptions_ClampZoom(o->zoom_percent);
    gboolean again = zoom != rw->zoom ||
                     (o->show_vote_types != 0) != EeUiResults_ShowVoteTypes(rw->res) ||
                     o->party_first != EeUiResults_PartyFirst(rw->res);
    if (zoom != rw->zoom)
    {
        rw->zoom = zoom;
        apply_zoom_css(GTK_WIDGET(rw->window), zoom);
    }
    if (save)
    {
        EeUiResultsOptions s = *o;
        s.zoom_percent = zoom;
        if (!EeUiResultsOptions_Save(&s))
        {
            fprintf(stderr, "could not save the options\n");
        }
    }
    if (again)
    {
        set_view(rw, EeUiResults_ByPrecinct(rw->res), o->show_vote_types, o->party_first);
    }
}

static void ro_on_ok(GtkButton *button, gpointer user)
{
    ResOptions *d = (ResOptions *)user;
    ResultsWindow *rw = d->rw;
    EeUiResultsOptions o;
    (void)button;
    gtk_spin_button_update(d->zoom); /* (a number typed but not yet taken) */
    o.zoom_percent = gtk_spin_button_get_value_as_int(d->zoom);
    o.show_vote_types = gtk_check_button_get_active(d->vote_types) ? 1 : 0;
    o.party_first = gtk_check_button_get_active(d->dem) ? EE_UI_PARTY_DEM : EE_UI_PARTY_REP;
    gtk_window_destroy(d->win);
    if (!rw->closed)
    {
        set_options(rw, &o, TRUE);
    }
}

static void ro_on_cancel(GtkButton *button, gpointer user)
{
    (void)button;
    gtk_window_destroy(((ResOptions *)user)->win);
}

static void ro_on_destroy(GtkWidget *widget, gpointer user)
{
    ResOptions *d = (ResOptions *)user;
    (void)widget;
    if (d->rw->options == d)
    {
        d->rw->options = NULL;
    }
    results_unref(d->rw);
    g_free(d);
}

/* Edit > Options...: Windows' "Election Results Options", OK and Cancel (Esc) */
static void show_options(ResultsWindow *rw)
{
    ResOptions *d;
    GtkWidget *box, *row, *bottom, *ok, *cancel, *radios;
    GtkShortcutController *keys;
    if (rw->options != NULL)
    {
        gtk_window_present(rw->options->win);
        return;
    }
    d = g_new0(ResOptions, 1);
    d->rw = rw;
    rw->refs++;
    rw->options = d;
    d->win = GTK_WINDOW(gtk_window_new());
    gtk_window_set_title(d->win, "Election Results Options");
    gtk_window_set_transient_for(d->win, rw->window);
    gtk_window_set_modal(d->win, TRUE);
    gtk_window_set_resizable(d->win, FALSE);
    gtk_window_set_destroy_with_parent(d->win, TRUE);
    box = gtk_box_new(GTK_ORIENTATION_VERTICAL, 10);
    gtk_widget_set_margin_start(box, 16);
    gtk_widget_set_margin_end(box, 16);
    gtk_widget_set_margin_top(box, 16);
    gtk_widget_set_margin_bottom(box, 16);

    row = gtk_box_new(GTK_ORIENTATION_HORIZONTAL, 6);
    gtk_box_append(GTK_BOX(row), gtk_label_new("Zoom:"));
    d->zoom = GTK_SPIN_BUTTON(gtk_spin_button_new_with_range(EE_UI_ZOOM_MIN, EE_UI_ZOOM_MAX, 1));
    gtk_spin_button_set_numeric(d->zoom, TRUE);
    gtk_spin_button_set_value(d->zoom, rw->zoom);
    gtk_editable_set_width_chars(GTK_EDITABLE(d->zoom), 4);
    gtk_box_append(GTK_BOX(row), GTK_WIDGET(d->zoom));
    gtk_box_append(GTK_BOX(row), gtk_label_new("%  (50\342\200\223250)")); /* (an en dash) */
    gtk_box_append(GTK_BOX(box), row);

    d->vote_types =
        GTK_CHECK_BUTTON(gtk_check_button_new_with_label("Show a column for each vote type"));
    gtk_check_button_set_active(d->vote_types, EeUiResults_ShowVoteTypes(rw->res) != 0);
    gtk_box_append(GTK_BOX(box), GTK_WIDGET(d->vote_types));

    row = gtk_label_new("Party to display first in a primary:");
    gtk_label_set_xalign(GTK_LABEL(row), 0.0f);
    gtk_box_append(GTK_BOX(box), row);
    radios = gtk_box_new(GTK_ORIENTATION_HORIZONTAL, 18);
    gtk_widget_set_margin_start(radios, 10);
    d->rep = GTK_CHECK_BUTTON(gtk_check_button_new_with_label("Republican"));
    d->dem = GTK_CHECK_BUTTON(gtk_check_button_new_with_label("Democratic"));
    gtk_check_button_set_group(d->dem, d->rep);
    gtk_check_button_set_active(EeUiResults_PartyFirst(rw->res) == EE_UI_PARTY_DEM ? d->dem
                                                                                   : d->rep,
                                TRUE);
    gtk_box_append(GTK_BOX(radios), GTK_WIDGET(d->rep));
    gtk_box_append(GTK_BOX(radios), GTK_WIDGET(d->dem));
    gtk_box_append(GTK_BOX(box), radios);

    bottom = gtk_box_new(GTK_ORIENTATION_HORIZONTAL, 6);
    gtk_widget_set_halign(bottom, GTK_ALIGN_END);
    gtk_widget_set_margin_top(bottom, 8);
    ok = gtk_button_new_with_mnemonic("_OK");
    gtk_widget_add_css_class(ok, "suggested-action");
    g_signal_connect(ok, "clicked", G_CALLBACK(ro_on_ok), d);
    cancel = gtk_button_new_with_mnemonic("_Cancel");
    g_signal_connect(cancel, "clicked", G_CALLBACK(ro_on_cancel), d);
    gtk_box_append(GTK_BOX(bottom), ok);
    gtk_box_append(GTK_BOX(bottom), cancel);
    gtk_box_append(GTK_BOX(box), bottom);

    /* Enter is OK (also in the zoom box), Esc is Cancel, as in a Windows dialog */
    gtk_window_set_default_widget(d->win, ok);
    {
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
    g_signal_connect(d->win, "destroy", G_CALLBACK(ro_on_destroy), d);
    gtk_window_present(d->win);
}

/* -------------------------------------------------------------------------- */
/* Commands                                                                   */
/* -------------------------------------------------------------------------- */

static void action_open(GSimpleAction *action, GVariant *param, gpointer user)
{
    ResultsWindow *rw = (ResultsWindow *)user;
    (void)action;
    (void)param;
    ee_gtk_choose_voter_list(rw->app, rw->window);
}

static void action_open_roster(GSimpleAction *action, GVariant *param, gpointer user)
{
    ResultsWindow *rw = (ResultsWindow *)user;
    (void)action;
    (void)param;
    ee_gtk_choose_roster(rw->app, rw->window);
}

static void action_open_results(GSimpleAction *action, GVariant *param, gpointer user)
{
    ResultsWindow *rw = (ResultsWindow *)user;
    (void)action;
    (void)param;
    ee_gtk_choose_results(rw->app, rw->window);
}

static void action_close(GSimpleAction *action, GVariant *param, gpointer user)
{
    (void)action;
    (void)param;
    gtk_window_close(((ResultsWindow *)user)->window);
}

static void action_quit(GSimpleAction *action, GVariant *param, gpointer user)
{
    (void)action;
    (void)param;
    g_application_quit(G_APPLICATION(((ResultsWindow *)user)->app));
}

static void action_copy(GSimpleAction *action, GVariant *param, gpointer user)
{
    (void)action;
    (void)param;
    copy_selection((ResultsWindow *)user);
}

static void action_options(GSimpleAction *action, GVariant *param, gpointer user)
{
    (void)action;
    (void)param;
    show_options((ResultsWindow *)user);
}

/* View > Totals and By Precinct: check items, as on Windows (By Precinct grayed without
 * precinct detail) */
static void action_view(GSimpleAction *action, GVariant *value, gpointer user)
{
    ResultsWindow *rw = (ResultsWindow *)user;
    gboolean precinct = strcmp(g_action_get_name(G_ACTION(action)), "view-precinct") == 0;
    (void)value;
    if (precinct != (EeUiResults_ByPrecinct(rw->res) != 0))
    {
        set_view(rw, precinct, EeUiResults_ShowVoteTypes(rw->res), EeUiResults_PartyFirst(rw->res));
    }
    else
    {
        show_view_state(rw); /* (the view shown again: it stays checked, as on Windows) */
    }
}

/* View > Reset View: back to the order the rows had when loaded */
static void action_reset_view(GSimpleAction *action, GVariant *param, gpointer user)
{
    ResultsWindow *rw = (ResultsWindow *)user;
    (void)action;
    (void)param;
    if (EeUiResults_SortColumn(rw->res, NULL) >= 0)
    {
        EeUiResults_Sort(rw->res, -1, 1);
        show_sort_arrow(rw);
        rows_changed(rw);
    }
}

static void action_summary(GSimpleAction *action, GVariant *param, gpointer user)
{
    (void)action;
    (void)param;
    show_load_summary((ResultsWindow *)user);
}

static void action_about(GSimpleAction *action, GVariant *param, gpointer user)
{
    (void)action;
    (void)param;
    ee_gtk_show_about(((ResultsWindow *)user)->window);
}

/* -------------------------------------------------------------------------- */
/* Filter (Windows' results Filter window, the shared one of app_main.c)      */
/* -------------------------------------------------------------------------- */

/* Filter > Reset Filter: available while rules are listed, as on Windows */
static void show_filter_state(ResultsWindow *rw)
{
    g_simple_action_set_enabled(
        G_SIMPLE_ACTION(g_action_map_lookup_action(G_ACTION_MAP(rw->window), "reset-filter")),
        EeUiResults_RuleCount(rw->res) > 0);
}

/* the rules applied (OK, Apply, Reset Filter): what is counted and shown, made again */
static void set_rules(ResultsWindow *rw, const EeUiRule *rules, uint32_t n)
{
    if (!EeUiResults_SetRules(rw->res, rules, n))
    {
        alert(rw->window, "Out of memory.");
    }
    rebuild(rw); /* (a vote type rule takes its column away) */
    show_filter_state(rw);
}

static const char *filter_title(void *ctx, uint32_t column)
{
    return EeUiResults_FilterTitle(((ResultsWindow *)ctx)->res, column);
}

static unsigned filter_relations(void *ctx, uint32_t column)
{
    (void)ctx;
    (void)column;
    return (1u << EE_UI_REL_IS) | (1u << EE_UI_REL_IS_NOT); /* (Windows' results filter) */
}

static char **filter_values(void *ctx, uint32_t column)
{
    uint32_t n = 0, i;
    char **vals = EeUiResults_FilterValues(((ResultsWindow *)ctx)->res, column, &n);
    char **out = g_new0(char *, n + 1);
    for (i = 0; i < n; i++)
    {
        out[i] = g_strdup(vals[i]);
    }
    EeUiResults_FreeValues(vals, n);
    return out;
}

static int filter_rule_is_valid(void *ctx, const EeUiRule *rule, char *why, size_t whycap)
{
    (void)ctx;
    return EeUiResults_RuleIsValid(rule, why, whycap);
}

static void filter_apply(void *ctx, const EeUiRule *rules, uint32_t n)
{
    ResultsWindow *rw = (ResultsWindow *)ctx;
    if (!rw->closed)
    {
        set_rules(rw, rules, n);
    }
}

static void filter_file_name(void *ctx, char *out, size_t cap)
{
    EeUiResults_FilterFileName(((ResultsWindow *)ctx)->res, out, cap);
}

static int filter_save(void *ctx,
                       const char *path,
                       const EeUiRule *rules,
                       uint32_t n,
                       char delim,
                       char *err,
                       size_t errcap)
{
    return EeUiResults_SaveFilter(((ResultsWindow *)ctx)->res, path, rules, n, delim, err, errcap);
}

static int filter_load(void *ctx,
                       const char *path,
                       EeUiRule **rules,
                       uint32_t *n,
                       char *msg,
                       size_t msgcap,
                       char *detail,
                       size_t detailcap)
{
    return EeUiResults_LoadFilter(((ResultsWindow *)ctx)->res,
                                  path,
                                  rules,
                                  n,
                                  msg,
                                  msgcap,
                                  detail,
                                  detailcap);
}

static void filter_closed(void *ctx)
{
    ResultsWindow *rw = (ResultsWindow *)ctx;
    rw->filter = NULL;
    results_unref(rw);
}

/* Filter > Filter... (Ctrl+L): Windows' "Election Explorer Results Filter" */
static void show_filter(ResultsWindow *rw)
{
    EeGtkFilterHost host;
    EeUiRule *rules;
    uint32_t i, n = EeUiResults_RuleCount(rw->res);
    if (rw->filter != NULL)
    {
        gtk_window_present(ee_gtk_filter_window_widget(rw->filter));
        return;
    }
    memset(&host, 0, sizeof(host));
    host.title = "Election Explorer Results Filter";
    host.prompt = "Count and display results matching these conditions:";
    host.ncols = EE_UI_RF_COUNT;
    host.column_title = filter_title;
    host.relations = filter_relations;
    host.values = filter_values;
    host.rule_is_valid = filter_rule_is_valid;
    host.apply = filter_apply;
    host.file_name = filter_file_name;
    host.save = filter_save;
    host.load = filter_load;
    host.closed = filter_closed;
    host.ctx = rw;
    rules = g_new0(EeUiRule, n > 0 ? n : 1);
    for (i = 0; i < n; i++)
    {
        EeUiResults_GetRule(rw->res, i, &rules[i]);
    }
    rw->refs++; /* (filter_closed) */
    rw->filter = ee_gtk_filter_window(rw->window, &host, rules, n);
    g_free(rules);
}

static void action_filter(GSimpleAction *action, GVariant *param, gpointer user)
{
    (void)action;
    (void)param;
    show_filter((ResultsWindow *)user);
}

static void action_reset_filter(GSimpleAction *action, GVariant *param, gpointer user)
{
    (void)action;
    (void)param;
    set_rules((ResultsWindow *)user, NULL, 0);
}

/* -------------------------------------------------------------------------- */
/* The window                                                                 */
/* -------------------------------------------------------------------------- */

static void on_destroy(GtkWidget *widget, gpointer user)
{
    ResultsWindow *rw = (ResultsWindow *)user;
    (void)widget;
    rw->closed = TRUE;
    if (rw->menu != NULL)
    {
        gtk_widget_unparent(rw->menu);
        rw->menu = NULL;
    }
    results_unref(rw);
}

static gboolean snapshot_png(gpointer user);

/* --snapshot: what the menus would do (View, Options, a heading click), then the PNG */
static gboolean snapshot_actions(gpointer user)
{
    ResultsWindow *rw = (ResultsWindow *)user;
    const EeGtkResultsSnapshot *s = &rw->snap;
    if (rw->closed)
    {
        results_unref(rw);
        results_unref(rw); /* (snapshot_png's too) */
        return G_SOURCE_REMOVE;
    }
    if (rw->summary != NULL && !s->load_summary)
    {
        gtk_window_destroy(rw->summary); /* (opened by itself: the PNG is of the window) */
    }
    if (s->by_precinct)
    {
        set_view(rw, TRUE, EeUiResults_ShowVoteTypes(rw->res), EeUiResults_PartyFirst(rw->res));
    }
    if (s->zoom > 0 || s->vote_types >= 0 || s->party_first >= 0)
    {
        EeUiResultsOptions o;
        o.zoom_percent = s->zoom > 0 ? s->zoom : rw->zoom;
        o.show_vote_types = s->vote_types >= 0 ? s->vote_types : EeUiResults_ShowVoteTypes(rw->res);
        o.party_first = s->party_first >= 0 ? s->party_first : EeUiResults_PartyFirst(rw->res);
        set_options(rw, &o, FALSE); /* (as OK does, without saving) */
    }
    if (s->sort >= 0 && s->sort < EeUiResults_Columns(rw->res))
    {
        GtkColumnViewColumn *col =
            g_list_model_get_item(gtk_column_view_get_columns(rw->view), (guint)s->sort);
        int asc = 1, k;
        for (k = 0; k < 2 && col != NULL; k++)
        {
            /* (a heading click; a second when the first sorted the other way) */
            gtk_column_view_sort_by_column(rw->view, NULL, GTK_SORT_ASCENDING);
            gtk_column_view_sort_by_column(rw->view, col, GTK_SORT_ASCENDING);
            if (EeUiResults_SortColumn(rw->res, &asc) == s->sort &&
                (asc != 0) == (s->ascending != 0))
            {
                break;
            }
        }
        g_clear_object(&col);
    }
    if (s->rules != NULL && s->rules->len > 0)
    {
        /* --include / --exclude <column>=<value>: rules "is", as the Filter window's OK applies
         * them (the column: 0 Precinct, 1 Contest, 2 Choice, 3 Party, 4 Vote Type) */
        GArray *rules = g_array_new(FALSE, TRUE, sizeof(EeUiRule));
        guint i;
        for (i = 0; i < s->rules->len; i++)
        {
            const char *q = (const char *)g_ptr_array_index(s->rules, i);
            const char *eq = strchr(q, '=');
            EeUiRule r;
            if (eq == NULL)
            {
                continue;
            }
            memset(&r, 0, sizeof(r));
            r.column = (uint32_t)atoi(q + 1);
            r.relation = EE_UI_REL_IS;
            r.exclude = q[0] == '-';
            r.enabled = 1;
            g_strlcpy(r.value, eq + 1, sizeof(r.value));
            g_array_append_val(rules, r);
        }
        set_rules(rw, (const EeUiRule *)(void *)rules->data, rules->len);
        g_array_unref(rules);
    }
    if (s->load_summary)
    {
        show_load_summary(rw);
    }
    else if (s->options_window)
    {
        show_options(rw);
    }
    else if (s->filter_window || s->load_filter != NULL || s->export_filter != NULL)
    {
        show_filter(rw);
        if (s->load_filter != NULL)
        {
            ee_gtk_filter_load(rw->filter, s->load_filter);
        }
        if (s->export_filter != NULL)
        {
            ee_gtk_filter_export(rw->filter, s->export_filter);
        }
    }
    g_timeout_add(500, snapshot_png, rw); /* (once drawn) */
    return G_SOURCE_REMOVE;
}

static gboolean snapshot_png(gpointer user)
{
    ResultsWindow *rw = (ResultsWindow *)user;
    GtkWindow *target = rw->snap.load_summary && rw->summary != NULL     ? rw->summary
                        : rw->snap.options_window && rw->options != NULL ? rw->options->win
                        : rw->filter != NULL ? ee_gtk_filter_window_widget(rw->filter)
                                             : rw->window;
    int wrote = rw->closed ? -1 : ee_gtk_window_png(target, rw->snap_png);
    if (wrote == 0 && ++rw->snap_tries < 15)
    {
        return G_SOURCE_CONTINUE;
    }
    if (wrote == 0)
    {
        fprintf(stderr, "nothing to write: the window has not drawn\n");
    }
    g_application_quit(G_APPLICATION(rw->app));
    results_unref(rw);
    return G_SOURCE_REMOVE;
}

/* a window for one election (Windows' App_CreateResultsWindow), taking @p res over */
static ResultsWindow *results_window_new(GtkApplication *app,
                                         EeUiResults *res,
                                         const EeGtkResultsSnapshot *snap)
{
    static const GActionEntry actions[] = {
        {"open", action_open, NULL, NULL, NULL, {0}},
        {"open-roster", action_open_roster, NULL, NULL, NULL, {0}},
        {"open-results", action_open_results, NULL, NULL, NULL, {0}},
        {"close", action_close, NULL, NULL, NULL, {0}},
        {"quit", action_quit, NULL, NULL, NULL, {0}},
        {"copy", action_copy, NULL, NULL, NULL, {0}},
        {"options", action_options, NULL, NULL, NULL, {0}},
        {"view-totals", NULL, NULL, "true", action_view, {0}},
        {"view-precinct", NULL, NULL, "false", action_view, {0}},
        {"reset-view", action_reset_view, NULL, NULL, NULL, {0}},
        {"filter", action_filter, NULL, NULL, NULL, {0}},
        {"reset-filter", action_reset_filter, NULL, NULL, NULL, {0}},
        {"report-summary", action_summary, NULL, NULL, NULL, {0}},
        {"about", action_about, NULL, NULL, NULL, {0}},
    };
    const char *open_accels[] = {"<Control>o", NULL}, *close_accels[] = {"<Control>w", NULL},
               *quit_accels[] = {"<Control>q", NULL}, *copy_accels[] = {"<Control>c", NULL},
               *filter_accels[] = {"<Control>l", NULL};
    ResultsWindow *rw = g_new0(ResultsWindow, 1);
    GMenu *menu = g_menu_new(), *file = g_menu_new(), *edit = g_menu_new(), *view = g_menu_new(),
          *reset = g_menu_new(), *filt = g_menu_new(), *reports = g_menu_new(),
          *help = g_menu_new();
    GtkWidget *header = gtk_header_bar_new(), *menu_button = gtk_menu_button_new();
    GtkWidget *box = gtk_box_new(GTK_ORIENTATION_VERTICAL, 0), *scroll = gtk_scrolled_window_new();
    EeUiResultsOptions o;
    GAction *a;

    rw->app = app;
    rw->res = res;
    rw->refs = 1;
    EeUiResultsOptions_Load(&o); /* (as saved last run) */
    rw->zoom = EeUiOptions_ClampZoom(o.zoom_percent);
    if (snap != NULL && snap->png != NULL)
    {
        rw->snap = *snap;
        rw->snap_png = g_strdup(snap->png);
    }
    rw->window = GTK_WINDOW(gtk_application_window_new(app));
    g_object_set_data(G_OBJECT(rw->window), "ee-results-window", rw);
    gtk_window_set_title(rw->window, EeUiResults_WindowTitle(res));
    gtk_window_set_default_size(rw->window, 1000, 600); /* (Windows' size) */
    g_action_map_add_action_entries(G_ACTION_MAP(rw->window), actions, G_N_ELEMENTS(actions), rw);
    gtk_application_set_accels_for_action(app, "win.open", open_accels);
    gtk_application_set_accels_for_action(app, "win.close", close_accels);
    gtk_application_set_accels_for_action(app, "win.quit", quit_accels);
    gtk_application_set_accels_for_action(app, "win.copy", copy_accels);
    gtk_application_set_accels_for_action(app,
                                          "win.filter",
                                          filter_accels); /* (Ctrl+L, as on Windows) */
    a = g_action_map_lookup_action(G_ACTION_MAP(rw->window), "view-precinct");
    g_simple_action_set_enabled(G_SIMPLE_ACTION(a), EeUiResults_HasPrecinctDetail(res));

    /* the menu: Windows' File, Edit, View, Filter and Reports (Turnout, Export and Compare are
     * still to come) */
    g_menu_append(file, "Open Voter List\xE2\x80\xA6", "win.open");
    g_menu_append(file, "Open Voter Roster\xE2\x80\xA6", "win.open-roster");
    g_menu_append(file, "Open Election Results\xE2\x80\xA6", "win.open-results");
    g_menu_append(file, "Close", "win.close");
    g_menu_append(file, "Quit", "win.quit");
    g_menu_append(edit, "Copy", "win.copy");
    g_menu_append(edit, "Options\xE2\x80\xA6", "win.options");
    g_menu_append(view, "Totals", "win.view-totals");
    g_menu_append(view, "By Precinct", "win.view-precinct");
    g_menu_append(reset, "Reset View", "win.reset-view");
    g_menu_append(filt, "Filter\xE2\x80\xA6", "win.filter");
    g_menu_append(filt, "Reset Filter", "win.reset-filter");
    g_menu_append(reports, "Display Load Summary\xE2\x80\xA6", "win.report-summary");
    g_menu_append(help, "About Election Explorer", "win.about");
    g_menu_append_section(menu, NULL, G_MENU_MODEL(file));
    g_menu_append_section(menu, NULL, G_MENU_MODEL(edit));
    g_menu_append_section(menu, "View", G_MENU_MODEL(view));
    g_menu_append_section(menu, NULL, G_MENU_MODEL(reset));
    g_menu_append_section(menu, NULL, G_MENU_MODEL(filt));
    g_menu_append_section(menu, "Reports", G_MENU_MODEL(reports));
    g_menu_append_section(menu, NULL, G_MENU_MODEL(help));
    gtk_menu_button_set_menu_model(GTK_MENU_BUTTON(menu_button), G_MENU_MODEL(menu));
    gtk_menu_button_set_icon_name(GTK_MENU_BUTTON(menu_button), "open-menu-symbolic");
    gtk_header_bar_pack_end(GTK_HEADER_BAR(header), menu_button);
    gtk_window_set_titlebar(rw->window, header);
    g_object_unref(menu);
    g_object_unref(file);
    g_object_unref(edit);
    g_object_unref(view);
    g_object_unref(reset);
    g_object_unref(filt);
    g_object_unref(reports);
    g_object_unref(help);

    /* the table: Windows' list with grid lines */
    rw->view = GTK_COLUMN_VIEW(gtk_column_view_new(NULL));
    gtk_column_view_set_show_column_separators(rw->view, TRUE);
    gtk_column_view_set_show_row_separators(rw->view, TRUE);
    gtk_column_view_set_reorderable(rw->view, FALSE);
    gtk_widget_add_css_class(GTK_WIDGET(rw->view), "data-table");
    gtk_widget_add_css_class(GTK_WIDGET(rw->view), "results-list"); /* (the zoom) */
    add_table_css(gtk_widget_get_display(GTK_WIDGET(rw->window)));
    apply_zoom_css(GTK_WIDGET(rw->window), rw->zoom);
    g_signal_connect(gtk_column_view_get_sorter(rw->view),
                     "changed",
                     G_CALLBACK(on_sorter_changed),
                     rw);
    gtk_scrolled_window_set_child(GTK_SCROLLED_WINDOW(scroll), GTK_WIDGET(rw->view));
    gtk_widget_set_vexpand(scroll, TRUE);

    rw->status = GTK_LABEL(gtk_label_new(NULL));
    gtk_label_set_xalign(rw->status, 0.0f);
    gtk_label_set_ellipsize(rw->status, PANGO_ELLIPSIZE_END);
    gtk_widget_set_margin_start(GTK_WIDGET(rw->status), 8);
    gtk_widget_set_margin_end(GTK_WIDGET(rw->status), 8);
    gtk_widget_set_margin_top(GTK_WIDGET(rw->status), 4);
    gtk_widget_set_margin_bottom(GTK_WIDGET(rw->status), 4);
    gtk_box_append(GTK_BOX(box), scroll);
    gtk_box_append(GTK_BOX(box), GTK_WIDGET(rw->status));
    gtk_window_set_child(rw->window, box);
    g_signal_connect(rw->window, "destroy", G_CALLBACK(on_destroy), rw);

    /* the saved options' view (Totals) */
    set_view(rw, FALSE, o.show_vote_types, o.party_first);
    show_filter_state(rw);
    gtk_window_present(rw->window);
    if (EeUiResults_HasDifferences(res))
    {
        show_load_summary(rw); /* (as on Windows: when the files disagree) */
    }
    if (rw->snap_png != NULL)
    {
        rw->refs += 2; /* (snapshot_actions, then snapshot_png) */
        g_timeout_add(400, snapshot_actions, rw);
    }
    return rw;
}

/* -------------------------------------------------------------------------- */
/* Loading (a worker thread behind Windows' progress window)                  */
/* -------------------------------------------------------------------------- */

typedef struct ResultsLoad
{
    GtkApplication *app;
    GtkWindow *parent;
    char **paths;
    guint n;
    EeGtkResultsSnapshot snap;
    char *png;
    volatile int32_t cancel;
    volatile gint percent;
    int status;
    EeUiResults **out;
    uint32_t nout;
    char err[600];
    GThread *thread;
    GtkWindow *progress;
    GtkProgressBar *bar;
    guint timer;
    gint64 started;
} ResultsLoad;

static int on_progress(void *user, unsigned percent, unsigned rows)
{
    ResultsLoad *job = (ResultsLoad *)user;
    (void)rows;
    g_atomic_int_set(&job->percent, (gint)percent);
    return job->cancel == 0;
}

static gboolean show_progress(gpointer user)
{
    ResultsLoad *job = (ResultsLoad *)user;
    gtk_progress_bar_set_fraction(job->bar, g_atomic_int_get(&job->percent) / 100.0);
    return G_SOURCE_CONTINUE;
}

static gboolean load_done(gpointer user)
{
    ResultsLoad *job = (ResultsLoad *)user;
    uint32_t i;
    g_thread_join(job->thread);
    g_source_remove(job->timer);
    gtk_window_destroy(job->progress);
    fprintf(stderr,
            "results: %u files, %u elections in %.2f s\n",
            job->n,
            job->nout,
            (g_get_monotonic_time() - job->started) / 1e6);
    if (job->status == EE_UI_RESULTS_OK)
    {
        for (i = 0; i < job->nout; i++)
        {
            /* (the first window makes the --snapshot) */
            results_window_new(job->app,
                               job->out[i],
                               i == 0 && job->png != NULL ? &job->snap : NULL);
        }
        if (job->nout == 0 && job->png != NULL)
        {
            g_application_quit(G_APPLICATION(job->app));
        }
    }
    else if (job->status == EE_UI_RESULTS_ERROR)
    {
        fprintf(stderr, "results: %s\n", job->err);
        if (job->png != NULL)
        {
            g_application_quit(G_APPLICATION(job->app));
        }
        else
        {
            alert(job->parent, job->err);
        }
    }
    free(job->out);
    g_strfreev(job->paths);
    g_free(job->png);
    g_object_unref(job->parent);
    g_free(job);
    return G_SOURCE_REMOVE;
}

static gpointer load_worker(gpointer user)
{
    ResultsLoad *job = (ResultsLoad *)user;
    job->status = EeUiResults_Load((const char *const *)job->paths,
                                   job->n,
                                   on_progress,
                                   job,
                                   &job->cancel,
                                   &job->out,
                                   &job->nout,
                                   job->err,
                                   sizeof(job->err));
    g_idle_add(load_done, job);
    return NULL;
}

static void on_cancel(GtkButton *button, gpointer user)
{
    ((ResultsLoad *)user)->cancel = 1;
    gtk_widget_set_sensitive(GTK_WIDGET(button), FALSE);
}

void ee_gtk_open_results(GtkApplication *app,
                         GtkWindow *parent,
                         GPtrArray *paths,
                         const EeGtkResultsSnapshot *snap)
{
    ResultsLoad *job;
    GtkWidget *box, *label, *cancel;
    guint i;
    if (paths == NULL || paths->len == 0)
    {
        return;
    }
    job = g_new0(ResultsLoad, 1);
    job->app = app;
    job->parent = GTK_WINDOW(g_object_ref(parent));
    job->n = paths->len;
    job->paths = g_new0(char *, paths->len + 1);
    for (i = 0; i < paths->len; i++)
    {
        job->paths[i] = g_strdup((const char *)g_ptr_array_index(paths, i));
    }
    if (snap != NULL && snap->png != NULL)
    {
        job->snap = *snap;
        job->png = g_strdup(snap->png);
        job->snap.png = job->png;
    }
    job->status = EE_UI_RESULTS_ERROR;
    job->started = g_get_monotonic_time();

    /* Windows' "Loading Election Results": a label, a bar and Cancel, modal over the window */
    job->progress = GTK_WINDOW(gtk_window_new());
    gtk_window_set_title(job->progress, "Loading Election Results");
    gtk_window_set_transient_for(job->progress, parent);
    gtk_window_set_modal(job->progress, TRUE);
    gtk_window_set_deletable(job->progress, FALSE);
    gtk_window_set_resizable(job->progress, FALSE);
    box = gtk_box_new(GTK_ORIENTATION_VERTICAL, 10);
    gtk_widget_set_margin_start(box, 16);
    gtk_widget_set_margin_end(box, 16);
    gtk_widget_set_margin_top(box, 16);
    gtk_widget_set_margin_bottom(box, 16);
    label = gtk_label_new("Loading election results\xE2\x80\xA6");
    gtk_label_set_xalign(GTK_LABEL(label), 0.0f);
    job->bar = GTK_PROGRESS_BAR(gtk_progress_bar_new());
    gtk_widget_set_size_request(GTK_WIDGET(job->bar), 320, -1);
    cancel = gtk_button_new_with_label("Cancel");
    gtk_widget_set_halign(cancel, GTK_ALIGN_END);
    g_signal_connect(cancel, "clicked", G_CALLBACK(on_cancel), job);
    gtk_box_append(GTK_BOX(box), label);
    gtk_box_append(GTK_BOX(box), GTK_WIDGET(job->bar));
    gtk_box_append(GTK_BOX(box), cancel);
    gtk_window_set_child(job->progress, box);
    gtk_window_present(job->progress);
    job->timer = g_timeout_add(100, show_progress, job);
    job->thread = g_thread_new("ee-results", load_worker, job);
}

static void on_results_chosen(GObject *source, GAsyncResult *result, gpointer user)
{
    GtkWindow *parent = GTK_WINDOW(user);
    GListModel *files = gtk_file_dialog_open_multiple_finish(GTK_FILE_DIALOG(source), result, NULL);
    GPtrArray *paths;
    guint i, n;
    if (files != NULL)
    {
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
        if (paths->len > 0)
        {
            ee_gtk_open_results(gtk_window_get_application(parent), parent, paths, NULL);
        }
        g_ptr_array_unref(paths);
    }
    g_object_unref(parent);
}

/* File > Open Election Results...: one or more files, Windows' types */
void ee_gtk_choose_results(GtkApplication *app, GtkWindow *parent)
{
    static const char *const all_types[] =
        {"zip", "csv", "tsv", "txt", "xml", "xls", "xlsx", "pdf"};
    GtkFileDialog *d = gtk_file_dialog_new();
    GtkFileFilter *results = gtk_file_filter_new(), *zips = gtk_file_filter_new(),
                  *pdfs = gtk_file_filter_new(), *all = gtk_file_filter_new();
    GListStore *filters = g_list_store_new(GTK_TYPE_FILE_FILTER);
    size_t i;
    (void)app;
    gtk_file_filter_set_name(results, "Election Results (ZIP, CSV, TSV, TXT, XML, XLS, XLSX, PDF)");
    for (i = 0; i < G_N_ELEMENTS(all_types); i++)
    {
        gtk_file_filter_add_suffix(results, all_types[i]);
    }
    gtk_file_filter_set_name(zips, "Clarity ZIP files (*.zip)");
    gtk_file_filter_add_suffix(zips, "zip");
    gtk_file_filter_set_name(pdfs, "Results report PDFs (*.pdf)");
    gtk_file_filter_add_suffix(pdfs, "pdf");
    gtk_file_filter_set_name(all, "All files");
    gtk_file_filter_add_pattern(all, "*");
    g_list_store_append(filters, results);
    g_list_store_append(filters, zips);
    g_list_store_append(filters, pdfs);
    g_list_store_append(filters, all);
    gtk_file_dialog_set_title(d, "Open Election Results");
    gtk_file_dialog_set_filters(d, G_LIST_MODEL(filters));
    gtk_file_dialog_open_multiple(d, parent, NULL, on_results_chosen, g_object_ref(parent));
    g_object_unref(filters);
    g_object_unref(results);
    g_object_unref(zips);
    g_object_unref(pdfs);
    g_object_unref(all);
    g_object_unref(d);
}
