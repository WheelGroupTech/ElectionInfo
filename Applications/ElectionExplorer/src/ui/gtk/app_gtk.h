/**
 * @file app_gtk.h
 * @brief What the GTK windows share: the voter list and roster windows (app_main.c) and the
 *        Election Results window (results_window.c).
 */
#pragma once

#include <gtk/gtk.h>

#include <stdint.h>

/* A list model that makes a small row object only for the rows on screen (app_main.c): a
 * window's table shows rows 0 .. count - 1 of its bridge, in the bridge's order. */
#define EE_TYPE_ROW (ee_row_get_type())
G_DECLARE_FINAL_TYPE(EeRow, ee_row, EE, ROW, GObject)

struct _EeRow
{
    GObject parent_instance;
    guint index; /* display row */
};

#define EE_TYPE_ROWS (ee_rows_get_type())
G_DECLARE_FINAL_TYPE(EeRows, ee_rows, EE, ROWS, GObject)

struct _EeRows
{
    GObject parent_instance;
    guint count;
};

EeRows *ee_rows_new(guint count);

/* the tables' digits of one width, for the display (once) */
void add_table_css(GdkDisplay *display);

/* @p text's width in @p widget's font, with the tables' digits of one width */
int text_width(GtkWidget *widget, const char *text);

/* a column view's selected rows (below @p count), in order (g_free() it) */
uint32_t *view_selection(GtkColumnView *view, guint count, guint *n);

/* @p section appended to @p menu, then released */
void menu_section(GMenu *menu, GMenu *section);

/* --snapshot: @p target's content as a PNG. 1 written, 0 not drawn yet (try again later),
 * -1 failed (said on stderr) */
int ee_gtk_window_png(GtkWindow *target, const char *png);

/* File > Open Voter List... and Open Voter Roster... from a window that is neither (a results
 * window): the file dialog over @p parent, then a window of its own, as on Windows */
void ee_gtk_choose_voter_list(GtkApplication *app, GtkWindow *parent);
void ee_gtk_choose_roster(GtkApplication *app, GtkWindow *parent);

/* About Election Explorer */
void ee_gtk_show_about(GtkWindow *parent);

/* --snapshot of an Election Results window (results_window.c) */
typedef struct EeGtkResultsSnapshot
{
    const char *png;         /* NULL: no snapshot */
    gboolean by_precinct;    /* --by-precinct: View > By Precinct */
    int sort;                /* --sort <column>[:desc]: a heading click (twice for desc) */
    gboolean ascending;
    gboolean load_summary;   /* --load-summary: the PNG is of the Load Summary */
    gboolean options_window; /* --options-window: the PNG is of the Options window */
    int zoom;                /* --zoom <percent>: 0 for none */
    int vote_types;          /* --vote-types 0|1: -1 for none */
    int party_first;         /* --party-first rep|dem: -1 for none */
} EeGtkResultsSnapshot;

/* File > Open Election Results... (results_window.c): the file dialog over @p parent, then
 * the files load behind a progress window and each election opens a window */
void ee_gtk_choose_results(GtkApplication *app, GtkWindow *parent);

/* Open these files (char *, kept by the caller) as election results, as the dialog would;
 * @p snap (or NULL) for --snapshot */
void ee_gtk_open_results(GtkApplication *app,
                         GtkWindow *parent,
                         GPtrArray *paths,
                         const EeGtkResultsSnapshot *snap);
