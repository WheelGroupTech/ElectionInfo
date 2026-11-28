/**
 * @file app_gtk.h
 * @brief What the GTK windows share: the voter list and roster windows (app_main.c) and the
 *        Election Results window (results_window.c).
 */
#pragma once

#include <gtk/gtk.h>

#include <stdint.h>

#include "ee_ui_voters.h" /* EeUiRule */

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

/* The Filter window (app_main.c), for the window it filters, its host: Windows' Filter window
 * for a voter list or roster, and its rule window for results (is / is not, a value chosen from
 * the column's). "<column> <relation> <value> then Include/Exclude" rules, each on or off;
 * Reset, Add, Remove; Export Filter... and Load Filter...; OK, Cancel, Apply. */
typedef struct EeGtkFilterHost
{
    const char *title;  /* the window's title */
    const char *prompt; /* "Display entries matching these conditions:" */
    uint32_t ncols;
    /* a column's title, or NULL when the column is not offered */
    const char *(*column_title)(void *ctx, uint32_t column);
    /* the relations a column offers (1u << EE_UI_REL_*); NULL: all of them */
    unsigned (*relations)(void *ctx, uint32_t column);
    /* the values to choose from (g_strfreev() them), or NULL to type one; the callback itself
     * may be NULL (always typed) */
    char **(*values)(void *ctx, uint32_t column);
    int (*rule_is_valid)(void *ctx, const EeUiRule *rule, char *why, size_t whycap);
    void (*apply)(void *ctx, const EeUiRule *rules, uint32_t n); /* OK, Apply */
    void (*file_name)(void *ctx, char *out, size_t cap);         /* Export Filter's name */
    int (*save)(void *ctx,
                const char *path,
                const EeUiRule *rules,
                uint32_t n,
                char delim,
                char *err,
                size_t errcap);
    int (*load)(void *ctx,
                const char *path,
                EeUiRule **rules,
                uint32_t *n,
                char *msg,
                size_t msgcap,
                char *detail,
                size_t detailcap);
    void (*closed)(void *ctx); /* the window has gone */
    void *ctx;
} EeGtkFilterHost;

typedef struct FilterDialog FilterDialog;

/* the Filter window, modal over @p owner, listing @p rules (the host's applied rules) */
FilterDialog *ee_gtk_filter_window(GtkWindow *owner,
                                   const EeGtkFilterHost *host,
                                   const EeUiRule *rules,
                                   guint n);
GtkWindow *ee_gtk_filter_window_widget(FilterDialog *d);

/* --load-filter / --export-filter: Load Filter... and Export Filter... without the dialogs */
void ee_gtk_filter_load(FilterDialog *d, const char *path);
void ee_gtk_filter_export(FilterDialog *d, const char *path);

/* --snapshot of an Election Results window (results_window.c) */
typedef struct EeGtkResultsSnapshot
{
    const char *png;           /* NULL: no snapshot */
    gboolean by_precinct;      /* --by-precinct: View > By Precinct */
    int sort;                  /* --sort <column>[:desc]: a heading click (twice for desc) */
    gboolean ascending;
    gboolean load_summary;     /* --load-summary: the PNG is of the Load Summary */
    gboolean options_window;   /* --options-window: the PNG is of the Options window */
    int zoom;                  /* --zoom <percent>: 0 for none */
    int vote_types;            /* --vote-types 0|1: -1 for none */
    int party_first;           /* --party-first rep|dem: -1 for none */
    gboolean filter_window;    /* --filter-window: the PNG is of the Filter window */
    GPtrArray *rules;          /* --include / --exclude: "+<column>=<value>" / "-..." (or NULL) */
    const char *load_filter;   /* --load-filter <file>: Load Filter... in the Filter window */
    const char *export_filter; /* --export-filter <file>: then Export Filter... */
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
