/**
 * @file app_mac.h
 * @brief What the AppKit windows share: the voter list and roster windows (app_main.m) and the
 *        Election Results window (results_window.m).
 */
#import <Cocoa/Cocoa.h>

/* the kinds of window, for the menu bar they share: an item's or a menu's tag says which kinds
 * show it (0: every kind) */
enum
{
    EE_MENU_ALL = 0,
    EE_MENU_VOTERS = 1,  /* voter list and roster windows */
    EE_MENU_RESULTS = 2, /* Election Results windows */
};

/* --snapshot of an Election Results window */
@interface EeResultsSnapshot : NSObject
@property(copy) NSString *png;
@property(assign) BOOL byPrecinct;    /* --by-precinct: View > By Precinct */
@property(assign) int sort;           /* --sort <column>[:desc]: heading clicks (-1: none) */
@property(assign) BOOL ascending;
@property(assign) BOOL loadSummary;   /* --load-summary: the PNG is of the Load Summary */
@property(assign) BOOL optionsWindow; /* --options-window: the PNG is of the Settings sheet */
@property(assign) int zoom;           /* --zoom <percent>: 0 for none */
@property(assign) int voteTypes;      /* --vote-types 0|1: -1 for none */
@property(assign) int partyFirst;     /* --party-first rep|dem: -1 for none */
@end

/* Windows' Election Results window (results_window.m) */
@interface EeResultsWindow : NSObject
@property(strong) NSWindow *window;

/* File > Open Election Results...: the open panel over @p parent (or on its own), then the files
 * load behind a progress sheet and each election opens a window */
+ (void)chooseOver:(NSWindow *)parent;

/* open these files as election results, as the panel would; @p snap (or nil) for --snapshot */
+ (void)openPaths:(NSArray<NSString *> *)paths over:(NSWindow *)parent snapshot:(EeResultsSnapshot *)snap;
@end
