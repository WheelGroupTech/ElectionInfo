/**
 * @file app_mac.h
 * @brief What the AppKit windows share: the voter list and roster windows (app_main.m) and the
 *        Election Results window (results_window.m).
 */
#import <Cocoa/Cocoa.h>

#include "ee_ui_voters.h" /* EeUiRule */

/* The Filter sheet's host (app_main.m): the window it filters. Windows' Filter window for a
 * voter list or roster, and its rule window for results (is / is not, a value chosen from the
 * column's). */
@protocol EeFilterHost <NSObject>
@property(strong) NSWindow *window;
- (NSString *)filterTitle;
- (NSString *)filterPrompt; /* "Display entries matching these conditions:" */
- (uint32_t)filterColumnCount;
- (const char *)filterColumnTitle:(uint32_t)column; /* NULL: not offered */
- (unsigned)filterRelations:(uint32_t)column;       /* 1u << EE_UI_REL_* */
- (NSArray<NSString *> *)filterValues:(uint32_t)column; /* nil: a typed value */
- (BOOL)filterRule:(const EeUiRule *)rule isValid:(char *)why cap:(size_t)cap;
- (void)applyFilterRules:(const EeUiRule *)rules count:(NSUInteger)n; /* OK, Apply */
- (void)filterFileName:(char *)out cap:(size_t)cap;                   /* Export Filter's name */
- (int)saveFilterTo:(const char *)path
              rules:(const EeUiRule *)rules
              count:(uint32_t)n
              delim:(char)delim
              error:(char *)err
                cap:(size_t)cap;
- (int)loadFilterFrom:(const char *)path
                rules:(EeUiRule **)rules
                count:(uint32_t *)n
              message:(char *)msg
                  cap:(size_t)msgcap
               detail:(char *)detail
                  cap:(size_t)detailcap;
- (void)filterSheetClosed; /* the sheet has gone */
@end

/* The Filter sheet (app_main.m): "<column> <relation> <value> then Include/Exclude" rules, each
 * on or off; Reset, Add, Remove; Export Filter... and Load Filter...; OK, Cancel, Apply */
@interface EeFilterSheet : NSObject
@property(strong) NSWindow *sheet;
- (instancetype)initWithHost:(id<EeFilterHost>)host rules:(const EeUiRule *)rules count:(NSUInteger)n;
- (void)loadFilterFrom:(NSString *)path;                       /* --load-filter */
- (void)exportFilterTo:(NSString *)chosen type:(NSInteger)type; /* --export-filter */
- (void)close;
@end

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
@property(assign) BOOL filterWindow;  /* --filter-window: the PNG is of the Filter sheet */
@property(strong) NSArray<NSString *> *rules; /* --include / --exclude: "+<column>=<value>", "-..." */
@property(copy) NSString *loadFilter;   /* --load-filter <file>: Load Filter... in the sheet */
@property(copy) NSString *exportFilter; /* --export-filter <file>: then Export Filter... */
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
