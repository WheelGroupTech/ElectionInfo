/**
 * @file app_main.m
 * @brief Election Explorer for macOS, prototype: the voter list window in AppKit.
 *
 * One window with the loaded list in a table: File > Open... (or a file given on the command
 * line, or opened from the Finder), the load in the background with a progress bar, sorting by
 * a column's heading (again to reverse), numbers and dates right-aligned, the Filter sheet
 * (Filter > Filter..., Cmd-L, as Ctrl+L on Windows, with Export Filter... and Load Filter...),
 * Reset Filter, Include / Exclude a cell's
 * value from the right-click menu, Copy (Cmd-C), Export Voter List... and Export Selected...,
 * Show Duplicate Voter IDs / Voters, Reset View, the Precinct and Address reports (Reports menu,
 * a window each), Settings... (Windows' Options, kept between runs), Show in Maps... on an Address
 * cell, Open link... on a cell with an https:// link, and a status line. The list comes from the
 * engine through ee_ui_voters.h (plain C). Compiled with ARC.
 *
 * File > Open Voter Roster... opens a roster's files in a Voter Roster window of its own, as on
 * Windows: the same window with Windows' roster columns and words; the menu bar, shared by every
 * window, hides Show Duplicate Voters and the Address report there and shows Display Voting
 * Totals... and Display Load Summary... Open Voter List... loads into the window in front while
 * it is an empty voter list window, else into a new one, as on Windows.
 *
 * Sorting and filtering are the engine's, computed on a background queue
 * (EeUiVoters_ComputeView) while the table keeps showing the old order, then swapped in on the
 * main thread (EeUiVoters_SetView).
 *
 * For checking over SSH:
 * - `--selftest <file>` loads, sorts and filters the list without a window and prints what the
 *   window would show.
 * - `--snapshot <png> [--sort <column>[:desc]] [--include|--exclude <column>=<value>]...
 *   [--filter-window] <file>` opens the window, loads the list, sorts it as a heading click
 *   would and filters it as the right-click menu would, writes the window (or, with
 *   --filter-window, the Filter sheet) as a PNG and quits. The window draws itself, so no
 *   screen recording permission is needed. `--duplicates ids|voters` shows a duplicates view
 *   first (as the Filter menu does); `--export <file>` exports the shown rows before the PNG
 *   (as Export Voter List... does, without the save sheet). `--report precinct|address` opens
 *   that report and the PNG is of its window; then `--sort 0|1[:desc]` clicks the report's
 *   heading and `--export` exports the report (Export All...). `--zoom <percent>` and
 *   `--names given|surname` set those options once the list is loaded, as the Settings sheet's
 *   OK does (without saving them); `--options-window` makes the PNG of the Settings sheet.
 *   `--load-filter <file>` does what the Filter sheet's Load Filter... does, and
 *   `--export-filter <file>` its Export Filter... (without the panels; either opens the sheet).
 *   `--roster` opens the files named as one voter roster (Open Voter Roster...), and the PNG is
 *   of its window; `--load-summary` makes it of its Load Summary, and `--voting-totals` of its
 *   Voting Totals (then `--export` exports them, as Export All... does).
 * - `--selftest-roster <files...>` loads the files as a roster without a window and prints what
 *   the Voter Roster window would show.
 */

#import <Cocoa/Cocoa.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "ee_ui_voters.h"

static NSString *ns_utf8(const char *s)
{
    return [NSString stringWithUTF8String:s != NULL ? s : ""] ?: @"";
}

@class EeFilterSheet;
@class EeOptionsSheet;
@class EeReportWindow;
@class EeTotalsWindow;

/* an export's field separator, as on Windows: a typed .csv or .tsv wins, else the chosen type
 * (0 CSV, 1 TSV), whose extension is then added to *path */
static char export_delim(NSString **path, NSInteger type)
{
    if ([(*path).pathExtension caseInsensitiveCompare:@"tsv"] == NSOrderedSame)
    {
        return '\t';
    }
    if ([(*path).pathExtension caseInsensitiveCompare:@"csv"] == NSOrderedSame)
    {
        return ',';
    }
    *path = [*path stringByAppendingPathExtension:type == 1 ? @"tsv" : @"csv"];
    return type == 1 ? '\t' : ',';
}

/* the save sheet's file type below it (UTF-8 CSV or TSV, as on Windows), calling
 * exportTypeChanged: on @p target */
static NSPopUpButton *export_type_popup(NSView *extra, id target)
{
    NSTextField *label = [NSTextField labelWithString:@"Format:"];
    NSPopUpButton *type = [[NSPopUpButton alloc] initWithFrame:NSMakeRect(80, 4, 200, 26) pullsDown:NO];
    label.frame = NSMakeRect(20, 8, 60, 20);
    [type addItemsWithTitles:@[ @"UTF-8 CSV (*.csv)", @"UTF-8 TSV (*.tsv)" ]];
    type.target = target;
    type.action = NSSelectorFromString(@"exportTypeChanged:");
    [extra addSubview:label];
    [extra addSubview:type];
    return type;
}

/* the type chosen: the name's extension follows it */
static void export_type_changed(NSPopUpButton *type)
{
    NSSavePanel *panel = (NSSavePanel *)type.window;
    if ([panel isKindOfClass:[NSSavePanel class]])
    {
        NSString *base = panel.nameFieldStringValue.stringByDeletingPathExtension;
        panel.nameFieldStringValue = [base stringByAppendingPathExtension:type.indexOfSelectedItem == 1 ? @"tsv" : @"csv"];
    }
}

/* -------------------------------------------------------------------------- */
/* The voter list window                                                      */
/* -------------------------------------------------------------------------- */

@interface EeVoterWindow
    : NSObject <NSTableViewDataSource, NSTableViewDelegate, NSWindowDelegate, NSMenuDelegate, NSMenuItemValidation>
@property(strong) NSWindow *window;
@property(strong) NSTableView *table;
@property(strong) NSTextField *status;
@property(strong) NSProgressIndicator *progress;
@property(copy) NSString *path;
@property(copy) NSString *name; /* the file's name; a roster of several "<first> (+2 more)" */
@property(assign) EeUiVoters *voters;
@property(assign) BOOL loading;
@property(assign) BOOL roster;               /* a Voter Roster window (Windows' roster mode) */
@property(weak) EeVoterWindow *opener;       /* the window whose Open Voter Roster... made it */
@property(strong) NSAlert *summaryAlert;     /* the Load Summary while it is open */
@property(assign) BOOL snapshotting;         /* --snapshot: no Load Summary on its own */
/* the view the window asks for; a job computes it in the background (the latest wins) */
@property(assign) int wantSort;
@property(assign) BOOL wantAscending;
@property(strong) NSMutableData *rules; /* EeUiRule[] */
@property(assign) int wantMarks;        /* EE_UI_MARKS_*: a duplicates view */
@property(assign) NSUInteger wantGen;
@property(assign) BOOL jobRunning;
@property(strong) EeFilterSheet *filterSheet;
@property(strong) EeReportWindow *precinctReport; /* the open reports */
@property(strong) EeReportWindow *addressReport;
@property(strong) EeTotalsWindow *totalsWindow; /* a roster's open Voting Totals */
@property(strong) EeOptionsSheet *optionsSheet;
@property(assign) EeUiOptions options; /* Settings... (loaded at start, saved on OK) */
@property(assign) int zoomShown;       /* the zoom the columns are sized for */
@property(assign) BOOL namesPending;   /* the Name column to rebuild once a sort or filter is done */
/* the right-clicked cell */
@property(assign) uint32_t quickColumn;
@property(copy) NSString *quickValue;
@property(copy) NSString *quickLink; /* the cell's link for Open link..., or nil */
- (instancetype)initRoster:(BOOL)roster;
- (void)openPath:(NSString *)path;
- (void)openRosterPaths:(NSArray<NSString *> *)paths;
- (IBAction)showLoadSummary:(id)sender;
- (void)setRules:(const EeUiRule *)rules count:(NSUInteger)n;
- (void)addRules:(NSData *)rules;
- (void)showReport:(int)kind;
- (EeReportWindow *)reportOfKind:(int)kind;
- (void)reportClosed:(EeReportWindow *)report;
- (IBAction)showVotingTotals:(id)sender;
- (void)totalsClosed:(EeTotalsWindow *)totals;
- (void)addQuickRuleColumn:(uint32_t)column value:(NSString *)value exclude:(BOOL)exclude;
- (void)sortByColumn:(int)column ascending:(BOOL)ascending;
- (void)showDuplicates:(int)marks;
- (BOOL)exportShownTo:(NSString *)path;
- (IBAction)showFilter:(id)sender;
- (IBAction)showOptions:(id)sender;
- (void)setOptions:(EeUiOptions)options save:(BOOL)save;
@end

/* -------------------------------------------------------------------------- */
/* The Filter sheet (as on Windows: "Display entries matching these           */
/* conditions:", column / relation / value / then Include or Exclude)        */
/* -------------------------------------------------------------------------- */

@interface EeFilterSheet : NSObject <NSTableViewDataSource, NSTableViewDelegate>
@property(weak) EeVoterWindow *owner;
@property(strong) NSWindow *sheet;
@property(strong) NSPopUpButton *column;
@property(strong) NSPopUpButton *relation;
@property(strong) NSPopUpButton *action;
@property(strong) NSTextField *value;
@property(strong) NSTableView *list;
@property(strong) NSMutableData *draft; /* EeUiRule[] */
@property(assign) BOOL filePanelOpen;   /* an Export / Load Filter panel or question is open */
- (void)loadFilterFrom:(NSString *)path;
- (void)exportFilterTo:(NSString *)chosen type:(NSInteger)type;
@end

static NSURL *g_filterFolder; /* the last exported or loaded filter's folder (this run) */

@implementation EeFilterSheet

- (NSUInteger)count
{
    return self.draft.length / sizeof(EeUiRule);
}

- (EeUiRule *)rule:(NSUInteger)i
{
    return (EeUiRule *)self.draft.mutableBytes + i;
}

static NSButton *sheet_button(NSString *title, id target, SEL action)
{
    NSButton *b = [NSButton buttonWithTitle:title target:target action:action];
    b.translatesAutoresizingMaskIntoConstraints = NO;
    return b;
}

- (instancetype)initWithOwner:(EeVoterWindow *)owner
{
    NSView *v;
    NSTextField *prompt, *then;
    NSScrollView *scroll;
    NSTableColumn *on, *text;
    NSButton *reset, *add, *remove, *ok, *cancel, *apply, *exportFilter, *loadFilter;
    uint32_t c;
    int i;
    self = [super init];
    if (self == nil)
    {
        return nil;
    }
    self.owner = owner;
    self.draft = [owner.rules mutableCopy];
    self.sheet = [[NSWindow alloc] initWithContentRect:NSMakeRect(0, 0, 760, 420)
                                             styleMask:NSWindowStyleMaskTitled | NSWindowStyleMaskResizable
                                               backing:NSBackingStoreBuffered
                                                 defer:NO];
    self.sheet.title = @"Filter";
    v = self.sheet.contentView;

    prompt = [NSTextField labelWithString:@"Display entries matching these conditions:"];
    self.column = [[NSPopUpButton alloc] initWithFrame:NSZeroRect pullsDown:NO];
    for (c = 0; c < EeUiVoters_Columns(owner.voters); c++)
    {
        /* (addItemWithTitle drops a duplicate title; a menu item each keeps the indexes) */
        [self.column.menu addItem:[[NSMenuItem alloc] initWithTitle:ns_utf8(EeUiVoters_Title(owner.voters, c))
                                                             action:nil
                                                      keyEquivalent:@""]];
    }
    self.relation = [[NSPopUpButton alloc] initWithFrame:NSZeroRect pullsDown:NO];
    for (i = 0; i < EE_UI_REL_COUNT; i++)
    {
        [self.relation.menu addItem:[[NSMenuItem alloc] initWithTitle:ns_utf8(EeUiVoters_RelationName(i))
                                                               action:nil
                                                        keyEquivalent:@""]];
    }
    self.value = [[NSTextField alloc] initWithFrame:NSZeroRect];
    self.value.target = self;
    self.value.action = @selector(add:);
    then = [NSTextField labelWithString:@"then"];
    self.action = [[NSPopUpButton alloc] initWithFrame:NSZeroRect pullsDown:NO];
    [self.action addItemsWithTitles:@[ @"Include", @"Exclude" ]];

    self.list = [[NSTableView alloc] initWithFrame:NSZeroRect];
    on = [[NSTableColumn alloc] initWithIdentifier:@"on"];
    on.title = @"";
    on.width = 24;
    on.dataCell = [[NSButtonCell alloc] init];
    [(NSButtonCell *)on.dataCell setButtonType:NSButtonTypeSwitch];
    [(NSButtonCell *)on.dataCell setTitle:@""];
    text = [[NSTableColumn alloc] initWithIdentifier:@"rule"];
    text.title = @"Rule";
    text.width = 680;
    [self.list addTableColumn:on];
    [self.list addTableColumn:text];
    self.list.headerView = nil;
    self.list.dataSource = self;
    self.list.delegate = self;
    scroll = [[NSScrollView alloc] initWithFrame:NSZeroRect];
    scroll.documentView = self.list;
    scroll.hasVerticalScroller = YES;
    scroll.borderType = NSBezelBorder;

    reset = sheet_button(@"Reset", self, @selector(reset:));
    add = sheet_button(@"Add", self, @selector(add:));
    remove = sheet_button(@"Remove", self, @selector(remove:));
    ok = sheet_button(@"OK", self, @selector(ok:));
    ok.keyEquivalent = @"\r";
    cancel = sheet_button(@"Cancel", self, @selector(cancel:));
    cancel.keyEquivalent = @"\033";
    apply = sheet_button(@"Apply", self, @selector(apply:));
    exportFilter = sheet_button(@"Export Filter…", self, @selector(exportFilter:));
    loadFilter = sheet_button(@"Load Filter…", self, @selector(loadFilter:));

    for (NSView *sub in @[ prompt, self.column, self.relation, self.value, then, self.action, scroll ])
    {
        sub.translatesAutoresizingMaskIntoConstraints = NO;
        [v addSubview:sub];
    }
    for (NSView *sub in @[ reset, add, remove, ok, cancel, apply, exportFilter, loadFilter ])
    {
        [v addSubview:sub];
    }
    [NSLayoutConstraint activateConstraints:@[
        [prompt.topAnchor constraintEqualToAnchor:v.topAnchor constant:14],
        [prompt.leadingAnchor constraintEqualToAnchor:v.leadingAnchor constant:14],
        [self.column.topAnchor constraintEqualToAnchor:prompt.bottomAnchor constant:8],
        [self.column.leadingAnchor constraintEqualToAnchor:v.leadingAnchor constant:12],
        [self.column.widthAnchor constraintEqualToConstant:170],
        [self.relation.centerYAnchor constraintEqualToAnchor:self.column.centerYAnchor],
        [self.relation.leadingAnchor constraintEqualToAnchor:self.column.trailingAnchor constant:6],
        [self.value.centerYAnchor constraintEqualToAnchor:self.column.centerYAnchor],
        [self.value.leadingAnchor constraintEqualToAnchor:self.relation.trailingAnchor constant:6],
        [then.centerYAnchor constraintEqualToAnchor:self.column.centerYAnchor],
        [then.leadingAnchor constraintEqualToAnchor:self.value.trailingAnchor constant:6],
        [self.action.centerYAnchor constraintEqualToAnchor:self.column.centerYAnchor],
        [self.action.leadingAnchor constraintEqualToAnchor:then.trailingAnchor constant:6],
        [self.action.trailingAnchor constraintEqualToAnchor:v.trailingAnchor constant:-12],
        [remove.topAnchor constraintEqualToAnchor:self.column.bottomAnchor constant:10],
        [remove.trailingAnchor constraintEqualToAnchor:v.trailingAnchor constant:-12],
        [add.centerYAnchor constraintEqualToAnchor:remove.centerYAnchor],
        [add.trailingAnchor constraintEqualToAnchor:remove.leadingAnchor constant:-6],
        [reset.centerYAnchor constraintEqualToAnchor:remove.centerYAnchor],
        [reset.trailingAnchor constraintEqualToAnchor:add.leadingAnchor constant:-6],
        [scroll.topAnchor constraintEqualToAnchor:remove.bottomAnchor constant:10],
        [scroll.leadingAnchor constraintEqualToAnchor:v.leadingAnchor constant:14],
        [scroll.trailingAnchor constraintEqualToAnchor:v.trailingAnchor constant:-14],
        [scroll.bottomAnchor constraintEqualToAnchor:apply.topAnchor constant:-12],
        [apply.bottomAnchor constraintEqualToAnchor:v.bottomAnchor constant:-14],
        [apply.trailingAnchor constraintEqualToAnchor:v.trailingAnchor constant:-12],
        [cancel.centerYAnchor constraintEqualToAnchor:apply.centerYAnchor],
        [cancel.trailingAnchor constraintEqualToAnchor:apply.leadingAnchor constant:-6],
        [ok.centerYAnchor constraintEqualToAnchor:apply.centerYAnchor],
        [ok.trailingAnchor constraintEqualToAnchor:cancel.leadingAnchor constant:-6],
        /* (Windows' bottom row: Export Filter... and Load Filter... at the left) */
        [exportFilter.centerYAnchor constraintEqualToAnchor:apply.centerYAnchor],
        [exportFilter.leadingAnchor constraintEqualToAnchor:v.leadingAnchor constant:12],
        [loadFilter.centerYAnchor constraintEqualToAnchor:apply.centerYAnchor],
        [loadFilter.leadingAnchor constraintEqualToAnchor:exportFilter.trailingAnchor constant:6],
    ]];
    [self.list reloadData];
    return self;
}

- (NSInteger)numberOfRowsInTableView:(NSTableView *)tableView
{
    return (NSInteger)[self count];
}

- (id)tableView:(NSTableView *)tableView objectValueForTableColumn:(NSTableColumn *)column row:(NSInteger)row
{
    EeUiRule *r = [self rule:(NSUInteger)row];
    if ([column.identifier isEqualToString:@"on"])
    {
        return @(r->enabled != 0);
    }
    return [NSString stringWithFormat:@"%@ %@ \"%@\" then %@", ns_utf8(EeUiVoters_Title(self.owner.voters, r->column)),
                                      ns_utf8(EeUiVoters_RelationName(r->relation)), ns_utf8(r->value),
                                      r->exclude ? @"Exclude" : @"Include"];
}

- (void)tableView:(NSTableView *)tableView
    setObjectValue:(id)object
    forTableColumn:(NSTableColumn *)column
               row:(NSInteger)row
{
    if ([column.identifier isEqualToString:@"on"] && (NSUInteger)row < [self count])
    {
        [self rule:(NSUInteger)row]->enabled = [object boolValue] ? 1 : 0;
    }
}

- (BOOL)tableView:(NSTableView *)tableView shouldEditTableColumn:(NSTableColumn *)column row:(NSInteger)row
{
    return NO;
}

/* (as on Windows: a selected rule fills the fields, to change it and Add again) */
- (void)tableViewSelectionDidChange:(NSNotification *)note
{
    NSInteger row = self.list.selectedRow;
    EeUiRule *r;
    if (row < 0 || (NSUInteger)row >= [self count])
    {
        return;
    }
    r = [self rule:(NSUInteger)row];
    [self.column selectItemAtIndex:(NSInteger)r->column];
    [self.relation selectItemAtIndex:r->relation];
    [self.action selectItemAtIndex:r->exclude ? 1 : 0];
    self.value.stringValue = ns_utf8(r->value);
}

- (void)alert:(NSString *)message detail:(NSString *)detail
{
    NSAlert *a = [[NSAlert alloc] init];
    a.messageText = message;
    if (detail.length > 0)
    {
        a.informativeText = detail;
    }
    [a beginSheetModalForWindow:self.sheet completionHandler:nil];
}

- (void)alert:(NSString *)message
{
    [self alert:message detail:nil];
}

/* Export Filter... / Load Filter... (as on Windows: the listed rules, on or off, to a UTF-8 CSV
 * or TSV that names the columns by title) */

- (void)saveFilterTo:(NSString *)path delim:(char)delim
{
    char err[256];
    if (!EeUiVoters_SaveFilter(self.owner.voters, path.fileSystemRepresentation, (const EeUiRule *)self.draft.bytes,
                               (uint32_t)[self count], delim, err, sizeof(err)))
    {
        [self alert:ns_utf8(err)];
        return;
    }
    g_filterFolder = [NSURL fileURLWithPath:path.stringByDeletingLastPathComponent isDirectory:YES];
}

/* A name without .csv / .tsv gets the chosen type's ".filter.csv" or ".filter.tsv" (@p type 0
 * or 1), and Windows then asks before replacing a file of that name (the save panel asked only
 * about the name typed) */
- (void)exportFilterTo:(NSString *)chosen type:(NSInteger)type
{
    char path[4096];
    char delim;
    NSString *file;
    if ([self count] == 0)
    {
        [self alert:@"There are no filter rules to export."]; /* (Windows' words) */
        return;
    }
    delim = EeUiFilterFile_Path(chosen.fileSystemRepresentation, (int)type, path, sizeof(path));
    if (delim == 0)
    {
        [self alert:@"Could not create the filter file."];
        return;
    }
    file = [NSString stringWithUTF8String:path] ?: chosen;
    if (![file isEqualToString:chosen] && [[NSFileManager defaultManager] fileExistsAtPath:file])
    {
        NSAlert *a = [[NSAlert alloc] init];
        a.messageText = [NSString stringWithFormat:@"%@ already exists.", file.lastPathComponent];
        a.informativeText = @"Do you want to replace it?";
        [a addButtonWithTitle:@"Cancel"];
        [a addButtonWithTitle:@"Replace"];
        self.filePanelOpen = YES;
        [a beginSheetModalForWindow:self.sheet
                  completionHandler:^(NSModalResponse r) {
                    self.filePanelOpen = NO;
                    if (r == NSAlertSecondButtonReturn && self.owner.voters != NULL)
                    {
                        [self saveFilterTo:file delim:delim];
                    }
                  }];
        return;
    }
    [self saveFilterTo:file delim:delim];
}

- (void)exportTypeChanged:(NSPopUpButton *)sender
{
    export_type_changed(sender);
}

- (void)exportFilter:(id)sender
{
    NSSavePanel *panel;
    NSView *extra;
    NSPopUpButton *type;
    char name[512];
    if (self.filePanelOpen)
    {
        return;
    }
    if ([self count] == 0)
    {
        [self alert:@"There are no filter rules to export."];
        return;
    }
    panel = [NSSavePanel savePanel];
    panel.title = @"Export Filter";
    EeUiFilterFile_Name(self.owner.path.fileSystemRepresentation, name, sizeof(name));
    panel.nameFieldStringValue = ns_utf8(name);
    if (g_filterFolder != nil)
    {
        panel.directoryURL = g_filterFolder;
    }
    extra = [[NSView alloc] initWithFrame:NSMakeRect(0, 0, 420, 36)];
    type = export_type_popup(extra, self);
    [type removeAllItems];
    [type addItemsWithTitles:@[
        @"Election Explorer filter, CSV (*.filter.csv)", @"Election Explorer filter, tab-delimited (*.filter.tsv)"
    ]];
    type.frame = NSMakeRect(80, 4, 330, 26);
    panel.accessoryView = extra;
    self.filePanelOpen = YES;
    [panel beginSheetModalForWindow:self.sheet
                  completionHandler:^(NSModalResponse r) {
                    self.filePanelOpen = NO;
                    if (r == NSModalResponseOK && panel.URL.path != nil && self.owner.voters != NULL)
                    {
                        /* (after the panel has gone, so a question can take its place) */
                        NSString *chosen = panel.URL.path;
                        NSInteger t = type.indexOfSelectedItem;
                        dispatch_async(dispatch_get_main_queue(), ^{
                          [self exportFilterTo:chosen type:t];
                        });
                    }
                  }];
}

/* Its rules replace the listed ones (they take effect on Apply or OK); the rules this list
 * cannot use are listed, as on Windows */
- (void)loadFilterFrom:(NSString *)path
{
    EeUiRule *rules = NULL;
    uint32_t n = 0;
    char msg[256], detail[2048];
    int r = EeUiVoters_LoadFilter(self.owner.voters, path.fileSystemRepresentation, &rules, &n, msg, sizeof(msg),
                                  detail, sizeof(detail));
    g_filterFolder = [NSURL fileURLWithPath:path.stringByDeletingLastPathComponent isDirectory:YES];
    if (r <= 0)
    {
        [self alert:ns_utf8(msg)];
        return;
    }
    self.draft.length = 0;
    if (n > 0)
    {
        [self.draft appendBytes:rules length:n * sizeof(EeUiRule)];
    }
    free(rules);
    [self.list reloadData];
    if (msg[0] != '\0')
    {
        [self alert:ns_utf8(msg) detail:ns_utf8(detail)];
    }
}

- (void)loadFilter:(id)sender
{
    NSOpenPanel *panel;
    if (self.filePanelOpen)
    {
        return;
    }
    panel = [NSOpenPanel openPanel];
    panel.title = @"Load Filter";
    panel.canChooseFiles = YES;
    panel.canChooseDirectories = NO;
    panel.allowsMultipleSelection = NO;
    if (g_filterFolder != nil)
    {
        panel.directoryURL = g_filterFolder;
    }
    self.filePanelOpen = YES;
    [panel beginSheetModalForWindow:self.sheet
                  completionHandler:^(NSModalResponse r) {
                    self.filePanelOpen = NO;
                    if (r == NSModalResponseOK && panel.URL.path != nil && self.owner.voters != NULL)
                    {
                        NSString *path = panel.URL.path;
                        dispatch_async(dispatch_get_main_queue(), ^{
                          [self loadFilterFrom:path];
                        });
                    }
                  }];
}

- (void)add:(id)sender
{
    EeUiRule r;
    char why[256];
    NSUInteger i;
    memset(&r, 0, sizeof(r));
    r.column = (uint32_t)MAX(self.column.indexOfSelectedItem, 0);
    r.relation = (int)MAX(self.relation.indexOfSelectedItem, 0);
    r.exclude = self.action.indexOfSelectedItem == 1;
    r.enabled = 1;
    strncpy(r.value, self.value.stringValue.UTF8String ?: "", sizeof(r.value) - 1);
    if (!EeUiVoters_RuleIsValid(self.owner.voters, &r, why, sizeof(why)))
    {
        [self alert:ns_utf8(why)];
        return;
    }
    for (i = 0; i < [self count]; i++)
    {
        EeUiRule *e = [self rule:i];
        if (e->column == r.column && e->relation == r.relation && e->exclude == r.exclude &&
            strcmp(e->value, r.value) == 0)
        {
            e->enabled = 1; /* (already listed: just turn it on) */
            [self.list reloadData];
            return;
        }
    }
    [self.draft appendBytes:&r length:sizeof(r)];
    [self.list reloadData];
}

- (void)remove:(id)sender
{
    NSInteger row = self.list.selectedRow;
    if (row >= 0 && (NSUInteger)row < [self count])
    {
        [self.draft replaceBytesInRange:NSMakeRange((NSUInteger)row * sizeof(EeUiRule), sizeof(EeUiRule))
                              withBytes:NULL
                                 length:0];
        [self.list reloadData];
    }
}

- (void)reset:(id)sender
{
    self.draft.length = 0;
    [self.list reloadData];
}

- (void)apply:(id)sender
{
    [self.owner setRules:(const EeUiRule *)self.draft.bytes count:[self count]];
}

- (void)close
{
    [self.owner.window endSheet:self.sheet];
    self.owner.filterSheet = nil;
}

- (void)ok:(id)sender
{
    [self apply:sender];
    [self close];
}

- (void)cancel:(id)sender
{
    [self close];
}

@end

/* -------------------------------------------------------------------------- */
/* Settings (Windows' Edit > Options...: the same four options, kept between  */
/* runs; the Mac's place for them is the application menu)                    */
/* -------------------------------------------------------------------------- */

@interface EeOptionsSheet : NSObject
@property(weak) EeVoterWindow *owner;
@property(strong) NSWindow *sheet;
@property(strong) NSButton *prepend;
@property(strong) NSButton *surname;
@property(strong) NSTextField *zoom;
@property(strong) NSStepper *stepper;
@property(strong) NSPopUpButton *map;
@end

@implementation EeOptionsSheet

- (instancetype)initWithOwner:(EeVoterWindow *)owner
{
    NSView *v;
    NSTextField *zoomLabel, *percent, *mapLabel;
    NSButton *ok, *cancel;
    NSNumberFormatter *digits;
    EeUiOptions o = owner.options;
    BOOL roster = owner.roster;
    int i;
    self = [super init];
    if (self == nil)
    {
        return nil;
    }
    self.owner = owner;
    self.sheet = [[NSWindow alloc] initWithContentRect:NSMakeRect(0, 0, 470, roster ? 130 : 210)
                                             styleMask:NSWindowStyleMaskTitled
                                               backing:NSBackingStoreBuffered
                                                 defer:NO];
    self.sheet.title = @"Settings";
    v = self.sheet.contentView;

    self.prepend = [NSButton checkboxWithTitle:@"Pre-pend normalized data for copies (ID, Precinct, Name, Address)"
                                        target:nil
                                        action:nil];
    self.prepend.state = o.copy_normalized ? NSControlStateValueOn : NSControlStateValueOff;
    self.surname = [NSButton checkboxWithTitle:@"Display name in surname-first format" target:nil action:nil];
    self.surname.state = o.surname_first ? NSControlStateValueOn : NSControlStateValueOff;

    zoomLabel = [NSTextField labelWithString:@"Zoom:"];
    digits = [[NSNumberFormatter alloc] init];
    digits.numberStyle = NSNumberFormatterNoStyle;
    digits.minimum = @(EE_UI_ZOOM_MIN);
    digits.maximum = @(EE_UI_ZOOM_MAX);
    digits.allowsFloats = NO;
    self.zoom = [NSTextField textFieldWithString:[NSString stringWithFormat:@"%d", EeUiOptions_ClampZoom(o.zoom_percent)]];
    self.zoom.formatter = digits;
    self.zoom.alignment = NSTextAlignmentRight;
    self.stepper = [[NSStepper alloc] initWithFrame:NSZeroRect];
    self.stepper.minValue = EE_UI_ZOOM_MIN;
    self.stepper.maxValue = EE_UI_ZOOM_MAX;
    self.stepper.increment = 1;
    self.stepper.valueWraps = NO;
    self.stepper.integerValue = EeUiOptions_ClampZoom(o.zoom_percent);
    self.stepper.target = self;
    self.stepper.action = @selector(stepped:);
    percent = [NSTextField labelWithString:@"%  (50–250)"];

    mapLabel = [NSTextField labelWithString:@"Map engine:"];
    self.map = [[NSPopUpButton alloc] initWithFrame:NSZeroRect pullsDown:NO];
    for (i = 0; i < EE_UI_MAP_COUNT; i++)
    {
        [self.map addItemWithTitle:ns_utf8(EeUiOptions_MapName(i))];
    }
    [self.map selectItemAtIndex:o.map_engine];

    ok = sheet_button(@"OK", self, @selector(ok:));
    ok.keyEquivalent = @"\r";
    cancel = sheet_button(@"Cancel", self, @selector(cancel:));
    cancel.keyEquivalent = @"\033";

    for (NSView *sub in @[ self.prepend, self.surname, zoomLabel, self.zoom, self.stepper, percent, mapLabel, self.map ])
    {
        sub.translatesAutoresizingMaskIntoConstraints = NO;
        [v addSubview:sub];
    }
    [v addSubview:ok];
    [v addSubview:cancel];
    /* (a roster: no Copy prefix, since it copies its own columns, and no map, since it has no
     * addresses; the two stay laid out, hidden, so the zoom box keeps its place) */
    self.prepend.hidden = roster;
    mapLabel.hidden = roster;
    self.map.hidden = roster;
    [NSLayoutConstraint activateConstraints:@[
        [self.prepend.topAnchor constraintEqualToAnchor:v.topAnchor constant:18],
        [self.prepend.leadingAnchor constraintEqualToAnchor:v.leadingAnchor constant:18],
        [self.prepend.trailingAnchor constraintLessThanOrEqualToAnchor:v.trailingAnchor constant:-18],
        [self.surname.topAnchor constraintEqualToAnchor:roster ? v.topAnchor : self.prepend.bottomAnchor
                                               constant:roster ? 18 : 10],
        [self.surname.leadingAnchor constraintEqualToAnchor:self.prepend.leadingAnchor],
        [zoomLabel.leadingAnchor constraintEqualToAnchor:self.prepend.leadingAnchor],
        [zoomLabel.centerYAnchor constraintEqualToAnchor:self.zoom.centerYAnchor],
        [mapLabel.leadingAnchor constraintEqualToAnchor:self.prepend.leadingAnchor],
        [mapLabel.centerYAnchor constraintEqualToAnchor:self.map.centerYAnchor],
        [self.zoom.topAnchor constraintEqualToAnchor:self.surname.bottomAnchor constant:14],
        [self.zoom.leadingAnchor constraintEqualToAnchor:mapLabel.trailingAnchor constant:8],
        [self.zoom.widthAnchor constraintEqualToConstant:52],
        [self.stepper.leadingAnchor constraintEqualToAnchor:self.zoom.trailingAnchor constant:2],
        [self.stepper.centerYAnchor constraintEqualToAnchor:self.zoom.centerYAnchor],
        [percent.leadingAnchor constraintEqualToAnchor:self.stepper.trailingAnchor constant:6],
        [percent.centerYAnchor constraintEqualToAnchor:self.zoom.centerYAnchor],
        [self.map.topAnchor constraintEqualToAnchor:self.zoom.bottomAnchor constant:12],
        [self.map.leadingAnchor constraintEqualToAnchor:self.zoom.leadingAnchor],
        [self.map.widthAnchor constraintEqualToConstant:180],
        [cancel.topAnchor constraintEqualToAnchor:roster ? self.zoom.bottomAnchor : self.map.bottomAnchor
                                         constant:20],
        [cancel.bottomAnchor constraintEqualToAnchor:v.bottomAnchor constant:-16],
        [cancel.trailingAnchor constraintEqualToAnchor:v.trailingAnchor constant:-16],
        [ok.centerYAnchor constraintEqualToAnchor:cancel.centerYAnchor],
        [ok.trailingAnchor constraintEqualToAnchor:cancel.leadingAnchor constant:-8],
        [ok.widthAnchor constraintEqualToAnchor:cancel.widthAnchor],
    ]];
    return self;
}

- (void)stepped:(id)sender
{
    self.zoom.integerValue = self.stepper.integerValue;
}

- (void)close
{
    [self.owner.window endSheet:self.sheet];
    self.owner.optionsSheet = nil;
}

- (void)ok:(id)sender
{
    EeUiOptions o = self.owner.options;
    if (self.owner.roster)
    {
        /* (the two it does not show are kept as saved, perhaps by another window since) */
        EeUiOptions saved;
        EeUiOptions_Load(&saved);
        self.prepend.state = saved.copy_normalized ? NSControlStateValueOn : NSControlStateValueOff;
        [self.map selectItemAtIndex:saved.map_engine];
    }
    o.copy_normalized = self.prepend.state == NSControlStateValueOn;
    o.surname_first = self.surname.state == NSControlStateValueOn;
    /* (Windows keeps the zoom when the box holds no number) */
    o.zoom_percent = self.zoom.stringValue.length > 0 ? (int)self.zoom.integerValue : o.zoom_percent;
    o.map_engine = (int)self.map.indexOfSelectedItem;
    [self close];
    [self.owner setOptions:o save:YES];
}

- (void)cancel:(id)sender
{
    [self close];
}

@end

/* -------------------------------------------------------------------------- */

typedef struct LoadCx
{
    void *win;            /* (__bridge) EeVoterWindow */
    volatile int percent; /* last percent shown */
} LoadCx;

static int on_progress(void *user, unsigned percent, unsigned rows)
{
    LoadCx *cx = (LoadCx *)user;
    if ((int)percent != cx->percent)
    {
        EeVoterWindow *w = (__bridge EeVoterWindow *)cx->win;
        cx->percent = (int)percent;
        dispatch_async(dispatch_get_main_queue(), ^{
          w.progress.doubleValue = percent;
          w.status.stringValue = [NSString stringWithFormat:@"Loading… %u%% (%u rows)", percent, rows];
        });
    }
    return 1;
}

/* -------------------------------------------------------------------------- */
/* Reports (as on Windows: Display Precinct / Address Report..., a window     */
/* each, a row per value with its Number of Voters, over all loaded rows)     */
/* -------------------------------------------------------------------------- */

@interface EeReportWindow
    : NSObject <NSTableViewDataSource, NSTableViewDelegate, NSWindowDelegate, NSMenuDelegate, NSMenuItemValidation>
@property(weak) EeVoterWindow *owner;
@property(strong) NSWindow *window;
@property(strong) NSTableView *table;
@property(strong) NSTextField *status;
@property(assign) EeUiReport *report;
@property(assign) int kind;
@property(copy) NSString *listPath;
@property(assign) NSInteger menuRow; /* the right-clicked row */
- (instancetype)initWithOwner:(EeVoterWindow *)owner report:(EeUiReport *)report path:(NSString *)path;
- (void)sortByColumn:(int)column ascending:(BOOL)ascending;
- (BOOL)exportAllTo:(NSString *)path;
- (void)close;
@end

@implementation EeReportWindow

- (instancetype)initWithOwner:(EeVoterWindow *)owner report:(EeUiReport *)report path:(NSString *)path
{
    NSScrollView *scroll;
    NSView *content;
    /* (the system font with digits of one width, as Windows' Segoe UI has them, so the
     * counts line up) */
    NSFont *font = [NSFont monospacedDigitSystemFontOfSize:NSFont.smallSystemFontSize weight:NSFontWeightRegular];
    NSRect frame;
    CGFloat width;
    char text[600];
    int c;
    self = [super init];
    if (self == nil)
    {
        EeUiReport_Free(report);
        return nil;
    }
    self.owner = owner;
    self.report = report;
    self.kind = EeUiReport_Kind(report);
    self.listPath = path;
    width = self.kind == EE_UI_REPORT_ADDRESS ? 560 : 400;
    self.window = [[NSWindow alloc]
        initWithContentRect:NSMakeRect(0, 0, width, 600)
                  styleMask:NSWindowStyleMaskTitled | NSWindowStyleMaskClosable | NSWindowStyleMaskMiniaturizable |
                            NSWindowStyleMaskResizable
                    backing:NSBackingStoreBuffered
                      defer:NO];
    EeUiReport_WindowTitle(report, path.fileSystemRepresentation, text, sizeof(text));
    self.window.title = ns_utf8(text);
    self.window.delegate = self;
    self.window.releasedWhenClosed = NO;
    /* (as on Windows: below and right of the voter list's corner, and not kept above it) */
    frame = owner.window.frame;
    [self.window setFrameTopLeftPoint:NSMakePoint(NSMinX(frame) + 48, NSMaxY(frame) - 48)];
    content = self.window.contentView;

    self.table = [[NSTableView alloc] initWithFrame:NSZeroRect];
    self.table.dataSource = self;
    self.table.delegate = self;
    self.table.allowsColumnReordering = NO;
    self.table.allowsColumnResizing = YES;
    self.table.allowsMultipleSelection = YES;
    self.table.usesAlternatingRowBackgroundColors = YES;
    self.table.columnAutoresizingStyle = NSTableViewFirstColumnOnlyAutoresizingStyle;
    self.table.gridStyleMask = NSTableViewSolidVerticalGridLineMask | NSTableViewSolidHorizontalGridLineMask;
    if (@available(macOS 11.0, *))
    {
        self.table.style = NSTableViewStylePlain; /* (rows from edge to edge, as on Windows) */
    }
    self.table.menu = [[NSMenu alloc] initWithTitle:@""];
    self.table.menu.delegate = self;
    for (c = 0; c < 2; c++)
    {
        NSTableColumn *col = [[NSTableColumn alloc] initWithIdentifier:[NSString stringWithFormat:@"%d", c]];
        NSTextFieldCell *cell = [[NSTextFieldCell alloc] initTextCell:@""];
        col.title = c == EE_UI_REPORT_COLUMN_VALUE ? ns_utf8(EeUiReport_Heading(self.kind)) : @"Number of Voters";
        col.headerCell.alignment = NSTextAlignmentCenter; /* (as on Windows) */
        cell.font = font;
        cell.lineBreakMode = NSLineBreakByTruncatingTail;
        /* (as on Windows: Precinct and Number of Voters centered, Address at the left) */
        cell.alignment = c == EE_UI_REPORT_COLUMN_VALUE && self.kind == EE_UI_REPORT_ADDRESS ? NSTextAlignmentLeft
                                                                                             : NSTextAlignmentCenter;
        col.dataCell = cell;
        col.minWidth = 60;
        col.resizingMask = c == EE_UI_REPORT_COLUMN_VALUE
                               ? (NSTableColumnAutoresizingMask | NSTableColumnUserResizingMask)
                               : NSTableColumnUserResizingMask;
        [self.table addTableColumn:col];
    }
    self.table.rowHeight = font.boundingRectForFont.size.height + 3;

    scroll = [[NSScrollView alloc] initWithFrame:NSZeroRect];
    scroll.documentView = self.table;
    scroll.hasVerticalScroller = YES;
    scroll.translatesAutoresizingMaskIntoConstraints = NO;
    EeUiReport_StatusText(report, text, sizeof(text));
    self.status = [NSTextField labelWithString:ns_utf8(text)];
    self.status.translatesAutoresizingMaskIntoConstraints = NO;
    [content addSubview:scroll];
    [content addSubview:self.status];
    [NSLayoutConstraint activateConstraints:@[
        [scroll.topAnchor constraintEqualToAnchor:content.topAnchor],
        [scroll.leadingAnchor constraintEqualToAnchor:content.leadingAnchor],
        [scroll.trailingAnchor constraintEqualToAnchor:content.trailingAnchor],
        [scroll.bottomAnchor constraintEqualToAnchor:self.status.topAnchor constant:-6],
        [self.status.leadingAnchor constraintEqualToAnchor:content.leadingAnchor constant:10],
        [self.status.trailingAnchor constraintLessThanOrEqualToAnchor:content.trailingAnchor constant:-10],
        [self.status.bottomAnchor constraintEqualToAnchor:content.bottomAnchor constant:-6],
    ]];
    [content layoutSubtreeIfNeeded];
    {
        /* Number of Voters: its heading and the sort arrow; the value column takes the rest of
         * the laid-out table (widths set before layout leave the table wider than the window) */
        NSTableColumn *value = self.table.tableColumns[0], *count = self.table.tableColumns[1];
        CGFloat room = scroll.contentSize.width - 2 * self.table.intercellSpacing.width;
        count.width = MAX(130, ceil(count.headerCell.cellSize.width) + 28);
        value.width = MAX(value.minWidth, room - count.width);
    }
    [self showSortIndicator];
    [self.window makeKeyAndOrderFront:nil];
    return self;
}

- (void)dealloc
{
    EeUiReport_Free(_report);
}

- (void)close
{
    [self.window close]; /* (windowWillClose: tells the owner) */
}

- (void)windowWillClose:(NSNotification *)note
{
    [self.owner reportClosed:self];
}

- (void)alert:(NSString *)message
{
    NSAlert *a = [[NSAlert alloc] init];
    a.messageText = message;
    [a beginSheetModalForWindow:self.window completionHandler:nil];
}

/* ---- the table ---- */

- (NSInteger)numberOfRowsInTableView:(NSTableView *)tableView
{
    return EeUiReport_Rows(self.report);
}

- (id)tableView:(NSTableView *)tableView objectValueForTableColumn:(NSTableColumn *)column row:(NSInteger)row
{
    if (column.identifier.integerValue == EE_UI_REPORT_COLUMN_COUNT)
    {
        return [NSString stringWithFormat:@"%u", EeUiReport_Count(self.report, (uint32_t)row)];
    }
    return ns_utf8(EeUiReport_Value(self.report, (uint32_t)row));
}

- (BOOL)tableView:(NSTableView *)tableView shouldEditTableColumn:(NSTableColumn *)column row:(NSInteger)row
{
    return NO;
}

- (void)showSortIndicator
{
    int asc = 1, sorted = EeUiReport_SortColumn(self.report, &asc);
    for (NSTableColumn *col in self.table.tableColumns)
    {
        BOOL is = col.identifier.integerValue == sorted;
        [self.table
            setIndicatorImage:(is ? [NSImage imageNamed:asc ? @"NSAscendingSortIndicator" : @"NSDescendingSortIndicator"]
                                  : nil)
                inTableColumn:col];
        if (is)
        {
            self.table.highlightedTableColumn = col;
        }
    }
}

/* a heading click (the same one again reverses it); a report is small: sorted here, as on
 * Windows */
- (void)sortByColumn:(int)column ascending:(BOOL)ascending
{
    EeUiReport_Sort(self.report, column, ascending);
    [self.table deselectAll:nil];
    [self.table reloadData];
    [self showSortIndicator];
}

- (void)tableView:(NSTableView *)tableView didClickTableColumn:(NSTableColumn *)column
{
    int c = (int)column.identifier.integerValue, asc = 1, sorted = EeUiReport_SortColumn(self.report, &asc);
    [self sortByColumn:c ascending:(sorted == c ? !asc : YES)];
}

- (NSMutableData *)selectedRows
{
    NSMutableData *rows = [NSMutableData data];
    [self.table.selectedRowIndexes enumerateIndexesUsingBlock:^(NSUInteger i, BOOL *stop) {
      uint32_t r = (uint32_t)i;
      [rows appendBytes:&r length:sizeof(r)];
    }];
    return rows;
}

/* ---- commands ---- */

/* Copy (Cmd-C or the right-click menu): "<value>\t<count>" a line, as on Windows */
- (IBAction)copy:(id)sender
{
    NSMutableData *rows = [self selectedRows];
    char *text;
    if (rows.length == 0)
    {
        return;
    }
    text = EeUiReport_CopyText(self.report, (const uint32_t *)rows.bytes, (uint32_t)(rows.length / sizeof(uint32_t)),
                               NULL);
    if (text != NULL)
    {
        [NSPasteboard.generalPasteboard clearContents];
        [NSPasteboard.generalPasteboard setString:ns_utf8(text) forType:NSPasteboardTypeString];
        free(text);
    }
}

/* Include / Exclude: a rule "<Precinct or Address> is <value>" in the voter list's filter for
 * each selected row (the blank row matches the empty cells) */
- (void)filterSelected:(BOOL)exclude
{
    NSMutableData *rows = [self selectedRows], *rules = [NSMutableData data];
    const uint32_t *r = (const uint32_t *)rows.bytes;
    NSUInteger i, n = rows.length / sizeof(uint32_t);
    for (i = 0; i < n; i++)
    {
        EeUiRule rule;
        EeUiReport_Rule(self.report, r[i], exclude ? 1 : 0, &rule);
        [rules appendBytes:&rule length:sizeof(rule)];
    }
    [self.owner addRules:rules];
}

- (void)includeSelected:(id)sender
{
    [self filterSelected:NO];
}

- (void)excludeSelected:(id)sender
{
    [self filterSelected:YES];
}

/* Show in Maps... (Address report): the address in the browser's map */
- (void)showInMaps:(id)sender
{
    char *url = EeUiReport_MapUrl(self.report, (uint32_t)self.menuRow, self.owner.options.map_engine);
    NSURL *u = url != NULL ? [NSURL URLWithString:ns_utf8(url)] : nil;
    free(url);
    if (u == nil)
    {
        [self alert:@"Could not build a map link for this address."];
        return;
    }
    if (![NSWorkspace.sharedWorkspace openURL:u])
    {
        [self alert:@"Could not open the map in a browser."];
    }
}

/* Export Selected... / Export All...: a save sheet with the file type (a report has no
 * normalized fields, so no Export Options) */
- (void)exportRows:(BOOL)selection
{
    NSSavePanel *panel;
    NSView *extra;
    NSPopUpButton *type;
    NSMutableData *rows = nil;
    char name[512];
    if (selection)
    {
        rows = [self selectedRows];
        if (rows.length == 0)
        {
            return;
        }
    }
    panel = [NSSavePanel savePanel];
    panel.title = @"Export";
    EeUiReport_ExportName(self.report, self.listPath.fileSystemRepresentation, selection, name, sizeof(name));
    panel.nameFieldStringValue = ns_utf8(name);
    extra = [[NSView alloc] initWithFrame:NSMakeRect(0, 0, 420, 36)];
    type = export_type_popup(extra, self);
    panel.accessoryView = extra;
    [panel beginSheetModalForWindow:self.window
                  completionHandler:^(NSModalResponse r) {
                    NSString *path = panel.URL.path;
                    char delim, err[256];
                    if (r != NSModalResponseOK || path == nil)
                    {
                        return;
                    }
                    delim = export_delim(&path, type.indexOfSelectedItem);
                    if (EeUiReport_Export(self.report, path.fileSystemRepresentation,
                                          rows != nil ? (const uint32_t *)rows.bytes : NULL,
                                          (uint32_t)(rows.length / sizeof(uint32_t)), delim, err, sizeof(err)) < 0)
                    {
                        [self alert:ns_utf8(err)];
                    }
                  }];
}

- (void)exportTypeChanged:(NSPopUpButton *)type
{
    export_type_changed(type);
}

- (IBAction)exportSelected:(id)sender
{
    [self exportRows:YES];
}

- (IBAction)exportAll:(id)sender
{
    [self exportRows:NO];
}

/* --export with --report: Export All..., as a CSV, without the sheet */
- (BOOL)exportAllTo:(NSString *)path
{
    char err[256];
    long wrote = EeUiReport_Export(self.report, path.fileSystemRepresentation, NULL, 0, ',', err, sizeof(err));
    if (wrote < 0)
    {
        fprintf(stderr, "report export: %s\n", err);
        return NO;
    }
    fprintf(stderr, "exported %ld report rows\n", wrote);
    return YES;
}

/* the right-click menu, in Windows' order: Copy; Include, Exclude (on the value column); Show
 * in Maps... (an address); Export Selected..., Export All... */
- (void)menuNeedsUpdate:(NSMenu *)menu
{
    NSInteger row = self.table.clickedRow, col = self.table.clickedColumn;
    [menu removeAllItems];
    if (row < 0)
    {
        return;
    }
    if (![self.table.selectedRowIndexes containsIndex:(NSUInteger)row])
    {
        [self.table selectRowIndexes:[NSIndexSet indexSetWithIndex:(NSUInteger)row] byExtendingSelection:NO];
    }
    self.menuRow = row;
    [menu addItemWithTitle:@"Copy" action:@selector(copy:) keyEquivalent:@""].target = self;
    if (col >= 0 && self.table.tableColumns[(NSUInteger)col].identifier.integerValue == EE_UI_REPORT_COLUMN_VALUE)
    {
        NSString *value = ns_utf8(EeUiReport_Value(self.report, (uint32_t)row));
        /* (at most 40 characters, as on Windows) */
        NSString *shown = value.length >= 40 ? [[value substringToIndex:36] stringByAppendingString:@"..."] : value;
        [menu addItem:[NSMenuItem separatorItem]];
        [menu addItemWithTitle:[NSString stringWithFormat:@"Include \"%@\"", shown]
                        action:@selector(includeSelected:)
                 keyEquivalent:@""]
            .target = self;
        [menu addItemWithTitle:[NSString stringWithFormat:@"Exclude \"%@\"", shown]
                        action:@selector(excludeSelected:)
                 keyEquivalent:@""]
            .target = self;
        if (self.kind == EE_UI_REPORT_ADDRESS && !EeUiReport_IsBlank(self.report, (uint32_t)row))
        {
            [menu addItem:[NSMenuItem separatorItem]];
            [menu addItemWithTitle:@"Show in Maps…" action:@selector(showInMaps:) keyEquivalent:@""].target = self;
        }
    }
    [menu addItem:[NSMenuItem separatorItem]];
    [menu addItemWithTitle:@"Export Selected…" action:@selector(exportSelected:) keyEquivalent:@""].target = self;
    [menu addItemWithTitle:@"Export All…" action:@selector(exportAll:) keyEquivalent:@""].target = self;
}

- (BOOL)validateMenuItem:(NSMenuItem *)item
{
    if (item.action == @selector(copy:) || item.action == @selector(exportSelected:))
    {
        return self.table.numberOfSelectedRows > 0;
    }
    if (item.action == @selector(includeSelected:) || item.action == @selector(excludeSelected:))
    {
        return self.owner.voters != NULL && !self.owner.loading;
    }
    return YES;
}

@end

/* -------------------------------------------------------------------------- */
/* Voting Totals (a roster's Reports > Display Voting Totals..., as on        */
/* Windows: voters per Date Voted, Party and Voting Method; not sortable)     */
/* -------------------------------------------------------------------------- */

@interface EeTotalsWindow
    : NSObject <NSTableViewDataSource, NSTableViewDelegate, NSWindowDelegate, NSMenuDelegate, NSMenuItemValidation>
@property(weak) EeVoterWindow *owner;
@property(strong) NSWindow *window;
@property(strong) NSTableView *table;
@property(strong) NSTextField *status;
@property(assign) EeUiTotals *totals;
@property(copy) NSString *listPath;
@property(strong) NSFont *font;
@property(strong) NSFont *boldFont; /* the Total and All parties rows, as on Windows */
- (instancetype)initWithOwner:(EeVoterWindow *)owner totals:(EeUiTotals *)totals path:(NSString *)path;
- (BOOL)exportAllTo:(NSString *)path;
- (void)close;
@end

@implementation EeTotalsWindow

- (instancetype)initWithOwner:(EeVoterWindow *)owner totals:(EeUiTotals *)totals path:(NSString *)path
{
    NSScrollView *scroll;
    NSView *content;
    NSRect frame;
    CGFloat width = 20; /* (the columns, and room for the scroll bar) */
    char text[600];
    uint32_t c;
    self = [super init];
    if (self == nil)
    {
        EeUiTotals_Free(totals);
        return nil;
    }
    self.owner = owner;
    self.totals = totals;
    self.listPath = path;
    /* (digits of one width, as Windows' Segoe UI has them, so the counts line up) */
    self.font = [NSFont monospacedDigitSystemFontOfSize:NSFont.smallSystemFontSize weight:NSFontWeightRegular];
    self.boldFont = [NSFont monospacedDigitSystemFontOfSize:NSFont.smallSystemFontSize weight:NSFontWeightBold];

    self.table = [[NSTableView alloc] initWithFrame:NSZeroRect];
    self.table.dataSource = self;
    self.table.delegate = self;
    self.table.allowsColumnReordering = NO;
    self.table.allowsColumnResizing = YES;
    self.table.allowsColumnSelection = NO;
    self.table.allowsMultipleSelection = YES;
    self.table.usesAlternatingRowBackgroundColors = YES;
    self.table.columnAutoresizingStyle = NSTableViewNoColumnAutoresizing;
    self.table.gridStyleMask = NSTableViewSolidVerticalGridLineMask | NSTableViewSolidHorizontalGridLineMask;
    if (@available(macOS 11.0, *))
    {
        self.table.style = NSTableViewStylePlain; /* (rows from edge to edge, as on Windows) */
    }
    self.table.menu = [[NSMenu alloc] initWithTitle:@""];
    self.table.menu.delegate = self;
    for (c = 0; c < EeUiTotals_Columns(totals); c++)
    {
        NSTableColumn *col = [[NSTableColumn alloc] initWithIdentifier:[NSString stringWithFormat:@"%u", c]];
        NSTextFieldCell *cell = [[NSTextFieldCell alloc] initTextCell:@""];
        /* (as on Windows: the date and party at the left, the counts at the right) */
        NSTextAlignment align = EeUiTotals_IsTextColumn(totals, c) ? NSTextAlignmentLeft : NSTextAlignmentRight;
        col.title = ns_utf8(EeUiTotals_Title(totals, c));
        col.headerCell.alignment = align;
        cell.font = self.font;
        cell.lineBreakMode = NSLineBreakByTruncatingTail;
        cell.alignment = align;
        col.dataCell = cell;
        col.minWidth = 40;
        col.resizingMask = NSTableColumnUserResizingMask;
        /* (Windows' width, or the heading's when it is wider) */
        col.width = MAX(EeUiTotals_ColumnWidth(totals, c), ceil(col.headerCell.cellSize.width) + 16);
        width += col.width + self.table.intercellSpacing.width;
        [self.table addTableColumn:col];
    }
    self.table.rowHeight = self.boldFont.boundingRectForFont.size.height + 3;

    self.window = [[NSWindow alloc]
        initWithContentRect:NSMakeRect(0, 0, width, 600)
                  styleMask:NSWindowStyleMaskTitled | NSWindowStyleMaskClosable | NSWindowStyleMaskMiniaturizable |
                            NSWindowStyleMaskResizable
                    backing:NSBackingStoreBuffered
                      defer:NO];
    EeUiTotals_WindowTitle(totals, path.fileSystemRepresentation, text, sizeof(text));
    self.window.title = ns_utf8(text);
    self.window.delegate = self;
    self.window.releasedWhenClosed = NO;
    /* (as on Windows: below and right of the roster's corner, and not kept above it) */
    frame = owner.window.frame;
    [self.window setFrameTopLeftPoint:NSMakePoint(NSMinX(frame) + 48, NSMaxY(frame) - 48)];
    content = self.window.contentView;

    scroll = [[NSScrollView alloc] initWithFrame:NSZeroRect];
    scroll.documentView = self.table;
    scroll.hasVerticalScroller = YES;
    scroll.hasHorizontalScroller = YES;
    scroll.translatesAutoresizingMaskIntoConstraints = NO;
    EeUiTotals_StatusText(totals, text, sizeof(text));
    self.status = [NSTextField labelWithString:ns_utf8(text)];
    self.status.lineBreakMode = NSLineBreakByTruncatingTail;
    self.status.translatesAutoresizingMaskIntoConstraints = NO;
    [self.status setContentCompressionResistancePriority:NSLayoutPriorityDefaultLow
                                          forOrientation:NSLayoutConstraintOrientationHorizontal];
    [content addSubview:scroll];
    [content addSubview:self.status];
    [NSLayoutConstraint activateConstraints:@[
        [scroll.topAnchor constraintEqualToAnchor:content.topAnchor],
        [scroll.leadingAnchor constraintEqualToAnchor:content.leadingAnchor],
        [scroll.trailingAnchor constraintEqualToAnchor:content.trailingAnchor],
        [scroll.bottomAnchor constraintEqualToAnchor:self.status.topAnchor constant:-6],
        [self.status.leadingAnchor constraintEqualToAnchor:content.leadingAnchor constant:10],
        [self.status.trailingAnchor constraintLessThanOrEqualToAnchor:content.trailingAnchor constant:-10],
        [self.status.bottomAnchor constraintEqualToAnchor:content.bottomAnchor constant:-6],
    ]];
    [self.window makeKeyAndOrderFront:nil];
    return self;
}

- (void)dealloc
{
    EeUiTotals_Free(_totals);
}

- (void)close
{
    [self.window close]; /* (windowWillClose: tells the owner) */
}

- (void)windowWillClose:(NSNotification *)note
{
    [self.owner totalsClosed:self];
}

- (void)alert:(NSString *)message
{
    NSAlert *a = [[NSAlert alloc] init];
    a.messageText = message;
    [a beginSheetModalForWindow:self.window completionHandler:nil];
}

/* ---- the table ---- */

- (NSInteger)numberOfRowsInTableView:(NSTableView *)tableView
{
    return EeUiTotals_Rows(self.totals);
}

- (id)tableView:(NSTableView *)tableView objectValueForTableColumn:(NSTableColumn *)column row:(NSInteger)row
{
    char text[256];
    EeUiTotals_Cell(self.totals, (uint32_t)row, (uint32_t)column.identifier.integerValue, 1, text, sizeof(text));
    return ns_utf8(text);
}

- (void)tableView:(NSTableView *)tableView
    willDisplayCell:(id)cell
     forTableColumn:(NSTableColumn *)column
                row:(NSInteger)row
{
    ((NSTextFieldCell *)cell).font = EeUiTotals_IsBold(self.totals, (uint32_t)row) ? self.boldFont : self.font;
}

- (BOOL)tableView:(NSTableView *)tableView shouldEditTableColumn:(NSTableColumn *)column row:(NSInteger)row
{
    return NO;
}

- (NSMutableData *)selectedRows
{
    NSMutableData *rows = [NSMutableData data];
    [self.table.selectedRowIndexes enumerateIndexesUsingBlock:^(NSUInteger i, BOOL *stop) {
      uint32_t r = (uint32_t)i;
      [rows appendBytes:&r length:sizeof(r)];
    }];
    return rows;
}

/* ---- commands ---- */

/* Copy (Cmd-C or the right-click menu): a heading line, then the selected rows, as on Windows */
- (IBAction)copy:(id)sender
{
    NSMutableData *rows = [self selectedRows];
    char *text;
    if (rows.length == 0)
    {
        return;
    }
    text = EeUiTotals_CopyText(self.totals, (const uint32_t *)rows.bytes, (uint32_t)(rows.length / sizeof(uint32_t)),
                               NULL);
    if (text != NULL)
    {
        [NSPasteboard.generalPasteboard clearContents];
        [NSPasteboard.generalPasteboard setString:ns_utf8(text) forType:NSPasteboardTypeString];
        free(text);
    }
}

/* Export Selected... / Export All...: a save sheet with the file type */
- (void)exportRows:(BOOL)selection
{
    NSSavePanel *panel;
    NSView *extra;
    NSPopUpButton *type;
    NSMutableData *rows = nil;
    char name[512];
    if (selection)
    {
        rows = [self selectedRows];
        if (rows.length == 0)
        {
            return;
        }
    }
    panel = [NSSavePanel savePanel];
    panel.title = @"Export";
    EeUiTotals_ExportName(self.totals, self.listPath.fileSystemRepresentation, selection, name, sizeof(name));
    panel.nameFieldStringValue = ns_utf8(name);
    extra = [[NSView alloc] initWithFrame:NSMakeRect(0, 0, 420, 36)];
    type = export_type_popup(extra, self);
    panel.accessoryView = extra;
    [panel beginSheetModalForWindow:self.window
                  completionHandler:^(NSModalResponse r) {
                    NSString *path = panel.URL.path;
                    char delim, err[256];
                    if (r != NSModalResponseOK || path == nil)
                    {
                        return;
                    }
                    delim = export_delim(&path, type.indexOfSelectedItem);
                    if (EeUiTotals_Export(self.totals, path.fileSystemRepresentation,
                                          rows != nil ? (const uint32_t *)rows.bytes : NULL,
                                          (uint32_t)(rows.length / sizeof(uint32_t)), delim, err, sizeof(err)) < 0)
                    {
                        [self alert:ns_utf8(err)];
                    }
                  }];
}

- (void)exportTypeChanged:(NSPopUpButton *)type
{
    export_type_changed(type);
}

- (IBAction)exportSelected:(id)sender
{
    [self exportRows:YES];
}

- (IBAction)exportAll:(id)sender
{
    [self exportRows:NO];
}

/* --export with --voting-totals: Export All..., as a CSV, without the sheet */
- (BOOL)exportAllTo:(NSString *)path
{
    char err[256];
    long wrote = EeUiTotals_Export(self.totals, path.fileSystemRepresentation, NULL, 0, ',', err, sizeof(err));
    if (wrote < 0)
    {
        fprintf(stderr, "voting totals export: %s\n", err);
        return NO;
    }
    fprintf(stderr, "exported %ld voting totals rows\n", wrote);
    return YES;
}

/* the right-click menu, as on Windows: Copy; Export Selected..., Export All... */
- (void)menuNeedsUpdate:(NSMenu *)menu
{
    NSInteger row = self.table.clickedRow;
    [menu removeAllItems];
    if (row < 0)
    {
        return;
    }
    if (![self.table.selectedRowIndexes containsIndex:(NSUInteger)row])
    {
        [self.table selectRowIndexes:[NSIndexSet indexSetWithIndex:(NSUInteger)row] byExtendingSelection:NO];
    }
    [menu addItemWithTitle:@"Copy" action:@selector(copy:) keyEquivalent:@""].target = self;
    [menu addItem:[NSMenuItem separatorItem]];
    [menu addItemWithTitle:@"Export Selected…" action:@selector(exportSelected:) keyEquivalent:@""].target = self;
    [menu addItemWithTitle:@"Export All…" action:@selector(exportAll:) keyEquivalent:@""].target = self;
}

- (BOOL)validateMenuItem:(NSMenuItem *)item
{
    if (item.action == @selector(copy:) || item.action == @selector(exportSelected:))
    {
        return self.table.numberOfSelectedRows > 0;
    }
    return YES;
}

@end

static void app_window_closed(EeVoterWindow *w);

@implementation EeVoterWindow

- (instancetype)init
{
    return [self initRoster:NO];
}

- (instancetype)initRoster:(BOOL)roster
{
    NSScrollView *scroll;
    NSView *content;
    static NSPoint cascade;
    self = [super init];
    if (self == nil)
    {
        return nil;
    }
    self.roster = roster;
    self.rules = [NSMutableData data];
    self.wantSort = -1;
    self.wantAscending = YES;
    {
        EeUiOptions o;
        EeUiOptions_Load(&o); /* (as saved last run) */
        self.options = o;
        self.zoomShown = EeUiOptions_ClampZoom(o.zoom_percent);
    }
    self.window = [[NSWindow alloc]
        initWithContentRect:NSMakeRect(0, 0, 1100, 700)
                  styleMask:NSWindowStyleMaskTitled | NSWindowStyleMaskClosable | NSWindowStyleMaskMiniaturizable |
                            NSWindowStyleMaskResizable
                    backing:NSBackingStoreBuffered
                      defer:NO];
    self.window.title = roster ? @"Voter Roster" : @"Election Explorer";
    self.window.delegate = self;
    self.window.releasedWhenClosed = NO;
    [self.window center];
    if (![self.window setFrameAutosaveName:@"VoterWindow"])
    {
        /* (the name is the first window's: the others step down from it) */
        cascade = [self.window cascadeTopLeftFromPoint:cascade];
    }
    content = self.window.contentView;

    self.table = [[NSTableView alloc] initWithFrame:NSZeroRect];
    self.table.dataSource = self;
    self.table.delegate = self;
    self.table.allowsColumnReordering = YES;
    self.table.allowsColumnResizing = YES;
    self.table.allowsMultipleSelection = YES;
    self.table.usesAlternatingRowBackgroundColors = YES;
    self.table.columnAutoresizingStyle = NSTableViewNoColumnAutoresizing;
    self.table.gridStyleMask = NSTableViewSolidVerticalGridLineMask;
    if (@available(macOS 11.0, *))
    {
        /* (rows from edge to edge, as on Windows: the default insets them, with round ends,
         * once the columns no longer fill the window, as at a small zoom) */
        self.table.style = NSTableViewStylePlain;
    }
    self.table.menu = [[NSMenu alloc] initWithTitle:@""];
    self.table.menu.delegate = self; /* (built for the clicked cell in menuNeedsUpdate:) */

    scroll = [[NSScrollView alloc] initWithFrame:NSZeroRect];
    scroll.documentView = self.table;
    scroll.hasVerticalScroller = YES;
    scroll.hasHorizontalScroller = YES;
    scroll.autohidesScrollers = NO;
    scroll.translatesAutoresizingMaskIntoConstraints = NO;

    self.status = [NSTextField labelWithString:roster ? @"No voter roster loaded." /* (Windows' words) */
                                                      : @"Open a voter list with File > Open…"];
    self.status.translatesAutoresizingMaskIntoConstraints = NO;
    self.status.lineBreakMode = NSLineBreakByTruncatingMiddle;
    self.progress = [[NSProgressIndicator alloc] initWithFrame:NSZeroRect];
    self.progress.style = NSProgressIndicatorStyleBar;
    self.progress.indeterminate = NO;
    self.progress.minValue = 0;
    self.progress.maxValue = 100;
    self.progress.hidden = YES;
    self.progress.translatesAutoresizingMaskIntoConstraints = NO;

    [content addSubview:scroll];
    [content addSubview:self.status];
    [content addSubview:self.progress];
    [NSLayoutConstraint activateConstraints:@[
        [scroll.topAnchor constraintEqualToAnchor:content.topAnchor],
        [scroll.leadingAnchor constraintEqualToAnchor:content.leadingAnchor],
        [scroll.trailingAnchor constraintEqualToAnchor:content.trailingAnchor],
        [scroll.bottomAnchor constraintEqualToAnchor:self.status.topAnchor constant:-6],
        [self.status.leadingAnchor constraintEqualToAnchor:content.leadingAnchor constant:10],
        [self.status.bottomAnchor constraintEqualToAnchor:content.bottomAnchor constant:-6],
        [self.progress.leadingAnchor constraintEqualToAnchor:self.status.trailingAnchor constant:12],
        [self.progress.trailingAnchor constraintEqualToAnchor:content.trailingAnchor constant:-10],
        [self.progress.centerYAnchor constraintEqualToAnchor:self.status.centerYAnchor],
        [self.progress.widthAnchor constraintEqualToConstant:200],
    ]];
    [self.window makeKeyAndOrderFront:nil];
    return self;
}

- (void)dealloc
{
    EeUiVoters_Free(_voters);
}

/* The table's columns for the loaded list: titles, widths from the title and the first rows,
 * numbers and dates right-aligned. */
- (NSFont *)listFont
{
    /* (the system font with digits of one width, as Windows' Segoe UI has them: San Francisco's
     * own digits are proportional, so IDs of the same length came out of different widths) */
    return [NSFont monospacedDigitSystemFontOfSize:NSFont.smallSystemFontSize *
                                                   EeUiOptions_ClampZoom(self.options.zoom_percent) / 100.0
                                            weight:NSFontWeightRegular];
}

- (void)rebuildColumns
{
    uint32_t c, ncols = EeUiVoters_Columns(self.voters), nrows = EeUiVoters_Rows(self.voters);
    NSFont *font = [self listFont];
    NSDictionary *attrs = @{NSFontAttributeName : font};
    while (self.table.tableColumns.count > 0)
    {
        [self.table removeTableColumn:self.table.tableColumns.lastObject];
    }
    for (c = 0; c < ncols; c++)
    {
        NSString *title = ns_utf8(EeUiVoters_Title(self.voters, c));
        NSTableColumn *col = [[NSTableColumn alloc] initWithIdentifier:[NSString stringWithFormat:@"%u", c]];
        /* (the heading keeps its size at any zoom) */
        CGFloat w = [title sizeWithAttributes:@{NSFontAttributeName : col.headerCell.font}].width + 24;
        uint32_t r;
        for (r = 0; r < nrows && r < 200; r++)
        {
            w = MAX(w, [ns_utf8(EeUiVoters_Cell(self.voters, r, c)) sizeWithAttributes:attrs].width + 12);
        }
        col.title = title;
        col.width = MIN(MAX(w, 40), 320);
        col.minWidth = 30;
        col.dataCell = [[NSTextFieldCell alloc] initTextCell:@""];
        ((NSTextFieldCell *)col.dataCell).font = font;
        ((NSTextFieldCell *)col.dataCell).lineBreakMode = NSLineBreakByTruncatingTail;
        ((NSTextFieldCell *)col.dataCell).alignment =
            EeUiVoters_IsNumeric(self.voters, c) ? NSTextAlignmentRight : NSTextAlignmentLeft;
        [self.table addTableColumn:col];
    }
    self.table.rowHeight = font.boundingRectForFont.size.height + 3;
    self.zoomShown = EeUiOptions_ClampZoom(self.options.zoom_percent);
}

/* Zoom: the list's text size (its table, as on Windows), and the columns' widths with it */
- (void)applyZoom
{
    int zoom = EeUiOptions_ClampZoom(self.options.zoom_percent), old = self.zoomShown > 0 ? self.zoomShown : 100;
    NSFont *font = [self listFont];
    if (zoom == old)
    {
        return;
    }
    for (NSTableColumn *col in self.table.tableColumns)
    {
        /* (the text scales, the cell's padding of 12 does not: see rebuildColumns; the heading
         * keeps its size, and so stays whole) */
        CGFloat title = [col.title sizeWithAttributes:@{NSFontAttributeName : col.headerCell.font}].width + 24;
        ((NSTextFieldCell *)col.dataCell).font = font;
        col.width = MAX(title, MAX(col.width - 12, 0) * zoom / old + 12);
    }
    self.table.rowHeight = font.boundingRectForFont.size.height + 3;
    self.zoomShown = zoom;
    [self.table reloadData];
}

/* Display name in surname-first format: rebuild the Name column on the main thread, as on
 * Windows (not while a sort or filter reads the cells: once it is done; a load in progress
 * composes them as the options had them, and its completion brings them up to date) */
- (void)applyNames
{
    CFAbsoluteTime started;
    int r;
    if (self.voters == NULL || self.loading ||
        EeUiVoters_SurnameFirst(self.voters) == (self.options.surname_first != 0))
    {
        self.namesPending = NO;
        return;
    }
    if (self.jobRunning)
    {
        self.namesPending = YES;
        return;
    }
    self.namesPending = NO;
    started = CFAbsoluteTimeGetCurrent();
    r = EeUiVoters_SetSurnameFirst(self.voters, self.options.surname_first);
    if (r < 0)
    {
        NSAlert *a = [[NSAlert alloc] init];
        a.messageText = @"Could not update the Name column.";
        [a beginSheetModalForWindow:self.window completionHandler:nil];
        return;
    }
    fprintf(stderr, "names took %.2f s\n", CFAbsoluteTimeGetCurrent() - started);
    [self.table reloadData];
    self.status.stringValue = self.options.surname_first ? @"Names shown surname-first." : @"Names shown given-name first.";
    if (r > 0)
    {
        [self requestView]; /* (sorted by Name, or a rule on it) */
    }
}

/* the options as the Settings sheet's OK sets them; saved for the next run when @p save */
- (void)setOptions:(EeUiOptions)options save:(BOOL)save
{
    options.zoom_percent = EeUiOptions_ClampZoom(options.zoom_percent);
    self.options = options;
    if (self.voters != NULL)
    {
        [self applyZoom];
    }
    else
    {
        self.zoomShown = options.zoom_percent; /* (the columns are made at this zoom) */
    }
    [self applyNames];
    if (save && !EeUiOptions_Save(&options))
    {
        fprintf(stderr, "could not save the options\n");
    }
}

- (IBAction)showOptions:(id)sender
{
    if (self.optionsSheet != nil || self.filterSheet != nil)
    {
        return;
    }
    if (!self.window.visible)
    {
        [self.window makeKeyAndOrderFront:nil];
    }
    self.optionsSheet = [[EeOptionsSheet alloc] initWithOwner:self];
    [self.window beginSheet:self.optionsSheet.sheet completionHandler:nil];
}

- (void)showStatus
{
    NSMutableString *s = [NSMutableString string];
    int asc = 0, sc = EeUiVoters_SortColumn(self.voters, &asc);
    char count[256];
    EeUiVoters_CountText(self.voters, count, sizeof(count)); /* (the Windows words) */
    [s appendString:ns_utf8(count)];
    [s appendFormat:@", %u columns — %@", EeUiVoters_Columns(self.voters), self.name ?: @""];
    if (sc >= 0)
    {
        [s appendFormat:@" — sorted by %@, %@", ns_utf8(EeUiVoters_Title(self.voters, (uint32_t)sc)),
                        asc ? @"ascending" : @"descending"];
    }
    self.status.stringValue = s;
}

- (void)showSortIndicator
{
    int asc = 1, sorted = EeUiVoters_SortColumn(self.voters, &asc);
    NSTableColumn *hi = nil;
    for (NSTableColumn *col in self.table.tableColumns)
    {
        BOOL is = col.identifier.integerValue == sorted;
        [self.table
            setIndicatorImage:(is ? [NSImage imageNamed:asc ? @"NSAscendingSortIndicator" : @"NSDescendingSortIndicator"]
                                  : nil)
                inTableColumn:col];
        if (is)
        {
            hi = col;
        }
    }
    self.table.highlightedTableColumn = hi;
}

/* ---- sorting and filtering in the background ---- */

- (void)startViewJob
{
    int sort = self.wantSort, asc = 0, shown = EeUiVoters_SortColumn(self.voters, &asc);
    BOOL ascending = self.wantAscending;
    NSData *rules = [self.rules copy];
    NSUInteger gen = self.wantGen;
    EeUiVoters *v = self.voters;
    int marks = self.wantMarks;
    NSString *label;
    CFAbsoluteTime started = CFAbsoluteTimeGetCurrent();
    /* (named for what changes: duplicates to find, a new order, else the filter) */
    if (marks != EE_UI_MARKS_NONE && marks != EeUiVoters_Marks(v, NULL))
    {
        label = @"Finding duplicates";
    }
    else
    {
        label = (shown != sort || (sort >= 0 && (asc != 0) != ascending)) ? @"Sorting" : @"Filtering";
    }
    self.jobRunning = YES;
    self.status.stringValue = [NSString stringWithFormat:@"%@%@ %u voters…", label,
                                                         [label hasPrefix:@"Finding"] ? @" in" : @"",
                                                         EeUiVoters_TotalRows(v)];
    self.progress.indeterminate = YES;
    self.progress.hidden = NO;
    [self.progress startAnimation:nil];
    dispatch_async(dispatch_get_global_queue(QOS_CLASS_USER_INITIATED, 0), ^{
      EeUiView *view = EeUiVoters_ComputeView(v, sort, ascending, (const EeUiRule *)rules.bytes,
                                              (uint32_t)(rules.length / sizeof(EeUiRule)), marks);
      dispatch_async(dispatch_get_main_queue(), ^{
        uint32_t marked = 0;
        int kind = EeUiView_Marks(view, &marked);
        self.jobRunning = NO;
        if (view == NULL)
        {
            self.status.stringValue = @"Not enough memory to sort or filter the list.";
        }
        else if (kind != EE_UI_MARKS_NONE && marked == 0)
        {
            /* (as on Windows: no duplicates is said, and the shown view stays) */
            int shownAsc = 0;
            NSAlert *a = [[NSAlert alloc] init];
            EeUiView_Free(view);
            self.wantMarks = EeUiVoters_Marks(self.voters, NULL);
            self.wantSort = EeUiVoters_SortColumn(self.voters, &shownAsc);
            self.wantAscending = shownAsc != 0;
            [self showStatus];
            a.messageText = ns_utf8(EeUiVoters_MarksNoneText(kind));
            [a beginSheetModalForWindow:self.window completionHandler:nil];
        }
        else
        {
            fprintf(stderr, "%s took %.2f s\n", label.UTF8String, CFAbsoluteTimeGetCurrent() - started);
            EeUiVoters_SetView(self.voters, view);
            [self.table reloadData];
            [self showSortIndicator];
            [self showStatus];
        }
        if (gen != self.wantGen)
        {
            [self startViewJob]; /* (asked for something else meanwhile) */
        }
        else
        {
            [self.progress stopAnimation:nil];
            self.progress.hidden = YES;
            self.progress.indeterminate = NO;
            [self applyNames]; /* (if the Settings changed them meanwhile) */
        }
      });
    });
}

- (void)requestView
{
    if (self.voters == NULL)
    {
        return;
    }
    self.wantGen++;
    if (!self.jobRunning)
    {
        [self startViewJob];
    }
}

- (void)sortByColumn:(int)column ascending:(BOOL)ascending
{
    self.wantSort = column;
    self.wantAscending = ascending;
    [self requestView];
}

- (void)setRules:(const EeUiRule *)rules count:(NSUInteger)n
{
    self.rules = [NSMutableData dataWithBytes:rules length:n * sizeof(EeUiRule)];
    [self requestView];
}

/* Include / Exclude a value: a rule "<column> is <value>", as the Windows right-click menu adds */
- (void)addQuickRuleColumn:(uint32_t)column value:(NSString *)value exclude:(BOOL)exclude
{
    EeUiRule r;
    if (self.voters == NULL || column >= EeUiVoters_Columns(self.voters))
    {
        return;
    }
    memset(&r, 0, sizeof(r));
    r.column = column;
    r.relation = EE_UI_REL_IS;
    r.exclude = exclude ? 1 : 0;
    r.enabled = 1;
    strncpy(r.value, value.UTF8String ?: "", sizeof(r.value) - 1);
    [self.rules appendBytes:&r length:sizeof(r)];
    [self requestView];
}

/* Show Duplicate Voter IDs / Voters: only the duplicates, grouped by Voter ID or Name (as on
 * Windows), within the filter */
- (void)showDuplicates:(int)marks
{
    int column = marks == EE_UI_MARKS_DUP_VOTERS ? EE_UI_COLUMN_NAME : EE_UI_COLUMN_VOTER_ID;
    if (self.voters == NULL || self.loading)
    {
        return;
    }
    if (marks == EE_UI_MARKS_DUP_VOTERS && !EeUiVoters_HasBirthDate(self.voters))
    {
        NSAlert *a = [[NSAlert alloc] init];
        a.messageText = @"No birth date data is available";
        [a beginSheetModalForWindow:self.window completionHandler:nil];
        return;
    }
    self.wantMarks = marks;
    [self sortByColumn:(uint32_t)column < EeUiVoters_Columns(self.voters) ? column : -1 ascending:YES];
}

- (IBAction)showDuplicateVoterIds:(id)sender
{
    [self showDuplicates:EE_UI_MARKS_DUP_IDS];
}

- (IBAction)showDuplicateVoters:(id)sender
{
    [self showDuplicates:EE_UI_MARKS_DUP_VOTERS];
}

/* a report's Include / Exclude: its rules added to the filter */
- (void)addRules:(NSData *)rules
{
    if (self.voters == NULL || self.loading || rules.length == 0)
    {
        return;
    }
    [self.rules appendData:rules];
    [self requestView];
}

/* ---- Reports ---- */

- (EeReportWindow *)reportOfKind:(int)kind
{
    return kind == EE_UI_REPORT_ADDRESS ? self.addressReport : self.precinctReport;
}

/* Reports > Display Precinct / Address Report...: a window of its own (the open one is brought
 * forward); it closes when the list is reloaded or this window closes */
- (void)showReport:(int)kind
{
    EeReportWindow *open = [self reportOfKind:kind];
    EeUiReport *report;
    char err[256];
    CFAbsoluteTime started = CFAbsoluteTimeGetCurrent();
    if (self.voters == NULL || self.loading)
    {
        return;
    }
    if (open != nil)
    {
        [open.window makeKeyAndOrderFront:nil];
        return;
    }
    report = EeUiVoters_Report(self.voters, kind, err, sizeof(err));
    if (report == NULL)
    {
        NSAlert *a = [[NSAlert alloc] init];
        a.messageText = ns_utf8(err); /* ("No precinct information available", as on Windows) */
        [a beginSheetModalForWindow:self.window completionHandler:nil];
        return;
    }
    fprintf(stderr, "%s report: %u rows in %.2f s\n", EeUiReport_Heading(kind), EeUiReport_Rows(report),
            CFAbsoluteTimeGetCurrent() - started);
    open = [[EeReportWindow alloc] initWithOwner:self report:report path:self.path];
    if (kind == EE_UI_REPORT_ADDRESS)
    {
        self.addressReport = open;
    }
    else
    {
        self.precinctReport = open;
    }
}

- (IBAction)showPrecinctReport:(id)sender
{
    [self showReport:EE_UI_REPORT_PRECINCT];
}

- (IBAction)showAddressReport:(id)sender
{
    [self showReport:EE_UI_REPORT_ADDRESS];
}

- (void)reportClosed:(EeReportWindow *)report
{
    if (self.precinctReport == report)
    {
        self.precinctReport = nil;
    }
    if (self.addressReport == report)
    {
        self.addressReport = nil;
    }
    dispatch_async(dispatch_get_main_queue(), ^{
      (void)report; /* (kept until its window has finished closing) */
    });
}

/* Reports > Display Voting Totals... (a roster): a window of its own (the open one is brought
 * forward); it closes when the roster is reloaded or this window closes, as on Windows */
- (IBAction)showVotingTotals:(id)sender
{
    EeUiTotals *totals;
    char err[256];
    CFAbsoluteTime started = CFAbsoluteTimeGetCurrent();
    if (self.voters == NULL || self.loading || !self.roster)
    {
        return;
    }
    if (self.totalsWindow != nil)
    {
        [self.totalsWindow.window makeKeyAndOrderFront:nil];
        return;
    }
    totals = EeUiVoters_Totals(self.voters, err, sizeof(err));
    if (totals == NULL)
    {
        NSAlert *a = [[NSAlert alloc] init];
        a.messageText = ns_utf8(err);
        [a beginSheetModalForWindow:self.window completionHandler:nil];
        return;
    }
    fprintf(stderr, "voting totals: %u rows in %.2f s\n", EeUiTotals_Rows(totals), CFAbsoluteTimeGetCurrent() - started);
    self.totalsWindow = [[EeTotalsWindow alloc] initWithOwner:self totals:totals path:self.path];
}

- (void)totalsClosed:(EeTotalsWindow *)totals
{
    if (self.totalsWindow == totals)
    {
        self.totalsWindow = nil;
    }
    dispatch_async(dispatch_get_main_queue(), ^{
      (void)totals; /* (kept until its window has finished closing) */
    });
}

- (void)closeReports
{
    [self.precinctReport close];
    [self.addressReport close];
    [self.totalsWindow close];
}

- (void)windowWillClose:(NSNotification *)note
{
    [self closeReports];
    app_window_closed(self);
}

/* the selected shown rows, in order */
- (NSMutableData *)selectedRows
{
    NSMutableData *rows = [NSMutableData data];
    [self.table.selectedRowIndexes enumerateIndexesUsingBlock:^(NSUInteger i, BOOL *stop) {
      uint32_t r = (uint32_t)i;
      [rows appendBytes:&r length:sizeof(r)];
    }];
    return rows;
}

/* Copy: the selected rows, separated as in the list's file, with the Voter ID / Precinct /
 * Name / Address columns first unless the Settings say not to (Windows' default is first) */
- (IBAction)copy:(id)sender
{
    NSMutableData *rows;
    uint32_t n;
    char *text;
    if (self.voters == NULL || self.loading || self.jobRunning)
    {
        return;
    }
    rows = [self selectedRows];
    n = (uint32_t)(rows.length / sizeof(uint32_t));
    if (n == 0)
    {
        self.status.stringValue = @"Nothing selected to copy.";
        return;
    }
    text = EeUiVoters_CopyText(self.voters, (const uint32_t *)rows.bytes, n, self.options.copy_normalized, NULL);
    if (text == NULL)
    {
        self.status.stringValue = @"Not enough memory to copy the rows.";
        return;
    }
    [NSPasteboard.generalPasteboard clearContents];
    [NSPasteboard.generalPasteboard setString:ns_utf8(text) forType:NSPasteboardTypeString];
    free(text);
    self.status.stringValue = [NSString stringWithFormat:@"Copied %u row%@ to the clipboard.", n, n == 1 ? @"" : @"s"];
}

/* Export: a save sheet with Windows' Export Options (include the normalized fields?) and the
 * file type (CSV or TSV) below it */
- (void)exportRows:(BOOL)selection
{
    NSSavePanel *panel;
    NSView *extra;
    NSButton *normalized;
    NSPopUpButton *type;
    NSMutableData *rows = nil;
    char name[512];
    if (self.voters == NULL || self.loading || self.jobRunning)
    {
        return;
    }
    if (selection)
    {
        rows = [self selectedRows];
        if (rows.length == 0)
        {
            self.status.stringValue = @"Nothing selected to export.";
            return;
        }
    }
    else if (EeUiVoters_Rows(self.voters) == 0)
    {
        self.status.stringValue = @"Nothing to export.";
        return;
    }
    panel = [NSSavePanel savePanel];
    panel.title = @"Export";
    EeUiVoters_ExportName(self.voters, self.path.fileSystemRepresentation, selection, name, sizeof(name));
    panel.nameFieldStringValue = ns_utf8(name);
    extra = [[NSView alloc] initWithFrame:NSMakeRect(0, 0, 420, self.roster ? 36 : 64)];
    normalized = [NSButton checkboxWithTitle:@"Include normalized data fields" target:nil action:nil];
    normalized.frame = NSMakeRect(20, 36, 380, 20);
    if (!self.roster) /* (as on Windows: a roster exports its own columns, which reload as one) */
    {
        [extra addSubview:normalized];
    }
    type = export_type_popup(extra, self);
    panel.accessoryView = extra;
    [panel beginSheetModalForWindow:self.window
                  completionHandler:^(NSModalResponse r) {
                    NSString *path = panel.URL.path;
                    char delim;
                    char err[256];
                    long wrote;
                    if (r != NSModalResponseOK || path == nil || self.voters == NULL)
                    {
                        return;
                    }
                    delim = export_delim(&path, type.indexOfSelectedItem);
                    wrote = EeUiVoters_Export(self.voters, path.fileSystemRepresentation,
                                              rows != nil ? (const uint32_t *)rows.bytes : NULL,
                                              (uint32_t)(rows.length / sizeof(uint32_t)),
                                              normalized.state == NSControlStateValueOn, delim, err, sizeof(err));
                    if (wrote < 0)
                    {
                        NSAlert *a = [[NSAlert alloc] init];
                        a.messageText = ns_utf8(err);
                        [a beginSheetModalForWindow:self.window completionHandler:nil];
                        return;
                    }
                    self.status.stringValue =
                        [NSString stringWithFormat:@"Exported %ld row%@.", wrote, wrote == 1 ? @"" : @"s"];
                  }];
}

- (void)exportTypeChanged:(NSPopUpButton *)type
{
    export_type_changed(type);
}

- (IBAction)exportVoters:(id)sender
{
    [self exportRows:NO];
}

- (IBAction)exportSelected:(id)sender
{
    [self exportRows:YES];
}

/* --export: the shown rows, as a CSV, without the sheet */
- (BOOL)exportShownTo:(NSString *)path
{
    char err[256];
    CFAbsoluteTime started = CFAbsoluteTimeGetCurrent();
    long wrote = EeUiVoters_Export(self.voters, path.fileSystemRepresentation, NULL, 0, 0, ',', err, sizeof(err));
    if (wrote < 0)
    {
        fprintf(stderr, "export failed: %s\n", err);
        return NO;
    }
    fprintf(stderr, "exported %ld rows in %.2f s\n", wrote, CFAbsoluteTimeGetCurrent() - started);
    self.status.stringValue = [NSString stringWithFormat:@"Exported %ld row%@.", wrote, wrote == 1 ? @"" : @"s"];
    return YES;
}

/* ---- loading ---- */

- (void)openPath:(NSString *)path
{
    [self startLoad:@[ path ] roster:NO];
}

- (void)openRosterPaths:(NSArray<NSString *> *)paths
{
    [self startLoad:paths roster:YES];
}

/* a roster that did not load: as on Windows, no window is left; say so over the window it was
 * opened from */
- (void)rosterFailed:(NSString *)msg
{
    NSAlert *a = [[NSAlert alloc] init];
    NSWindow *over = self.opener != nil && self.opener.window.visible ? self.opener.window : nil;
    if (self.snapshotting)
    {
        fprintf(stderr, "roster load failed: %s\n", msg.UTF8String);
        [NSApp terminate:nil];
        return;
    }
    a.messageText = @"Could not load the voter roster.";
    a.informativeText = msg;
    [self.window close];
    if (over != nil)
    {
        [a beginSheetModalForWindow:over completionHandler:nil];
    }
    else
    {
        [a runModal];
    }
}

- (void)startLoad:(NSArray<NSString *> *)paths roster:(BOOL)roster
{
    LoadCx *cx;
    NSString *path = paths.firstObject;
    if (self.loading || self.jobRunning || path == nil)
    {
        return;
    }
    self.loading = YES;
    self.path = path;
    self.name = paths.count > 1 ? [NSString stringWithFormat:@"%@ (+%lu more)", path.lastPathComponent,
                                                             (unsigned long)(paths.count - 1)]
                                : path.lastPathComponent;
    self.window.title = roster ? [NSString stringWithFormat:@"%@ — Voter Roster", self.name]
                               : [NSString stringWithFormat:@"Election Explorer — %@", self.name];
    self.progress.doubleValue = 0;
    self.progress.hidden = NO;
    self.status.stringValue = @"Loading…";
    cx = (LoadCx *)calloc(1, sizeof(LoadCx));
    cx->win = (__bridge void *)self;
    cx->percent = -1;
    int surname = self.options.surname_first; /* (the names as the options have them now) */
    dispatch_async(dispatch_get_global_queue(QOS_CLASS_USER_INITIATED, 0), ^{
      char err[1024] = "";
      EeUiVoters *v;
      if (roster)
      {
          const char **files = (const char **)calloc(paths.count, sizeof(char *));
          NSUInteger i;
          for (i = 0; files != NULL && i < paths.count; i++)
          {
              files[i] = paths[i].fileSystemRepresentation;
          }
          v = files != NULL ? EeUiVoters_LoadRoster(files, (uint32_t)paths.count, surname, on_progress, cx, NULL,
                                                    err, sizeof(err))
                            : NULL;
          free(files);
      }
      else
      {
          v = EeUiVoters_Load(path.fileSystemRepresentation, surname, on_progress, cx, NULL, err, sizeof(err));
      }
      NSString *msg = ns_utf8(err);
      dispatch_async(dispatch_get_main_queue(), ^{
        free(cx);
        self.loading = NO;
        self.progress.hidden = YES;
        if (v == NULL && roster)
        {
            [self rosterFailed:msg];
            return;
        }
        if (v == NULL)
        {
            NSAlert *a = [[NSAlert alloc] init];
            a.messageText = [NSString stringWithFormat:@"%@ could not be opened.", path.lastPathComponent];
            a.informativeText = msg;
            [a beginSheetModalForWindow:self.window completionHandler:nil];
            self.status.stringValue = msg;
            return;
        }
        if (self.filterSheet != nil)
        {
            [self.filterSheet close];
        }
        [self closeReports]; /* (as on Windows: a report is of the list it was made from) */
        EeUiVoters_Free(self.voters);
        self.voters = v;
        if (EeUiVoters_SurnameFirst(v) != (self.options.surname_first != 0))
        {
            /* (the Settings changed while it loaded; nothing is sorted or filtered yet) */
            EeUiVoters_SetSurnameFirst(v, self.options.surname_first);
        }
        self.rules = [NSMutableData data]; /* (a new list starts unfiltered, in file order) */
        self.wantMarks = EE_UI_MARKS_NONE;
        self.wantSort = -1;
        self.wantAscending = YES;
        [self rebuildColumns];
        [self.table reloadData];
        [self showSortIndicator];
        [self showStatus];
        if (!roster)
        {
            [[NSDocumentController sharedDocumentController] noteNewRecentDocumentURL:[NSURL fileURLWithPath:path]];
        }
        else if (!self.snapshotting)
        {
            int notable = 0;
            EeUiVoters_LoadSummary(v, &notable);
            if (notable)
            {
                [self showLoadSummary:nil]; /* (as on Windows: when it says more than the counts) */
            }
        }
      });
    });
}

/* Reports > Display Load Summary...: what the load read and skipped, as Windows shows it */
- (IBAction)showLoadSummary:(id)sender
{
    const char *text = EeUiVoters_LoadSummary(self.voters, NULL);
    NSAlert *a;
    NSScrollView *scroll;
    NSTextView *view;
    if (text == NULL || self.window.attachedSheet != nil)
    {
        return;
    }
    a = [[NSAlert alloc] init];
    a.messageText = @"Voter Roster Load Summary";
    scroll = [[NSScrollView alloc] initWithFrame:NSMakeRect(0, 0, 520, 220)];
    scroll.hasVerticalScroller = YES;
    scroll.borderType = NSBezelBorder;
    view = [[NSTextView alloc] initWithFrame:NSMakeRect(0, 0, scroll.contentSize.width, scroll.contentSize.height)];
    view.editable = NO;
    view.font = [NSFont systemFontOfSize:NSFont.systemFontSize];
    view.textContainerInset = NSMakeSize(4, 4);
    view.verticallyResizable = YES;
    view.horizontallyResizable = NO;
    view.autoresizingMask = NSViewWidthSizable;
    view.textContainer.widthTracksTextView = YES;
    view.string = ns_utf8(text);
    scroll.documentView = view;
    a.accessoryView = scroll;
    [a addButtonWithTitle:@"OK"];
    self.summaryAlert = a;
    [a beginSheetModalForWindow:self.window
              completionHandler:^(NSModalResponse r) {
                self.summaryAlert = nil;
              }];
}

/* ---- the table: cells on demand, sorting by a heading ---- */

- (NSInteger)numberOfRowsInTableView:(NSTableView *)tableView
{
    return EeUiVoters_Rows(self.voters);
}

- (id)tableView:(NSTableView *)tableView objectValueForTableColumn:(NSTableColumn *)column row:(NSInteger)row
{
    return ns_utf8(EeUiVoters_Cell(self.voters, (uint32_t)row, (uint32_t)column.identifier.integerValue));
}

- (BOOL)tableView:(NSTableView *)tableView shouldEditTableColumn:(NSTableColumn *)column row:(NSInteger)row
{
    return NO;
}

- (void)tableView:(NSTableView *)tableView didClickTableColumn:(NSTableColumn *)column
{
    int c = (int)column.identifier.integerValue;
    if (self.voters == NULL || self.loading)
    {
        return;
    }
    /* (the same column again reverses it) */
    [self sortByColumn:c ascending:(self.wantSort == c ? !self.wantAscending : YES)];
}

/* ---- the right-click menu: Include / Exclude the clicked cell's value, Filter... ---- */

- (void)menuNeedsUpdate:(NSMenu *)menu
{
    NSInteger row = self.table.clickedRow, col = self.table.clickedColumn;
    [menu removeAllItems];
    self.quickLink = nil;
    if (self.voters != NULL && !self.jobRunning && row >= 0 && ![self.table.selectedRowIndexes containsIndex:(NSUInteger)row])
    {
        /* (as on Windows: a right-click outside the selection selects that row) */
        [self.table selectRowIndexes:[NSIndexSet indexSetWithIndex:(NSUInteger)row] byExtendingSelection:NO];
    }
    /* Windows' order: Copy; Include, Exclude; Show in Maps... (an address); Open link... (a
     * file column's https:// link); Filter...; Export Selected... */
    [menu addItemWithTitle:@"Copy" action:@selector(copy:) keyEquivalent:@""].target = self;
    [menu addItem:[NSMenuItem separatorItem]];
    if (self.voters != NULL && !self.jobRunning && row >= 0 && col >= 0)
    {
        NSString *value, *shown;
        self.quickColumn = (uint32_t)self.table.tableColumns[(NSUInteger)col].identifier.integerValue;
        value = ns_utf8(EeUiVoters_Cell(self.voters, (uint32_t)row, self.quickColumn));
        self.quickValue = value;
        /* (at most 40 characters, as on Windows) */
        shown = value.length >= 40 ? [[value substringToIndex:36] stringByAppendingString:@"..."] : value;
        [menu addItemWithTitle:[NSString stringWithFormat:@"Include \"%@\"", shown]
                        action:@selector(quickInclude:)
                 keyEquivalent:@""]
            .target = self;
        [menu addItemWithTitle:[NSString stringWithFormat:@"Exclude \"%@\"", shown]
                        action:@selector(quickExclude:)
                 keyEquivalent:@""]
            .target = self;
        [menu addItem:[NSMenuItem separatorItem]];
        if ((int)self.quickColumn == EeUiVoters_AddressColumn(self.voters) && value.length > 0)
        {
            [menu addItemWithTitle:@"Show in Maps…" action:@selector(showCellInMaps:) keyEquivalent:@""].target = self;
            [menu addItem:[NSMenuItem separatorItem]];
        }
        {
            char *link = EeUiVoters_CellLink(self.voters, self.quickColumn, value.UTF8String);
            self.quickLink = link != NULL ? ns_utf8(link) : nil;
            free(link);
        }
        if (self.quickLink != nil)
        {
            [menu addItemWithTitle:@"Open link…" action:@selector(openCellLink:) keyEquivalent:@""].target = self;
            [menu addItem:[NSMenuItem separatorItem]];
        }
    }
    [menu addItemWithTitle:@"Filter…" action:@selector(showFilter:) keyEquivalent:@""].target = self;
    [menu addItem:[NSMenuItem separatorItem]];
    [menu addItemWithTitle:@"Export Selected…" action:@selector(exportSelected:) keyEquivalent:@""].target = self;
}

/* Show in Maps... on an Address cell: the address in the Settings' map, in the browser */
- (void)showCellInMaps:(id)sender
{
    char *url = EeUiOptions_MapUrl(self.options.map_engine, self.quickValue.UTF8String);
    NSURL *u = url != NULL ? [NSURL URLWithString:ns_utf8(url)] : nil;
    NSAlert *a;
    free(url);
    if (u != nil && [NSWorkspace.sharedWorkspace openURL:u])
    {
        return;
    }
    a = [[NSAlert alloc] init];
    a.messageText = u == nil ? @"Could not build a map link for this address." : @"Could not open the map in a browser.";
    [a beginSheetModalForWindow:self.window completionHandler:nil];
}

/* Open link... on a cell of the file's own columns: its https:// link, in the browser */
- (void)openCellLink:(id)sender
{
    NSURL *u;
    NSAlert *a;
    if (self.quickLink == nil)
    {
        return;
    }
    u = [NSURL URLWithString:self.quickLink];
    if (u == nil)
    {
        /* (Windows opens what the cell has; NSURL wants characters such as { } | or non-ASCII
         * ones escaped: escape those, leaving the link's own %XX as they are) */
        NSMutableCharacterSet *keep = [NSCharacterSet.URLFragmentAllowedCharacterSet mutableCopy];
        [keep addCharactersInString:@"#%[]"];
        u = [NSURL URLWithString:[self.quickLink stringByAddingPercentEncodingWithAllowedCharacters:keep]];
    }
    if (u != nil && [NSWorkspace.sharedWorkspace openURL:u])
    {
        return;
    }
    a = [[NSAlert alloc] init];
    a.messageText = @"Could not open the link in a browser.";
    [a beginSheetModalForWindow:self.window completionHandler:nil];
}

- (void)quickInclude:(id)sender
{
    [self addQuickRuleColumn:self.quickColumn value:self.quickValue exclude:NO];
}

- (void)quickExclude:(id)sender
{
    [self addQuickRuleColumn:self.quickColumn value:self.quickValue exclude:YES];
}

/* ---- Filter menu (the window's delegate is on the responder chain) ---- */

- (IBAction)showFilter:(id)sender
{
    if (self.voters == NULL || self.filterSheet != nil)
    {
        return;
    }
    self.filterSheet = [[EeFilterSheet alloc] initWithOwner:self];
    [self.window beginSheet:self.filterSheet.sheet completionHandler:nil];
}

/* Reset Filter / Reset View: all rows (no rules, no duplicates view), as on Windows */
- (IBAction)resetFilter:(id)sender
{
    if (self.voters != NULL && (self.rules.length > 0 || self.wantMarks != EE_UI_MARKS_NONE))
    {
        self.wantMarks = EE_UI_MARKS_NONE;
        [self setRules:NULL count:0];
    }
}

- (IBAction)resetView:(id)sender
{
    [self resetFilter:sender];
}

- (BOOL)validateMenuItem:(NSMenuItem *)item
{
    /* (one menu bar for every window: a roster window has Windows' roster menus, without
     * Show Duplicate Voters or the Address report, and with the Load Summary) */
    if (item.action == @selector(showDuplicateVoters:) || item.action == @selector(showAddressReport:))
    {
        item.hidden = self.roster;
    }
    if (item.action == @selector(exportVoters:))
    {
        item.title = self.roster ? @"Export Voter Roster…" : @"Export Voter List…";
    }
    if (item.action == @selector(showLoadSummary:))
    {
        item.hidden = !self.roster;
        return self.roster && self.voters != NULL && !self.loading && self.window.attachedSheet == nil;
    }
    if (item.action == @selector(showVotingTotals:))
    {
        item.hidden = !self.roster;
        return self.roster && self.voters != NULL && !self.loading;
    }
    if (item.action == @selector(showOptions:))
    {
        return self.optionsSheet == nil && self.filterSheet == nil; /* (Windows: any time) */
    }
    if (item.action == @selector(showFilter:))
    {
        return self.voters != NULL && !self.loading;
    }
    if (item.action == @selector(resetFilter:) || item.action == @selector(resetView:))
    {
        return self.voters != NULL && !self.loading && (self.rules.length > 0 || self.wantMarks != EE_UI_MARKS_NONE);
    }
    if (item.action == @selector(copy:) || item.action == @selector(exportSelected:))
    {
        return self.voters != NULL && !self.loading && !self.jobRunning && self.table.numberOfSelectedRows > 0;
    }
    if (item.action == @selector(exportVoters:) || item.action == @selector(showDuplicateVoterIds:) ||
        item.action == @selector(showDuplicateVoters:))
    {
        return self.voters != NULL && !self.loading && !self.jobRunning;
    }
    if (item.action == @selector(showPrecinctReport:) || item.action == @selector(showAddressReport:))
    {
        return self.voters != NULL && !self.loading; /* (as on Windows) */
    }
    return YES;
}

@end

/* -------------------------------------------------------------------------- */
/* The application                                                            */
/* -------------------------------------------------------------------------- */

@interface EeAppDelegate : NSObject <NSApplicationDelegate>
@property(strong) EeVoterWindow *main;               /* the first window (the snapshot's) */
@property(strong) NSMutableArray<EeVoterWindow *> *windows; /* every open window */
@property(copy) NSString *pending;  /* a file to open once launched */
@property(strong) NSMutableArray<NSString *> *pendingFiles; /* the files named (a roster's with --roster) */
@property(assign) BOOL roster;      /* --roster: open the files as one voter roster */
@property(assign) BOOL snapshotSummary; /* --load-summary: the PNG is of the Load Summary */
@property(assign) BOOL snapshotTotals;  /* --voting-totals: the PNG is of the Voting Totals */
@property(copy) NSString *snapshot; /* --snapshot: write the window here as PNG once loaded, then quit */
@property(assign) int snapshotSort; /* --sort: -1 for none */
@property(assign) BOOL snapshotAscending;
@property(strong) NSMutableArray<NSString *> *snapshotQuick; /* --include / --exclude: "+c=v" / "-c=v" */
@property(assign) BOOL snapshotFilterWindow;
@property(assign) int snapshotMarks;          /* --duplicates: EE_UI_MARKS_* */
@property(copy) NSString *snapshotExport;     /* --export: export the shown rows here first */
@property(assign) int snapshotReport;         /* --report: EE_UI_REPORT_* (-1: none) */
@property(assign) int snapshotZoom;           /* --zoom: 0 for none */
@property(assign) int snapshotNames;          /* --names: 1 surname first, 0 given name first, -1 none */
@property(assign) BOOL snapshotOptionsWindow; /* --options-window: the PNG is of the Settings sheet */
@property(copy) NSString *snapshotLoadFilter;   /* --load-filter: Load Filter... in the Filter sheet */
@property(copy) NSString *snapshotExportFilter; /* --export-filter: then Export Filter... */
@property(assign) BOOL snapshotActionsDone;
@end

@implementation EeAppDelegate

- (void)buildMenu
{
    NSMenu *bar = [[NSMenu alloc] init];
    NSMenuItem *appItem = [[NSMenuItem alloc] init], *fileItem = [[NSMenuItem alloc] init],
               *editItem = [[NSMenuItem alloc] init], *filterItem = [[NSMenuItem alloc] init],
               *reportsItem = [[NSMenuItem alloc] init], *winItem = [[NSMenuItem alloc] init];
    NSMenu *app = [[NSMenu alloc] init], *file = [[NSMenu alloc] initWithTitle:@"File"],
           *edit = [[NSMenu alloc] initWithTitle:@"Edit"], *filter = [[NSMenu alloc] initWithTitle:@"Filter"],
           *reports = [[NSMenu alloc] initWithTitle:@"Reports"], *win = [[NSMenu alloc] initWithTitle:@"Window"];
    [bar addItem:appItem];
    [bar addItem:fileItem];
    [bar addItem:editItem];
    [bar addItem:filterItem];
    [bar addItem:reportsItem];
    [bar addItem:winItem];
    [app addItemWithTitle:@"About Election Explorer" action:@selector(orderFrontStandardAboutPanel:) keyEquivalent:@""];
    [app addItem:[NSMenuItem separatorItem]];
    [app addItemWithTitle:@"Settings…" action:@selector(showOptions:) keyEquivalent:@","]; /* (Edit > Options... on Windows) */
    [app addItem:[NSMenuItem separatorItem]];
    [app addItemWithTitle:@"Hide Election Explorer" action:@selector(hide:) keyEquivalent:@"h"];
    [app addItem:[NSMenuItem separatorItem]];
    [app addItemWithTitle:@"Quit Election Explorer" action:@selector(terminate:) keyEquivalent:@"q"];
    appItem.submenu = app;
    [file addItemWithTitle:@"Open Voter List…" action:@selector(openDocument:) keyEquivalent:@"o"];
    [file addItemWithTitle:@"Open Voter Roster…" action:@selector(openRoster:) keyEquivalent:@""];
    [file addItemWithTitle:@"Export Voter List…" action:@selector(exportVoters:) keyEquivalent:@""];
    [file addItem:[NSMenuItem separatorItem]];
    [file addItemWithTitle:@"Close" action:@selector(performClose:) keyEquivalent:@"w"];
    fileItem.submenu = file;
    [edit addItemWithTitle:@"Copy" action:@selector(copy:) keyEquivalent:@"c"];
    [edit addItemWithTitle:@"Select All" action:@selector(selectAll:) keyEquivalent:@"a"];
    editItem.submenu = edit;
    [filter addItemWithTitle:@"Filter…" action:@selector(showFilter:) keyEquivalent:@"l"]; /* (Ctrl+L on Windows) */
    [filter addItemWithTitle:@"Reset Filter" action:@selector(resetFilter:) keyEquivalent:@""];
    [filter addItemWithTitle:@"Show Duplicate Voter IDs…" action:@selector(showDuplicateVoterIds:) keyEquivalent:@""];
    [filter addItemWithTitle:@"Show Duplicate Voters…" action:@selector(showDuplicateVoters:) keyEquivalent:@""];
    [filter addItem:[NSMenuItem separatorItem]];
    [filter addItemWithTitle:@"Reset View…" action:@selector(resetView:) keyEquivalent:@""];
    filterItem.submenu = filter;
    [reports addItemWithTitle:@"Display Precinct Report…" action:@selector(showPrecinctReport:) keyEquivalent:@""];
    [reports addItemWithTitle:@"Display Voting Totals…" action:@selector(showVotingTotals:) keyEquivalent:@""];
    [reports addItemWithTitle:@"Display Address Report…" action:@selector(showAddressReport:) keyEquivalent:@""];
    [reports addItemWithTitle:@"Display Load Summary…" action:@selector(showLoadSummary:) keyEquivalent:@""];
    reportsItem.submenu = reports;
    [win addItemWithTitle:@"Minimize" action:@selector(performMiniaturize:) keyEquivalent:@"m"];
    winItem.submenu = win;
    NSApp.mainMenu = bar;
    NSApp.windowsMenu = win;
}

/* a new window, kept until it closes */
- (EeVoterWindow *)addWindowRoster:(BOOL)roster
{
    EeVoterWindow *w = [[EeVoterWindow alloc] initRoster:roster];
    w.snapshotting = self.snapshot != nil;
    [self.windows addObject:w];
    return w;
}

- (void)windowClosed:(EeVoterWindow *)w
{
    dispatch_async(dispatch_get_main_queue(), ^{
      /* (kept until its window has finished closing) */
      [self.windows removeObject:w];
    });
}

/* the voter list or roster window in front (a report's window counts as its list's) */
- (EeVoterWindow *)currentWindow
{
    id d = (NSApp.keyWindow ?: NSApp.mainWindow).delegate;
    if ([d isKindOfClass:[EeVoterWindow class]])
    {
        return d;
    }
    if ([d isKindOfClass:[EeReportWindow class]] && ((EeReportWindow *)d).owner != nil)
    {
        return ((EeReportWindow *)d).owner;
    }
    if ([d isKindOfClass:[EeTotalsWindow class]] && ((EeTotalsWindow *)d).owner != nil)
    {
        return ((EeTotalsWindow *)d).owner;
    }
    return self.windows.lastObject;
}

/* where a voter list opens, as on Windows: in @p w while it is an empty voter list window, else
 * in a new one */
- (EeVoterWindow *)listWindowFrom:(EeVoterWindow *)w
{
    if (w != nil && !w.roster && w.voters == NULL && !w.loading && w.window.visible)
    {
        return w;
    }
    return [self addWindowRoster:NO];
}

- (void)applicationDidFinishLaunching:(NSNotification *)note
{
    [self buildMenu];
    self.windows = [NSMutableArray array];
    self.main = [self addWindowRoster:NO];
    if (self.roster && self.pendingFiles.count > 0)
    {
        /* (the roster's own window, as Open Voter Roster... opens it; the empty voter list
         * window stays, as on Windows) */
        EeVoterWindow *r = [self addWindowRoster:YES];
        r.opener = self.main;
        self.main = r;
        [r openRosterPaths:self.pendingFiles];
    }
    else if (self.pending != nil)
    {
        [self.main openPath:self.pending];
    }
    [NSApp activateIgnoringOtherApps:YES];
    if (self.snapshot != nil)
    {
        [self takeSnapshotWhenReady];
    }
}

/* --snapshot: once the list is loaded, sort and filter it as the heading and the right-click
 * menu would, then, once that is done, draw the window (or the Filter sheet) into a PNG and
 * quit. The window draws itself; no screen recording permission is needed. */
- (void)takeSnapshotWhenReady
{
    EeVoterWindow *w = self.main;
    BOOL waiting = w.loading || w.jobRunning ||
                   ((self.pending != nil || self.roster) && w.voters == NULL &&
                    ![w.status.stringValue containsString:@"could"]);
    if (!waiting && !self.snapshotActionsDone)
    {
        self.snapshotActionsDone = YES;
        if (self.snapshotZoom > 0 || self.snapshotNames >= 0)
        {
            /* --zoom / --names: as the Settings sheet's OK sets them, but not saved */
            EeUiOptions o = w.options;
            if (self.snapshotZoom > 0)
            {
                o.zoom_percent = self.snapshotZoom;
            }
            if (self.snapshotNames >= 0)
            {
                o.surname_first = self.snapshotNames;
            }
            [w setOptions:o save:NO];
        }
        if (self.snapshotMarks != EE_UI_MARKS_NONE)
        {
            [w showDuplicates:self.snapshotMarks];
        }
        for (NSString *q in self.snapshotQuick)
        {
            NSRange eq = [q rangeOfString:@"="];
            if (eq.location != NSNotFound)
            {
                [w addQuickRuleColumn:(uint32_t)[q substringWithRange:NSMakeRange(1, eq.location - 1)].intValue
                                value:[q substringFromIndex:eq.location + 1]
                              exclude:[q hasPrefix:@"-"]];
            }
        }
        if (self.snapshotReport >= 0 && w.voters != NULL)
        {
            /* --report: the report's window (and --sort clicks its heading, 0 or 1) */
            [w showReport:self.snapshotReport];
            if (self.snapshotSort >= 0 && self.snapshotSort <= EE_UI_REPORT_COLUMN_COUNT)
            {
                [[w reportOfKind:self.snapshotReport] sortByColumn:self.snapshotSort ascending:self.snapshotAscending];
            }
        }
        else if (self.snapshotSort >= 0 && w.voters != NULL)
        {
            [w sortByColumn:self.snapshotSort ascending:self.snapshotAscending];
        }
        if (self.snapshotFilterWindow || self.snapshotLoadFilter != nil || self.snapshotExportFilter != nil)
        {
            [w showFilter:nil];
            /* --load-filter / --export-filter: the Filter sheet's Load Filter... and Export
             * Filter..., without the panels */
            if (self.snapshotLoadFilter != nil)
            {
                [w.filterSheet loadFilterFrom:self.snapshotLoadFilter];
            }
            if (self.snapshotExportFilter != nil)
            {
                [w.filterSheet exportFilterTo:self.snapshotExportFilter type:0];
            }
        }
        if (self.snapshotOptionsWindow)
        {
            [w showOptions:nil];
        }
        if (self.snapshotSummary)
        {
            [w showLoadSummary:nil];
        }
        if (self.snapshotTotals)
        {
            [w showVotingTotals:nil];
        }
        waiting = YES;
    }
    if (waiting)
    {
        dispatch_after(dispatch_time(DISPATCH_TIME_NOW, 200 * NSEC_PER_MSEC), dispatch_get_main_queue(), ^{
          [self takeSnapshotWhenReady];
        });
        return;
    }
    EeReportWindow *report = self.snapshotReport >= 0 ? [w reportOfKind:self.snapshotReport] : nil;
    EeTotalsWindow *totals = self.snapshotTotals ? w.totalsWindow : nil;
    if (self.snapshotExport != nil && totals != nil)
    {
        [totals exportAllTo:self.snapshotExport]; /* (Export All..., without the sheet) */
        self.snapshotExport = nil;
    }
    else if (self.snapshotExport != nil && report != nil)
    {
        [report exportAllTo:self.snapshotExport]; /* (Export All..., without the sheet) */
        self.snapshotExport = nil;
    }
    else if (self.snapshotExport != nil && w.voters != NULL)
    {
        [w exportShownTo:self.snapshotExport];
        self.snapshotExport = nil;
    }
    dispatch_after(dispatch_time(DISPATCH_TIME_NOW, 500 * NSEC_PER_MSEC), dispatch_get_main_queue(), ^{
      NSWindow *target = totals != nil               ? totals.window
                         : report != nil         ? report.window
                         : w.summaryAlert != nil ? w.summaryAlert.window
                         : w.optionsSheet != nil ? w.optionsSheet.sheet
                         : w.filterSheet != nil  ? w.filterSheet.sheet
                                                 : w.window;
      NSView *v = target.contentView.superview ?: target.contentView;
      NSBitmapImageRep *rep = [v bitmapImageRepForCachingDisplayInRect:v.bounds];
      [v cacheDisplayInRect:v.bounds toBitmapImageRep:rep];
      [[rep representationUsingType:NSBitmapImageFileTypePNG properties:@{}] writeToFile:self.snapshot atomically:YES];
      if (w.filterSheet != nil)
      {
          [w.filterSheet close]; /* (terminate: does nothing while a sheet is open) */
      }
      if (w.optionsSheet != nil)
      {
          [w.optionsSheet close];
      }
      if (w.window.attachedSheet != nil)
      {
          [w.window endSheet:w.window.attachedSheet]; /* (an alert, such as no duplicates) */
      }
      [NSApp terminate:nil];
    });
}

- (BOOL)application:(NSApplication *)sender openFile:(NSString *)filename
{
    if (self.windows == nil)
    {
        self.pending = filename; /* (before launching finished: the file named at start) */
    }
    else
    {
        EeVoterWindow *w = [self listWindowFrom:[self currentWindow]];
        [w.window makeKeyAndOrderFront:nil];
        [w openPath:filename];
    }
    return YES;
}

/* File > Open Voter List...: into the window in front while it is an empty voter list, else into
 * a new window, as on Windows */
- (void)openDocument:(id)sender
{
    NSOpenPanel *panel = [NSOpenPanel openPanel];
    EeVoterWindow *from = [self currentWindow];
    panel.allowsMultipleSelection = NO;
    panel.message = @"Choose a voter list (CSV, TSV, TXT or XLSX).";
    if (from == nil || !from.window.visible)
    {
        from = [self addWindowRoster:NO];
    }
    [panel beginSheetModalForWindow:from.window
                  completionHandler:^(NSModalResponse r) {
                    if (r == NSModalResponseOK && panel.URL != nil)
                    {
                        [[self listWindowFrom:from] openPath:panel.URL.path];
                    }
                  }];
}

/* File > Open Voter Roster...: one or more files, in a Voter Roster window of its own, as on
 * Windows */
- (void)openRoster:(id)sender
{
    NSOpenPanel *panel = [NSOpenPanel openPanel];
    EeVoterWindow *from = [self currentWindow];
    void (^chosen)(NSModalResponse) = ^(NSModalResponse r) {
      NSMutableArray<NSString *> *paths = [NSMutableArray array];
      EeVoterWindow *w;
      if (r != NSModalResponseOK)
      {
          return;
      }
      for (NSURL *u in panel.URLs)
      {
          [paths addObject:u.path];
      }
      if (paths.count == 0)
      {
          return;
      }
      w = [self addWindowRoster:YES];
      w.opener = from;
      [w openRosterPaths:paths];
    };
    panel.allowsMultipleSelection = YES;
    panel.message = @"Choose a voter roster's files (ZIP, XLSX, XLS, CSV, TSV, TXT or PDF).";
    if (from != nil && from.window.visible && from.window.attachedSheet == nil)
    {
        [panel beginSheetModalForWindow:from.window completionHandler:chosen];
    }
    else
    {
        chosen([panel runModal]);
    }
}

- (BOOL)applicationShouldTerminateAfterLastWindowClosed:(NSApplication *)sender
{
    return YES;
}

@end

static void app_window_closed(EeVoterWindow *w)
{
    id d = NSApp.delegate;
    if ([d isKindOfClass:[EeAppDelegate class]])
    {
        [(EeAppDelegate *)d windowClosed:w];
    }
}

int main(int argc, const char **argv)
{
    @autoreleasepool
    {
        EeAppDelegate *delegate;
        int i;
        for (i = 1; i + 1 < argc; i++)
        {
            if (strcmp(argv[i], "--selftest") == 0)
            {
                return EeUiVoters_SelfTest(argv[i + 1]);
            }
            if (strcmp(argv[i], "--selftest-roster") == 0)
            {
                return EeUiVoters_RosterSelfTest((const char *const *)&argv[i + 1], (uint32_t)(argc - i - 1));
            }
        }
        [NSApplication sharedApplication];
        NSApp.activationPolicy = NSApplicationActivationPolicyRegular;
        delegate = [[EeAppDelegate alloc] init];
        delegate.snapshotSort = -1;
        delegate.snapshotReport = -1;
        delegate.snapshotNames = -1;
        delegate.snapshotAscending = YES;
        delegate.snapshotQuick = [NSMutableArray array];
        delegate.pendingFiles = [NSMutableArray array];
        for (i = 1; i < argc; i++)
        {
            if (strcmp(argv[i], "--snapshot") == 0 && i + 1 < argc)
            {
                delegate.snapshot = ns_utf8(argv[++i]);
            }
            else if (strcmp(argv[i], "--roster") == 0)
            {
                delegate.roster = YES;
            }
            else if (strcmp(argv[i], "--load-summary") == 0)
            {
                delegate.snapshotSummary = YES;
            }
            else if (strcmp(argv[i], "--voting-totals") == 0)
            {
                delegate.snapshotTotals = YES;
            }
            else if (strcmp(argv[i], "--sort") == 0 && i + 1 < argc)
            {
                /* <column>[:desc] */
                i++;
                delegate.snapshotSort = atoi(argv[i]);
                delegate.snapshotAscending = strstr(argv[i], ":desc") == NULL;
            }
            else if ((strcmp(argv[i], "--include") == 0 || strcmp(argv[i], "--exclude") == 0) && i + 1 < argc)
            {
                /* <column>=<value>: what the right-click Include / Exclude adds */
                [delegate.snapshotQuick
                    addObject:[NSString stringWithFormat:@"%c%@", argv[i][2] == 'i' ? '+' : '-', ns_utf8(argv[i + 1])]];
                i++;
            }
            else if (strcmp(argv[i], "--filter-window") == 0)
            {
                delegate.snapshotFilterWindow = YES;
            }
            else if (strcmp(argv[i], "--duplicates") == 0 && i + 1 < argc)
            {
                i++;
                delegate.snapshotMarks = strcmp(argv[i], "voters") == 0 ? EE_UI_MARKS_DUP_VOTERS : EE_UI_MARKS_DUP_IDS;
            }
            else if (strcmp(argv[i], "--export") == 0 && i + 1 < argc)
            {
                delegate.snapshotExport = ns_utf8(argv[++i]);
            }
            else if (strcmp(argv[i], "--report") == 0 && i + 1 < argc)
            {
                i++;
                delegate.snapshotReport = strcmp(argv[i], "address") == 0 ? EE_UI_REPORT_ADDRESS : EE_UI_REPORT_PRECINCT;
            }
            else if (strcmp(argv[i], "--zoom") == 0 && i + 1 < argc)
            {
                delegate.snapshotZoom = atoi(argv[++i]);
            }
            else if (strcmp(argv[i], "--names") == 0 && i + 1 < argc)
            {
                i++;
                delegate.snapshotNames = strcmp(argv[i], "given") == 0 ? 0 : 1;
            }
            else if (strcmp(argv[i], "--options-window") == 0)
            {
                delegate.snapshotOptionsWindow = YES;
            }
            else if (strcmp(argv[i], "--load-filter") == 0 && i + 1 < argc)
            {
                delegate.snapshotLoadFilter = ns_utf8(argv[++i]);
            }
            else if (strcmp(argv[i], "--export-filter") == 0 && i + 1 < argc)
            {
                delegate.snapshotExportFilter = ns_utf8(argv[++i]);
            }
            else if (argv[i][0] != '-')
            {
                delegate.pending = ns_utf8(argv[i]); /* (also the file the Finder opens with) */
                [delegate.pendingFiles addObject:delegate.pending];
            }
        }
        NSApp.delegate = delegate;
        [NSApp run];
    }
    return 0;
}
