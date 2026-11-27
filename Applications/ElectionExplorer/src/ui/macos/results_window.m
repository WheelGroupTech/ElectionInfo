/**
 * @file results_window.m
 * @brief The Election Results window in AppKit, as Windows' (main.c, "Election Results window
 *        (1.5)"): File > Open Election Results... loads the files behind a progress sheet with
 *        Cancel, and each election opens a window of its own: a row per contest and choice
 *        (Over Votes and Under Votes too when the files give them), View > Totals or By
 *        Precinct, Reset View, a heading click to sort (again to reverse; counts largest first),
 *        Copy (Cmd-C, or the right-click menu), Reports > Display Load Summary... (open by itself
 *        when the files disagree), Settings... (Windows' Options: zoom, a column per vote type,
 *        the party shown first in a primary) and Windows' status line. The rows come from the
 *        engine through ee_ui_results.h (plain C). Compiled with ARC.
 */

#import "app_mac.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "ee_ui_results.h"

static NSString *ns_utf8(const char *s)
{
    return [NSString stringWithUTF8String:s != NULL ? s : ""] ?: @"";
}

static NSButton *sheet_button(NSString *title, id target, SEL action)
{
    NSButton *b = [NSButton buttonWithTitle:title target:target action:action];
    b.translatesAutoresizingMaskIntoConstraints = NO;
    return b;
}

@implementation EeResultsSnapshot
@end

@class EeResultsOptionsSheet;

@interface EeResultsWindow () <NSTableViewDataSource, NSTableViewDelegate, NSWindowDelegate, NSMenuDelegate,
                               NSMenuItemValidation>
@property(strong) NSTableView *table;
@property(strong) NSTextField *status;
@property(assign) EeUiResults *res;
@property(assign) int zoom;                          /* the list's text size (Settings) */
@property(strong) NSAlert *summaryAlert;             /* the Load Summary while it is open */
@property(strong) EeResultsOptionsSheet *optionsSheet; /* Settings while it is open */
@property(strong) EeResultsSnapshot *snap;           /* --snapshot (nil: none) */
- (void)setOptions:(EeUiResultsOptions)o save:(BOOL)save;
@end

static NSMutableArray<EeResultsWindow *> *g_results; /* every open results window */

/* -------------------------------------------------------------------------- */
/* Settings (Windows' Election Results Options)                               */
/* -------------------------------------------------------------------------- */

@interface EeResultsOptionsSheet : NSObject
@property(weak) EeResultsWindow *owner;
@property(strong) NSWindow *sheet;
@property(strong) NSTextField *zoom;
@property(strong) NSStepper *stepper;
@property(strong) NSButton *voteTypes;
@property(strong) NSButton *rep;
@property(strong) NSButton *dem;
@end

@implementation EeResultsOptionsSheet

- (instancetype)initWithOwner:(EeResultsWindow *)owner
{
    NSView *v;
    NSTextField *zoomLabel, *percent, *party;
    NSButton *ok, *cancel;
    NSNumberFormatter *digits;
    self = [super init];
    if (self == nil)
    {
        return nil;
    }
    self.owner = owner;
    self.sheet = [[NSWindow alloc] initWithContentRect:NSMakeRect(0, 0, 400, 190)
                                             styleMask:NSWindowStyleMaskTitled
                                               backing:NSBackingStoreBuffered
                                                 defer:NO];
    self.sheet.title = @"Election Results Options";
    v = self.sheet.contentView;

    zoomLabel = [NSTextField labelWithString:@"Zoom:"];
    digits = [[NSNumberFormatter alloc] init];
    digits.numberStyle = NSNumberFormatterNoStyle;
    digits.minimum = @(EE_UI_ZOOM_MIN);
    digits.maximum = @(EE_UI_ZOOM_MAX);
    digits.allowsFloats = NO;
    self.zoom = [NSTextField textFieldWithString:[NSString stringWithFormat:@"%d", owner.zoom]];
    self.zoom.formatter = digits;
    self.zoom.alignment = NSTextAlignmentRight;
    self.stepper = [[NSStepper alloc] initWithFrame:NSZeroRect];
    self.stepper.minValue = EE_UI_ZOOM_MIN;
    self.stepper.maxValue = EE_UI_ZOOM_MAX;
    self.stepper.increment = 1;
    self.stepper.valueWraps = NO;
    self.stepper.integerValue = owner.zoom;
    self.stepper.target = self;
    self.stepper.action = @selector(stepped:);
    percent = [NSTextField labelWithString:@"%  (50–250)"];
    self.voteTypes = [NSButton checkboxWithTitle:@"Show a column for each vote type" target:nil action:nil];
    self.voteTypes.state = EeUiResults_ShowVoteTypes(owner.res) ? NSControlStateValueOn : NSControlStateValueOff;
    party = [NSTextField labelWithString:@"Party to display first in a primary:"];
    /* (radio buttons with one action are one group) */
    self.rep = [NSButton radioButtonWithTitle:@"Republican" target:self action:@selector(partyChosen:)];
    self.dem = [NSButton radioButtonWithTitle:@"Democratic" target:self action:@selector(partyChosen:)];
    (EeUiResults_PartyFirst(owner.res) == EE_UI_PARTY_DEM ? self.dem : self.rep).state = NSControlStateValueOn;
    ok = sheet_button(@"OK", self, @selector(ok:));
    ok.keyEquivalent = @"\r";
    cancel = sheet_button(@"Cancel", self, @selector(cancel:));
    cancel.keyEquivalent = @"\033";
    for (NSView *sub in @[ zoomLabel, self.zoom, self.stepper, percent, self.voteTypes, party, self.rep, self.dem ])
    {
        sub.translatesAutoresizingMaskIntoConstraints = NO;
        [v addSubview:sub];
    }
    [v addSubview:ok];
    [v addSubview:cancel];
    [NSLayoutConstraint activateConstraints:@[
        [self.zoom.topAnchor constraintEqualToAnchor:v.topAnchor constant:18],
        [zoomLabel.leadingAnchor constraintEqualToAnchor:v.leadingAnchor constant:18],
        [zoomLabel.centerYAnchor constraintEqualToAnchor:self.zoom.centerYAnchor],
        [self.zoom.leadingAnchor constraintEqualToAnchor:zoomLabel.trailingAnchor constant:8],
        [self.zoom.widthAnchor constraintEqualToConstant:52],
        [self.stepper.leadingAnchor constraintEqualToAnchor:self.zoom.trailingAnchor constant:2],
        [self.stepper.centerYAnchor constraintEqualToAnchor:self.zoom.centerYAnchor],
        [percent.leadingAnchor constraintEqualToAnchor:self.stepper.trailingAnchor constant:6],
        [percent.centerYAnchor constraintEqualToAnchor:self.zoom.centerYAnchor],
        [self.voteTypes.topAnchor constraintEqualToAnchor:self.zoom.bottomAnchor constant:14],
        [self.voteTypes.leadingAnchor constraintEqualToAnchor:zoomLabel.leadingAnchor],
        [party.topAnchor constraintEqualToAnchor:self.voteTypes.bottomAnchor constant:14],
        [party.leadingAnchor constraintEqualToAnchor:zoomLabel.leadingAnchor],
        [self.rep.topAnchor constraintEqualToAnchor:party.bottomAnchor constant:8],
        [self.rep.leadingAnchor constraintEqualToAnchor:zoomLabel.leadingAnchor constant:10],
        [self.dem.centerYAnchor constraintEqualToAnchor:self.rep.centerYAnchor],
        [self.dem.leadingAnchor constraintEqualToAnchor:self.rep.trailingAnchor constant:24],
        [cancel.topAnchor constraintEqualToAnchor:self.rep.bottomAnchor constant:20],
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

- (void)partyChosen:(id)sender
{
}

- (void)close
{
    [self.owner.window endSheet:self.sheet];
    self.owner.optionsSheet = nil;
}

- (void)ok:(id)sender
{
    EeUiResultsOptions o;
    /* (Windows keeps the zoom when the box holds no number) */
    o.zoom_percent = self.zoom.stringValue.length > 0 ? (int)self.zoom.integerValue : self.owner.zoom;
    o.show_vote_types = self.voteTypes.state == NSControlStateValueOn;
    o.party_first = self.dem.state == NSControlStateValueOn ? EE_UI_PARTY_DEM : EE_UI_PARTY_REP;
    [self close];
    [self.owner setOptions:o save:YES];
}

- (void)cancel:(id)sender
{
    [self close];
}

@end

/* -------------------------------------------------------------------------- */
/* The window                                                                 */
/* -------------------------------------------------------------------------- */

@implementation EeResultsWindow

- (NSFont *)listFont
{
    /* (as the voter list's: the system font with digits of one width) */
    return [NSFont monospacedDigitSystemFontOfSize:NSFont.smallSystemFontSize * self.zoom / 100.0
                                            weight:NSFontWeightRegular];
}

/* the view's columns, at Windows' widths for the zoom (wider when the heading needs it) */
- (void)rebuildColumns
{
    NSFont *font = [self listFont];
    int c, nc = EeUiResults_Columns(self.res);
    while (self.table.tableColumns.count > 0)
    {
        [self.table removeTableColumn:self.table.tableColumns.lastObject];
    }
    for (c = 0; c < nc; c++)
    {
        NSTableColumn *col = [[NSTableColumn alloc] initWithIdentifier:[NSString stringWithFormat:@"%d", c]];
        NSTextFieldCell *cell = [[NSTextFieldCell alloc] initTextCell:@""];
        NSString *title = ns_utf8(EeUiResults_ColumnTitle(self.res, c));
        /* (the heading keeps its size at any zoom, as in the voter list) */
        CGFloat need = [title sizeWithAttributes:@{NSFontAttributeName : col.headerCell.font}].width + 24;
        col.title = title;
        cell.font = font;
        cell.lineBreakMode = NSLineBreakByTruncatingTail;
        cell.alignment = EeUiResults_ColumnNumeric(self.res, c) ? NSTextAlignmentRight : NSTextAlignmentLeft;
        col.dataCell = cell;
        col.minWidth = 30;
        col.resizingMask = NSTableColumnUserResizingMask;
        col.width = MAX(need, EeUiResults_ColumnWidth(self.res, c) * self.zoom / 100.0);
        [self.table addTableColumn:col];
    }
    self.table.rowHeight = font.boundingRectForFont.size.height + 3;
    [self showSortArrow];
    [self.table reloadData];
    self.status.stringValue = ns_utf8(EeUiResults_StatusText(self.res));
}

/* the heading's arrow where the rows are sorted (none in the loaded order) */
- (void)showSortArrow
{
    int asc = 1, sorted = EeUiResults_SortColumn(self.res, &asc);
    self.table.highlightedTableColumn = nil;
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

/* View > Totals / By Precinct, and Settings: the rows and columns again (Res_Rebuild) */
- (void)setViewByPrecinct:(BOOL)byPrecinct voteTypes:(int)voteTypes partyFirst:(int)partyFirst
{
    if (!EeUiResults_SetView(self.res, byPrecinct, voteTypes, partyFirst))
    {
        NSAlert *a = [[NSAlert alloc] init];
        a.messageText = @"Out of memory.";
        [a beginSheetModalForWindow:self.window completionHandler:nil];
    }
    [self.table deselectAll:nil];
    [self rebuildColumns];
}

/* Settings' OK (and --zoom, --vote-types, --party-first): the view again when the vote types,
 * the party or the zoom changed (Windows' App_ShowResultsOptions) */
- (void)setOptions:(EeUiResultsOptions)o save:(BOOL)save
{
    int zoom = EeUiOptions_ClampZoom(o.zoom_percent);
    BOOL again = zoom != self.zoom || (o.show_vote_types != 0) != (EeUiResults_ShowVoteTypes(self.res) != 0) ||
                 o.party_first != EeUiResults_PartyFirst(self.res);
    self.zoom = zoom;
    if (save)
    {
        o.zoom_percent = zoom;
        if (!EeUiResultsOptions_Save(&o))
        {
            fprintf(stderr, "could not save the options\n");
        }
    }
    if (again)
    {
        [self setViewByPrecinct:EeUiResults_ByPrecinct(self.res) voteTypes:o.show_vote_types partyFirst:o.party_first];
    }
}

- (instancetype)initWithResults:(EeUiResults *)res snapshot:(EeResultsSnapshot *)snap
{
    NSScrollView *scroll;
    NSView *content;
    EeUiResultsOptions o;
    self = [super init];
    if (self == nil)
    {
        EeUiResults_Free(res);
        return nil;
    }
    self.res = res;
    self.snap = snap;
    EeUiResultsOptions_Load(&o); /* (as saved last run) */
    self.zoom = EeUiOptions_ClampZoom(o.zoom_percent);
    self.table = [[NSTableView alloc] initWithFrame:NSZeroRect];
    self.table.dataSource = self;
    self.table.delegate = self;
    self.table.allowsColumnReordering = NO;
    self.table.allowsColumnSelection = NO;
    self.table.allowsMultipleSelection = YES;
    self.table.usesAlternatingRowBackgroundColors = YES;
    self.table.columnAutoresizingStyle = NSTableViewNoColumnAutoresizing;
    self.table.gridStyleMask = NSTableViewSolidVerticalGridLineMask | NSTableViewSolidHorizontalGridLineMask;
    if (@available(macOS 11.0, *))
    {
        self.table.style = NSTableViewStylePlain;
    }
    self.table.menu = [[NSMenu alloc] initWithTitle:@""];
    self.table.menu.delegate = self;

    self.window = [[NSWindow alloc]
        initWithContentRect:NSMakeRect(0, 0, 1000, 600) /* (Windows' size) */
                  styleMask:NSWindowStyleMaskTitled | NSWindowStyleMaskClosable | NSWindowStyleMaskMiniaturizable |
                            NSWindowStyleMaskResizable
                    backing:NSBackingStoreBuffered
                      defer:NO];
    self.window.title = ns_utf8(EeUiResults_WindowTitle(res));
    self.window.delegate = self;
    self.window.releasedWhenClosed = NO;
    self.window.tabbingMode = NSWindowTabbingModeDisallowed;
    [self.window center];
    if (g_results.count > 0)
    {
        NSRect last = g_results.lastObject.window.frame;
        [self.window setFrameTopLeftPoint:NSMakePoint(NSMinX(last) + 24, NSMaxY(last) - 24)];
    }
    content = self.window.contentView;
    scroll = [[NSScrollView alloc] initWithFrame:NSZeroRect];
    scroll.documentView = self.table;
    scroll.hasVerticalScroller = YES;
    scroll.hasHorizontalScroller = YES;
    scroll.translatesAutoresizingMaskIntoConstraints = NO;
    self.status = [NSTextField labelWithString:@""];
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
    /* the saved options' view (Totals) */
    [self setViewByPrecinct:NO voteTypes:o.show_vote_types partyFirst:o.party_first];
    if (g_results == nil)
    {
        g_results = [NSMutableArray array];
    }
    [g_results addObject:self];
    [self.window makeKeyAndOrderFront:nil];
    [self.window makeFirstResponder:self.table];
    if (EeUiResults_HasDifferences(res))
    {
        [self showLoadSummary:nil]; /* (as on Windows: when the files disagree) */
    }
    if (snap != nil)
    {
        dispatch_after(dispatch_time(DISPATCH_TIME_NOW, 400 * NSEC_PER_MSEC), dispatch_get_main_queue(), ^{
          [self snapshotActions];
        });
    }
    return self;
}

- (void)dealloc
{
    EeUiResults_Free(_res);
}

- (void)windowWillClose:(NSNotification *)note
{
    dispatch_async(dispatch_get_main_queue(), ^{
      /* (kept until its window has finished closing) */
      [g_results removeObject:self];
    });
}

/* ---- the table: cells on demand, sorting by a heading ---- */

- (NSInteger)numberOfRowsInTableView:(NSTableView *)tableView
{
    return EeUiResults_Rows(self.res);
}

- (id)tableView:(NSTableView *)tableView objectValueForTableColumn:(NSTableColumn *)column row:(NSInteger)row
{
    char cell[1024];
    return ns_utf8(EeUiResults_Cell(self.res, (uint32_t)row, (int)column.identifier.integerValue, cell, sizeof(cell)));
}

- (BOOL)tableView:(NSTableView *)tableView shouldEditTableColumn:(NSTableColumn *)column row:(NSInteger)row
{
    return NO;
}

/* a heading click, as on Windows: the same column again turns the order around; a new one
 * sorts text A to Z and counts largest first (sorted here, on the main thread) */
- (void)clickColumn:(int)column
{
    EeUiResults_ClickHeading(self.res, column);
    [self showSortArrow];
    [self.table deselectAll:nil];
    [self.table reloadData];
}

- (void)tableView:(NSTableView *)tableView didClickTableColumn:(NSTableColumn *)column
{
    [self clickColumn:(int)column.identifier.integerValue];
}

/* ---- commands ---- */

- (IBAction)copy:(id)sender
{
    NSMutableData *rows = [NSMutableData data];
    char *text;
    [self.table.selectedRowIndexes enumerateIndexesUsingBlock:^(NSUInteger i, BOOL *stop) {
      uint32_t r = (uint32_t)i;
      [rows appendBytes:&r length:sizeof(r)];
    }];
    if (rows.length == 0)
    {
        return;
    }
    text = EeUiResults_CopyText(self.res, (const uint32_t *)rows.bytes, (uint32_t)(rows.length / sizeof(uint32_t)));
    if (text != NULL)
    {
        [NSPasteboard.generalPasteboard clearContents];
        [NSPasteboard.generalPasteboard setString:ns_utf8(text) forType:NSPasteboardTypeString];
        free(text);
    }
}

/* View > Totals and By Precinct (check marks, as on Windows) */
- (IBAction)showTotals:(id)sender
{
    if (EeUiResults_ByPrecinct(self.res))
    {
        [self setViewByPrecinct:NO
                      voteTypes:EeUiResults_ShowVoteTypes(self.res)
                     partyFirst:EeUiResults_PartyFirst(self.res)];
    }
}

- (IBAction)showByPrecinct:(id)sender
{
    if (!EeUiResults_ByPrecinct(self.res) && EeUiResults_HasPrecinctDetail(self.res))
    {
        [self setViewByPrecinct:YES
                      voteTypes:EeUiResults_ShowVoteTypes(self.res)
                     partyFirst:EeUiResults_PartyFirst(self.res)];
    }
}

/* View > Reset View: back to the order the rows had when loaded */
- (IBAction)resetSort:(id)sender
{
    if (EeUiResults_SortColumn(self.res, NULL) >= 0)
    {
        EeUiResults_Sort(self.res, -1, 1);
        [self showSortArrow];
        [self.table deselectAll:nil];
        [self.table reloadData];
    }
}

/* Reports > Display Load Summary...: the files, the one shown, every note and difference */
- (IBAction)showLoadSummary:(id)sender
{
    NSAlert *a;
    NSScrollView *scroll;
    NSTextView *view;
    if (self.window.attachedSheet != nil)
    {
        return;
    }
    a = [[NSAlert alloc] init];
    a.messageText = @"Election Results Load Summary";
    scroll = [[NSScrollView alloc] initWithFrame:NSMakeRect(0, 0, 560, 240)];
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
    view.string = ns_utf8(EeUiResults_LoadSummary(self.res));
    scroll.documentView = view;
    a.accessoryView = scroll;
    [a addButtonWithTitle:@"OK"];
    self.summaryAlert = a;
    [a beginSheetModalForWindow:self.window
              completionHandler:^(NSModalResponse r) {
                self.summaryAlert = nil;
              }];
}

/* Settings... (Windows' Edit > Options...) */
- (IBAction)showOptions:(id)sender
{
    if (self.optionsSheet != nil || self.window.attachedSheet != nil)
    {
        return;
    }
    self.optionsSheet = [[EeResultsOptionsSheet alloc] initWithOwner:self];
    [self.window beginSheet:self.optionsSheet.sheet completionHandler:nil];
}

/* the right-click menu, as on Windows: Copy (the row clicked selected first) */
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
}

- (BOOL)validateMenuItem:(NSMenuItem *)item
{
    if (item.action == @selector(copy:))
    {
        return self.table.numberOfSelectedRows > 0;
    }
    if (item.action == @selector(showTotals:))
    {
        item.state = EeUiResults_ByPrecinct(self.res) ? NSControlStateValueOff : NSControlStateValueOn;
    }
    if (item.action == @selector(showByPrecinct:))
    {
        item.state = EeUiResults_ByPrecinct(self.res) ? NSControlStateValueOn : NSControlStateValueOff;
        return EeUiResults_HasPrecinctDetail(self.res); /* (grayed without precinct detail) */
    }
    if (item.action == @selector(resetSort:))
    {
        return EeUiResults_SortColumn(self.res, NULL) >= 0;
    }
    if (item.action == @selector(showLoadSummary:))
    {
        item.hidden = NO;
        return self.window.attachedSheet == nil;
    }
    if (item.action == @selector(showOptions:))
    {
        return self.window.attachedSheet == nil;
    }
    return YES;
}

/* ---- --snapshot ---- */

- (void)snapshotActions
{
    EeResultsSnapshot *s = self.snap;
    if (self.summaryAlert != nil && !s.loadSummary)
    {
        [self.window endSheet:self.summaryAlert.window]; /* (opened by itself: the PNG is of the window) */
    }
    if (s.byPrecinct)
    {
        [self showByPrecinct:nil];
    }
    if (s.zoom > 0 || s.voteTypes >= 0 || s.partyFirst >= 0)
    {
        EeUiResultsOptions o;
        o.zoom_percent = s.zoom > 0 ? s.zoom : self.zoom;
        o.show_vote_types = s.voteTypes >= 0 ? s.voteTypes : EeUiResults_ShowVoteTypes(self.res);
        o.party_first = s.partyFirst >= 0 ? s.partyFirst : EeUiResults_PartyFirst(self.res);
        [self setOptions:o save:NO]; /* (as OK does, without saving) */
    }
    if (s.sort >= 0 && s.sort < EeUiResults_Columns(self.res))
    {
        int asc = 1, k;
        for (k = 0; k < 2; k++)
        {
            /* (a heading click; a second when the first sorted the other way) */
            [self clickColumn:s.sort];
            if (EeUiResults_SortColumn(self.res, &asc) == s.sort && (asc != 0) == s.ascending)
            {
                break;
            }
        }
    }
    if (s.loadSummary && self.summaryAlert == nil)
    {
        [self showLoadSummary:nil];
    }
    else if (s.optionsWindow)
    {
        [self showOptions:nil];
    }
    dispatch_after(dispatch_time(DISPATCH_TIME_NOW, 500 * NSEC_PER_MSEC), dispatch_get_main_queue(), ^{
      NSWindow *target = s.loadSummary && self.summaryAlert != nil    ? self.summaryAlert.window
                         : s.optionsWindow && self.optionsSheet != nil ? self.optionsSheet.sheet
                                                                       : self.window;
      NSView *v = target.contentView.superview ?: target.contentView;
      NSBitmapImageRep *rep = [v bitmapImageRepForCachingDisplayInRect:v.bounds];
      [v cacheDisplayInRect:v.bounds toBitmapImageRep:rep];
      [[rep representationUsingType:NSBitmapImageFileTypePNG properties:@{}] writeToFile:s.png atomically:YES];
      if (self.optionsSheet != nil)
      {
          [self.optionsSheet close]; /* (terminate: does nothing while a sheet is open) */
      }
      if (self.window.attachedSheet != nil)
      {
          [self.window endSheet:self.window.attachedSheet];
      }
      [NSApp terminate:nil];
    });
}

/* ---- loading (a background queue behind Windows' progress window, as a sheet) ---- */

typedef struct ResLoadCx
{
    volatile int32_t cancel;
    volatile int percent; /* last percent shown */
    void *bar;            /* (__bridge) NSProgressIndicator */
} ResLoadCx;

static int on_progress(void *user, unsigned percent, unsigned rows)
{
    ResLoadCx *cx = (ResLoadCx *)user;
    (void)rows;
    if ((int)percent != cx->percent)
    {
        NSProgressIndicator *bar = (__bridge NSProgressIndicator *)cx->bar;
        cx->percent = (int)percent;
        dispatch_async(dispatch_get_main_queue(), ^{
          bar.doubleValue = percent;
        });
    }
    return cx->cancel == 0;
}

+ (void)openPaths:(NSArray<NSString *> *)paths over:(NSWindow *)parent snapshot:(EeResultsSnapshot *)snap
{
    NSAlert *a;
    NSProgressIndicator *bar;
    ResLoadCx *cx;
    NSMutableArray<NSString *> *copies = [NSMutableArray arrayWithArray:paths];
    CFAbsoluteTime started = CFAbsoluteTimeGetCurrent();
    if (paths.count == 0 || (cx = (ResLoadCx *)calloc(1, sizeof(ResLoadCx))) == NULL)
    {
        return;
    }
    /* Windows' "Loading Election Results": the words, a bar and Cancel */
    a = [[NSAlert alloc] init];
    a.messageText = @"Loading Election Results";
    a.informativeText = @"Loading election results…";
    bar = [[NSProgressIndicator alloc] initWithFrame:NSMakeRect(0, 0, 300, 20)];
    bar.indeterminate = NO;
    bar.minValue = 0;
    bar.maxValue = 100;
    bar.style = NSProgressIndicatorStyleBar;
    a.accessoryView = bar;
    [a addButtonWithTitle:@"Cancel"];
    cx->percent = -1;
    cx->bar = (__bridge void *)bar;
    if (parent != nil && parent.visible)
    {
        [a beginSheetModalForWindow:parent
                  completionHandler:^(NSModalResponse r) {
                    if (r == NSAlertFirstButtonReturn)
                    {
                        cx->cancel = 1; /* (Cancel: the load stops; nothing opens) */
                    }
                  }];
    }
    dispatch_async(dispatch_get_global_queue(QOS_CLASS_USER_INITIATED, 0), ^{
      const char **utf8 = (const char **)calloc(copies.count, sizeof(char *));
      EeUiResults **out = NULL;
      uint32_t nout = 0, i;
      char err[600];
      int st = EE_UI_RESULTS_ERROR;
      NSString *errText;
      if (utf8 != NULL)
      {
          for (i = 0; i < copies.count; i++)
          {
              utf8[i] = copies[i].fileSystemRepresentation;
          }
          st = EeUiResults_Load(utf8, (uint32_t)copies.count, on_progress, cx, &cx->cancel, &out, &nout, err,
                                sizeof(err));
          free(utf8);
      }
      else
      {
          snprintf(err, sizeof(err), "Out of memory.");
      }
      errText = ns_utf8(err); /* (a block cannot use the array) */
      fprintf(stderr, "results: %u files, %u elections in %.2f s\n", (unsigned)copies.count, nout,
              CFAbsoluteTimeGetCurrent() - started);
      dispatch_async(dispatch_get_main_queue(), ^{
        uint32_t k;
        if (parent != nil && parent.attachedSheet == a.window)
        {
            [parent endSheet:a.window returnCode:NSModalResponseStop];
        }
        free(cx);
        if (st == EE_UI_RESULTS_OK)
        {
            for (k = 0; k < nout; k++)
            {
                /* (the first window makes the --snapshot) */
                (void)[[EeResultsWindow alloc] initWithResults:out[k] snapshot:k == 0 ? snap : nil];
            }
            free(out);
            if (nout == 0 && snap != nil)
            {
                [NSApp terminate:nil];
            }
        }
        else if (st == EE_UI_RESULTS_ERROR)
        {
            fprintf(stderr, "results: %s\n", errText.UTF8String);
            if (snap != nil)
            {
                [NSApp terminate:nil];
            }
            else
            {
                NSAlert *e = [[NSAlert alloc] init];
                e.messageText = errText;
                if (parent != nil && parent.visible)
                {
                    [e beginSheetModalForWindow:parent completionHandler:nil];
                }
                else
                {
                    [e runModal];
                }
            }
        }
      });
    });
}

+ (void)chooseOver:(NSWindow *)parent
{
    NSOpenPanel *panel = [NSOpenPanel openPanel];
    void (^chosen)(NSModalResponse) = ^(NSModalResponse r) {
      NSMutableArray<NSString *> *paths = [NSMutableArray array];
      if (r != NSModalResponseOK)
      {
          return;
      }
      for (NSURL *u in panel.URLs)
      {
          [paths addObject:u.path];
      }
      /* (after the panel's sheet has gone: the progress sheet takes its place) */
      dispatch_async(dispatch_get_main_queue(), ^{
        [EeResultsWindow openPaths:paths over:parent snapshot:nil];
      });
    };
    panel.allowsMultipleSelection = YES;
    panel.message = @"Choose election results files (ZIP, CSV, TSV, TXT, XML, XLS, XLSX or PDF).";
    if (parent != nil && parent.visible && parent.attachedSheet == nil)
    {
        [panel beginSheetModalForWindow:parent completionHandler:chosen];
    }
    else
    {
        chosen([panel runModal]);
    }
}

@end
