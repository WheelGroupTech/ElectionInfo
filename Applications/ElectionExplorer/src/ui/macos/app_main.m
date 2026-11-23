/**
 * @file app_main.m
 * @brief Election Explorer for macOS, prototype: the voter list window in AppKit.
 *
 * One window with the loaded list in a table: File > Open... (or a file given on the command
 * line, or opened from the Finder), the load in the background with a progress bar, sorting by
 * a column's heading (again to reverse), numbers and dates right-aligned, and a status line.
 * The list comes from the engine through ee_ui_voters.h (plain C). Compiled with ARC.
 *
 * For checking over SSH:
 * - `--selftest <file>` loads and sorts the list without a window and prints what the window
 *   would show (rows, columns, the first rows).
 * - `--snapshot <png> <file>` opens the window, loads the list, writes the window as a PNG
 *   and quits (the window draws itself, so no screen recording permission is needed).
 */

#import <Cocoa/Cocoa.h>

#include <stdio.h>
#include <string.h>

#include "ee_ui_voters.h"

/* -------------------------------------------------------------------------- */
/* The voter list window                                                      */
/* -------------------------------------------------------------------------- */

@interface EeVoterWindow : NSObject <NSTableViewDataSource, NSTableViewDelegate, NSWindowDelegate>
@property(strong) NSWindow *window;
@property(strong) NSTableView *table;
@property(strong) NSTextField *status;
@property(strong) NSProgressIndicator *progress;
@property(copy) NSString *path;
@property(assign) EeUiVoters *voters;
@property(assign) BOOL loading;
- (void)openPath:(NSString *)path;
@end

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

@implementation EeVoterWindow

- (instancetype)init
{
    NSScrollView *scroll;
    NSView *content;
    self = [super init];
    if (self == nil)
    {
        return nil;
    }
    self.window = [[NSWindow alloc]
        initWithContentRect:NSMakeRect(0, 0, 1100, 700)
                  styleMask:NSWindowStyleMaskTitled | NSWindowStyleMaskClosable | NSWindowStyleMaskMiniaturizable |
                            NSWindowStyleMaskResizable
                    backing:NSBackingStoreBuffered
                      defer:NO];
    self.window.title = @"Election Explorer";
    self.window.delegate = self;
    self.window.releasedWhenClosed = NO;
    [self.window center];
    [self.window setFrameAutosaveName:@"VoterWindow"];
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

    scroll = [[NSScrollView alloc] initWithFrame:NSZeroRect];
    scroll.documentView = self.table;
    scroll.hasVerticalScroller = YES;
    scroll.hasHorizontalScroller = YES;
    scroll.autohidesScrollers = NO;
    scroll.translatesAutoresizingMaskIntoConstraints = NO;

    self.status = [NSTextField labelWithString:@"Open a voter list with File > Open…"];
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
- (void)rebuildColumns
{
    uint32_t c, ncols = EeUiVoters_Columns(self.voters), nrows = EeUiVoters_Rows(self.voters);
    NSFont *font = [NSFont systemFontOfSize:NSFont.smallSystemFontSize];
    NSDictionary *attrs = @{NSFontAttributeName : font};
    while (self.table.tableColumns.count > 0)
    {
        [self.table removeTableColumn:self.table.tableColumns.lastObject];
    }
    for (c = 0; c < ncols; c++)
    {
        NSString *title = [NSString stringWithUTF8String:EeUiVoters_Title(self.voters, c)] ?: @"";
        NSTableColumn *col = [[NSTableColumn alloc] initWithIdentifier:[NSString stringWithFormat:@"%u", c]];
        CGFloat w = [title sizeWithAttributes:attrs].width + 24;
        uint32_t r;
        for (r = 0; r < nrows && r < 200; r++)
        {
            NSString *s = [NSString stringWithUTF8String:EeUiVoters_Cell(self.voters, r, c)];
            if (s != nil)
            {
                w = MAX(w, [s sizeWithAttributes:attrs].width + 12);
            }
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
}

- (void)showStatus
{
    uint32_t nrows = EeUiVoters_Rows(self.voters);
    self.status.stringValue = [NSString
        stringWithFormat:@"%@ voters, %u columns — %@",
                         [NSNumberFormatter localizedStringFromNumber:@(nrows)
                                                          numberStyle:NSNumberFormatterDecimalStyle],
                         EeUiVoters_Columns(self.voters), self.path.lastPathComponent ?: @""];
}

- (void)openPath:(NSString *)path
{
    LoadCx *cx;
    if (self.loading)
    {
        return;
    }
    self.loading = YES;
    self.path = path;
    self.window.title = [NSString stringWithFormat:@"Election Explorer — %@", path.lastPathComponent];
    self.progress.doubleValue = 0;
    self.progress.hidden = NO;
    self.status.stringValue = @"Loading…";
    cx = (LoadCx *)calloc(1, sizeof(LoadCx));
    cx->win = (__bridge void *)self;
    cx->percent = -1;
    dispatch_async(dispatch_get_global_queue(QOS_CLASS_USER_INITIATED, 0), ^{
      char err[1024] = "";
      EeUiVoters *v = EeUiVoters_Load(path.fileSystemRepresentation, on_progress, cx, NULL, err, sizeof(err));
      NSString *msg = [NSString stringWithUTF8String:err] ?: @"";
      dispatch_async(dispatch_get_main_queue(), ^{
        free(cx);
        self.loading = NO;
        self.progress.hidden = YES;
        if (v == NULL)
        {
            NSAlert *a = [[NSAlert alloc] init];
            a.messageText = [NSString stringWithFormat:@"%@ could not be opened.", path.lastPathComponent];
            a.informativeText = msg;
            [a beginSheetModalForWindow:self.window completionHandler:nil];
            self.status.stringValue = msg;
            return;
        }
        EeUiVoters_Free(self.voters);
        self.voters = v;
        [self rebuildColumns];
        [self.table reloadData];
        [self showStatus];
        [[NSDocumentController sharedDocumentController] noteNewRecentDocumentURL:[NSURL fileURLWithPath:path]];
      });
    });
}

/* NSTableViewDataSource / NSTableViewDelegate: the cells on demand, sorting by a heading */

- (NSInteger)numberOfRowsInTableView:(NSTableView *)tableView
{
    return EeUiVoters_Rows(self.voters);
}

- (id)tableView:(NSTableView *)tableView objectValueForTableColumn:(NSTableColumn *)column row:(NSInteger)row
{
    uint32_t c = (uint32_t)column.identifier.integerValue;
    return [NSString stringWithUTF8String:EeUiVoters_Cell(self.voters, (uint32_t)row, c)] ?: @"";
}

- (BOOL)tableView:(NSTableView *)tableView shouldEditTableColumn:(NSTableColumn *)column row:(NSInteger)row
{
    return NO;
}

- (void)tableView:(NSTableView *)tableView didClickTableColumn:(NSTableColumn *)column
{
    int asc = 1, sorted;
    if (self.voters == NULL || self.loading)
    {
        return;
    }
    EeUiVoters_Sort(self.voters, (uint32_t)column.identifier.integerValue);
    sorted = EeUiVoters_SortColumn(self.voters, &asc);
    for (NSTableColumn *col in tableView.tableColumns)
    {
        [tableView setIndicatorImage:(col.identifier.integerValue == sorted
                                          ? [NSImage imageNamed:asc ? @"NSAscendingSortIndicator"
                                                                    : @"NSDescendingSortIndicator"]
                                          : nil)
                       inTableColumn:col];
    }
    tableView.highlightedTableColumn = column;
    [tableView reloadData];
}

@end

/* -------------------------------------------------------------------------- */
/* The application                                                            */
/* -------------------------------------------------------------------------- */

@interface EeAppDelegate : NSObject <NSApplicationDelegate>
@property(strong) EeVoterWindow *main;
@property(copy) NSString *pending;  /* a file to open once launched */
@property(copy) NSString *snapshot; /* --snapshot: write the window here as PNG once loaded, then quit */
@end

@implementation EeAppDelegate

- (void)buildMenu
{
    NSMenu *bar = [[NSMenu alloc] init];
    NSMenuItem *appItem = [[NSMenuItem alloc] init], *fileItem = [[NSMenuItem alloc] init],
               *winItem = [[NSMenuItem alloc] init];
    NSMenu *app = [[NSMenu alloc] init], *file = [[NSMenu alloc] initWithTitle:@"File"],
           *win = [[NSMenu alloc] initWithTitle:@"Window"];
    [bar addItem:appItem];
    [bar addItem:fileItem];
    [bar addItem:winItem];
    [app addItemWithTitle:@"About Election Explorer" action:@selector(orderFrontStandardAboutPanel:) keyEquivalent:@""];
    [app addItem:[NSMenuItem separatorItem]];
    [app addItemWithTitle:@"Hide Election Explorer" action:@selector(hide:) keyEquivalent:@"h"];
    [app addItem:[NSMenuItem separatorItem]];
    [app addItemWithTitle:@"Quit Election Explorer" action:@selector(terminate:) keyEquivalent:@"q"];
    appItem.submenu = app;
    [file addItemWithTitle:@"Open Voter List…" action:@selector(openDocument:) keyEquivalent:@"o"];
    [file addItem:[NSMenuItem separatorItem]];
    [file addItemWithTitle:@"Close" action:@selector(performClose:) keyEquivalent:@"w"];
    fileItem.submenu = file;
    [win addItemWithTitle:@"Minimize" action:@selector(performMiniaturize:) keyEquivalent:@"m"];
    winItem.submenu = win;
    NSApp.mainMenu = bar;
    NSApp.windowsMenu = win;
}

- (void)applicationDidFinishLaunching:(NSNotification *)note
{
    [self buildMenu];
    self.main = [[EeVoterWindow alloc] init];
    if (self.pending != nil)
    {
        [self.main openPath:self.pending];
    }
    [NSApp activateIgnoringOtherApps:YES];
    if (self.snapshot != nil)
    {
        [self takeSnapshotWhenLoaded];
    }
}

/* --snapshot: once the list is loaded, draw the window's content into a PNG and quit (the
 * window draws itself; no screen recording permission is needed). */
- (void)takeSnapshotWhenLoaded
{
    if (self.main.loading || (self.pending != nil && self.main.voters == NULL && ![self.main.status.stringValue
                                                                                      containsString:@"could"]))
    {
        dispatch_after(dispatch_time(DISPATCH_TIME_NOW, 200 * NSEC_PER_MSEC), dispatch_get_main_queue(), ^{
          [self takeSnapshotWhenLoaded];
        });
        return;
    }
    dispatch_after(dispatch_time(DISPATCH_TIME_NOW, 500 * NSEC_PER_MSEC), dispatch_get_main_queue(), ^{
      NSView *v = self.main.window.contentView.superview ?: self.main.window.contentView;
      NSBitmapImageRep *rep = [v bitmapImageRepForCachingDisplayInRect:v.bounds];
      [v cacheDisplayInRect:v.bounds toBitmapImageRep:rep];
      [[rep representationUsingType:NSBitmapImageFileTypePNG properties:@{}] writeToFile:self.snapshot atomically:YES];
      [NSApp terminate:nil];
    });
}

- (BOOL)application:(NSApplication *)sender openFile:(NSString *)filename
{
    if (self.main == nil)
    {
        self.pending = filename;
    }
    else
    {
        [self.main.window makeKeyAndOrderFront:nil];
        [self.main openPath:filename];
    }
    return YES;
}

- (void)openDocument:(id)sender
{
    NSOpenPanel *panel = [NSOpenPanel openPanel];
    panel.allowsMultipleSelection = NO;
    panel.message = @"Choose a voter list (CSV, TSV, TXT or XLSX).";
    if (self.main == nil || !self.main.window.visible)
    {
        self.main = [[EeVoterWindow alloc] init];
    }
    [panel beginSheetModalForWindow:self.main.window
                  completionHandler:^(NSModalResponse r) {
                    if (r == NSModalResponseOK && panel.URL != nil)
                    {
                        [self.main openPath:panel.URL.path];
                    }
                  }];
}

- (BOOL)applicationShouldTerminateAfterLastWindowClosed:(NSApplication *)sender
{
    return YES;
}

@end

/* -------------------------------------------------------------------------- */
/* Self-test (no window)                                                      */
/* -------------------------------------------------------------------------- */

int main(int argc, const char **argv)
{
    @autoreleasepool
    {
        EeAppDelegate *delegate;
        if (argc >= 3 && strcmp(argv[1], "--selftest") == 0)
        {
            return EeUiVoters_SelfTest(argv[2]);
        }
        [NSApplication sharedApplication];
        NSApp.activationPolicy = NSApplicationActivationPolicyRegular;
        delegate = [[EeAppDelegate alloc] init];
        if (argc >= 4 && strcmp(argv[1], "--snapshot") == 0)
        {
            delegate.snapshot = [NSString stringWithUTF8String:argv[2]];
        }
        if (argc >= 2 && argv[argc - 1][0] != '-' && (delegate.snapshot == nil || argc >= 4))
        {
            delegate.pending = [NSString stringWithUTF8String:argv[argc - 1]];
        }
        NSApp.delegate = delegate;
        [NSApp run];
    }
    return 0;
}
