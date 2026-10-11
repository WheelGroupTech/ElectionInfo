/**
 * @file voter_roster.c
 * @brief Voter roster loader. See voter_roster.h and docs/voter-roster-design.md.
 *
 * Pipeline: expand the inputs into sources (ZIP entries / loose files) -> parse each
 * file name (date, voting method, "_Updated" correction flag) -> drop originals that
 * have a correction and file names repeated across inputs -> read every worksheet of
 * every remaining source into rows, find its header row, map its columns, validate
 * and split each row into a record -> drop whole-file copies of another voting method
 * -> emit the records, in date order, through the EeVoterTable builder.
 *
 * Travis County notes (from a survey of 12 elections): one workbook per day per
 * method named "MM.DD.YYYY <method>.xlsx"; 3-4 title rows above the header; primaries
 * have a Democrat and a Republican sheet; 9 header layouts; clerical errors include a
 * party sheet missing its header row, a misnamed copy of the Election Day roster,
 * pasted Voter IDs with no names, footer totals rows, and malformed Voter IDs.
 */
#include "voter_roster.h"

#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strsafe.h>

#include "csv_sheet.h"
#include "xlsx.h"
#include "third_party/miniz/miniz.h"

#define ROSTER_MAX_EXTRA  24 /* extra (non-canonical) columns kept */
#define ROSTER_HEADER_SCAN 15 /* rows searched for the header row */
#define ROSTER_COPY_MIN   100 /* smallest file considered for whole-file-copy detection */
#define ROSTER_LIST_MAX   8  /* file names listed per note line */

/* -------------------------------------------------------------------------- */
/* Labels                                                                     */
/* -------------------------------------------------------------------------- */

static const char *const k_MethodLabel[EE_VM_COUNT] = {
    "", "Mail Ballot", "Early Vote In-Person", "Election Day In-Person", "Provisional", "Limited"};

const char *EeRoster_MethodLabel(EeVotingMethod m)
{
    return ((int)m >= 0 && m < EE_VM_COUNT) ? k_MethodLabel[m] : "";
}

EeVotingMethod EeRoster_MethodFromLabel(const char *label)
{
    int i;
    if (label == NULL)
    {
        return EE_VM_NONE;
    }
    while (*label == ' ')
    {
        label++;
    }
    for (i = 1; i < EE_VM_COUNT; i++)
    {
        size_t n = strlen(k_MethodLabel[i]);
        if (_strnicmp(label, k_MethodLabel[i], n) == 0)
        {
            const char *rest = label + n;
            while (*rest == ' ')
            {
                rest++;
            }
            if (*rest == '\0')
            {
                return (EeVotingMethod)i;
            }
        }
    }
    return EE_VM_NONE;
}

/* -------------------------------------------------------------------------- */
/* Small utilities                                                            */
/* -------------------------------------------------------------------------- */

static void set_err(wchar_t *dst, size_t cch, const wchar_t *msg)
{
    if (dst != NULL && cch > 0)
    {
        StringCchCopyW(dst, cch, msg);
    }
}

static char lc(char c)
{
    return (c >= 'A' && c <= 'Z') ? (char)(c - 'A' + 'a') : c;
}

/* Case-insensitive substring search (ASCII fold). */
static BOOL contains_ci(const char *s, const char *needle)
{
    size_t n = strlen(needle);
    if (n == 0)
    {
        return TRUE;
    }
    for (; *s != '\0'; s++)
    {
        size_t i = 0;
        while (i < n && s[i] != '\0' && lc(s[i]) == lc(needle[i]))
        {
            i++;
        }
        if (i == n)
        {
            return TRUE;
        }
    }
    return FALSE;
}

static BOOL is_digit(char c)
{
    return c >= '0' && c <= '9';
}

static BOOL has_letter(const char *s)
{
    for (; *s != '\0'; s++)
    {
        unsigned char c = (unsigned char)*s;
        if ((c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || c >= 0x80)
        {
            return TRUE;
        }
    }
    return FALSE;
}

/* Copy @p s into @p out (cap) trimmed, with runs of whitespace collapsed to one space. */
static void clean_text(const char *s, char *out, size_t cap)
{
    size_t o = 0;
    BOOL space = FALSE;
    if (cap == 0)
    {
        return;
    }
    for (; *s != '\0' && o + 1 < cap; s++)
    {
        char c = *s;
        if (c == ' ' || c == '\t' || c == '\r' || c == '\n')
        {
            space = (o > 0);
            continue;
        }
        if (space && o + 2 < cap)
        {
            out[o++] = ' ';
        }
        space = FALSE;
        out[o++] = c;
    }
    out[o] = '\0';
}

/* A header cell as a matching label: first line only, text before "(" dropped,
 * lowercased, trimmed ("Name of Voter\n(Nombre del Votante)" -> "name of voter"). */
static void header_label(const char *s, char *out, size_t cap)
{
    char tmp[160];
    size_t i = 0;
    size_t o = 0;
    for (; s[i] != '\0' && s[i] != '\n' && s[i] != '\r' && s[i] != '(' && o + 1 < sizeof(tmp); i++)
    {
        tmp[o++] = lc(s[i]);
    }
    tmp[o] = '\0';
    clean_text(tmp, out, cap);
}

/* Parse a date "M[M]<sep>D[D]<sep>YYYY" or "YYYY-MM-DD" found anywhere in @p s;
 * returns yyyymmdd or 0. */
static uint32_t find_date(const char *s)
{
    size_t n = strlen(s);
    size_t i;
    for (i = 0; i < n; i++)
    {
        const char *p = s + i;
        unsigned a = 0;
        unsigned b = 0;
        unsigned y = 0;
        int k;
        if (!is_digit(*p) || (i > 0 && is_digit(s[i - 1])))
        {
            continue;
        }
        /* YYYY-MM-DD */
        if (n - i >= 10 && is_digit(p[1]) && is_digit(p[2]) && is_digit(p[3]) && p[4] == '-' &&
            is_digit(p[5]) && is_digit(p[6]) && p[7] == '-' && is_digit(p[8]) && is_digit(p[9]))
        {
            y = (unsigned)((p[0] - '0') * 1000 + (p[1] - '0') * 100 + (p[2] - '0') * 10 + (p[3] - '0'));
            a = (unsigned)((p[5] - '0') * 10 + (p[6] - '0'));
            b = (unsigned)((p[8] - '0') * 10 + (p[9] - '0'));
            if (y >= 1900 && y <= 2199 && a >= 1 && a <= 12 && b >= 1 && b <= 31)
            {
                return y * 10000u + a * 100u + b;
            }
            continue;
        }
        /* M[M] sep D[D] sep YYYY */
        for (k = 0; k < 2 && is_digit(*p); k++, p++)
        {
            a = a * 10u + (unsigned)(*p - '0');
        }
        if (is_digit(*p) || (*p != '.' && *p != '-' && *p != '/' && *p != '_'))
        {
            continue;
        }
        p++;
        for (k = 0; k < 2 && is_digit(*p); k++, p++)
        {
            b = b * 10u + (unsigned)(*p - '0');
        }
        if (k == 0 || is_digit(*p) || (*p != '.' && *p != '-' && *p != '/' && *p != '_'))
        {
            continue;
        }
        p++;
        for (k = 0; k < 4 && is_digit(*p); k++, p++)
        {
            y = y * 10u + (unsigned)(*p - '0');
        }
        if (k != 4 || is_digit(*p))
        {
            continue;
        }
        if (y >= 1900 && y <= 2199 && a >= 1 && a <= 12 && b >= 1 && b <= 31)
        {
            return y * 10000u + a * 100u + b;
        }
    }
    return 0;
}

/* An ISO date cell ("2024-10-21", optionally followed by a time) -> yyyymmdd, else 0. */
static uint32_t iso_date_cell(const char *s)
{
    while (*s == ' ')
    {
        s++;
    }
    if (strlen(s) >= 10 && s[4] == '-' && s[7] == '-')
    {
        return find_date(s);
    }
    return 0;
}

static EeVotingMethod method_from_text(const char *s)
{
    if (contains_ci(s, "provisional"))
        return EE_VM_PROVISIONAL;
    if (contains_ci(s, "limited"))
        return EE_VM_LIMITED;
    if (contains_ci(s, "mail"))
        return EE_VM_MAIL;
    if (contains_ci(s, "early"))
        return EE_VM_EARLY;
    if (contains_ci(s, "election day") || contains_ci(s, "electionday") ||
        contains_ci(s, "election_day"))
        return EE_VM_ELECTION_DAY;
    return EE_VM_NONE;
}

/* Party from a sheet name or title row ("Democrat", "Democratic Primary",
 * "Republican Primary Runoff"; exact "DEM"/"REP"). Not "Report". */
static const char *party_from_text(const char *s)
{
    char lab[64];
    header_label(s, lab, sizeof(lab));
    if (contains_ci(s, "democrat") || strcmp(lab, "dem") == 0)
        return "DEM";
    if (contains_ci(s, "republican") || strcmp(lab, "rep") == 0)
        return "REP";
    if (contains_ci(s, "libertarian"))
        return "LIB";
    return NULL;
}

/* Party from a "Party Ballot" cell (DEM / REP / D / R / Democratic ...). */
static void party_from_cell(const char *s, char *out, size_t cap)
{
    char t[64];
    const char *p;
    clean_text(s, t, sizeof(t));
    p = party_from_text(t);
    if (p == NULL && (strcmp(t, "D") == 0 || strcmp(t, "d") == 0))
        p = "DEM";
    if (p == NULL && (strcmp(t, "R") == 0 || strcmp(t, "r") == 0))
        p = "REP";
    if (p != NULL)
    {
        StringCchCopyA(out, cap, p);
        return;
    }
    StringCchCopyA(out, cap, t);
    CharUpperBuffA(out, (DWORD)strlen(out));
}

static BOOL is_name_suffix(const char *t)
{
    static const char *const k[] = {"JR", "JR.", "SR", "SR.", "II", "III", "IV", "V", "VI"};
    size_t i;
    for (i = 0; i < ARRAYSIZE(k); i++)
    {
        if (_stricmp(t, k[i]) == 0)
        {
            return TRUE;
        }
    }
    return FALSE;
}

/* -------------------------------------------------------------------------- */
/* String arena with interning                                                */
/* -------------------------------------------------------------------------- */

typedef struct Intern
{
    char *p;
    size_t len;
    size_t cap;
    uint32_t *slots; /* offset + 1; 0 = empty */
    uint32_t nslots;
    uint32_t count;
} Intern;

static uint32_t fnv1a(const char *s, size_t n)
{
    uint32_t h = 2166136261u;
    size_t i;
    for (i = 0; i < n; i++)
    {
        h ^= (unsigned char)s[i];
        h *= 16777619u;
    }
    return h;
}

static BOOL intern_init(Intern *t)
{
    ZeroMemory(t, sizeof(*t));
    t->cap = 1 << 16;
    t->p = (char *)malloc(t->cap);
    t->nslots = 1 << 14;
    t->slots = (uint32_t *)calloc(t->nslots, sizeof(uint32_t));
    if (t->p == NULL || t->slots == NULL)
    {
        return FALSE;
    }
    t->p[0] = '\0'; /* offset 0 is the empty string */
    t->len = 1;
    return TRUE;
}

static void intern_free(Intern *t)
{
    free(t->p);
    free(t->slots);
    ZeroMemory(t, sizeof(*t));
}

static const char *istr(const Intern *t, uint32_t off)
{
    return t->p + off;
}

static BOOL intern_grow(Intern *t)
{
    uint32_t ns = t->nslots * 2;
    uint32_t *slots = (uint32_t *)calloc(ns, sizeof(uint32_t));
    uint32_t i;
    if (slots == NULL)
    {
        return FALSE;
    }
    for (i = 0; i < t->nslots; i++)
    {
        if (t->slots[i] != 0)
        {
            const char *s = t->p + (t->slots[i] - 1);
            uint32_t j = fnv1a(s, strlen(s)) & (ns - 1);
            while (slots[j] != 0)
            {
                j = (j + 1) & (ns - 1);
            }
            slots[j] = t->slots[i];
        }
    }
    free(t->slots);
    t->slots = slots;
    t->nslots = ns;
    return TRUE;
}

/* Intern @p s; returns its offset (0 for ""), or UINT32_MAX on out of memory. */
static uint32_t intern(Intern *t, const char *s)
{
    size_t n = strlen(s);
    uint32_t h;
    uint32_t j;
    if (n == 0)
    {
        return 0;
    }
    if ((t->count + 1) * 2 > t->nslots && !intern_grow(t))
    {
        return UINT32_MAX;
    }
    h = fnv1a(s, n);
    for (j = h & (t->nslots - 1); t->slots[j] != 0; j = (j + 1) & (t->nslots - 1))
    {
        if (strcmp(t->p + (t->slots[j] - 1), s) == 0)
        {
            return t->slots[j] - 1;
        }
    }
    if (t->len + n + 1 > t->cap)
    {
        size_t nc = t->cap;
        char *np;
        while (t->len + n + 1 > nc)
        {
            nc *= 2;
        }
        if (nc > 0xFFFFFFF0u)
        {
            return UINT32_MAX;
        }
        np = (char *)realloc(t->p, nc);
        if (np == NULL)
        {
            return UINT32_MAX;
        }
        t->p = np;
        t->cap = nc;
    }
    memcpy(t->p + t->len, s, n + 1);
    t->slots[j] = (uint32_t)t->len + 1;
    t->count++;
    t->len += n + 1;
    return (uint32_t)(t->len - n - 1);
}

/* -------------------------------------------------------------------------- */
/* Wide string builder (the load note)                                        */
/* -------------------------------------------------------------------------- */

typedef struct WBuf
{
    wchar_t *p;
    size_t len;
    size_t cap;
} WBuf;

static void wb_append(WBuf *b, const wchar_t *s)
{
    size_t n = wcslen(s);
    if (b->len + n + 1 > b->cap)
    {
        size_t nc = b->cap ? b->cap : 512;
        wchar_t *np;
        while (b->len + n + 1 > nc)
        {
            nc *= 2;
        }
        np = (wchar_t *)realloc(b->p, nc * sizeof(wchar_t));
        if (np == NULL)
        {
            return;
        }
        b->p = np;
        b->cap = nc;
    }
    memcpy(b->p + b->len, s, (n + 1) * sizeof(wchar_t));
    b->len += n;
}

static void wb_printf(WBuf *b, const wchar_t *fmt, ...)
{
    wchar_t tmp[1024];
    va_list ap;
    va_start(ap, fmt);
    StringCchVPrintfW(tmp, ARRAYSIZE(tmp), fmt, ap);
    va_end(ap);
    wb_append(b, tmp);
}

/* "1,234,567" */
static void fmt_count(uint32_t v, wchar_t *out, size_t cch)
{
    wchar_t raw[16];
    size_t n;
    size_t o = 0;
    size_t i;
    StringCchPrintfW(raw, ARRAYSIZE(raw), L"%lu", (unsigned long)v);
    n = wcslen(raw);
    for (i = 0; i < n && o + 1 < cch; i++)
    {
        if (i > 0 && (n - i) % 3 == 0 && o + 1 < cch)
        {
            out[o++] = L',';
        }
        out[o++] = raw[i];
    }
    out[o] = L'\0';
}

/* -------------------------------------------------------------------------- */
/* Sources                                                                    */
/* -------------------------------------------------------------------------- */

typedef enum SrcKind
{
    SRC_XLSX,
    SRC_TEXT,
    SRC_PDF,
    SRC_OTHER
} SrcKind;

typedef struct RosterSource
{
    wchar_t name[MAX_PATH];  /* display name: file name or ZIP entry base name */
    char name8[MAX_PATH * 3]; /* the same, UTF-8 */
    char key[MAX_PATH * 3];   /* lowercase name without extension / "_updated" */
    const wchar_t *path;      /* the input path (the ZIP for an entry) */
    int zip;                  /* index into the open-ZIP list, -1 for a loose file */
    mz_uint entry;            /* ZIP entry index */
    uint64_t size;            /* uncompressed size (progress) */
    SrcKind kind;
    EeVotingMethod method;    /* from the file name */
    uint32_t date;            /* yyyymmdd from the file name, 0 unknown */
    BOOL updated;             /* "<name>_Updated" correction */
    int skip;                 /* SKIP_* */
    int copy_of;              /* source index this file duplicates (SKIP_COPY) */
    uint32_t rec_begin;
    uint32_t rec_end;
} RosterSource;

enum
{
    SKIP_NONE = 0,
    SKIP_SUPERSEDED,
    SKIP_REPEATED,
    SKIP_COPY,
    SKIP_UNSUPPORTED
};

typedef struct OpenZip
{
    FILE *fp;
    mz_zip_archive zip;
} OpenZip;

/* -------------------------------------------------------------------------- */
/* Records                                                                    */
/* -------------------------------------------------------------------------- */

typedef struct RosterRec
{
    uint32_t vuid;
    uint32_t pct;
    uint32_t last;
    uint32_t first;
    uint32_t middle;
    uint32_t suffix;
    uint32_t party;
    uint32_t source_name; /* interned "Source File" text */
    uint32_t xfirst;      /* index into extras */
    uint32_t date;        /* yyyymmdd, 0 unknown */
    uint16_t source;      /* source index */
    uint8_t method;
    uint8_t xcount;
} RosterRec;

typedef struct RosterExtra
{
    uint32_t col; /* extra-column index */
    uint32_t val; /* interned value */
} RosterExtra;

/* Column roles in one worksheet's header. */
typedef struct HeaderMap
{
    int vuid, pct, first, last, middle, suffix, full, party, notes, aname, aaddr, method, date,
        source;
    int ncols;
    int nx;
    int xcol[ROSTER_MAX_EXTRA];
    uint32_t xid[ROSTER_MAX_EXTRA];
    int nblank;
    int blank[8];
} HeaderMap;

/* Rows of one worksheet as read. */
typedef struct SheetRows
{
    char *pool;
    size_t pool_len;
    size_t pool_cap;
    uint32_t *cells; /* pool offsets */
    size_t ncells;
    size_t cells_cap;
    uint32_t *row_start;
    uint32_t *row_len;
    uint32_t nrows;
    uint32_t rows_cap;
} SheetRows;

typedef struct Loader
{
    Intern str;
    RosterSource *src;
    int nsrc;
    OpenZip *zips;
    int nzips;
    RosterRec *rec;
    uint32_t nrec;
    uint32_t rec_cap;
    RosterExtra *x;
    uint32_t nx;
    uint32_t x_cap;
    uint32_t xname[ROSTER_MAX_EXTRA]; /* interned extra-column titles */
    int nxname;
    HeaderMap last_map[17]; /* last header seen per column count (missing-header recovery) */
    BOOL have_last_map[17];
    EeRosterLoadInfo *info;
    WBuf recovered; /* names of sheets with a recovered header */
    WBuf skipped;   /* names of sheets that could not be read */
    WBuf odd_ids;   /* examples of malformed Voter IDs */
    uint32_t odd_examples;
    BOOL date_fix_noted;
    WBuf date_fixes;
    /* progress */
    volatile LONG *cancel;
    EeLoadProgressFn progress_fn;
    void *progress_user;
    uint64_t total_bytes;
    uint64_t done_bytes;
    uint64_t cur_size;
    uint32_t last_pct;
    BOOL oom;
    wchar_t *err;
    size_t errcch;
} Loader;

static BOOL cancelled(const Loader *L)
{
    return L->cancel != NULL && *L->cancel != 0;
}

static void report(Loader *L, uint64_t done)
{
    EeLoadProgress pr;
    uint32_t pct;
    if (L->progress_fn == NULL)
    {
        return;
    }
    pct = L->total_bytes ? (uint32_t)((done * 95u) / L->total_bytes) : 0u;
    if (pct > 95u)
    {
        pct = 95u;
    }
    if (pct == L->last_pct)
    {
        return;
    }
    L->last_pct = pct;
    ZeroMemory(&pr, sizeof(pr));
    pr.percent = pct;
    pr.rows_loaded = L->nrec;
    pr.bytes_read = done;
    pr.bytes_total = L->total_bytes;
    if (!L->progress_fn(&pr, L->progress_user) && L->cancel != NULL)
    {
        InterlockedExchange(L->cancel, 1);
    }
}

/* Progress from an inner read (one workbook / text file), scaled into the whole load. */
static BOOL inner_progress(const EeLoadProgress *pr, void *user)
{
    Loader *L = (Loader *)user;
    report(L, L->done_bytes + (L->cur_size * (pr->percent > 100 ? 100 : pr->percent)) / 100u);
    return !cancelled(L);
}

static uint32_t S(Loader *L, const char *s)
{
    uint32_t off = intern(&L->str, s);
    if (off == UINT32_MAX)
    {
        L->oom = TRUE;
        return 0;
    }
    return off;
}

/* Index of the extra column titled @p title (registered on first use), or -1. */
static int extra_col(Loader *L, const char *title)
{
    int i;
    uint32_t off;
    for (i = 0; i < L->nxname; i++)
    {
        if (_stricmp(istr(&L->str, L->xname[i]), title) == 0)
        {
            return i;
        }
    }
    if (L->nxname >= ROSTER_MAX_EXTRA)
    {
        return -1;
    }
    off = S(L, title);
    L->xname[L->nxname] = off;
    return L->nxname++;
}

/* -------------------------------------------------------------------------- */
/* Sheet reading                                                              */
/* -------------------------------------------------------------------------- */

static void sheet_reset(SheetRows *r)
{
    r->pool_len = 0;
    r->ncells = 0;
    r->nrows = 0;
}

static void sheet_free(SheetRows *r)
{
    free(r->pool);
    free(r->cells);
    free(r->row_start);
    free(r->row_len);
    ZeroMemory(r, sizeof(*r));
}

typedef struct SinkCtx
{
    Loader *L;
    SheetRows *rows;
    BOOL failed;
} SinkCtx;

static BOOL rows_sink(void *vctx, const char *const *cells, uint32_t ncells)
{
    SinkCtx *c = (SinkCtx *)vctx;
    SheetRows *r = c->rows;
    uint32_t i;
    if (r->nrows == r->rows_cap)
    {
        uint32_t nc = r->rows_cap ? r->rows_cap * 2 : 1024;
        uint32_t *a = (uint32_t *)realloc(r->row_start, nc * sizeof(uint32_t));
        uint32_t *b;
        if (a == NULL)
        {
            c->failed = TRUE;
            return FALSE;
        }
        r->row_start = a;
        b = (uint32_t *)realloc(r->row_len, nc * sizeof(uint32_t));
        if (b == NULL)
        {
            c->failed = TRUE;
            return FALSE;
        }
        r->row_len = b;
        r->rows_cap = nc;
    }
    /* Trim trailing empty cells so the row width reflects its data. */
    while (ncells > 0 && (cells[ncells - 1] == NULL || cells[ncells - 1][0] == '\0'))
    {
        ncells--;
    }
    if (r->ncells + ncells > r->cells_cap)
    {
        size_t nc = r->cells_cap ? r->cells_cap : 4096;
        uint32_t *a;
        while (r->ncells + ncells > nc)
        {
            nc *= 2;
        }
        a = (uint32_t *)realloc(r->cells, nc * sizeof(uint32_t));
        if (a == NULL)
        {
            c->failed = TRUE;
            return FALSE;
        }
        r->cells = a;
        r->cells_cap = nc;
    }
    r->row_start[r->nrows] = (uint32_t)r->ncells;
    r->row_len[r->nrows] = ncells;
    for (i = 0; i < ncells; i++)
    {
        const char *s = (cells[i] != NULL) ? cells[i] : "";
        size_t n = strlen(s);
        if (r->pool_len + n + 1 > r->pool_cap)
        {
            size_t nc = r->pool_cap ? r->pool_cap : 65536;
            char *p;
            while (r->pool_len + n + 1 > nc)
            {
                nc *= 2;
            }
            p = (char *)realloc(r->pool, nc);
            if (p == NULL)
            {
                c->failed = TRUE;
                return FALSE;
            }
            r->pool = p;
            r->pool_cap = nc;
        }
        memcpy(r->pool + r->pool_len, s, n + 1);
        r->cells[r->ncells++] = (uint32_t)r->pool_len;
        r->pool_len += n + 1;
    }
    r->nrows++;
    if ((r->nrows & 0xFFF) == 0 && cancelled(c->L))
    {
        return FALSE;
    }
    return TRUE;
}

static const char *cell_at(const SheetRows *r, uint32_t row, int col)
{
    if (row >= r->nrows || col < 0 || (uint32_t)col >= r->row_len[row])
    {
        return "";
    }
    return r->pool + r->cells[r->row_start[row] + (uint32_t)col];
}

static BOOL row_empty(const SheetRows *r, uint32_t row)
{
    return row >= r->nrows || r->row_len[row] == 0;
}

/* A Voter ID-like value: only digits, '_' and '-', with a run of at least 6 digits.
 * (The run rule keeps a title-row date such as "2024-02-27" from passing.) */
static BOOL vuid_like(const char *v)
{
    int run = 0;
    int best = 0;
    for (; *v != '\0'; v++)
    {
        if (is_digit(*v))
        {
            if (++run > best)
            {
                best = run;
            }
        }
        else if (*v == '_' || *v == '-')
        {
            run = 0;
        }
        else
        {
            return FALSE;
        }
    }
    return best >= 6;
}

static BOOL vuid_valid(const char *v)
{
    int i;
    for (i = 0; i < 10; i++)
    {
        if (!is_digit(v[i]))
        {
            return FALSE;
        }
    }
    return v[10] == '\0';
}

/* Clean a Voter ID / precinct cell: trim, drop a leading apostrophe and a ".0" tail. */
static void clean_id(const char *s, char *out, size_t cap)
{
    size_t n;
    clean_text(s, out, cap);
    if (out[0] == '\'')
    {
        memmove(out, out + 1, strlen(out));
    }
    n = strlen(out);
    if (n > 2 && out[n - 2] == '.' && out[n - 1] == '0')
    {
        out[n - 2] = '\0';
    }
}

/* -------------------------------------------------------------------------- */
/* Header mapping                                                             */
/* -------------------------------------------------------------------------- */

static void map_init(HeaderMap *m)
{
    ZeroMemory(m, sizeof(*m));
    m->vuid = m->pct = m->first = m->last = m->middle = m->suffix = m->full = m->party =
        m->notes = m->aname = m->aaddr = m->method = m->date = m->source = -1;
}

static BOOL row_is_header(const SheetRows *r, uint32_t row)
{
    uint32_t c;
    for (c = 0; c < r->row_len[row]; c++)
    {
        char lab[96];
        header_label(cell_at(r, row, (int)c), lab, sizeof(lab));
        if (contains_ci(lab, "vuid") || strcmp(lab, "voter id") == 0 || strcmp(lab, "voterid") == 0 ||
            contains_ci(lab, "name of voter"))
        {
            return TRUE;
        }
    }
    return FALSE;
}

/* Map one header row's columns to roles. Unknown titled columns become extras. */
static void map_header(Loader *L, const SheetRows *r, uint32_t row, HeaderMap *m)
{
    uint32_t c;
    BOOL limited_form = FALSE;
    map_init(m);
    for (c = 0; c < r->row_len[row]; c++)
    {
        char lab[96];
        header_label(cell_at(r, row, (int)c), lab, sizeof(lab));
        if (contains_ci(lab, "name of voter"))
        {
            limited_form = TRUE; /* "Name" / "Address" then describe the assisting person */
        }
    }
    m->ncols = (int)r->row_len[row];
    for (c = 0; c < r->row_len[row]; c++)
    {
        char lab[96];
        char title[160];
        char line[160];
        int ci = (int)c;
        StringCchCopyA(line, ARRAYSIZE(line), cell_at(r, row, ci));
        {
            char *nl = strpbrk(line, "\r\n");
            if (nl != NULL)
            {
                *nl = '\0';
            }
        }
        clean_text(line, title, sizeof(title));
        header_label(cell_at(r, row, ci), lab, sizeof(lab));
        if (lab[0] == '\0')
        {
            if (m->nblank < (int)ARRAYSIZE(m->blank))
            {
                m->blank[m->nblank++] = ci;
            }
        }
        else if (m->vuid < 0 && (contains_ci(lab, "vuid") || strcmp(lab, "voter id") == 0 ||
                                 strcmp(lab, "voterid") == 0))
            m->vuid = ci;
        else if (m->method < 0 && contains_ci(lab, "voting method"))
            m->method = ci;
        else if (m->date < 0 && contains_ci(lab, "date voted"))
            m->date = ci;
        else if (m->source < 0 && contains_ci(lab, "source file"))
            m->source = ci;
        else if (m->pct < 0 && (contains_ci(lab, "precinct") || strncmp(lab, "pct", 3) == 0))
            m->pct = ci;
        else if (m->first < 0 && contains_ci(lab, "first"))
            m->first = ci;
        else if (m->middle < 0 && contains_ci(lab, "middle"))
            m->middle = ci;
        else if (m->last < 0 && contains_ci(lab, "last"))
            m->last = ci;
        else if (m->suffix < 0 && contains_ci(lab, "suffix"))
            m->suffix = ci;
        else if (m->full < 0 && (contains_ci(lab, "name of voter") || strcmp(lab, "voter name") == 0 ||
                                 (!limited_form && strcmp(lab, "name") == 0)))
            m->full = ci;
        else if (limited_form && m->aname < 0 && strcmp(lab, "name") == 0)
            m->aname = ci;
        else if (limited_form && m->aaddr < 0 && strcmp(lab, "address") == 0)
            m->aaddr = ci;
        else if (m->party < 0 && contains_ci(lab, "party"))
            m->party = ci;
        else if (m->notes < 0 && (contains_ci(lab, "note") || contains_ci(lab, "comment")))
            m->notes = ci;
        else if (strcmp(lab, "no.") == 0 || strcmp(lab, "no") == 0)
        {
            /* row number column (Limited Ballot form) */
        }
        else if (m->nx < ROSTER_MAX_EXTRA)
        {
            int x = extra_col(L, title);
            if (x >= 0)
            {
                m->xcol[m->nx] = ci;
                m->xid[m->nx] = (uint32_t)x;
                m->nx++;
            }
        }
    }
}

/* Data width of a sheet (max row length over the first rows after @p from). */
static int data_width(const SheetRows *r, uint32_t from)
{
    uint32_t i;
    uint32_t w = 0;
    for (i = from; i < r->nrows && i < from + 40; i++)
    {
        if (r->row_len[i] > w)
        {
            w = r->row_len[i];
        }
    }
    return (int)w;
}

/* -------------------------------------------------------------------------- */
/* Records                                                                    */
/* -------------------------------------------------------------------------- */

static RosterRec *new_rec(Loader *L)
{
    if (L->nrec == L->rec_cap)
    {
        uint32_t nc = L->rec_cap ? L->rec_cap * 2 : 65536;
        RosterRec *p = (RosterRec *)realloc(L->rec, (size_t)nc * sizeof(RosterRec));
        if (p == NULL)
        {
            L->oom = TRUE;
            return NULL;
        }
        L->rec = p;
        L->rec_cap = nc;
    }
    ZeroMemory(&L->rec[L->nrec], sizeof(RosterRec));
    return &L->rec[L->nrec++];
}

static void add_extra(Loader *L, RosterRec *rec, int col, const char *value)
{
    char v[512];
    if (col < 0)
    {
        return;
    }
    clean_text(value, v, sizeof(v));
    if (v[0] == '\0' || rec->xcount == 255)
    {
        return;
    }
    if (L->nx == L->x_cap)
    {
        uint32_t nc = L->x_cap ? L->x_cap * 2 : 4096;
        RosterExtra *p = (RosterExtra *)realloc(L->x, (size_t)nc * sizeof(RosterExtra));
        if (p == NULL)
        {
            L->oom = TRUE;
            return;
        }
        L->x = p;
        L->x_cap = nc;
    }
    if (rec->xcount == 0)
    {
        rec->xfirst = L->nx;
    }
    L->x[L->nx].col = (uint32_t)col;
    L->x[L->nx].val = S(L, v);
    L->nx++;
    rec->xcount++;
}

/* Split a combined name: "LAST,FIRST MIDDLE" or "FIRST MIDDLE LAST [SUFFIX]". */
static void split_full_name(const char *full, char *last, char *first, char *middle, char *suffix,
                            size_t cap)
{
    char buf[256];
    char *tok[16];
    int nt = 0;
    char *comma;
    char *p;
    last[0] = first[0] = middle[0] = suffix[0] = '\0';
    clean_text(full, buf, sizeof(buf));
    comma = strchr(buf, ',');
    if (comma != NULL)
    {
        *comma = '\0';
        clean_text(buf, last, cap);
        p = comma + 1;
    }
    else
    {
        p = buf;
    }
    {
        char *ctx = NULL;
        char *t = strtok_s(p, " ", &ctx);
        while (t != NULL && nt < (int)ARRAYSIZE(tok))
        {
            tok[nt++] = t;
            t = strtok_s(NULL, " ", &ctx);
        }
    }
    if (nt > 0 && is_name_suffix(tok[nt - 1]) && (comma != NULL || nt > 2))
    {
        StringCchCopyA(suffix, cap, tok[nt - 1]);
        nt--;
    }
    if (comma == NULL)
    {
        if (nt == 0)
        {
            return;
        }
        StringCchCopyA(last, cap, tok[nt - 1]);
        nt--;
    }
    if (nt > 0)
    {
        int i;
        StringCchCopyA(first, cap, tok[0]);
        for (i = 1; i < nt; i++)
        {
            if (i > 1)
            {
                StringCchCatA(middle, cap, " ");
            }
            StringCchCatA(middle, cap, tok[i]);
        }
    }
}

/* Turn one data row into a record (or count why it is skipped). */
static void emit_row(Loader *L, int si, const SheetRows *r, uint32_t row, const HeaderMap *m,
                     EeVotingMethod sheet_method, uint32_t sheet_date, const char *sheet_party)
{
    char vuid[64], pct[32], last[128], first[128], middle[128], suffix[32], party[32];
    BOOL named;
    RosterRec *rec;
    EeVotingMethod method = sheet_method;
    uint32_t date = sheet_date;
    int i;

    if (row_empty(r, row))
    {
        return;
    }
    clean_id(cell_at(r, row, m->vuid), vuid, sizeof(vuid));
    clean_id(cell_at(r, row, m->pct), pct, sizeof(pct));
    if (m->full >= 0)
    {
        split_full_name(cell_at(r, row, m->full), last, first, middle, suffix, sizeof(last));
    }
    else
    {
        last[0] = first[0] = middle[0] = suffix[0] = '\0';
    }
    if (m->last >= 0)
        clean_text(cell_at(r, row, m->last), last, sizeof(last));
    if (m->first >= 0)
        clean_text(cell_at(r, row, m->first), first, sizeof(first));
    if (m->middle >= 0)
        clean_text(cell_at(r, row, m->middle), middle, sizeof(middle));
    if (m->suffix >= 0)
        clean_text(cell_at(r, row, m->suffix), suffix, sizeof(suffix));
    named = has_letter(last) || has_letter(first);

    if (m->vuid >= 0)
    {
        if (vuid[0] == '\0')
        {
            if (!named)
            {
                /* blank or row-number-only row: ignore; anything else is a non-voter row */
                for (i = 0; i < (int)r->row_len[row]; i++)
                {
                    if (has_letter(cell_at(r, row, i)))
                    {
                        L->info->rows_non_voter++;
                        break;
                    }
                }
                return;
            }
        }
        else if (!vuid_like(vuid))
        {
            L->info->rows_non_voter++; /* "Total Voters", "No ballots received.", ... */
            return;
        }
        else if (!named && pct[0] == '\0')
        {
            L->info->rows_vuid_only++; /* pasted Voter IDs with nothing else */
            return;
        }
        else if (!named && !has_letter(pct))
        {
            /* a Voter ID and precinct but no name: still a voter row */
        }
    }
    else if (!named)
    {
        return; /* Limited Ballot form: unused numbered lines */
    }

    /* Canonical export columns override the sheet-level method / date. */
    if (m->method >= 0)
    {
        EeVotingMethod mm = EeRoster_MethodFromLabel(cell_at(r, row, m->method));
        if (mm != EE_VM_NONE)
        {
            method = mm;
        }
    }
    if (m->date >= 0)
    {
        uint32_t d = find_date(cell_at(r, row, m->date));
        date = d; /* blank in the export stays blank */
    }

    rec = new_rec(L);
    if (rec == NULL)
    {
        return;
    }
    rec->vuid = S(L, vuid);
    rec->pct = S(L, pct);
    rec->last = S(L, last);
    rec->first = S(L, first);
    rec->middle = S(L, middle);
    rec->suffix = S(L, suffix);
    if (m->party >= 0 && cell_at(r, row, m->party)[0] != '\0')
    {
        party_from_cell(cell_at(r, row, m->party), party, sizeof(party));
        rec->party = S(L, party);
    }
    else if (sheet_party != NULL)
    {
        rec->party = S(L, sheet_party);
    }
    if (m->source >= 0 && cell_at(r, row, m->source)[0] != '\0')
    {
        char sf[MAX_PATH];
        clean_text(cell_at(r, row, m->source), sf, sizeof(sf));
        rec->source_name = S(L, sf);
    }
    else
    {
        rec->source_name = S(L, L->src[si].name8);
    }
    rec->date = date;
    rec->method = (uint8_t)method;
    rec->source = (uint16_t)si;

    /* Notes: a "Notes" column, else the first unlabeled column with a value. */
    if (m->notes >= 0)
    {
        add_extra(L, rec, extra_col(L, "Notes"), cell_at(r, row, m->notes));
    }
    else
    {
        BOOL done_notes = FALSE;
        for (i = 0; i < m->nblank && !done_notes; i++)
        {
            if (cell_at(r, row, m->blank[i])[0] != '\0')
            {
                add_extra(L, rec, extra_col(L, "Notes"), cell_at(r, row, m->blank[i]));
                done_notes = TRUE;
            }
        }
        /* A column with no title past the header's width (the trailing blank title cell
         * is trimmed when read): G24/G25 mail rosters put "CHAPTER 102" notes there. */
        for (i = m->ncols; i < (int)r->row_len[row] && !done_notes; i++)
        {
            if (cell_at(r, row, i)[0] != '\0')
            {
                add_extra(L, rec, extra_col(L, "Notes"), cell_at(r, row, i));
                done_notes = TRUE;
            }
        }
    }
    if (m->aname >= 0)
        add_extra(L, rec, extra_col(L, "Assisting Person"), cell_at(r, row, m->aname));
    if (m->aaddr >= 0)
        add_extra(L, rec, extra_col(L, "Assisting Address"), cell_at(r, row, m->aaddr));
    for (i = 0; i < m->nx; i++)
    {
        add_extra(L, rec, (int)m->xid[i], cell_at(r, row, m->xcol[i]));
    }

    if (m->vuid >= 0 && vuid[0] == '\0')
    {
        L->info->vuids_missing++; /* a named voter whose Voter ID cell is blank */
    }
    if (vuid[0] != '\0' && !vuid_valid(vuid))
    {
        L->info->vuids_malformed++;
        if (L->odd_examples < 3)
        {
            wchar_t w[64];
            MultiByteToWideChar(CP_UTF8, 0, vuid, -1, w, ARRAYSIZE(w));
            wb_printf(&L->odd_ids, L"%s%s", L->odd_examples ? L", " : L"", w);
            L->odd_examples++;
        }
    }
}

static void append_name(WBuf *b, const wchar_t *name, const char *sheet)
{
    wchar_t ws[128] = L"";
    if (sheet != NULL && sheet[0] != '\0')
    {
        MultiByteToWideChar(CP_UTF8, 0, sheet, -1, ws, ARRAYSIZE(ws));
    }
    if (b->len > 0)
    {
        wb_append(b, L"; ");
    }
    wb_append(b, name);
    if (ws[0] != L'\0')
    {
        wb_printf(b, L" (%s)", ws);
    }
}

/* Analyze one worksheet's rows and emit its records. @p wb_map / @p wb_have carry the
 * workbook's previous header so a sibling sheet missing its header can borrow it. */
static void process_sheet(Loader *L, int si, const char *sheet_name, const SheetRows *r,
                          HeaderMap *wb_map, BOOL *wb_have)
{
    RosterSource *s = &L->src[si];
    HeaderMap m;
    uint32_t h = UINT32_MAX;
    uint32_t data_from;
    uint32_t row;
    uint32_t title_date = 0;
    EeVotingMethod title_method = EE_VM_NONE;
    const char *party = NULL;
    EeVotingMethod method;
    uint32_t date;

    for (row = 0; row < r->nrows && row < ROSTER_HEADER_SCAN; row++)
    {
        if (row_is_header(r, row))
        {
            h = row;
            break;
        }
    }

    if (h != UINT32_MAX)
    {
        map_header(L, r, h, &m);
        data_from = h + 1;
        *wb_map = m;
        *wb_have = TRUE;
        if (m.ncols >= 1 && m.ncols <= 16)
        {
            L->last_map[m.ncols] = m;
            L->have_last_map[m.ncols] = TRUE;
        }
    }
    else
    {
        /* No header row: borrow the workbook's other sheet's header, else the most
         * recent header with the same column count; data starts at the first row whose
         * Voter ID cell looks like one. */
        int w = data_width(r, 0);
        const HeaderMap *use = NULL;
        if (*wb_have && wb_map->ncols == w)
        {
            use = wb_map;
        }
        else if (w >= 1 && w <= 16 && L->have_last_map[w])
        {
            use = &L->last_map[w];
        }
        if (use == NULL || use->vuid < 0)
        {
            L->info->skipped_sheets++;
            append_name(&L->skipped, s->name, sheet_name);
            return;
        }
        m = *use;
        for (data_from = 0; data_from < r->nrows && data_from < ROSTER_HEADER_SCAN; data_from++)
        {
            char v[64];
            clean_id(cell_at(r, data_from, m.vuid), v, sizeof(v));
            if (vuid_like(v))
            {
                break;
            }
        }
        h = data_from; /* title rows are the rows before the data */
        L->info->recovered_sheets++;
        append_name(&L->recovered, s->name, sheet_name);
    }

    /* Title rows: roster date, method words, and the party of a primary sheet. */
    for (row = 0; row < h && row < r->nrows; row++)
    {
        uint32_t c;
        for (c = 0; c < r->row_len[row]; c++)
        {
            const char *t = cell_at(r, row, (int)c);
            if (title_date == 0)
            {
                title_date = iso_date_cell(t);
            }
            if (title_method == EE_VM_NONE)
            {
                title_method = method_from_text(t);
            }
            if (party == NULL)
            {
                party = party_from_text(t);
            }
        }
    }
    if (sheet_name != NULL && party_from_text(sheet_name) != NULL)
    {
        party = party_from_text(sheet_name);
    }

    method = (s->method != EE_VM_NONE) ? s->method : title_method;
    date = s->date;
    if (date == 0)
    {
        date = title_date;
    }
    else if (title_date != 0 && date / 10000u != title_date / 10000u)
    {
        /* The file name's year disagrees with the sheet's own date: trust the sheet. */
        if (L->date_fixes.len > 0)
        {
            wb_append(&L->date_fixes, L"; ");
        }
        wb_append(&L->date_fixes, s->name);
        date = title_date;
    }
    if (method != EE_VM_NONE)
    {
        L->info->method_source[method] = TRUE;
        L->info->method_read[method] = TRUE;
    }

    for (row = data_from; row < r->nrows; row++)
    {
        emit_row(L, si, r, row, &m, method, date, party);
        if (L->oom)
        {
            return;
        }
    }
    L->info->sheets_read++;
}

/* -------------------------------------------------------------------------- */
/* Input expansion                                                            */
/* -------------------------------------------------------------------------- */

static const wchar_t *path_ext(const wchar_t *p)
{
    const wchar_t *dot = wcsrchr(p, L'.');
    const wchar_t *sep = wcspbrk(p, L"\\/");
    const wchar_t *last_sep = NULL;
    while (sep != NULL)
    {
        last_sep = sep;
        sep = wcspbrk(sep + 1, L"\\/");
    }
    return (dot != NULL && (last_sep == NULL || dot > last_sep)) ? dot : L"";
}

static const wchar_t *base_name(const wchar_t *p)
{
    const wchar_t *b = p;
    const wchar_t *q;
    for (q = p; *q != L'\0'; q++)
    {
        if (*q == L'\\' || *q == L'/')
        {
            b = q + 1;
        }
    }
    return b;
}

static SrcKind kind_from_ext(const wchar_t *ext)
{
    if (_wcsicmp(ext, L".xlsx") == 0)
        return SRC_XLSX;
    if (_wcsicmp(ext, L".csv") == 0 || _wcsicmp(ext, L".tsv") == 0 || _wcsicmp(ext, L".txt") == 0)
        return SRC_TEXT;
    if (_wcsicmp(ext, L".pdf") == 0)
        return SRC_PDF;
    return SRC_OTHER;
}

/* Fill name / key / date / method / updated from the display name. */
static void parse_source_name(RosterSource *s)
{
    char low[MAX_PATH * 3];
    char *dot;
    char *u;
    size_t i;
    WideCharToMultiByte(CP_UTF8, 0, s->name, -1, s->name8, (int)sizeof(s->name8), NULL, NULL);
    s->date = find_date(s->name8);
    s->method = method_from_text(s->name8);
    StringCchCopyA(low, ARRAYSIZE(low), s->name8);
    for (i = 0; low[i] != '\0'; i++)
    {
        low[i] = lc(low[i]);
    }
    dot = strrchr(low, '.');
    if (dot != NULL)
    {
        *dot = '\0';
    }
    /* "<name>_Updated" / " Updated" / "-Updated" / "(Updated)" marks a correction. */
    u = strstr(low, "updated");
    s->updated = FALSE;
    if (u != NULL && u > low &&
        (u[-1] == '_' || u[-1] == ' ' || u[-1] == '-' || u[-1] == '('))
    {
        char *cut = u - 1;
        s->updated = TRUE;
        while (cut > low && (cut[-1] == ' ' || cut[-1] == '_' || cut[-1] == '-'))
        {
            cut--;
        }
        *cut = '\0';
    }
    clean_text(low, s->key, sizeof(s->key));
}

static RosterSource *add_source(Loader *L, const wchar_t *path, const wchar_t *display, int zip,
                                mz_uint entry, uint64_t size)
{
    RosterSource *s;
    RosterSource *p = (RosterSource *)realloc(L->src, (size_t)(L->nsrc + 1) * sizeof(RosterSource));
    if (p == NULL)
    {
        L->oom = TRUE;
        return NULL;
    }
    L->src = p;
    s = &L->src[L->nsrc++];
    ZeroMemory(s, sizeof(*s));
    StringCchCopyW(s->name, ARRAYSIZE(s->name), display);
    s->path = path;
    s->zip = zip;
    s->entry = entry;
    s->size = size;
    s->kind = kind_from_ext(path_ext(display));
    s->copy_of = -1;
    parse_source_name(s);
    return s;
}

static BOOL expand_inputs(Loader *L, const wchar_t *const *paths, int count)
{
    int i;
    for (i = 0; i < count; i++)
    {
        const wchar_t *p = paths[i];
        if (_wcsicmp(path_ext(p), L".zip") == 0)
        {
            OpenZip *z;
            OpenZip *nz = (OpenZip *)realloc(L->zips, (size_t)(L->nzips + 1) * sizeof(OpenZip));
            __int64 fsize;
            mz_uint e;
            mz_uint n;
            if (nz == NULL)
            {
                L->oom = TRUE;
                return FALSE;
            }
            L->zips = nz;
            z = &L->zips[L->nzips];
            ZeroMemory(z, sizeof(*z));
            if (_wfopen_s(&z->fp, p, L"rb") != 0 || z->fp == NULL)
            {
                wchar_t msg[MAX_PATH + 64];
                StringCchPrintfW(msg, ARRAYSIZE(msg), L"Could not open %s.", base_name(p));
                set_err(L->err, L->errcch, msg);
                return FALSE;
            }
            _fseeki64(z->fp, 0, SEEK_END);
            fsize = _ftelli64(z->fp);
            _fseeki64(z->fp, 0, SEEK_SET);
            mz_zip_zero_struct(&z->zip);
            if (fsize <= 0 || !mz_zip_reader_init_cfile(&z->zip, z->fp, (mz_uint64)fsize, 0))
            {
                wchar_t msg[MAX_PATH + 64];
                fclose(z->fp);
                StringCchPrintfW(msg, ARRAYSIZE(msg), L"%s is not a valid ZIP file.", base_name(p));
                set_err(L->err, L->errcch, msg);
                return FALSE;
            }
            L->nzips++;
            n = mz_zip_reader_get_num_files(&z->zip);
            for (e = 0; e < n; e++)
            {
                mz_zip_archive_file_stat st;
                wchar_t wname[MAX_PATH];
                const wchar_t *b;
                if (!mz_zip_reader_file_stat(&z->zip, e, &st) || mz_zip_reader_is_file_a_directory(&z->zip, e))
                {
                    continue;
                }
                if (strncmp(st.m_filename, "__MACOSX/", 9) == 0)
                {
                    continue;
                }
                if (MultiByteToWideChar(CP_UTF8, MB_ERR_INVALID_CHARS, st.m_filename, -1, wname,
                                        ARRAYSIZE(wname)) == 0)
                {
                    MultiByteToWideChar(CP_ACP, 0, st.m_filename, -1, wname, ARRAYSIZE(wname));
                }
                b = base_name(wname);
                if (b[0] == L'\0' || b[0] == L'.' || (b[0] == L'~' && b[1] == L'$'))
                {
                    continue;
                }
                if (add_source(L, p, b, L->nzips - 1, e, st.m_uncomp_size) == NULL)
                {
                    return FALSE;
                }
            }
        }
        else
        {
            WIN32_FILE_ATTRIBUTE_DATA fa;
            uint64_t size = 0;
            if (GetFileAttributesExW(p, GetFileExInfoStandard, &fa))
            {
                size = ((uint64_t)fa.nFileSizeHigh << 32) | fa.nFileSizeLow;
            }
            if (add_source(L, p, base_name(p), -1, 0, size) == NULL)
            {
                return FALSE;
            }
        }
    }
    return TRUE;
}

/* Originals with a correction, and file names repeated across inputs, are skipped. */
static void mark_superseded(Loader *L)
{
    int i;
    int j;
    for (i = 0; i < L->nsrc; i++)
    {
        RosterSource *a = &L->src[i];
        if (a->kind == SRC_PDF || a->kind == SRC_OTHER || (a->kind == SRC_TEXT && a->zip >= 0))
        {
            a->skip = SKIP_UNSUPPORTED;
            if (a->method != EE_VM_NONE)
            {
                L->info->method_source[a->method] = TRUE;
            }
        }
    }
    for (i = 0; i < L->nsrc; i++)
    {
        RosterSource *a = &L->src[i];
        if (a->skip != SKIP_NONE)
        {
            continue;
        }
        for (j = 0; j < L->nsrc; j++)
        {
            RosterSource *b = &L->src[j];
            if (j == i || b->skip == SKIP_UNSUPPORTED || strcmp(a->key, b->key) != 0)
            {
                continue;
            }
            if (!a->updated && b->updated)
            {
                a->skip = SKIP_SUPERSEDED;
                break;
            }
            if (a->updated == b->updated && _wcsicmp(a->name, b->name) == 0 && j > i)
            {
                a->skip = SKIP_REPEATED; /* the later input's copy is used */
                break;
            }
        }
    }
}

static int __cdecl src_order_cmp(void *ctx, const void *pa, const void *pb)
{
    const Loader *L = (const Loader *)ctx;
    const RosterSource *a = &L->src[*(const int *)pa];
    const RosterSource *b = &L->src[*(const int *)pb];
    uint32_t da = a->date ? a->date : 99999999u;
    uint32_t db = b->date ? b->date : 99999999u;
    if (da != db)
        return (da < db) ? -1 : 1;
    if (a->method != b->method)
        return (a->method < b->method) ? -1 : 1;
    return _wcsicmp(a->name, b->name);
}

/* Read a whole loose file into memory. */
static BOOL read_file(const wchar_t *path, unsigned char **out, size_t *out_size)
{
    HANDLE h = CreateFileW(path, GENERIC_READ, FILE_SHARE_READ, NULL, OPEN_EXISTING,
                           FILE_FLAG_SEQUENTIAL_SCAN, NULL);
    LARGE_INTEGER sz;
    unsigned char *buf;
    size_t total = 0;
    *out = NULL;
    *out_size = 0;
    if (h == INVALID_HANDLE_VALUE)
    {
        return FALSE;
    }
    if (!GetFileSizeEx(h, &sz) || sz.QuadPart <= 0 || sz.QuadPart > (LONGLONG)0x7FFFFFFF)
    {
        CloseHandle(h);
        return FALSE;
    }
    buf = (unsigned char *)malloc((size_t)sz.QuadPart);
    if (buf == NULL)
    {
        CloseHandle(h);
        return FALSE;
    }
    while (total < (size_t)sz.QuadPart)
    {
        DWORD got = 0;
        DWORD want = (DWORD)min((size_t)sz.QuadPart - total, (size_t)(1 << 26));
        if (!ReadFile(h, buf + total, want, &got, NULL) || got == 0)
        {
            free(buf);
            CloseHandle(h);
            return FALSE;
        }
        total += got;
    }
    CloseHandle(h);
    *out = buf;
    *out_size = total;
    return TRUE;
}

/* Read every worksheet of one source and emit its records. */
static EeLoadStatus read_source(Loader *L, int si, SheetRows *rows)
{
    RosterSource *s = &L->src[si];
    SinkCtx sc;
    EeLoadStatus st = EeLoadStatus_Ok;
    HeaderMap wb_map;
    BOOL wb_have = FALSE;
    wchar_t err[256] = L"";

    ZeroMemory(&sc, sizeof(sc));
    sc.L = L;
    sc.rows = rows;
    map_init(&wb_map);
    s->rec_begin = L->nrec;
    L->cur_size = s->size;

    if (s->kind == SRC_TEXT)
    {
        sheet_reset(rows);
        st = EeCsv_ReadSheet(s->path, rows_sink, &sc, L->cancel, inner_progress, L, err, ARRAYSIZE(err));
        if (st == EeLoadStatus_Ok && !sc.failed)
        {
            process_sheet(L, si, "", rows, &wb_map, &wb_have);
        }
    }
    else
    {
        unsigned char *data = NULL;
        size_t size = 0;
        wchar_t names[EE_XLSX_MAX_SHEETS][EE_XLSX_SHEET_NAME_CCH];
        int nsheets = 0;
        int k;
        if (s->zip >= 0)
        {
            data = (unsigned char *)mz_zip_reader_extract_to_heap(&L->zips[s->zip].zip, s->entry, &size, 0);
        }
        else if (!read_file(s->path, &data, &size))
        {
            data = NULL;
        }
        if (data == NULL)
        {
            L->info->skipped_sheets++;
            append_name(&L->skipped, s->name, NULL);
            s->rec_end = L->nrec;
            return EeLoadStatus_Ok;
        }
        st = EeXlsx_ListSheetsMem(data, size, names, EE_XLSX_MAX_SHEETS, &nsheets, err, ARRAYSIZE(err));
        for (k = 0; st == EeLoadStatus_Ok && k < nsheets; k++)
        {
            char sheet8[EE_XLSX_SHEET_NAME_CCH * 3];
            WideCharToMultiByte(CP_UTF8, 0, names[k], -1, sheet8, (int)sizeof(sheet8), NULL, NULL);
            sheet_reset(rows);
            st = EeXlsx_ReadSheetMem(data, size, k, rows_sink, &sc, L->cancel, inner_progress, L, err,
                                     ARRAYSIZE(err));
            if (st == EeLoadStatus_Ok && !sc.failed)
            {
                process_sheet(L, si, sheet8, rows, &wb_map, &wb_have);
            }
        }
        if (s->zip >= 0)
        {
            mz_free(data);
        }
        else
        {
            free(data);
        }
        if (st == EeLoadStatus_Error)
        {
            /* An unreadable workbook is reported, not fatal to the whole load. */
            L->info->skipped_sheets++;
            append_name(&L->skipped, s->name, NULL);
            st = EeLoadStatus_Ok;
        }
    }
    if (sc.failed)
    {
        L->oom = TRUE;
    }
    if (st == EeLoadStatus_Ok && cancelled(L))
    {
        st = EeLoadStatus_Cancelled;
    }
    s->rec_end = L->nrec;
    L->info->files_read++;
    return st;
}

/* -------------------------------------------------------------------------- */
/* Whole-file copies and duplicates                                           */
/* -------------------------------------------------------------------------- */

typedef struct VuidIndex
{
    uint32_t *head;  /* slot -> first record + 1 */
    uint32_t nslots;
    uint32_t *next;  /* record -> next record with the same Voter ID + 1 */
} VuidIndex;

static BOOL build_vuid_index(Loader *L, VuidIndex *ix)
{
    uint32_t i;
    uint32_t ns = 1024;
    while (ns < L->nrec * 2u)
    {
        ns *= 2;
    }
    ix->nslots = ns;
    ix->head = (uint32_t *)calloc(ns, sizeof(uint32_t));
    ix->next = (uint32_t *)calloc(L->nrec ? L->nrec : 1, sizeof(uint32_t));
    if (ix->head == NULL || ix->next == NULL)
    {
        return FALSE;
    }
    /* Insert in reverse so each chain runs in record order. */
    for (i = L->nrec; i-- > 0;)
    {
        uint32_t v = L->rec[i].vuid;
        uint32_t j;
        if (v == 0)
        {
            continue;
        }
        for (j = (v * 2654435761u) & (ns - 1);; j = (j + 1) & (ns - 1))
        {
            uint32_t h = ix->head[j];
            if (h == 0)
            {
                ix->head[j] = i + 1;
                break;
            }
            if (L->rec[h - 1].vuid == v)
            {
                ix->next[i] = h;
                ix->head[j] = i + 1;
                break;
            }
        }
    }
    return TRUE;
}

static void free_vuid_index(VuidIndex *ix)
{
    free(ix->head);
    free(ix->next);
    ZeroMemory(ix, sizeof(*ix));
}

static uint32_t vuid_head(const Loader *L, const VuidIndex *ix, uint32_t v)
{
    uint32_t j;
    for (j = (v * 2654435761u) & (ix->nslots - 1);; j = (j + 1) & (ix->nslots - 1))
    {
        uint32_t h = ix->head[j];
        if (h == 0 || L->rec[h - 1].vuid == v)
        {
            return h;
        }
    }
}

/* Count, for source @p a, how many of its records' Voter IDs appear in source @p b. */
static uint32_t overlap(const Loader *L, const VuidIndex *ix, int a, int b)
{
    const RosterSource *sa = &L->src[a];
    uint32_t i;
    uint32_t n = 0;
    for (i = sa->rec_begin; i < sa->rec_end; i++)
    {
        uint32_t v = L->rec[i].vuid;
        uint32_t h;
        if (v == 0)
        {
            continue;
        }
        for (h = vuid_head(L, ix, v); h != 0; h = ix->next[h - 1])
        {
            if (L->rec[h - 1].source == (uint16_t)b)
            {
                n++;
                break;
            }
        }
    }
    return n;
}

/* D1: a file whose voters ALL appear in one other file of a different voting method is
 * a misnamed copy (Travis P24 "03.05.2024 Early Vote.xlsx" = the Election Day roster). */
static void mark_copies(Loader *L, const VuidIndex *ix)
{
    int a;
    for (a = 0; a < L->nsrc; a++)
    {
        RosterSource *sa = &L->src[a];
        uint32_t na = sa->rec_end - sa->rec_begin;
        uint32_t i;
        int cand = -1;
        if (sa->skip != SKIP_NONE || na < ROSTER_COPY_MIN)
        {
            continue;
        }
        /* Candidate: the other source holding the first record's Voter ID. */
        for (i = sa->rec_begin; i < sa->rec_end && cand < 0; i++)
        {
            uint32_t h;
            if (L->rec[i].vuid == 0)
            {
                continue;
            }
            for (h = vuid_head(L, ix, L->rec[i].vuid); h != 0; h = ix->next[h - 1])
            {
                int b = (int)L->rec[h - 1].source;
                if (b != a && L->src[b].skip == SKIP_NONE && L->src[b].method != sa->method)
                {
                    cand = b;
                    break;
                }
            }
            break;
        }
        if (cand < 0 || overlap(L, ix, a, cand) != na)
        {
            continue;
        }
        {
            RosterSource *sb = &L->src[cand];
            uint32_t nb = sb->rec_end - sb->rec_begin;
            BOOL mutual = (overlap(L, ix, cand, a) == nb);
            BOOL drop_a = TRUE;
            if (mutual)
            {
                /* Same voters both ways: early voting cannot be on Election Day, so a
                 * pair of Early Vote + Election Day files keeps the Election Day one;
                 * otherwise the later file is the copy. */
                if (sa->method == EE_VM_ELECTION_DAY && sb->method == EE_VM_EARLY)
                    drop_a = FALSE;
                else if (!(sa->method == EE_VM_EARLY && sb->method == EE_VM_ELECTION_DAY))
                    drop_a = (a > cand);
            }
            if (drop_a)
            {
                sa->skip = SKIP_COPY;
                sa->copy_of = cand;
            }
            else
            {
                sb->skip = SKIP_COPY;
                sb->copy_of = a;
            }
        }
    }
}

/* -------------------------------------------------------------------------- */
/* Note                                                                       */
/* -------------------------------------------------------------------------- */

static void note_file_list(Loader *L, WBuf *b, int skip, const wchar_t *label)
{
    int i;
    int n = 0;
    int shown = 0;
    for (i = 0; i < L->nsrc; i++)
    {
        if (L->src[i].skip == skip)
        {
            n++;
        }
    }
    if (n == 0)
    {
        return;
    }
    wb_printf(b, L"\r\n\x2022  %d %s: ", n, label);
    for (i = 0; i < L->nsrc; i++)
    {
        const RosterSource *s = &L->src[i];
        if (s->skip != skip)
        {
            continue;
        }
        if (shown == ROSTER_LIST_MAX)
        {
            wb_printf(b, L"; and %d more", n - shown);
            break;
        }
        if (shown > 0)
        {
            wb_append(b, L"; ");
        }
        wb_append(b, s->name);
        if (skip == SKIP_COPY && s->copy_of >= 0)
        {
            wb_printf(b, L" (same voters as %s)", L->src[s->copy_of].name);
        }
        shown++;
    }
}

static void build_note(Loader *L)
{
    EeRosterLoadInfo *in = L->info;
    WBuf b = {0};
    wchar_t n1[32];
    int m;
    BOOL first = TRUE;

    fmt_count(in->voters, n1, ARRAYSIZE(n1));
    wb_printf(&b, L"Loaded %s voter record%s from %lu file%s.\r\n", n1, in->voters == 1 ? L"" : L"s",
              (unsigned long)in->files_read, in->files_read == 1 ? L"" : L"s");
    for (m = 1; m < EE_VM_COUNT; m++)
    {
        if (in->method_rows[m] == 0)
        {
            continue;
        }
        fmt_count(in->method_rows[m], n1, ARRAYSIZE(n1));
        wb_printf(&b, L"%s%S: %s", first ? L"" : L"  \x2022  ", k_MethodLabel[m], n1);
        first = FALSE;
    }
    if (in->method_rows[EE_VM_NONE] > 0)
    {
        fmt_count(in->method_rows[EE_VM_NONE], n1, ARRAYSIZE(n1));
        wb_printf(&b, L"%sunknown method: %s", first ? L"" : L"  \x2022  ", n1);
    }

    if (in->superseded_files || in->repeated_files || in->copy_files || in->unsupported_files ||
        in->skipped_sheets)
    {
        wb_append(&b, L"\r\n\r\nNot loaded:");
        note_file_list(L, &b, SKIP_SUPERSEDED, L"original file(s) replaced by a corrected (_Updated) file");
        note_file_list(L, &b, SKIP_REPEATED, L"file(s) also in a later selection (loaded once)");
        note_file_list(L, &b, SKIP_COPY, L"file(s) whose voters all appear in a file of another voting method");
        note_file_list(L, &b, SKIP_UNSUPPORTED, L"PDF or other file(s) that cannot be read yet");
        if (in->skipped_sheets)
        {
            wb_printf(&b, L"\r\n\x2022  %lu worksheet(s) with no recognizable layout: %s",
                      (unsigned long)in->skipped_sheets, L->skipped.p ? L->skipped.p : L"");
        }
    }
    if (in->recovered_sheets)
    {
        wb_printf(&b, L"\r\n\r\n%lu worksheet(s) had no header row; the header of a sheet with the same "
                     L"columns was used: %s",
                  (unsigned long)in->recovered_sheets, L->recovered.p ? L->recovered.p : L"");
    }
    if (L->date_fixes.len > 0)
    {
        wb_printf(&b, L"\r\n\r\nThe date in these file names disagreed with the sheet's own date, which "
                     L"was used: %s",
                  L->date_fixes.p);
    }
    if (in->rows_vuid_only || in->rows_non_voter)
    {
        wb_append(&b, L"\r\n\r\nRows skipped:");
        if (in->rows_vuid_only)
        {
            fmt_count(in->rows_vuid_only, n1, ARRAYSIZE(n1));
            wb_printf(&b, L" %s Voter ID%s with no name or precinct;", n1, in->rows_vuid_only == 1 ? L"" : L"s");
        }
        if (in->rows_non_voter)
        {
            fmt_count(in->rows_non_voter, n1, ARRAYSIZE(n1));
            wb_printf(&b, L" %s non-voter row%s (totals, notes);", n1, in->rows_non_voter == 1 ? L"" : L"s");
        }
        if (b.len > 0 && b.p[b.len - 1] == L';')
        {
            b.p[--b.len] = L'.';
        }
    }
    if (in->vuids_malformed)
    {
        fmt_count(in->vuids_malformed, n1, ARRAYSIZE(n1));
        wb_printf(&b, L"\r\n\r\n%s voter record%s kept with a Voter ID that is not 10 digits (e.g. %s).", n1,
                  in->vuids_malformed == 1 ? L"" : L"s", L->odd_ids.p ? L->odd_ids.p : L"");
    }
    if (in->vuids_missing)
    {
        fmt_count(in->vuids_missing, n1, ARRAYSIZE(n1));
        wb_printf(&b, L"\r\n\r\n%s voter record%s kept with a blank Voter ID.", n1,
                  in->vuids_missing == 1 ? L"" : L"s");
    }
    if (in->vuids_duplicated)
    {
        fmt_count(in->vuids_duplicated, n1, ARRAYSIZE(n1));
        wb_printf(&b, L"\r\n\r\n%s Voter ID%s appear%s on more than one row. Use Filter \x2192 Show Duplicate "
                     L"Voter IDs to review them.",
                  n1, in->vuids_duplicated == 1 ? L"" : L"s", in->vuids_duplicated == 1 ? L"s" : L"");
    }
    in->note = b.p;
}

/* -------------------------------------------------------------------------- */
/* Emit                                                                       */
/* -------------------------------------------------------------------------- */

static void fmt_date(uint32_t d, char *out, size_t cap)
{
    if (d == 0)
    {
        out[0] = '\0';
        return;
    }
    StringCchPrintfA(out, cap, "%02u/%02u/%04u", (unsigned)((d / 100u) % 100u), (unsigned)(d % 100u),
                     (unsigned)(d / 10000u));
}

static EeLoadStatus emit_table(Loader *L, const int *order, EeVoterTable *out)
{
    BOOL has_middle = FALSE, has_suffix = FALSE, has_party = FALSE;
    BOOL x_used[ROSTER_MAX_EXTRA] = {0};
    const char *header[16 + ROSTER_MAX_EXTRA];
    const char *cells[16 + ROSTER_MAX_EXTRA];
    int xpos[ROSTER_MAX_EXTRA];
    int ncols = 0;
    int c_middle = -1, c_suffix = -1, c_party = -1, c_method, c_date, c_source;
    int oi;
    int i;
    uint32_t r;
    EeVoterTableBuilder *b;
    uint32_t emitted = 0;
    uint32_t keep = 0; /* rows in sources that are loaded (for progress) */

    for (oi = 0; oi < L->nsrc; oi++)
    {
        const RosterSource *s = &L->src[order[oi]];
        if (s->skip != SKIP_NONE)
        {
            continue;
        }
        for (r = s->rec_begin; r < s->rec_end; r++)
        {
            const RosterRec *rec = &L->rec[r];
            uint32_t k;
            has_middle |= (rec->middle != 0);
            has_suffix |= (rec->suffix != 0);
            has_party |= (rec->party != 0);
            for (k = 0; k < rec->xcount; k++)
            {
                x_used[L->x[rec->xfirst + k].col] = TRUE;
            }
        }
    }
    header[ncols++] = EE_ROSTER_COL_VUID;
    header[ncols++] = EE_ROSTER_COL_PCT;
    header[ncols++] = EE_ROSTER_COL_LAST;
    header[ncols++] = EE_ROSTER_COL_FIRST;
    if (has_middle)
    {
        c_middle = ncols;
        header[ncols++] = EE_ROSTER_COL_MIDDLE;
    }
    if (has_suffix)
    {
        c_suffix = ncols;
        header[ncols++] = EE_ROSTER_COL_SUFFIX;
    }
    c_method = ncols;
    header[ncols++] = EE_ROSTER_COL_METHOD;
    c_date = ncols;
    header[ncols++] = EE_ROSTER_COL_DATE;
    if (has_party)
    {
        c_party = ncols;
        header[ncols++] = EE_ROSTER_COL_PARTY;
    }
    for (i = 0; i < L->nxname; i++)
    {
        xpos[i] = -1;
        if (x_used[i])
        {
            xpos[i] = ncols;
            header[ncols++] = istr(&L->str, L->xname[i]);
        }
    }
    c_source = ncols;
    header[ncols++] = EE_ROSTER_COL_SOURCE;
    L->info->has_party = has_party;

    b = EeVoterTable_BuilderBegin(out, header, (uint32_t)ncols, L->err, L->errcch);
    if (b == NULL)
    {
        return EeLoadStatus_Error;
    }
    for (oi = 0; oi < L->nsrc; oi++)
    {
        if (L->src[oi].skip == SKIP_NONE)
        {
            keep += L->src[oi].rec_end - L->src[oi].rec_begin;
        }
    }
    for (oi = 0; oi < L->nsrc; oi++)
    {
        const RosterSource *s = &L->src[order[oi]];
        if (s->skip != SKIP_NONE)
        {
            continue;
        }
        for (r = s->rec_begin; r < s->rec_end; r++)
        {
            const RosterRec *rec = &L->rec[r];
            char date[16];
            uint32_t k;
            for (i = 0; i < ncols; i++)
            {
                cells[i] = "";
            }
            fmt_date(rec->date, date, sizeof(date));
            cells[0] = istr(&L->str, rec->vuid);
            cells[1] = istr(&L->str, rec->pct);
            cells[2] = istr(&L->str, rec->last);
            cells[3] = istr(&L->str, rec->first);
            if (c_middle >= 0)
                cells[c_middle] = istr(&L->str, rec->middle);
            if (c_suffix >= 0)
                cells[c_suffix] = istr(&L->str, rec->suffix);
            cells[c_method] = k_MethodLabel[rec->method < EE_VM_COUNT ? rec->method : 0];
            cells[c_date] = date;
            if (c_party >= 0)
                cells[c_party] = istr(&L->str, rec->party);
            for (k = 0; k < rec->xcount; k++)
            {
                const RosterExtra *x = &L->x[rec->xfirst + k];
                if (xpos[x->col] >= 0)
                {
                    cells[xpos[x->col]] = istr(&L->str, x->val);
                }
            }
            cells[c_source] = istr(&L->str, rec->source_name);
            if (!EeVoterTable_BuilderAppend(b, cells, (uint32_t)ncols, L->err, L->errcch))
            {
                EeVoterTable_BuilderEnd(b, FALSE);
                return EeLoadStatus_Error;
            }
            L->info->method_rows[rec->method < EE_VM_COUNT ? rec->method : 0]++;
            emitted++;
            if ((emitted & 0x3FFF) == 0)
            {
                if (cancelled(L))
                {
                    EeVoterTable_BuilderEnd(b, FALSE);
                    return EeLoadStatus_Cancelled;
                }
                if (L->progress_fn != NULL)
                {
                    EeLoadProgress pr;
                    ZeroMemory(&pr, sizeof(pr));
                    pr.percent = 95u + (uint32_t)((uint64_t)emitted * 4u / (keep ? keep : 1u));
                    pr.rows_loaded = keep; /* the voters kept; emitting only builds the table */
                    L->progress_fn(&pr, L->progress_user);
                }
            }
        }
    }
    EeVoterTable_BuilderEnd(b, TRUE);
    L->info->voters = emitted;
    return EeLoadStatus_Ok;
}

/* -------------------------------------------------------------------------- */
/* Entry point                                                                */
/* -------------------------------------------------------------------------- */

void EeRoster_FreeInfo(EeRosterLoadInfo *info)
{
    if (info != NULL)
    {
        free(info->note);
        ZeroMemory(info, sizeof(*info));
    }
}

static void loader_free(Loader *L)
{
    int i;
    for (i = 0; i < L->nzips; i++)
    {
        mz_zip_reader_end(&L->zips[i].zip);
        fclose(L->zips[i].fp);
    }
    free(L->zips);
    free(L->src);
    free(L->rec);
    free(L->x);
    free(L->recovered.p);
    free(L->skipped.p);
    free(L->odd_ids.p);
    free(L->date_fixes.p);
    intern_free(&L->str);
}

EeLoadStatus EeRoster_LoadFiles(const wchar_t *const *paths,
                                int count,
                                EeVoterTable *out,
                                EeRosterLoadInfo *info,
                                volatile LONG *cancel_flag,
                                EeLoadProgressFn progress_fn,
                                void *progress_user,
                                wchar_t *error_message,
                                size_t error_cch)
{
    Loader L;
    EeRosterLoadInfo local_info;
    EeLoadStatus st = EeLoadStatus_Ok;
    SheetRows rows;
    int *order = NULL;
    int i;
    VuidIndex ix;

    if (info == NULL)
    {
        info = &local_info;
    }
    ZeroMemory(info, sizeof(*info));
    if (paths == NULL || count <= 0 || out == NULL)
    {
        set_err(error_message, error_cch, L"Invalid arguments.");
        return EeLoadStatus_Error;
    }
    ZeroMemory(&L, sizeof(L));
    ZeroMemory(&rows, sizeof(rows));
    ZeroMemory(&ix, sizeof(ix));
    L.info = info;
    L.cancel = cancel_flag;
    L.progress_fn = progress_fn;
    L.progress_user = progress_user;
    L.err = error_message;
    L.errcch = error_cch;
    L.last_pct = UINT32_MAX;
    if (!intern_init(&L.str))
    {
        set_err(error_message, error_cch, L"Out of memory.");
        intern_free(&L.str);
        return EeLoadStatus_Error;
    }
    /* Known extra columns get fixed positions (only emitted when used), so a roster and
     * its own CSV/TSV export produce the same column order. */
    extra_col(&L, "Notes");
    extra_col(&L, "Assisting Person");
    extra_col(&L, "Assisting Address");

    if (!expand_inputs(&L, paths, count))
    {
        if (L.oom)
        {
            set_err(error_message, error_cch, L"Out of memory.");
        }
        st = EeLoadStatus_Error;
        goto done;
    }
    mark_superseded(&L);

    order = (int *)malloc((size_t)(L.nsrc ? L.nsrc : 1) * sizeof(int));
    if (order == NULL)
    {
        set_err(error_message, error_cch, L"Out of memory.");
        st = EeLoadStatus_Error;
        goto done;
    }
    for (i = 0; i < L.nsrc; i++)
    {
        order[i] = i;
        if (L.src[i].skip == SKIP_NONE)
        {
            L.total_bytes += L.src[i].size;
        }
    }
    qsort_s(order, (size_t)L.nsrc, sizeof(int), src_order_cmp, &L);
    if (L.nsrc > 0xFFFF)
    {
        set_err(error_message, error_cch, L"Too many roster files in one load.");
        st = EeLoadStatus_Error;
        goto done;
    }

    for (i = 0; i < L.nsrc && st == EeLoadStatus_Ok; i++)
    {
        int si = order[i];
        if (L.src[si].skip != SKIP_NONE)
        {
            continue;
        }
        st = read_source(&L, si, &rows);
        L.done_bytes += L.src[si].size;
        report(&L, L.done_bytes);
        if (L.oom)
        {
            set_err(error_message, error_cch, L"Out of memory loading the roster.");
            st = EeLoadStatus_Error;
        }
    }
    sheet_free(&rows);
    if (st != EeLoadStatus_Ok)
    {
        if (st == EeLoadStatus_Cancelled)
        {
            set_err(error_message, error_cch, L"Load cancelled.");
        }
        goto done;
    }

    /* Whole-file copies (D1), then duplicate Voter IDs among what is kept. */
    if (!build_vuid_index(&L, &ix))
    {
        set_err(error_message, error_cch, L"Out of memory.");
        st = EeLoadStatus_Error;
        goto done;
    }
    mark_copies(&L, &ix);
    {
        uint32_t s;
        for (s = 0; s < ix.nslots; s++)
        {
            uint32_t h;
            uint32_t kept = 0;
            for (h = ix.head[s]; h != 0 && kept < 2; h = ix.next[h - 1])
            {
                if (L.src[L.rec[h - 1].source].skip == SKIP_NONE)
                {
                    kept++;
                }
            }
            if (kept >= 2)
            {
                info->vuids_duplicated++;
            }
        }
    }
    for (i = 0; i < L.nsrc; i++)
    {
        switch (L.src[i].skip)
        {
            case SKIP_SUPERSEDED:
                info->superseded_files++;
                break;
            case SKIP_REPEATED:
                info->repeated_files++;
                break;
            case SKIP_COPY:
                info->copy_files++;
                info->files_read--;
                break;
            case SKIP_UNSUPPORTED:
                info->unsupported_files++;
                break;
            default:
                break;
        }
    }
    if (L.nrec == 0 && info->files_read == 0)
    {
        set_err(error_message, error_cch,
                L"No voter roster spreadsheets (.xlsx, .csv, .tsv) were found in the selection.");
        st = EeLoadStatus_Error;
        goto done;
    }

    st = emit_table(&L, order, out);
    if (st == EeLoadStatus_Ok)
    {
        build_note(&L);
    }

done:
    free_vuid_index(&ix);
    free(order);
    sheet_free(&rows);
    loader_free(&L);
    if (st != EeLoadStatus_Ok)
    {
        EeRoster_FreeInfo(info);
    }
    else if (info == &local_info)
    {
        EeRoster_FreeInfo(info);
    }
    return st;
}

/* -------------------------------------------------------------------------- */
/* Voting totals                                                              */
/* -------------------------------------------------------------------------- */

/* Source column titled @p title (ASCII, case-insensitive), or -1. */
static int totals_find_col(const EeVoterTable *t, const char *title)
{
    wchar_t w[64];
    uint32_t c;
    int i;
    for (i = 0; title[i] != '\0' && i < (int)ARRAYSIZE(w) - 1; i++)
    {
        w[i] = (wchar_t)(unsigned char)title[i];
    }
    w[i] = L'\0';
    for (c = EE_FROZEN_COLUMN_COUNT; c < t->column_count; c++)
    {
        if (t->column_titles[c] != NULL && _wcsicmp(t->column_titles[c], w) == 0)
        {
            return (int)c;
        }
    }
    return -1;
}

/* "MM/DD/YYYY" (the roster's Date Voted) or "YYYY-MM-DD" -> yyyymmdd, else 0. */
static uint32_t totals_parse_date(const char *s)
{
    unsigned m = 0, d = 0, y = 0;
    int i;
    while (*s == ' ')
    {
        s++;
    }
    if (strlen(s) >= 10 && s[4] == '-' && s[7] == '-')
    {
        for (i = 0; i < 10; i++)
        {
            if (i != 4 && i != 7 && (s[i] < '0' || s[i] > '9'))
            {
                return 0;
            }
        }
        y = (unsigned)atoi(s);
        m = (unsigned)atoi(s + 5);
        d = (unsigned)atoi(s + 8);
    }
    else
    {
        const char *p = s;
        unsigned *part[3] = {&m, &d, &y};
        for (i = 0; i < 3; i++)
        {
            int digits = 0;
            while (*p >= '0' && *p <= '9' && digits < 4)
            {
                *part[i] = *part[i] * 10u + (unsigned)(*p - '0');
                p++;
                digits++;
            }
            if (digits == 0 || (i < 2 && *p != '/'))
            {
                return 0;
            }
            if (i < 2)
            {
                p++;
            }
        }
    }
    if (y < 1900 || y > 2999 || m < 1 || m > 12 || d < 1 || d > 31)
    {
        return 0;
    }
    return y * 10000u + m * 100u + d;
}

static int totals_cmp_u32(const void *a, const void *b)
{
    uint32_t x = *(const uint32_t *)a;
    uint32_t y = *(const uint32_t *)b;
    return (x < y) ? -1 : (x > y) ? 1 : 0;
}

/* Sort parties alphabetically with the blank party last. */
static int totals_cmp_party(const void *a, const void *b)
{
    const char *x = *(const char *const *)a;
    const char *y = *(const char *const *)b;
    if (x[0] == '\0' || y[0] == '\0')
    {
        return (x[0] == '\0') - (y[0] == '\0');
    }
    return _stricmp(x, y);
}

static uint32_t totals_hash(const char *s)
{
    uint32_t h = 2166136261u;
    while (*s != '\0')
    {
        h = (h ^ (uint8_t)*s++) * 16777619u;
    }
    return h;
}

void EeRoster_FreeTotals(EeRosterTotals *t)
{
    uint32_t i;
    if (t == NULL)
    {
        return;
    }
    for (i = 0; i < t->nparties; i++)
    {
        free(t->parties[i]);
    }
    free(t->parties);
    free(t->dates);
    free(t->counts);
    ZeroMemory(t, sizeof(*t));
}

BOOL EeRoster_ComputeTotals(const EeVoterTable *table, EeRosterTotals *out)
{
    int c_method;
    int c_date;
    int c_party;
    uint32_t n;
    uint32_t r;
    uint32_t cap = 16;
    uint32_t *rdate = NULL;  /* per row: yyyymmdd (0 = none) */
    uint8_t *rmeth = NULL;   /* per row: EeVotingMethod */
    uint8_t *keep = NULL;    /* per row: counted */
    uint32_t *slot = NULL;   /* Voter ID hash -> row + 1 (0 = empty) */
    uint8_t *slot_rep = NULL; /* slot's Voter ID has a repeat */
    const char **party_of = NULL;
    uint32_t i;
    BOOL ok = FALSE;

    if (out == NULL)
    {
        return FALSE;
    }
    ZeroMemory(out, sizeof(*out));
    if (table == NULL)
    {
        return FALSE;
    }
    c_method = totals_find_col(table, EE_ROSTER_COL_METHOD);
    c_date = totals_find_col(table, EE_ROSTER_COL_DATE);
    c_party = totals_find_col(table, EE_ROSTER_COL_PARTY);
    if (c_method < 0)
    {
        return FALSE;
    }
    n = table->row_count;
    while (cap < n * 2u + 16u && cap < 0x40000000u)
    {
        cap <<= 1;
    }
    rdate = (uint32_t *)calloc(n ? n : 1, sizeof(uint32_t));
    rmeth = (uint8_t *)calloc(n ? n : 1, 1);
    keep = (uint8_t *)calloc(n ? n : 1, 1);
    party_of = (const char **)calloc(n ? n : 1, sizeof(const char *));
    slot = (uint32_t *)calloc(cap, sizeof(uint32_t));
    slot_rep = (uint8_t *)calloc(cap, 1);
    if (rdate == NULL || rmeth == NULL || keep == NULL || party_of == NULL || slot == NULL ||
        slot_rep == NULL)
    {
        goto done;
    }

    /* Pass 1: each Voter ID's earliest record (date, then method order, then row);
     * an unknown date sorts last. */
    out->records = n;
    for (r = 0; r < n; r++)
    {
        const char *vuid = EeVoterTable_GetCellUtf8(table, r, EE_COL_VOTER_ID);
        uint32_t h;
        rmeth[r] = (uint8_t)EeRoster_MethodFromLabel(
            EeVoterTable_GetCellUtf8(table, r, (uint32_t)c_method));
        rdate[r] = (c_date >= 0)
                       ? totals_parse_date(EeVoterTable_GetCellUtf8(table, r, (uint32_t)c_date))
                       : 0;
        party_of[r] = (c_party >= 0) ? EeVoterTable_GetCellUtf8(table, r, (uint32_t)c_party) : "";
        out->method_present[rmeth[r]] = TRUE;
        if (vuid[0] == '\0')
        {
            keep[r] = 1; /* no Voter ID to match: count the row */
            continue;
        }
        h = totals_hash(vuid) & (cap - 1u);
        for (;;)
        {
            uint32_t b;
            if (slot[h] == 0)
            {
                slot[h] = r + 1u;
                keep[r] = 1;
                break;
            }
            b = slot[h] - 1u;
            if (strcmp(EeVoterTable_GetCellUtf8(table, b, EE_COL_VOTER_ID), vuid) == 0)
            {
                uint32_t db = rdate[b] ? rdate[b] : 0xFFFFFFFFu;
                uint32_t dr = rdate[r] ? rdate[r] : 0xFFFFFFFFu;
                out->repeats++;
                if (!slot_rep[h])
                {
                    slot_rep[h] = 1;
                    out->repeated_ids++;
                }
                if (dr < db || (dr == db && rmeth[r] < rmeth[b] && rmeth[r] != EE_VM_NONE))
                {
                    keep[b] = 0;
                    keep[r] = 1;
                    slot[h] = r + 1u;
                }
                break;
            }
            h = (h + 1u) & (cap - 1u);
        }
    }

    /* Distinct dates and parties of the counted records. */
    out->dates = (uint32_t *)malloc(((size_t)n + 1u) * sizeof(uint32_t));
    out->parties = (char **)calloc(17, sizeof(char *)); /* 16 parties + blank */
    if (out->dates == NULL || out->parties == NULL)
    {
        goto done;
    }
    for (r = 0; r < n; r++)
    {
        if (keep[r])
        {
            out->dates[out->ndates++] = rdate[r];
        }
    }
    if (out->ndates == 0)
    {
        out->dates[out->ndates++] = 0;
    }
    qsort(out->dates, out->ndates, sizeof(uint32_t), totals_cmp_u32);
    {
        uint32_t w = 1;
        for (i = 1; i < out->ndates; i++)
        {
            if (out->dates[i] != out->dates[w - 1])
            {
                out->dates[w++] = out->dates[i];
            }
        }
        out->ndates = w;
    }
    for (r = 0; r < n; r++)
    {
        const char *pv = party_of[r];
        if (!keep[r])
        {
            continue;
        }
        for (i = 0; i < out->nparties; i++)
        {
            if (strcmp(out->parties[i], pv) == 0)
            {
                break;
            }
        }
        if (i == out->nparties)
        {
            if (out->nparties == 16)
            {
                pv = ""; /* more than 16 parties: not a primary's party column */
                for (i = 0; i < out->nparties && strcmp(out->parties[i], pv) != 0; i++)
                {
                }
            }
            if (i == out->nparties)
            {
                out->parties[out->nparties] = _strdup(pv);
                if (out->parties[out->nparties] == NULL)
                {
                    goto done;
                }
                out->nparties++;
            }
        }
        if (pv[0] != '\0')
        {
            out->has_party = TRUE;
        }
    }
    if (out->nparties == 0)
    {
        out->parties[0] = _strdup("");
        if (out->parties[0] == NULL)
        {
            goto done;
        }
        out->nparties = 1;
    }
    qsort(out->parties, out->nparties, sizeof(char *), totals_cmp_party);

    out->counts = (uint32_t *)calloc((size_t)out->ndates * out->nparties * EE_VM_COUNT,
                                     sizeof(uint32_t));
    if (out->counts == NULL)
    {
        goto done;
    }
    for (r = 0; r < n; r++)
    {
        uint32_t d;
        uint32_t p;
        const uint32_t *hit;
        if (!keep[r])
        {
            continue;
        }
        hit = (const uint32_t *)bsearch(&rdate[r], out->dates, out->ndates, sizeof(uint32_t),
                                        totals_cmp_u32);
        d = (hit != NULL) ? (uint32_t)(hit - out->dates) : 0;
        for (p = 0; p < out->nparties && strcmp(out->parties[p], party_of[r]) != 0; p++)
        {
        }
        if (p == out->nparties)
        {
            /* a party folded into blank (over 16 parties) */
            for (p = 0; p < out->nparties && out->parties[p][0] != '\0'; p++)
            {
            }
            if (p == out->nparties)
            {
                p = out->nparties - 1;
            }
        }
        out->counts[((size_t)d * out->nparties + p) * EE_VM_COUNT + rmeth[r]]++;
        out->counted++;
    }
    ok = TRUE;

done:
    free(rdate);
    free(rmeth);
    free(keep);
    free(party_of);
    free(slot);
    free(slot_rep);
    if (!ok)
    {
        EeRoster_FreeTotals(out);
    }
    return ok;
}
