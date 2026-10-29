/**
 * @file ee_audit.c
 * @brief ES&S Electionware audit event reports read into events. See ee_audit.h and
 *        docs/audit-log-design.md.
 */

#include "ee_audit.h"
#include "csv_sheet.h"
#include "pdf_reader.h"

#include <math.h>
#include <stdlib.h>
#include <string.h>
#include <strsafe.h>

/* -------------------------------------------------------------------------- */
/* Small helpers                                                              */
/* -------------------------------------------------------------------------- */

static void au_set_err(wchar_t *dst, size_t cch, const wchar_t *fmt, const wchar_t *path)
{
    if (dst != NULL && cch > 0)
    {
        const wchar_t *name = path != NULL ? wcsrchr(path, L'\\') : NULL;
        StringCchPrintfW(dst, cch, fmt, name != NULL ? name + 1 : (path != NULL ? path : L""));
    }
}

static BOOL au_grow(void **arr, size_t *cap, size_t need, size_t elem)
{
    if (need > *cap)
    {
        size_t ncap = *cap != 0 ? *cap : 16;
        void *p;
        while (ncap < need)
        {
            ncap *= 2;
        }
        p = realloc(*arr, ncap * elem);
        if (p == NULL)
        {
            return FALSE;
        }
        *arr = p;
        *cap = ncap;
    }
    return TRUE;
}

static char *au_strdup_n(const char *s, size_t n)
{
    char *p = (char *)malloc(n + 1);
    if (p != NULL)
    {
        memcpy(p, s, n);
        p[n] = '\0';
    }
    return p;
}

static wchar_t *au_wcsdup(const wchar_t *s)
{
    size_t n = wcslen(s);
    wchar_t *p = (wchar_t *)malloc((n + 1) * sizeof(wchar_t));
    if (p != NULL)
    {
        memcpy(p, s, (n + 1) * sizeof(wchar_t));
    }
    return p;
}

/* A growable UTF-8 string. */
typedef struct AuStr
{
    char *p;
    size_t len;
    size_t cap;
} AuStr;

static BOOL au_str_set(AuStr *s, const char *t, size_t n)
{
    if (!au_grow((void **)&s->p, &s->cap, n + 1, 1))
    {
        return FALSE;
    }
    memcpy(s->p, t, n);
    s->len = n;
    s->p[n] = '\0';
    return TRUE;
}

static BOOL au_is_alpha(char c)
{
    return (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z');
}

/* Append a wrapped line: a space between lines, none after a word broken at its hyphen
 * ("re-" + "try"). */
static BOOL au_str_join(AuStr *s, const char *t, size_t n)
{
    BOOL space;
    while (n > 0 && t[0] == ' ')
    {
        t++;
        n--;
    }
    while (n > 0 && t[n - 1] == ' ')
    {
        n--;
    }
    if (n == 0)
    {
        return TRUE;
    }
    space = s->len > 0 && !(s->len >= 2 && s->p[s->len - 1] == '-' && au_is_alpha(s->p[s->len - 2]) &&
                            au_is_alpha(t[0]));
    if (!au_grow((void **)&s->p, &s->cap, s->len + n + 2, 1))
    {
        return FALSE;
    }
    if (space)
    {
        s->p[s->len++] = ' ';
    }
    memcpy(s->p + s->len, t, n);
    s->len += n;
    s->p[s->len] = '\0';
    return TRUE;
}

/* -------------------------------------------------------------------------- */
/* Time                                                                       */
/* -------------------------------------------------------------------------- */

/* Days since 1970-01-01 of a civil date (proleptic Gregorian). */
static int64_t au_days_from_civil(int y, int m, int d)
{
    int64_t era;
    unsigned yoe, doy, doe;
    y -= m <= 2;
    era = (y >= 0 ? y : y - 399) / 400;
    yoe = (unsigned)(y - era * 400);
    doy = (unsigned)((153 * (m + (m > 2 ? -3 : 9)) + 2) / 5 + d - 1);
    doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    return era * 146097 + (int64_t)doe - 719468;
}

int64_t EeAudit_MakeTime(int year, int month, int day, int hour, int minute, int second)
{
    return au_days_from_civil(year, month, day) * 86400 + (int64_t)hour * 3600 + (int64_t)minute * 60 + second;
}

void EeAudit_FormatTime(int64_t ts, char *buf, size_t cap)
{
    int64_t days = ts >= 0 ? ts / 86400 : (ts - 86399) / 86400;
    int64_t secs = ts - days * 86400;
    int64_t z = days + 719468;
    int64_t era = (z >= 0 ? z : z - 146096) / 146097;
    unsigned doe = (unsigned)(z - era * 146097);
    unsigned yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    unsigned doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    unsigned mp = (5 * doy + 2) / 153;
    unsigned d = doy - (153 * mp + 2) / 5 + 1;
    unsigned m = mp < 10 ? mp + 3 : mp - 9;
    int64_t y = (int64_t)yoe + era * 400 + (m <= 2);
    int h = (int)(secs / 3600);
    int h12 = h % 12 == 0 ? 12 : h % 12;
    StringCchPrintfA(buf, cap, "%02u/%02u/%04lld %02d:%02d:%02d%s", m, d, (long long)y, h12, (int)(secs / 60 % 60),
                     (int)(secs % 60), h < 12 ? "AM" : "PM");
}

static BOOL au_digits(const char *s, int n, int *out)
{
    int v = 0, i;
    for (i = 0; i < n; i++)
    {
        if (s[i] < '0' || s[i] > '9')
        {
            return FALSE;
        }
        v = v * 10 + (s[i] - '0');
    }
    *out = v;
    return TRUE;
}

/* "MM/DD/YYYY hh:mm:ssAM" (with_seconds) or "MM/DD/YYYY hh:mmAM" (the print time). */
static BOOL au_parse_time(const char *s, size_t n, BOOL with_seconds, int64_t *out)
{
    int mo, d, y, h, mi, sec = 0;
    size_t ap = with_seconds ? 19 : 16;
    if (n != ap + 2 || s[2] != '/' || s[5] != '/' || s[10] != ' ' || s[13] != ':' ||
        (with_seconds && s[16] != ':') || (s[ap] != 'A' && s[ap] != 'P') || s[ap + 1] != 'M')
    {
        return FALSE;
    }
    if (!au_digits(s, 2, &mo) || !au_digits(s + 3, 2, &d) || !au_digits(s + 6, 4, &y) ||
        !au_digits(s + 11, 2, &h) || !au_digits(s + 14, 2, &mi) || (with_seconds && !au_digits(s + 17, 2, &sec)))
    {
        return FALSE;
    }
    if (mo < 1 || mo > 12 || d < 1 || d > 31 || h < 1 || h > 12 || mi > 59 || sec > 59)
    {
        return FALSE;
    }
    if (h == 12)
    {
        h = 0;
    }
    if (s[ap] == 'P')
    {
        h += 12;
    }
    *out = EeAudit_MakeTime(y, mo, d, h, mi, sec);
    return TRUE;
}

/* "November 5, 2024" -> 20241105. */
static int au_parse_long_date(const char *s)
{
    static const char *const months[] = {"January", "February", "March",     "April",   "May",      "June",
                                         "July",    "August",   "September", "October", "November", "December"};
    int m, d = 0, y = 0;
    for (m = 0; m < 12; m++)
    {
        size_t k = strlen(months[m]);
        if (strncmp(s, months[m], k) == 0 && s[k] == ' ')
        {
            const char *p = s + k + 1;
            while (*p >= '0' && *p <= '9')
            {
                d = d * 10 + (*p++ - '0');
            }
            if (*p++ != ',' || *p++ != ' ')
            {
                return 0;
            }
            while (*p >= '0' && *p <= '9')
            {
                y = y * 10 + (*p++ - '0');
            }
            if (*p != '\0' || d < 1 || d > 31 || y < 1990 || y > 2100)
            {
                return 0;
            }
            return y * 10000 + (m + 1) * 100 + d;
        }
    }
    return 0;
}

/* -------------------------------------------------------------------------- */
/* The copy: strings and events                                               */
/* -------------------------------------------------------------------------- */

void EeAudit_InitCopy(EeAuditCopy *c)
{
    memset(c, 0, sizeof(*c));
}

void EeAudit_FreeCopy(EeAuditCopy *c)
{
    uint32_t i;
    if (c == NULL)
    {
        return;
    }
    for (i = 0; i < c->nstrs; i++)
    {
        free(c->strs[i]);
    }
    free(c->strs);
    free(c->str_hash);
    free(c->events);
    free(c->pool);
    free(c->path);
    free(c->customer);
    free(c->election);
    memset(c, 0, sizeof(*c));
}

const char *EeAudit_Str(const EeAuditCopy *c, uint32_t id)
{
    return id < c->nstrs ? c->strs[id] : "";
}

const char *EeAudit_Desc(const EeAuditCopy *c, const EeAuditEvent *e)
{
    return e->desc < c->pool_len ? c->pool + e->desc : "";
}

static uint32_t au_hash(const char *s, size_t n)
{
    uint32_t h = 2166136261u;
    size_t i;
    for (i = 0; i < n; i++)
    {
        h = (h ^ (unsigned char)s[i]) * 16777619u;
    }
    return h;
}

/* The index of string @p s in c->strs, added when new; UINT32_MAX out of memory. */
static uint32_t au_intern(EeAuditCopy *c, const char *s, size_t n)
{
    uint32_t h, i;
    if (c->hash_cap == 0 || (c->nstrs + 1) * 2 > c->hash_cap)
    {
        uint32_t ncap = c->hash_cap != 0 ? c->hash_cap * 2 : 64;
        uint32_t *nt = (uint32_t *)calloc(ncap, sizeof(uint32_t));
        uint32_t k;
        if (nt == NULL)
        {
            return UINT32_MAX;
        }
        for (k = 0; k < c->nstrs; k++)
        {
            uint32_t j = au_hash(c->strs[k], strlen(c->strs[k])) & (ncap - 1);
            while (nt[j] != 0)
            {
                j = (j + 1) & (ncap - 1);
            }
            nt[j] = k + 1;
        }
        free(c->str_hash);
        c->str_hash = nt;
        c->hash_cap = ncap;
    }
    h = au_hash(s, n);
    for (i = h & (c->hash_cap - 1); c->str_hash[i] != 0; i = (i + 1) & (c->hash_cap - 1))
    {
        const char *t = c->strs[c->str_hash[i] - 1];
        if (strncmp(t, s, n) == 0 && t[n] == '\0')
        {
            return c->str_hash[i] - 1;
        }
    }
    {
        size_t cap = c->cap_strs;
        char *dup;
        if (!au_grow((void **)&c->strs, &cap, (size_t)c->nstrs + 1, sizeof(char *)))
        {
            return UINT32_MAX;
        }
        c->cap_strs = (uint32_t)cap;
        dup = au_strdup_n(s, n);
        if (dup == NULL)
        {
            return UINT32_MAX;
        }
        c->strs[c->nstrs] = dup;
        c->str_hash[i] = c->nstrs + 1;
        return c->nstrs++;
    }
}

/* Store a description in the pool; UINT32_MAX out of memory. */
static uint32_t au_pool_add(EeAuditCopy *c, const char *s, size_t n)
{
    uint32_t off;
    if (c->pool_len + n + 1 > UINT32_MAX || !au_grow((void **)&c->pool, &c->pool_cap, c->pool_len + n + 1, 1))
    {
        return UINT32_MAX;
    }
    off = (uint32_t)c->pool_len;
    memcpy(c->pool + c->pool_len, s, n);
    c->pool[c->pool_len + n] = '\0';
    c->pool_len += n + 1;
    return off;
}

/* -------------------------------------------------------------------------- */
/* Page layout                                                                */
/* -------------------------------------------------------------------------- */

/* Columns of the table, in reading order. */
enum
{
    AU_COL_EVENT = 0,
    AU_COL_DESC,
    AU_COL_TIME,
    AU_COL_USER,
    AU_COL_MODULE,
    AU_NCOLS
};

static const char *const k_au_headings[AU_NCOLS] = {"Event", "Description", "Timestamp", "User Id", "Module"};

/* A run placed on the upright page: row grows down the page, col across it. */
typedef struct AuRun
{
    float row;
    float col;
    float size;
    const char *text; /* NUL-terminated copy in the page buffer */
    size_t len;
    int column; /* AU_COL_*, or -1 */
    int owner;  /* event on the page (index into anchors), -1 = the previous page's last event */
} AuRun;

typedef struct AuPage
{
    AuRun *runs;
    size_t nruns;
    size_t cap_runs;
    char *buf;
    size_t buf_cap;
    float col[AU_NCOLS]; /* left edges, from the heading row (kept from the last page with one) */
    float heading_row;
    BOOL have_cols;
    float *anchor_row; /* row of each timestamp on the page */
    size_t cap_anchor;
    int64_t *anchor_ts;
    size_t cap_anchor_ts;
} AuPage;

/* Turn a run into upright page coordinates. JasperReports prints these landscape pages as
 * portrait pages with the text turned 90 degrees: x then runs down the page and y across. */
static BOOL au_place(const EePdfTextRun *r, float *row, float *col)
{
    if (r->rot == 90)
    {
        *row = r->x;
        *col = r->y;
        return TRUE;
    }
    if (r->rot == 0)
    {
        *row = -r->y;
        *col = r->x;
        return TRUE;
    }
    return FALSE;
}

static int au_cmp_run(const void *pa, const void *pb)
{
    const AuRun *a = (const AuRun *)pa;
    const AuRun *b = (const AuRun *)pb;
    if (a->row < b->row - 0.5f)
    {
        return -1;
    }
    if (a->row > b->row + 0.5f)
    {
        return 1;
    }
    return a->col < b->col ? -1 : (a->col > b->col ? 1 : 0);
}

/* Collect the page's runs (trimmed, empty ones dropped), sorted top down. */
static BOOL au_collect(AuPage *pg, const EePdfPageText *pt)
{
    uint32_t i;
    size_t need = 0, pos = 0;
    pg->nruns = 0;
    for (i = 0; i < pt->nruns; i++)
    {
        need += pt->runs[i].text_len + 1;
    }
    if (!au_grow((void **)&pg->buf, &pg->buf_cap, need + 1, 1) ||
        !au_grow((void **)&pg->runs, &pg->cap_runs, (size_t)pt->nruns + 1, sizeof(AuRun)))
    {
        return FALSE;
    }
    for (i = 0; i < pt->nruns; i++)
    {
        const EePdfTextRun *r = &pt->runs[i];
        const char *t = pt->text + r->text_off;
        size_t n = r->text_len;
        AuRun *a;
        float row, col;
        while (n > 0 && (t[0] == ' ' || t[0] == '\t'))
        {
            t++;
            n--;
        }
        while (n > 0 && (t[n - 1] == ' ' || t[n - 1] == '\t' || t[n - 1] == '\r' || t[n - 1] == '\n'))
        {
            n--;
        }
        if (n == 0 || !au_place(r, &row, &col))
        {
            continue;
        }
        a = &pg->runs[pg->nruns++];
        memcpy(pg->buf + pos, t, n);
        pg->buf[pos + n] = '\0';
        a->text = pg->buf + pos;
        a->len = n;
        a->row = row;
        a->col = col;
        a->size = r->font_size;
        a->column = -1;
        a->owner = -1;
        pos += n + 1;
    }
    qsort(pg->runs, pg->nruns, sizeof(AuRun), au_cmp_run);
    return TRUE;
}

/* Find the heading row (Event | Description | Timestamp | User Id | Module). */
static BOOL au_find_headings(AuPage *pg)
{
    size_t i, j;
    for (i = 0; i < pg->nruns; i++)
    {
        float col[AU_NCOLS];
        int found = 0, k;
        if (strcmp(pg->runs[i].text, "Timestamp") != 0)
        {
            continue;
        }
        for (k = 0; k < AU_NCOLS; k++)
        {
            col[k] = -1.0f;
        }
        for (j = 0; j < pg->nruns; j++)
        {
            if (fabsf(pg->runs[j].row - pg->runs[i].row) > 2.0f)
            {
                continue;
            }
            for (k = 0; k < AU_NCOLS; k++)
            {
                if (col[k] < 0.0f && strcmp(pg->runs[j].text, k_au_headings[k]) == 0)
                {
                    col[k] = pg->runs[j].col;
                    found++;
                }
            }
        }
        if (found == AU_NCOLS && col[0] < col[1] && col[1] < col[2] && col[2] < col[3] && col[3] < col[4])
        {
            memcpy(pg->col, col, sizeof(col));
            pg->heading_row = pg->runs[i].row;
            pg->have_cols = TRUE;
            return TRUE;
        }
    }
    return FALSE;
}

static int au_column_of(const AuPage *pg, float col)
{
    int k;
    for (k = AU_NCOLS - 1; k >= 0; k--)
    {
        if (col >= pg->col[k] - 3.0f)
        {
            return k;
        }
    }
    return -1;
}

/* The footer row ("Page N of" M), or a large value when the page has none. */
static float au_footer_row(const AuPage *pg)
{
    size_t i;
    for (i = 0; i < pg->nruns; i++)
    {
        const char *t = pg->runs[i].text;
        if (strncmp(t, "Page ", 5) == 0 && strstr(t, " of") != NULL && pg->runs[i].row > pg->heading_row)
        {
            return pg->runs[i].row;
        }
    }
    return 1.0e9f;
}

/* Page 1's header: title, print time, customer, election name, election date. */
static BOOL au_read_header(EeAuditCopy *c, const AuPage *pg)
{
    const AuRun *rest[16];
    size_t i, nrest = 0;
    int date_at = -1;
    for (i = 0; i < pg->nruns; i++)
    {
        const AuRun *r = &pg->runs[i];
        int64_t t;
        if (r->row >= pg->heading_row - 2.0f)
        {
            break;
        }
        if (strstr(r->text, "Audit Events Report") != NULL)
        {
            c->kind = strncmp(r->text, "Admin", 5) == 0 ? EE_AUDIT_ADMIN
                      : strncmp(r->text, "Election", 8) == 0 ? EE_AUDIT_ELECTION : 0;
        }
        else if (au_parse_time(r->text, r->len, FALSE, &t))
        {
            c->printed = t;
        }
        else if (nrest < 16)
        {
            rest[nrest++] = r;
        }
    }
    for (i = 0; i < nrest; i++)
    {
        int d = au_parse_long_date(rest[i]->text);
        if (d != 0)
        {
            c->election_date = d;
            date_at = (int)i;
        }
    }
    for (i = 0; i < nrest; i++)
    {
        if ((int)i == date_at)
        {
            continue;
        }
        if (c->customer == NULL)
        {
            c->customer = au_strdup_n(rest[i]->text, rest[i]->len);
        }
        else if (c->election == NULL)
        {
            c->election = au_strdup_n(rest[i]->text, rest[i]->len);
        }
    }
    return c->kind != 0;
}

/* -------------------------------------------------------------------------- */
/* Events                                                                     */
/* -------------------------------------------------------------------------- */

static BOOL au_same_text(const char *a, const char *b);

/* Join more text onto the copy's last event (a row continued from the previous page). */
static BOOL au_continue_last(EeAuditCopy *c, int column, const char *t, size_t n, AuStr *tmp)
{
    EeAuditEvent *e;
    uint32_t *field;
    const char *old;
    if (c->nevents == 0)
    {
        return TRUE; /* text above the first row: nothing to join it to */
    }
    e = &c->events[c->nevents - 1];
    if (column == AU_COL_DESC)
    {
        uint32_t off;
        old = EeAudit_Desc(c, e);
        if (!au_str_set(tmp, t, n))
        {
            return FALSE;
        }
        if (au_same_text(old, tmp->p))
        {
            return TRUE; /* the whole description printed again */
        }
        if (!au_str_set(tmp, old, strlen(old)) || !au_str_join(tmp, t, n))
        {
            return FALSE;
        }
        off = au_pool_add(c, tmp->p, tmp->len);
        if (off == UINT32_MAX)
        {
            return FALSE;
        }
        e->desc = off;
        return TRUE;
    }
    field = column == AU_COL_EVENT ? &e->level : column == AU_COL_USER ? &e->user
                                             : column == AU_COL_MODULE ? &e->module : NULL;
    if (field == NULL)
    {
        return TRUE;
    }
    old = EeAudit_Str(c, *field);
    if (!au_str_set(tmp, t, n))
    {
        return FALSE;
    }
    if (au_same_text(old, tmp->p))
    {
        return TRUE;
    }
    if (!au_str_set(tmp, old, strlen(old)) || !au_str_join(tmp, t, n))
    {
        return FALSE;
    }
    *field = au_intern(c, tmp->p, tmp->len);
    return *field != UINT32_MAX;
}

/* Read one page's rows into events. */
static BOOL au_page_events(EeAuditCopy *c, AuPage *pg, uint32_t page_no, AuStr *f, AuStr *tmp)
{
    float footer = au_footer_row(pg);
    size_t i, nanchor = 0;
    int k;

    /* columns, and the timestamps that anchor each row */
    for (i = 0; i < pg->nruns; i++)
    {
        AuRun *r = &pg->runs[i];
        int64_t t;
        if (r->row <= pg->heading_row + 2.0f || r->row >= footer - 2.0f)
        {
            continue;
        }
        r->column = au_column_of(pg, r->col);
        if (r->column == AU_COL_TIME && au_parse_time(r->text, r->len, TRUE, &t))
        {
            r->owner = (int)nanchor++;
        }
    }
    /* every other body run belongs to the last row whose timestamp is level with it or above
     * it (a row's first line sits a fraction lower than its timestamp; wrapped lines lower) */
    if (!au_grow((void **)&pg->anchor_row, &pg->cap_anchor, nanchor + 1, sizeof(float)) ||
        !au_grow((void **)&pg->anchor_ts, &pg->cap_anchor_ts, nanchor + 1, sizeof(int64_t)))
    {
        return FALSE;
    }
    for (i = 0; i < pg->nruns; i++)
    {
        const AuRun *r = &pg->runs[i];
        if (r->column == AU_COL_TIME && r->owner >= 0)
        {
            pg->anchor_row[r->owner] = r->row;
            au_parse_time(r->text, r->len, TRUE, &pg->anchor_ts[r->owner]);
        }
    }
    {
        size_t a = 0;
        for (i = 0; i < pg->nruns; i++)
        {
            AuRun *r = &pg->runs[i];
            if (r->column < 0 || (r->column == AU_COL_TIME && r->owner >= 0))
            {
                continue;
            }
            while (a < nanchor && pg->anchor_row[a] - 1.5f <= r->row)
            {
                a++;
            }
            r->owner = (int)a - 1; /* -1: above the page's first row */
        }
    }
    /* text above the page's first row continues the previous page's last row, a column at
     * a time: JasperReports prints the rest of a cell that ran over the page end, and
     * prints a cell that fit again in full when another cell of its row ran over (a
     * wrapped module), so a repeat of what the row already holds is dropped */
    for (k = 0; k < AU_NCOLS; k++)
    {
        f->len = 0;
        for (i = 0; i < pg->nruns; i++)
        {
            const AuRun *r = &pg->runs[i];
            if (r->column == k && k != AU_COL_TIME && r->owner < 0 && !au_str_join(f, r->text, r->len))
            {
                return FALSE;
            }
        }
        if (f->len > 0 && !au_continue_last(c, k, f->p, f->len, tmp))
        {
            return FALSE;
        }
    }
    /* one event per timestamp, in page order */
    for (k = 0; k < (int)nanchor; k++)
    {
        EeAuditEvent e;
        int col;
        size_t cap = c->cap_events;
        memset(&e, 0, sizeof(e));
        e.page = page_no;
        e.ts = pg->anchor_ts[k];
        for (col = 0; col < AU_NCOLS; col++)
        {
            uint32_t id;
            if (col == AU_COL_TIME)
            {
                continue;
            }
            f->len = 0;
            if (f->p != NULL)
            {
                f->p[0] = '\0';
            }
            for (i = 0; i < pg->nruns; i++)
            {
                const AuRun *r = &pg->runs[i];
                if (r->owner == k && r->column == col && !au_str_join(f, r->text, r->len))
                {
                    return FALSE;
                }
            }
            if (col == AU_COL_DESC)
            {
                id = au_pool_add(c, f->len > 0 ? f->p : "", f->len);
                e.desc = id;
            }
            else
            {
                id = au_intern(c, f->len > 0 ? f->p : "", f->len);
                if (col == AU_COL_EVENT)
                {
                    e.level = id;
                }
                else if (col == AU_COL_USER)
                {
                    e.user = id;
                }
                else
                {
                    e.module = id;
                }
            }
            if (id == UINT32_MAX)
            {
                return FALSE;
            }
        }
        if (!au_grow((void **)&c->events, &cap, (size_t)c->nevents + 1, sizeof(EeAuditEvent)))
        {
            return FALSE;
        }
        c->cap_events = (uint32_t)cap;
        c->events[c->nevents++] = e;
    }
    return TRUE;
}

/* -------------------------------------------------------------------------- */
/* Public                                                                     */
/* -------------------------------------------------------------------------- */

BOOL EeAudit_IsAuditPdf(const wchar_t *path)
{
    EePdf *pdf = NULL;
    char creator[256];
    BOOL yes = FALSE;
    if (!EePdf_Open(path, &pdf, NULL, 0))
    {
        return FALSE;
    }
    if (EePdf_GetInfoText(pdf, "Creator", creator, sizeof(creator)) &&
        (strstr(creator, "AuditEventsReport") != NULL || strstr(creator, "Audit Events Report") != NULL))
    {
        yes = TRUE;
    }
    else if (EePdf_PageCount(pdf) > 0)
    {
        EePdfPageText pt;
        AuPage pg;
        EePdf_PageTextInit(&pt);
        memset(&pg, 0, sizeof(pg));
        if (EePdf_ExtractPageText(pdf, 0, &pt) && au_collect(&pg, &pt) && au_find_headings(&pg))
        {
            size_t i;
            for (i = 0; i < pg.nruns && !yes; i++)
            {
                yes = pg.runs[i].row < pg.heading_row && strstr(pg.runs[i].text, "Audit Events Report") != NULL;
            }
        }
        free(pg.runs);
        free(pg.buf);
        free(pg.anchor_row);
        free(pg.anchor_ts);
        EePdf_PageTextFree(&pt);
    }
    EePdf_Close(pdf);
    return yes;
}

EeLoadStatus EeAudit_LoadCopy(const wchar_t *path,
                              EeAuditCopy *out,
                              volatile LONG *cancel_flag,
                              EeLoadProgressFn progress_fn,
                              void *progress_user,
                              wchar_t *err,
                              size_t errcch)
{
    EePdf *pdf = NULL;
    EePdfPageText pt;
    AuPage pg;
    AuStr f, tmp;
    wchar_t perr[256] = L"";
    uint32_t p, npages;
    EeLoadStatus st = EeLoadStatus_Ok;

    EeAudit_InitCopy(out);
    if (!EePdf_Open(path, &pdf, perr, ARRAYSIZE(perr)))
    {
        au_set_err(err, errcch, L"%s could not be read as a PDF.", path);
        return EeLoadStatus_Error;
    }
    out->path = au_wcsdup(path);
    npages = EePdf_PageCount(pdf);
    out->pages = npages;
    EePdf_PageTextInit(&pt);
    memset(&pg, 0, sizeof(pg));
    memset(&f, 0, sizeof(f));
    memset(&tmp, 0, sizeof(tmp));
    for (p = 0; p < npages; p++)
    {
        if (cancel_flag != NULL && *cancel_flag != 0)
        {
            st = EeLoadStatus_Cancelled;
            break;
        }
        if (progress_fn != NULL && (p % 64) == 0)
        {
            EeLoadProgress pr;
            memset(&pr, 0, sizeof(pr));
            pr.percent = npages > 0 ? (uint32_t)((uint64_t)p * 100 / npages) : 0;
            pr.rows_loaded = out->nevents;
            pr.bytes_read = p;
            pr.bytes_total = npages;
            if (!progress_fn(&pr, progress_user))
            {
                st = EeLoadStatus_Cancelled;
                break;
            }
        }
        if (!EePdf_ExtractPageText(pdf, p, &pt) || !au_collect(&pg, &pt))
        {
            au_set_err(err, errcch, L"A page of %s could not be read.", path);
            st = EeLoadStatus_Error;
            break;
        }
        if (!au_find_headings(&pg))
        {
            if (p == 0)
            {
                au_set_err(err, errcch, L"%s is not an ES&S Electionware audit events report.", path);
                st = EeLoadStatus_Error;
                break;
            }
            if (!pg.have_cols)
            {
                continue;
            }
        }
        if (p == 0 && !au_read_header(out, &pg))
        {
            au_set_err(err, errcch, L"%s is not an ES&S Electionware audit events report.", path);
            st = EeLoadStatus_Error;
            break;
        }
        if (!au_page_events(out, &pg, p + 1, &f, &tmp))
        {
            au_set_err(err, errcch, L"Out of memory reading %s.", path);
            st = EeLoadStatus_Error;
            break;
        }
    }
    free(pg.runs);
    free(pg.buf);
    free(pg.anchor_row);
    free(pg.anchor_ts);
    free(f.p);
    free(tmp.p);
    EePdf_PageTextFree(&pt);
    EePdf_Close(pdf);
    if (st != EeLoadStatus_Ok)
    {
        EeAudit_FreeCopy(out);
    }
    return st;
}

BOOL EeAudit_AddEvent(EeAuditCopy *c, int64_t ts, const char *level, const char *desc, const char *user,
                      const char *module, uint32_t page)
{
    EeAuditEvent e;
    size_t cap = c->cap_events;
    memset(&e, 0, sizeof(e));
    e.ts = ts;
    e.page = page;
    e.desc = au_pool_add(c, desc != NULL ? desc : "", desc != NULL ? strlen(desc) : 0);
    e.level = au_intern(c, level != NULL ? level : "", level != NULL ? strlen(level) : 0);
    e.user = au_intern(c, user != NULL ? user : "", user != NULL ? strlen(user) : 0);
    e.module = au_intern(c, module != NULL ? module : "", module != NULL ? strlen(module) : 0);
    if (e.desc == UINT32_MAX || e.level == UINT32_MAX || e.user == UINT32_MAX || e.module == UINT32_MAX ||
        !au_grow((void **)&c->events, &cap, (size_t)c->nevents + 1, sizeof(EeAuditEvent)))
    {
        return FALSE;
    }
    c->cap_events = (uint32_t)cap;
    c->events[c->nevents++] = e;
    return TRUE;
}

/* -------------------------------------------------------------------------- */
/* Merging the copies of one log                                              */
/* -------------------------------------------------------------------------- */

/* Events are the same event when their timestamp, level, user and description agree; the
 * description's whitespace is ignored (a line can wrap at another word in another copy)
 * and so is the module. */
static uint32_t au_event_hash(const EeAuditCopy *c, const EeAuditEvent *e)
{
    const char *s = EeAudit_Desc(c, e);
    uint32_t h = 2166136261u;
    int i;
    for (i = 0; i < 8; i++)
    {
        h = (h ^ (unsigned char)(e->ts >> (i * 8))) * 16777619u;
    }
    for (; *s != '\0'; s++)
    {
        if (*s != ' ')
        {
            h = (h ^ (unsigned char)*s) * 16777619u;
        }
    }
    s = EeAudit_Str(c, e->user);
    h = au_hash(s, strlen(s)) ^ (h * 31u);
    s = EeAudit_Str(c, e->level);
    return au_hash(s, strlen(s)) ^ (h * 31u);
}

static BOOL au_same_text(const char *a, const char *b)
{
    for (;;)
    {
        while (*a == ' ')
        {
            a++;
        }
        while (*b == ' ')
        {
            b++;
        }
        if (*a != *b)
        {
            return FALSE;
        }
        if (*a == '\0')
        {
            return TRUE;
        }
        a++;
        b++;
    }
}

static BOOL au_same_event(const EeAuditCopy *ca, const EeAuditEvent *a, const EeAuditCopy *cb, const EeAuditEvent *b)
{
    return a->ts == b->ts && strcmp(EeAudit_Str(ca, a->level), EeAudit_Str(cb, b->level)) == 0 &&
           strcmp(EeAudit_Str(ca, a->user), EeAudit_Str(cb, b->user)) == 0 &&
           au_same_text(EeAudit_Desc(ca, a), EeAudit_Desc(cb, b));
}

const EeAuditEvent *EeAudit_MergedEvent(const EeAuditLog *log, uint32_t i, const EeAuditCopy **copy)
{
    const EeAuditMerged *m = &log->events[i];
    const EeAuditCopy *c = &log->copies[m->copy];
    if (copy != NULL)
    {
        *copy = c;
    }
    return &c->events[m->index];
}

const EeAuditLog *EeAudit_Log(const EeAuditElection *el, int kind)
{
    return &el->logs[kind == EE_AUDIT_ADMIN ? 1 : 0];
}

/* An earlier copy's event the merged list does not hold yet, with the merged event it
 * follows in that copy (-1: before every merged event). */
typedef struct AuInsert
{
    int64_t after;
    uint32_t index;
} AuInsert;

static int au_cmp_insert(const void *pa, const void *pb)
{
    const AuInsert *a = (const AuInsert *)pa;
    const AuInsert *b = (const AuInsert *)pb;
    if (a->after != b->after)
    {
        return a->after < b->after ? -1 : 1;
    }
    return a->index < b->index ? -1 : (a->index > b->index ? 1 : 0);
}

/* Merge copy @p ci (printed before every copy merged so far) into log->events. Its events
 * that match keep the merged order; the others go in after the merged event that precedes
 * them in this copy, so each copy's own order is kept. */
static BOOL au_merge_copy(EeAuditLog *log, uint32_t ci)
{
    const EeAuditCopy *c = &log->copies[ci];
    uint32_t n = log->nevents, hcap = 1, i;
    uint32_t *head = NULL, *next = NULL, *tail = NULL;
    AuInsert *ins = NULL;
    uint32_t nins = 0;
    EeAuditMerged *merged = NULL;
    int64_t last = -1;
    BOOL ok = FALSE;
    uint64_t bit = (uint64_t)1 << ci;

    while (hcap < n * 2 + 16)
    {
        hcap *= 2;
    }
    head = (uint32_t *)malloc(hcap * sizeof(uint32_t));
    tail = (uint32_t *)malloc(hcap * sizeof(uint32_t));
    next = (uint32_t *)malloc(((size_t)n + 1) * sizeof(uint32_t));
    ins = (AuInsert *)malloc(((size_t)c->nevents + 1) * sizeof(AuInsert));
    if (head == NULL || tail == NULL || next == NULL || ins == NULL)
    {
        goto done;
    }
    memset(head, 0xFF, hcap * sizeof(uint32_t));
    for (i = 0; i < n; i++) /* chains in merged order */
    {
        const EeAuditCopy *mc;
        const EeAuditEvent *me = EeAudit_MergedEvent(log, i, &mc);
        uint32_t h = au_event_hash(mc, me) & (hcap - 1);
        next[i] = UINT32_MAX;
        if (head[h] == UINT32_MAX)
        {
            head[h] = i;
        }
        else
        {
            next[tail[h]] = i;
        }
        tail[h] = i;
    }
    for (i = 0; i < c->nevents; i++)
    {
        const EeAuditEvent *e = &c->events[i];
        uint32_t j, first_free = UINT32_MAX, pick = UINT32_MAX;
        for (j = head[au_event_hash(c, e) & (hcap - 1)]; j != UINT32_MAX; j = next[j])
        {
            const EeAuditCopy *mc;
            const EeAuditEvent *me;
            if ((log->events[j].copies & bit) != 0)
            {
                continue;
            }
            me = EeAudit_MergedEvent(log, j, &mc);
            if (!au_same_event(c, e, mc, me))
            {
                continue;
            }
            if (first_free == UINT32_MAX)
            {
                first_free = j;
            }
            if ((int64_t)j > last)
            {
                pick = j;
                break;
            }
        }
        if (pick == UINT32_MAX)
        {
            pick = first_free;
        }
        if (pick != UINT32_MAX)
        {
            log->events[pick].copies |= bit;
            log->events[pick].copy = ci;
            log->events[pick].index = i;
            last = pick;
        }
        else
        {
            ins[nins].after = last;
            ins[nins].index = i;
            nins++;
        }
    }
    if (nins > 0)
    {
        uint32_t k = 0, o = 0;
        int64_t b;
        merged = (EeAuditMerged *)malloc(((size_t)n + nins) * sizeof(EeAuditMerged));
        if (merged == NULL)
        {
            goto done;
        }
        qsort(ins, nins, sizeof(AuInsert), au_cmp_insert);
        for (b = -1; b < (int64_t)n; b++)
        {
            if (b >= 0)
            {
                merged[o++] = log->events[b];
            }
            while (k < nins && ins[k].after == b)
            {
                merged[o].copy = ci;
                merged[o].index = ins[k].index;
                merged[o].copies = bit;
                o++;
                k++;
            }
        }
        free(log->events);
        log->events = merged;
        log->nevents = o;
        merged = NULL;
    }
    ok = TRUE;
done:
    free(head);
    free(tail);
    free(next);
    free(ins);
    free(merged);
    return ok;
}

static void au_check_copies(EeAuditLog *log)
{
    uint32_t k, i;
    for (k = 0; k < log->ncopies; k++)
    {
        const EeAuditCopy *c = &log->copies[k];
        EeAuditCopyCheck *ck = &log->checks[k];
        memset(ck, 0, sizeof(*ck));
        ck->events = c->nevents;
        for (i = 0; i < c->nevents; i++)
        {
            int64_t t = c->events[i].ts;
            if (i == 0 || t < ck->first_ts)
            {
                ck->first_ts = t;
            }
            if (i == 0 || t > ck->last_ts)
            {
                ck->last_ts = t;
            }
        }
    }
    for (i = 0; i < log->nevents; i++)
    {
        const EeAuditMerged *m = &log->events[i];
        int64_t t = EeAudit_MergedEvent(log, i, NULL)->ts;
        for (k = 0; k < log->ncopies; k++)
        {
            uint64_t bit = (uint64_t)1 << k;
            uint64_t earlier = bit - 1;
            if ((m->copies & bit) != 0)
            {
                if ((m->copies & earlier) == 0)
                {
                    log->checks[k].new_events++;
                }
            }
            else if ((m->copies & earlier) != 0)
            {
                if (t < log->checks[k].first_ts || t > log->checks[k].last_ts)
                {
                    log->checks[k].lacks_outside++;
                }
                else
                {
                    log->checks[k].lacks_missing++;
                }
            }
        }
    }
}

static int au_cmp_copy(const void *pa, const void *pb)
{
    const EeAuditCopy *a = (const EeAuditCopy *)pa;
    const EeAuditCopy *b = (const EeAuditCopy *)pb;
    if (a->printed != b->printed)
    {
        return a->printed < b->printed ? -1 : 1;
    }
    return a->nevents < b->nevents ? -1 : (a->nevents > b->nevents ? 1 : 0);
}

/* Merge a log whose copies are in place: order them by print time, start from the last
 * printed and merge the earlier ones into it. */
static BOOL au_build_log(EeAuditLog *log)
{
    uint32_t i, last;
    if (log->ncopies == 0)
    {
        return TRUE;
    }
    qsort(log->copies, log->ncopies, sizeof(EeAuditCopy), au_cmp_copy);
    log->checks = (EeAuditCopyCheck *)calloc(log->ncopies, sizeof(EeAuditCopyCheck));
    last = log->ncopies - 1;
    log->events = (EeAuditMerged *)malloc(((size_t)log->copies[last].nevents + 1) * sizeof(EeAuditMerged));
    if (log->checks == NULL || log->events == NULL)
    {
        return FALSE;
    }
    for (i = 0; i < log->copies[last].nevents; i++)
    {
        log->events[i].copy = last;
        log->events[i].index = i;
        log->events[i].copies = (uint64_t)1 << last;
    }
    log->nevents = log->copies[last].nevents;
    for (i = last; i-- > 0;)
    {
        if (!au_merge_copy(log, i))
        {
            return FALSE;
        }
    }
    for (i = 1; i < log->ncopies; i++)
    {
        const char *a = log->copies[0].election, *b = log->copies[i].election;
        if ((a == NULL) != (b == NULL) || (a != NULL && strcmp(a, b) != 0))
        {
            log->names_differ = TRUE;
        }
    }
    au_check_copies(log);
    return TRUE;
}

/* "Dallas County" and "Dallas" name one customer. */
static BOOL au_same_customer(const char *a, const char *b)
{
    size_t na, nb;
    if (a == NULL || b == NULL)
    {
        return a == b;
    }
    na = strlen(a);
    nb = strlen(b);
    if (na > 7 && _stricmp(a + na - 7, " County") == 0)
    {
        na -= 7;
    }
    if (nb > 7 && _stricmp(b + nb - 7, " County") == 0)
    {
        nb -= 7;
    }
    return na == nb && _strnicmp(a, b, na) == 0;
}

static void au_free_log(EeAuditLog *log)
{
    uint32_t i;
    for (i = 0; i < log->ncopies; i++)
    {
        EeAudit_FreeCopy(&log->copies[i]);
    }
    free(log->copies);
    free(log->checks);
    free(log->events);
    memset(log, 0, sizeof(*log));
}

void EeAudit_FreeElections(EeAuditElection *els, uint32_t count)
{
    uint32_t i;
    if (els == NULL)
    {
        return;
    }
    for (i = 0; i < count; i++)
    {
        au_free_log(&els[i].logs[0]);
        au_free_log(&els[i].logs[1]);
        free(els[i].customer);
        free(els[i].election);
    }
    free(els);
}

static int au_cmp_election(const void *pa, const void *pb)
{
    const EeAuditElection *a = (const EeAuditElection *)pa;
    const EeAuditElection *b = (const EeAuditElection *)pb;
    return a->election_date < b->election_date ? -1 : (a->election_date > b->election_date ? 1 : 0);
}

/* The election name most of the election's copies show. */
static char *au_common_name(const EeAuditElection *el)
{
    const char *best = NULL;
    uint32_t best_n = 0, l, i, l2, j;
    for (l = 0; l < 2; l++)
    {
        for (i = 0; i < el->logs[l].ncopies; i++)
        {
            const char *s = el->logs[l].copies[i].election;
            uint32_t cnt = 0;
            if (s == NULL)
            {
                continue;
            }
            for (l2 = 0; l2 < 2; l2++)
            {
                for (j = 0; j < el->logs[l2].ncopies; j++)
                {
                    const char *t = el->logs[l2].copies[j].election;
                    cnt += t != NULL && strcmp(s, t) == 0;
                }
            }
            if (cnt > best_n)
            {
                best = s;
                best_n = cnt;
            }
        }
    }
    return best != NULL ? au_strdup_n(best, strlen(best)) : NULL;
}

BOOL EeAudit_GroupCopies(EeAuditCopy *copies, uint32_t ncopies, EeAuditElection **out, uint32_t *out_count)
{
    EeAuditElection *els = NULL;
    uint32_t nels = 0, i, g;
    BOOL ok = TRUE;

    *out = NULL;
    *out_count = 0;
    els = (EeAuditElection *)calloc(ncopies + 1, sizeof(EeAuditElection));
    if (els == NULL)
    {
        for (i = 0; i < ncopies; i++)
        {
            EeAudit_FreeCopy(&copies[i]);
        }
        return FALSE;
    }
    /* first the groups and their sizes */
    for (i = 0; i < ncopies; i++)
    {
        for (g = 0; g < nels; g++)
        {
            if (els[g].election_date == copies[i].election_date &&
                au_same_customer(els[g].customer, copies[i].customer))
            {
                break;
            }
        }
        if (g == nels)
        {
            els[g].election_date = copies[i].election_date;
            els[g].customer =
                copies[i].customer != NULL ? au_strdup_n(copies[i].customer, strlen(copies[i].customer)) : NULL;
            nels++;
        }
        els[g].logs[copies[i].kind == EE_AUDIT_ADMIN ? 1 : 0].ncopies++;
    }
    for (g = 0; g < nels && ok; g++)
    {
        int l;
        for (l = 0; l < 2; l++)
        {
            EeAuditLog *log = &els[g].logs[l];
            if (log->ncopies > EE_AUDIT_MAX_COPIES)
            {
                ok = FALSE;
            }
            else if (log->ncopies > 0)
            {
                log->kind = l == 1 ? EE_AUDIT_ADMIN : EE_AUDIT_ELECTION;
                log->copies = (EeAuditCopy *)calloc(log->ncopies, sizeof(EeAuditCopy));
                ok = ok && log->copies != NULL;
                log->ncopies = 0; /* refilled below */
            }
        }
    }
    /* then move each copy into its log (the copy's buffers change owner) */
    for (i = 0; i < ncopies; i++)
    {
        EeAuditLog *log = NULL;
        for (g = 0; g < nels && ok; g++)
        {
            if (els[g].election_date == copies[i].election_date &&
                au_same_customer(els[g].customer, copies[i].customer))
            {
                log = &els[g].logs[copies[i].kind == EE_AUDIT_ADMIN ? 1 : 0];
                break;
            }
        }
        if (log != NULL && log->copies != NULL)
        {
            log->copies[log->ncopies++] = copies[i];
            EeAudit_InitCopy(&copies[i]);
        }
        else
        {
            EeAudit_FreeCopy(&copies[i]);
        }
    }
    for (g = 0; g < nels && ok; g++)
    {
        ok = au_build_log(&els[g].logs[0]) && au_build_log(&els[g].logs[1]);
        els[g].election = au_common_name(&els[g]);
    }
    if (!ok)
    {
        EeAudit_FreeElections(els, nels);
        return FALSE;
    }
    qsort(els, nels, sizeof(EeAuditElection), au_cmp_election);
    *out = els;
    *out_count = nels;
    return TRUE;
}

/* Progress over several files: each file's pages count toward one total. */
typedef struct AuFilesProgress
{
    EeLoadProgressFn fn;
    void *user;
    uint64_t done;  /* bytes of the files before this one */
    uint64_t size;  /* this file's bytes */
    uint64_t total; /* all files' bytes */
} AuFilesProgress;

static BOOL au_files_progress(const EeLoadProgress *p, void *user)
{
    AuFilesProgress *fp = (AuFilesProgress *)user;
    EeLoadProgress q = *p;
    uint64_t at = fp->done + (p->bytes_total > 0 ? fp->size * p->bytes_read / p->bytes_total : 0);
    q.percent = fp->total > 0 ? (uint32_t)(at * 100 / fp->total) : 0;
    q.bytes_read = at;
    q.bytes_total = fp->total;
    return fp->fn(&q, fp->user);
}

static uint64_t au_file_size(const wchar_t *path)
{
    WIN32_FILE_ATTRIBUTE_DATA fa;
    if (!GetFileAttributesExW(path, GetFileExInfoStandard, &fa))
    {
        return 0;
    }
    return ((uint64_t)fa.nFileSizeHigh << 32) | fa.nFileSizeLow;
}

EeLoadStatus EeAudit_LoadFiles(const wchar_t *const *paths,
                               uint32_t count,
                               EeAuditElection **out,
                               uint32_t *out_count,
                               volatile LONG *cancel_flag,
                               EeLoadProgressFn progress_fn,
                               void *progress_user,
                               wchar_t *err,
                               size_t errcch)
{
    EeAuditCopy *copies;
    AuFilesProgress fp;
    uint32_t i, n = 0;
    size_t cap = (size_t)count + 1;
    EeLoadStatus st = EeLoadStatus_Ok;

    *out = NULL;
    *out_count = 0;
    copies = (EeAuditCopy *)calloc(cap, sizeof(EeAuditCopy));
    if (copies == NULL)
    {
        au_set_err(err, errcch, L"Out of memory.%s", L"");
        return EeLoadStatus_Error;
    }
    memset(&fp, 0, sizeof(fp));
    fp.fn = progress_fn;
    fp.user = progress_user;
    for (i = 0; i < count; i++)
    {
        fp.total += au_file_size(paths[i]);
    }
    for (i = 0; i < count && st == EeLoadStatus_Ok; i++)
    {
        fp.size = au_file_size(paths[i]);
        if (EeAudit_IsExportPath(paths[i]))
        {
            /* a reloadable export: its copies join the PDFs' */
            EeAuditCopy *more = NULL;
            uint32_t nmore = 0, k;
            st = EeAudit_LoadExport(paths[i], &more, &nmore, cancel_flag,
                                    progress_fn != NULL ? au_files_progress : NULL, &fp, err, errcch);
            if (st == EeLoadStatus_Ok &&
                !au_grow((void **)&copies, &cap, (size_t)n + nmore + (count - i), sizeof(EeAuditCopy)))
            {
                au_set_err(err, errcch, L"Out of memory.%s", L"");
                st = EeLoadStatus_Error;
            }
            for (k = 0; k < nmore; k++)
            {
                if (st == EeLoadStatus_Ok)
                {
                    copies[n++] = more[k];
                }
                else
                {
                    EeAudit_FreeCopy(&more[k]);
                }
            }
            free(more);
        }
        else
        {
            st = EeAudit_LoadCopy(paths[i], &copies[n], cancel_flag, progress_fn != NULL ? au_files_progress : NULL,
                                  &fp, err, errcch);
            if (st == EeLoadStatus_Ok)
            {
                n++;
            }
        }
        fp.done += fp.size;
    }
    if (st != EeLoadStatus_Ok)
    {
        for (i = 0; i < n; i++)
        {
            EeAudit_FreeCopy(&copies[i]);
        }
        free(copies);
        return st;
    }
    if (!EeAudit_GroupCopies(copies, n, out, out_count))
    {
        free(copies);
        au_set_err(err, errcch, L"Out of memory merging the audit logs.%s", L"");
        return EeLoadStatus_Error;
    }
    free(copies);
    return EeLoadStatus_Ok;
}

/* -------------------------------------------------------------------------- */
/* Ballot accounting                                                          */
/* -------------------------------------------------------------------------- */

static char au_lower(char c)
{
    return c >= 'A' && c <= 'Z' ? (char)(c - 'A' + 'a') : c;
}

/* @p word in @p s, any case. */
static const char *au_find_ci(const char *s, const char *word)
{
    size_t n = strlen(word);
    for (; *s != '\0'; s++)
    {
        size_t i;
        for (i = 0; i < n && s[i] != '\0' && au_lower(s[i]) == au_lower(word[i]); i++)
        {
        }
        if (i == n)
        {
            return s;
        }
    }
    return NULL;
}

/* @p token in @p s as a word of its own ("EV" in "EV_PfISD Rock Gym", not in "Every"). */
static BOOL au_has_token(const char *s, const char *token)
{
    const char *p = s;
    size_t n = strlen(token);
    while ((p = au_find_ci(p, token)) != NULL)
    {
        if ((p == s || !au_is_alpha(p[-1])) && !au_is_alpha(p[n]))
        {
            return TRUE;
        }
        p++;
    }
    return FALSE;
}

int EeAudit_VoteTypeOf(const char *place)
{
    if (place == NULL)
    {
        return EE_AUDIT_VT_OTHER;
    }
    if (au_find_ci(place, "provisional") != NULL || au_has_token(place, "prov"))
    {
        return EE_AUDIT_VT_PROVISIONAL;
    }
    if (au_find_ci(place, "mail") != NULL || au_has_token(place, "BBM") || au_find_ci(place, "absentee") != NULL)
    {
        return EE_AUDIT_VT_MAIL;
    }
    if (au_find_ci(place, "early") != NULL || au_has_token(place, "EV"))
    {
        return EE_AUDIT_VT_EARLY;
    }
    if (au_find_ci(place, "election day") != NULL || au_has_token(place, "ED") ||
        au_find_ci(place, "vote center") != NULL)
    {
        return EE_AUDIT_VT_ELECTION_DAY;
    }
    return EE_AUDIT_VT_OTHER;
}

const wchar_t *EeAudit_VoteTypeName(int vote_type)
{
    switch (vote_type)
    {
    case EE_AUDIT_VT_EARLY:
        return L"Early voting";
    case EE_AUDIT_VT_ELECTION_DAY:
        return L"Election Day";
    case EE_AUDIT_VT_MAIL:
        return L"Mail";
    case EE_AUDIT_VT_PROVISIONAL:
        return L"Provisional";
    default:
        return L"Other";
    }
}

BOOL EeAudit_IsTestDeck(const char *name)
{
    return name != NULL &&
           (au_find_ci(name, "logic") != NULL || au_find_ci(name, "accuracy") != NULL ||
            au_find_ci(name, "acurracy") != NULL || au_has_token(name, "LA") || au_find_ci(name, "L&A") != NULL ||
            au_has_token(name, "test"));
}

/* A number with thousands commas ("3,454"); FALSE when none starts at @p s. */
static BOOL au_number(const char *s, int64_t *out, const char **end)
{
    int64_t v = 0;
    BOOL any = FALSE;
    while ((*s >= '0' && *s <= '9') || (*s == ',' && any && s[1] >= '0' && s[1] <= '9'))
    {
        if (*s != ',')
        {
            v = v * 10 + (*s - '0');
            any = TRUE;
        }
        s++;
    }
    *out = v;
    if (end != NULL)
    {
        *end = s;
    }
    return any;
}

/* "<prefix><name> ID <id> Number of Ballots <n>[ Revision <r>]". */
static BOOL au_parse_batch(const char *d, const char *prefix, char **name, uint32_t *id, int64_t *ballots,
                           uint32_t *revision)
{
    size_t np = strlen(prefix);
    const char *idp, *nb, *p;
    int64_t v;
    if (strncmp(d, prefix, np) != 0)
    {
        return FALSE;
    }
    nb = strstr(d + np, " Number of Ballots ");
    if (nb == NULL)
    {
        return FALSE;
    }
    idp = NULL;
    for (p = d + np; (p = strstr(p, " ID ")) != NULL && p < nb; p++)
    {
        idp = p; /* the last " ID " before the count */
    }
    if (idp == NULL || !au_number(idp + 4, &v, &p) || p != nb)
    {
        return FALSE;
    }
    *id = (uint32_t)v;
    if (!au_number(nb + 19, ballots, &p))
    {
        return FALSE;
    }
    *revision = 0;
    if (strncmp(p, " Revision ", 10) == 0 && au_number(p + 10, &v, NULL))
    {
        *revision = (uint32_t)v;
    }
    *name = au_strdup_n(d + np, (size_t)(idp - (d + np)));
    return *name != NULL;
}

/* Split a batch name into poll place and media: "A0013-EV Vote Center - 33" -> "EV Vote
 * Center", 33, and central (the "A0013-" prefix of a central count batch). */
static char *au_batch_place(const char *name, int *media, BOOL *central)
{
    const char *s = name, *e = name + strlen(name), *dash;
    *media = -1;
    *central = FALSE;
    if (s[0] >= 'A' && s[0] <= 'Z' && s[1] >= '0' && s[1] <= '9' && s[2] >= '0' && s[2] <= '9' && s[3] >= '0' &&
        s[3] <= '9' && s[4] >= '0' && s[4] <= '9' && s[5] == '-')
    {
        s += 6;
        *central = TRUE;
    }
    for (dash = e; dash > s + 3; dash--)
    {
        if (dash[-3] == ' ' && dash[-2] == '-' && dash[-1] == ' ')
        {
            break;
        }
    }
    if (dash > s + 3)
    {
        const char *q = dash;
        int64_t v;
        if (au_number(q, &v, &q) && q == e && v < 1000000)
        {
            *media = (int)v;
            e = dash - 3;
        }
    }
    return au_strdup_n(s, (size_t)(e - s));
}

/* Media number from "Electionware election results for Central Count Media-86 will be
 * cleared": -1 when the warning is not a media clear. @p device gets 1 for Central Count,
 * 2 for Poll Place, 0 when it names neither. */
static int au_clear_media(const char *d, int *device)
{
    const char *m;
    int64_t v;
    *device = 0;
    if (strstr(d, " will be cleared") == NULL || (m = strstr(d, "Media-")) == NULL || !au_number(m + 6, &v, NULL))
    {
        return -1;
    }
    if (strstr(d, "Central Count") != NULL && strstr(d, "Central Count") < m)
    {
        *device = 1;
    }
    else if (strstr(d, "Poll Place") != NULL && strstr(d, "Poll Place") < m)
    {
        *device = 2;
    }
    return (int)v;
}

/* A batch a clear of @p device (au_clear_media) can reach. */
static BOOL au_on_device(const EeAuditBatch *b, int device)
{
    return device == 0 || (device == 1) == (b->central != FALSE);
}

void EeAudit_FreeAccounting(EeAuditAccounting *a)
{
    uint32_t i;
    if (a == NULL)
    {
        return;
    }
    for (i = 0; i < a->nbatches; i++)
    {
        free(a->batches[i].name);
        free(a->batches[i].place);
    }
    for (i = 0; i < a->nclears; i++)
    {
        free(a->clears[i].place);
    }
    free(a->batches);
    free(a->sessions);
    free(a->clears);
    memset(a, 0, sizeof(*a));
}

typedef struct AuReplay
{
    EeAuditAccounting *a;
    size_t cap_batches, cap_sessions, cap_clears;
    uint32_t *sheet_ids; /* open-addressing set of sheet IDs (+1) */
    uint32_t sheet_cap;
} AuReplay;

static BOOL au_new_session(AuReplay *rp, int64_t ts, BOOL after_clear)
{
    EeAuditAccounting *a = rp->a;
    EeAuditSession *s;
    if (!au_grow((void **)&a->sessions, &rp->cap_sessions, (size_t)a->nsessions + 1, sizeof(EeAuditSession)))
    {
        return FALSE;
    }
    s = &a->sessions[a->nsessions++];
    memset(s, 0, sizeof(*s));
    s->start = ts;
    s->end = ts;
    s->after_clear = after_clear;
    s->first_batch = a->nbatches;
    s->first_clear = a->nclears;
    return TRUE;
}

static BOOL au_sheet_seen(AuReplay *rp, uint32_t id)
{
    uint32_t i;
    if (rp->sheet_cap == 0 || (rp->a->sheet_error_ids + 1) * 2 > rp->sheet_cap)
    {
        uint32_t ncap = rp->sheet_cap != 0 ? rp->sheet_cap * 2 : 256, k;
        uint32_t *nt = (uint32_t *)calloc(ncap, sizeof(uint32_t));
        if (nt == NULL)
        {
            return FALSE;
        }
        for (k = 0; k < rp->sheet_cap; k++)
        {
            if (rp->sheet_ids[k] != 0)
            {
                uint32_t j = (rp->sheet_ids[k] * 2654435761u) & (ncap - 1);
                while (nt[j] != 0)
                {
                    j = (j + 1) & (ncap - 1);
                }
                nt[j] = rp->sheet_ids[k];
            }
        }
        free(rp->sheet_ids);
        rp->sheet_ids = nt;
        rp->sheet_cap = ncap;
    }
    for (i = ((id + 1) * 2654435761u) & (rp->sheet_cap - 1); rp->sheet_ids[i] != 0; i = (i + 1) & (rp->sheet_cap - 1))
    {
        if (rp->sheet_ids[i] == id + 1)
        {
            return TRUE;
        }
    }
    rp->sheet_ids[i] = id + 1;
    rp->a->sheet_error_ids++;
    return TRUE;
}

/* The poll place of the first batch committed after merged event @p from (oldest first
 * = decreasing index), past any further media clears; NULL when a full clear or the end
 * of the log comes first. Returned string is malloc'd. */
static char *au_next_place(const EeAuditLog *log, uint32_t from)
{
    uint32_t i;
    for (i = from; i-- > 0;)
    {
        const EeAuditCopy *c;
        const EeAuditEvent *e = EeAudit_MergedEvent(log, i, &c);
        const char *d = EeAudit_Desc(c, e);
        char *name;
        uint32_t id, rev;
        int64_t n;
        int media;
        BOOL central;
        if (au_parse_batch(d, "Committing new Batch ", &name, &id, &n, &rev))
        {
            char *place = au_batch_place(name, &media, &central);
            free(name);
            return place;
        }
        if (strncmp(d, "All Electionware election results will be cleared", 50) == 0)
        {
            return NULL;
        }
    }
    return NULL;
}

/* Apply a media clear to the current session's loaded batches. */
static BOOL au_media_clear(AuReplay *rp, const EeAuditLog *log, uint32_t ev, int64_t ts, int media, int device)
{
    EeAuditAccounting *a = rp->a;
    EeAuditSession *s = &a->sessions[a->nsessions - 1];
    EeAuditMediaClear *mc;
    char *next = au_next_place(log, ev);
    const char *only = NULL, *target = NULL;
    uint32_t i, places = 0;
    if (!au_grow((void **)&a->clears, &rp->cap_clears, (size_t)a->nclears + 1, sizeof(EeAuditMediaClear)))
    {
        free(next);
        return FALSE;
    }
    mc = &a->clears[a->nclears++];
    memset(mc, 0, sizeof(*mc));
    mc->event = ev;
    mc->ts = ts;
    mc->media = media;
    mc->device = device;
    mc->session = a->nsessions - 1;
    s->nclears++;
    /* the poll places with loaded batches on this media number */
    for (i = s->first_batch; i < a->nbatches; i++)
    {
        const EeAuditBatch *b = &a->batches[i];
        uint32_t j;
        BOOL seen = FALSE;
        if (b->status != EE_AUDIT_BATCH_COUNTED || b->media != media || !au_on_device(b, device))
        {
            continue;
        }
        mc->all_ballots += b->ballots;
        for (j = s->first_batch; j < i && !seen; j++)
        {
            const EeAuditBatch *o = &a->batches[j];
            seen = o->status == EE_AUDIT_BATCH_COUNTED && o->media == media && au_on_device(o, device) &&
                   strcmp(o->place, b->place) == 0;
        }
        if (!seen)
        {
            places++;
            only = b->place;
            if (next != NULL && strcmp(next, b->place) == 0)
            {
                target = b->place;
            }
        }
    }
    mc->places = places;
    if (target == NULL && places == 1)
    {
        target = only;
    }
    if (target != NULL)
    {
        mc->place = au_strdup_n(target, strlen(target));
        if (mc->place == NULL)
        {
            free(next);
            return FALSE;
        }
        for (i = s->first_batch; i < a->nbatches; i++)
        {
            EeAuditBatch *b = &a->batches[i];
            if (b->status == EE_AUDIT_BATCH_COUNTED && b->media == media && au_on_device(b, device) &&
                strcmp(b->place, mc->place) == 0)
            {
                b->status = EE_AUDIT_BATCH_CLEARED;
                b->clear = a->nclears - 1;
                mc->removed++;
                mc->removed_ballots += b->ballots;
            }
        }
    }
    free(next);
    return TRUE;
}

/* A commit or update. A batch ID committed again in the session replaces the earlier one;
 * an update revises it. */
static BOOL au_add_batch(AuReplay *rp, uint32_t ev, const EeAuditEvent *e, char *name, uint32_t id, int64_t ballots,
                         uint32_t revision, BOOL update)
{
    EeAuditAccounting *a = rp->a;
    EeAuditSession *s = &a->sessions[a->nsessions - 1];
    EeAuditBatch *b;
    uint32_t i;
    for (i = a->nbatches; i-- > s->first_batch;)
    {
        EeAuditBatch *o = &a->batches[i];
        if (o->id == id && o->status == EE_AUDIT_BATCH_COUNTED)
        {
            if (update)
            {
                o->ballots = ballots;
                o->revision = revision;
                o->event = ev;
                o->ts = e->ts;
                free(name);
                return TRUE;
            }
            o->status = EE_AUDIT_BATCH_REPLACED;
            break;
        }
    }
    if (!au_grow((void **)&a->batches, &rp->cap_batches, (size_t)a->nbatches + 1, sizeof(EeAuditBatch)))
    {
        free(name);
        return FALSE;
    }
    b = &a->batches[a->nbatches];
    memset(b, 0, sizeof(*b));
    b->event = ev;
    b->id = id;
    b->name = name;
    b->place = au_batch_place(name, &b->media, &b->central);
    if (b->place == NULL)
    {
        free(name);
        return FALSE;
    }
    b->ballots = ballots;
    b->ts = e->ts;
    b->session = a->nsessions - 1;
    b->status = EE_AUDIT_BATCH_COUNTED;
    b->vote_type = EeAudit_VoteTypeOf(b->place);
    b->test = EeAudit_IsTestDeck(b->place);
    b->revision = revision;
    a->nbatches++;
    s->nbatches++;
    return TRUE;
}

static void au_finish_sessions(EeAuditAccounting *a)
{
    uint32_t si, i;
    a->election_session = -1;
    for (si = 0; si < a->nsessions; si++)
    {
        EeAuditSession *s = &a->sessions[si];
        BOOL real = FALSE;
        for (i = s->first_batch; i < s->first_batch + s->nbatches; i++)
        {
            const EeAuditBatch *b = &a->batches[i];
            if (b->status != EE_AUDIT_BATCH_COUNTED)
            {
                continue;
            }
            s->counted++;
            s->ballots += b->ballots;
            s->ballots_vt[b->vote_type] += b->ballots;
            real = real || !b->test;
        }
        s->test = !real;
        if (real)
        {
            a->election_session = (int)si;
        }
    }
}

BOOL EeAudit_Accounting(const EeAuditLog *log, EeAuditAccounting *out)
{
    AuReplay rp;
    uint32_t i;
    int pending = -2; /* media of a clear warning awaiting its result; -1 = all results; -2 = none */
    int pending_device = 0;
    BOOL ok = TRUE;

    memset(out, 0, sizeof(*out));
    memset(&rp, 0, sizeof(rp));
    rp.a = out;
    out->election_session = -1;
    if (log == NULL || log->nevents == 0)
    {
        return TRUE;
    }
    ok = au_new_session(&rp, EeAudit_MergedEvent(log, log->nevents - 1, NULL)->ts, FALSE);
    for (i = log->nevents; ok && i-- > 0;) /* the merged log is newest first */
    {
        const EeAuditCopy *c;
        const EeAuditEvent *e = EeAudit_MergedEvent(log, i, &c);
        const char *d = EeAudit_Desc(c, e);
        char *name;
        uint32_t id, rev;
        int64_t n;
        out->sessions[out->nsessions - 1].end = e->ts;
        if (d[0] == 'C' && au_parse_batch(d, "Committing new Batch ", &name, &id, &n, &rev))
        {
            ok = au_add_batch(&rp, i, e, name, id, n, rev, FALSE);
        }
        else if (d[0] == 'U' && au_parse_batch(d, "Updating Batch ", &name, &id, &n, &rev))
        {
            ok = au_add_batch(&rp, i, e, name, id, n, rev, TRUE);
        }
        else if (strncmp(d, "All Electionware election results will be cleared", 50) == 0)
        {
            pending = -1;
        }
        else if (strncmp(d, "Electionware election results for ", 34) == 0 && au_clear_media(d, &pending_device) >= 0)
        {
            pending = au_clear_media(d, &pending_device);
        }
        else if (strncmp(d, "User Response: No", 17) == 0)
        {
            pending = -2; /* the clear was declined */
        }
        else if (strncmp(d, "Results successfully cleared", 28) == 0)
        {
            if (pending >= 0)
            {
                ok = au_media_clear(&rp, log, i, e->ts, pending, pending_device);
            }
            else
            {
                ok = au_new_session(&rp, e->ts, TRUE);
            }
            pending = -2;
        }
        else if (strstr(d, "previously loaded") != NULL)
        {
            out->previously_loaded++;
        }
        else if (strncmp(d, "An error occurred while loading Ballot Sheet ID ", 48) == 0 && au_number(d + 48, &n, NULL))
        {
            out->sheet_errors++;
            ok = au_sheet_seen(&rp, (uint32_t)n);
        }
    }
    free(rp.sheet_ids);
    if (!ok)
    {
        EeAudit_FreeAccounting(out);
        return FALSE;
    }
    au_finish_sessions(out);
    return TRUE;
}

/* -------------------------------------------------------------------------- */
/* Notable events                                                             */
/* -------------------------------------------------------------------------- */

static const struct
{
    int severity;
    const wchar_t *name;
    const wchar_t *meaning;
} k_au_notes[EE_AUDIT_N_COUNT] = {
    {EE_AUDIT_SEV_RED, L"Results output before Election Day",
     L"A results report was printed, saved to a file or exported before Election Day while ballots "
     L"(not test decks) were loaded. Ballots may be tabulated early, but results are not to be printed "
     L"or released before Election Day."},
    {EE_AUDIT_SEV_REVIEW, L"Ballots loaded early",
     L"Ballots (not test decks) were loaded more than 3 days before Election Day, earlier than "
     L"tabulation may start in a county of 100,000 or more. Test batches not named as test decks "
     L"also appear here."},
    {EE_AUDIT_SEV_REVIEW, L"Elections deleted",
     L"An election was deleted; its own audit log is deleted with it."},
    {EE_AUDIT_SEV_REVIEW, L"Failed restores and backups", L"A restore or a backup did not succeed."},
    {EE_AUDIT_SEV_REVIEW, L"Results validation errors",
     L"A results or media validation error while loading results (for example \"Results validation "
     L"error. Verify poll closed correctly\")."},
    {EE_AUDIT_SEV_REVIEW, L"Errors", L"Other events logged as Error or Fatal."},
    {EE_AUDIT_SEV_REVIEW, L"Ballot sheet load errors", L"A ballot sheet could not be loaded."},
    {EE_AUDIT_SEV_REVIEW, L"Media clears",
     L"The results of one media were cleared (see Ballot Accounting \x2192 Media Clears)."},
    {EE_AUDIT_SEV_REVIEW, L"Clock anomalies",
     L"An event's time is later than the event printed above it (the log is newest first): the "
     L"workstation's clock was set ahead, or the next one's behind."},
    {EE_AUDIT_SEV_INFO, L"Full clears",
     L"All results were cleared: a new session starts (see Ballot Accounting \x2192 Sessions)."},
    {EE_AUDIT_SEV_INFO, L"Zero-ballot batches", L"A batch (not a test deck) was committed with 0 ballots."},
    {EE_AUDIT_SEV_INFO, L"Previously loaded batches",
     L"A batch was reported as previously loaded and was not loaded again."},
    {EE_AUDIT_SEV_INFO, L"Results output before Election Day (testing)",
     L"A results report was printed, saved or exported before Election Day with only test decks "
     L"loaded (a logic and accuracy test) or no ballots counted (a zero report)."},
    {EE_AUDIT_SEV_INFO, L"Elections restored", L"An election was restored from a backup."},
    {EE_AUDIT_SEV_INFO, L"User accounts", L"A user account was created or updated, or passwords were viewed."}};

const wchar_t *EeAudit_NoteName(int category)
{
    return category >= 0 && category < EE_AUDIT_N_COUNT ? k_au_notes[category].name : L"";
}

const wchar_t *EeAudit_NoteMeaning(int category)
{
    return category >= 0 && category < EE_AUDIT_N_COUNT ? k_au_notes[category].meaning : L"";
}

int EeAudit_NoteSeverity(int category)
{
    return category >= 0 && category < EE_AUDIT_N_COUNT ? k_au_notes[category].severity : EE_AUDIT_SEV_INFO;
}

void EeAudit_FreeNotes(EeAuditNotes *n)
{
    if (n != NULL)
    {
        free(n->notes);
        free(n->other_election);
        memset(n, 0, sizeof(*n));
    }
}

static BOOL au_add_note(EeAuditNotes *n, size_t *cap, int category, int log, uint32_t event, uint32_t batches,
                        int64_t value)
{
    EeAuditNote *x;
    if (!au_grow((void **)&n->notes, cap, (size_t)n->count + 1, sizeof(EeAuditNote)))
    {
        return FALSE;
    }
    x = &n->notes[n->count++];
    x->category = category;
    x->log = log;
    x->event = event;
    x->batches = batches;
    x->value = value;
    n->per_category[category]++;
    return TRUE;
}

static BOOL au_ends_with(const char *s, const char *tail)
{
    size_t n = strlen(s), k = strlen(tail);
    return n >= k && strcmp(s + n - k, tail) == 0;
}

/* A results report printed, saved to a file or exported by the Reporting module. */
static BOOL au_is_results_output(const char *module, const char *d)
{
    return strcmp(module, "Reporting") == 0 &&
           (au_ends_with(d, " was printed successfully.") || au_ends_with(d, " was saved successfully.") ||
            strncmp(d, "Updated Plain Text Export Results", 33) == 0 ||
            strncmp(d, "Exporting Cast Vote Record 1 of", 31) == 0);
}

/* Midnight starting Election Day, or 0 when the header gave no date. */
static int64_t au_election_day(const EeAuditElection *el)
{
    int d = el->election_date;
    return d > 0 ? EeAudit_MakeTime(d / 10000, d / 100 % 100, d % 100, 0, 0, 0) : 0;
}

/* A clock anomaly: an event (newest first) later than the one printed above it by more than this. */
#define AU_CLOCK_SLACK 60

/* The election open at an Election log event. */
#define AU_ERA_THIS  0
#define AU_ERA_TEST  1 /* a test copy of the election (restored from it: same log) */
#define AU_ERA_OTHER 2 /* another election: the database's earlier life as a copy of it */

/* The date an election name gives ("March 3, 2026 Joint Primary Election" -> 20260303), or 0. */
static int au_name_date(const char *name)
{
    static const char *const months[] = {"January", "February", "March",     "April",   "May",      "June",
                                         "July",    "August",   "September", "October", "November", "December"};
    const char *s;
    for (s = name; *s != '\0'; s++)
    {
        int m;
        if (s != name && au_is_alpha(s[-1]))
        {
            continue;
        }
        for (m = 0; m < 12; m++)
        {
            size_t k = strlen(months[m]);
            if (strncmp(s, months[m], k) == 0 && s[k] == ' ')
            {
                const char *p = s + k + 1;
                int d = 0, y = 0, nd = 0;
                while (*p >= '0' && *p <= '9' && nd < 2)
                {
                    d = d * 10 + (*p++ - '0');
                    nd++;
                }
                if (nd == 0 || p[0] != ',' || p[1] != ' ')
                {
                    break;
                }
                p += 2;
                for (nd = 0; *p >= '0' && *p <= '9' && nd < 4; nd++)
                {
                    y = y * 10 + (*p++ - '0');
                }
                if (nd == 4 && d >= 1 && d <= 31 && y >= 1990 && y <= 2100)
                {
                    return y * 10000 + (m + 1) * 100 + d;
                }
                break;
            }
        }
    }
    return 0;
}

/* @p name is this election's: its name ("Election Opened: May 26, 2026 Joint Primary Runoff
 * Election" for a header cut to "May 26, 2026 Joint"), or a name without another election's
 * date (the same database while it was set up: "L26 Setup Election", "PR26 Prep Election"). */
static BOOL au_is_this_election(const EeAuditElection *el, const char *name)
{
    size_t n = el->election != NULL ? strlen(el->election) : 0;
    int date;
    if (n == 0 || strncmp(name, el->election, n) == 0)
    {
        return TRUE;
    }
    date = au_name_date(name);
    return date == 0 || el->election_date == 0 || date == el->election_date;
}

/* The election open at each merged Election log event (AU_ERA_*), from its "Election Opened:"
 * events; events before the first take its election. @p other_name receives the first other
 * election's name. NULL when out of memory. */
static uint8_t *au_election_eras(const EeAuditElection *el, char **other_name)
{
    const EeAuditLog *log = &el->logs[0];
    uint8_t *era = (uint8_t *)malloc((size_t)log->nevents + 1);
    BOOL known = FALSE;
    uint8_t cur = AU_ERA_THIS;
    uint32_t i, k;
    if (era == NULL)
    {
        return NULL;
    }
    for (i = log->nevents; i-- > 0;) /* oldest first */
    {
        const EeAuditCopy *c = NULL;
        const EeAuditEvent *e = EeAudit_MergedEvent(log, i, &c);
        const char *d = EeAudit_Desc(c, e);
        if (strncmp(d, "Election Opened: ", 17) == 0)
        {
            const char *nm = d + 17;
            cur = au_is_this_election(el, nm) ? AU_ERA_THIS : EeAudit_IsTestCopyName(nm) ? AU_ERA_TEST : AU_ERA_OTHER;
            if (cur == AU_ERA_OTHER && other_name != NULL && *other_name == NULL)
            {
                *other_name = au_strdup_n(nm, strlen(nm));
            }
            if (!known)
            {
                for (k = i + 1; k < log->nevents; k++)
                {
                    era[k] = cur; /* the older events before the first open */
                }
                known = TRUE;
            }
        }
        era[i] = cur;
    }
    return era;
}

static BOOL au_notes_election(const EeAuditElection *el, EeAuditNotes *n, size_t *cap)
{
    const EeAuditLog *log = &el->logs[0];
    int64_t eday = au_election_day(el);
    uint32_t i, real = 0, test = 0;
    int64_t real_ballots = 0;
    BOOL pending_full = FALSE, pending_media = FALSE;
    uint8_t *era = au_election_eras(el, &n->other_election);
    if (era == NULL)
    {
        return FALSE;
    }
    for (i = log->nevents; i-- > 0;) /* oldest first */
    {
        const EeAuditCopy *c;
        const EeAuditEvent *e = EeAudit_MergedEvent(log, i, &c);
        const char *d = EeAudit_Desc(c, e);
        const char *level = EeAudit_Str(c, e->level);
        const char *module = EeAudit_Str(c, e->module);
        int cat = -1;
        uint32_t batches = 0;
        int64_t value = 0;
        char *name = NULL;
        uint32_t id, rev;
        int64_t ballots;
        if (strncmp(d, "Committing new Batch ", 21) == 0 &&
            au_parse_batch(d, "Committing new Batch ", &name, &id, &ballots, &rev))
        {
            if (EeAudit_IsTestDeck(name))
            {
                test++;
            }
            else
            {
                real++;
                real_ballots += ballots;
                if (eday != 0 && ballots > 0 && e->ts < eday - (int64_t)EE_AUDIT_EARLY_COUNT_DAYS * 86400)
                {
                    cat = EE_AUDIT_N_LOADED_EARLY;
                    value = ballots;
                }
                else if (ballots == 0)
                {
                    cat = EE_AUDIT_N_ZERO_BATCHES;
                }
            }
            free(name);
        }
        else if (au_is_results_output(module, d))
        {
            if (eday != 0 && e->ts < eday)
            {
                /* zero totals (batches of 0 ballots, as a manual-entry test leaves) are no results */
                BOOL results = real > 0 && real_ballots > 0;
                cat = results ? EE_AUDIT_N_RESULTS_EARLY : EE_AUDIT_N_RESULTS_TEST;
                batches = results ? real : test;
                value = results ? real_ballots : 0;
            }
        }
        else if (strncmp(d, "All Electionware election results will be cleared", 50) == 0)
        {
            pending_full = TRUE;
            pending_media = FALSE;
        }
        else if (strncmp(d, "Electionware election results for ", 34) == 0 && strstr(d, " will be cleared") != NULL)
        {
            pending_media = TRUE;
            pending_full = FALSE;
        }
        else if (strncmp(d, "User Response: No", 17) == 0)
        {
            pending_full = pending_media = FALSE;
        }
        else if (strncmp(d, "Results successfully cleared", 28) == 0)
        {
            if (pending_media)
            {
                cat = EE_AUDIT_N_MEDIA_CLEARS;
            }
            else
            {
                cat = EE_AUDIT_N_FULL_CLEARS;
                real = test = 0;
                real_ballots = 0;
            }
            pending_full = pending_media = FALSE;
        }
        else if (strstr(d, "previously loaded") != NULL)
        {
            cat = EE_AUDIT_N_PREV_LOADED;
        }
        else if (strncmp(d, "An error occurred while loading Ballot Sheet ID ", 48) == 0)
        {
            cat = EE_AUDIT_N_SHEET_ERRORS;
        }
        else if (strcmp(module, "Reporting") == 0 &&
                 (au_find_ci(d, "validation error") != NULL || au_find_ci(d, "validation unsuccessful") != NULL ||
                  au_find_ci(d, "failed externally packaged media") != NULL ||
                  au_find_ci(d, "not closed or missing Poll Place collection") != NULL))
        {
            cat = EE_AUDIT_N_VALIDATION;
        }
        else if (_stricmp(level, "Error") == 0 || _stricmp(level, "Fatal") == 0)
        {
            cat = EE_AUDIT_N_ERRORS;
        }
        if (cat < 0 && i > 0)
        {
            /* the event printed above this one (i - 1) is newer: this one must not be later */
            const EeAuditCopy *cn;
            int64_t newer = EeAudit_MergedEvent(log, i - 1, &cn)->ts;
            if (e->ts > newer + AU_CLOCK_SLACK)
            {
                cat = EE_AUDIT_N_CLOCK;
                value = e->ts - newer;
            }
        }
        if (era[i] == AU_ERA_OTHER)
        {
            n->other_events++; /* another election's history: counted above, not noted */
            continue;
        }
        if (cat >= 0 && !au_add_note(n, cap, cat, 0, i, batches, value))
        {
            free(era);
            return FALSE;
        }
    }
    free(era);
    return TRUE;
}

static BOOL au_notes_admin(const EeAuditElection *el, EeAuditNotes *n, size_t *cap)
{
    const EeAuditLog *log = &el->logs[1];
    uint32_t i;
    for (i = log->nevents; i-- > 0;) /* oldest first */
    {
        const EeAuditCopy *c;
        const EeAuditEvent *e = EeAudit_MergedEvent(log, i, &c);
        const char *d = EeAudit_Desc(c, e);
        const char *level = EeAudit_Str(c, e->level);
        int cat = -1;
        int64_t value = 0;
        if (strncmp(d, "Election Deleted:", 17) == 0)
        {
            cat = EE_AUDIT_N_DELETED;
        }
        else if (strncmp(d, "Invalid code. Unable to decrypt restore package", 47) == 0 ||
                 strncmp(d, "An error occurred while creating backup", 39) == 0 ||
                 strncmp(d, "Unable to backup election", 25) == 0 ||
                 (au_find_ci(d, "restore") != NULL && au_find_ci(d, "fail") != NULL))
        {
            cat = EE_AUDIT_N_RESTORE_FAILED;
        }
        else if (_stricmp(level, "Error") == 0 || _stricmp(level, "Fatal") == 0)
        {
            cat = EE_AUDIT_N_ERRORS;
        }
        else if (strncmp(d, "Restored election ", 18) == 0)
        {
            cat = EE_AUDIT_N_RESTORES;
        }
        else if (strncmp(d, "Created user with ID", 20) == 0 || strncmp(d, "Updated user with ID", 20) == 0 ||
                 strncmp(d, "Deleted user", 12) == 0 || strstr(d, "viewed password information") != NULL)
        {
            cat = EE_AUDIT_N_USERS;
        }
        else if (i > 0)
        {
            const EeAuditCopy *cn;
            int64_t newer = EeAudit_MergedEvent(log, i - 1, &cn)->ts;
            if (e->ts > newer + AU_CLOCK_SLACK)
            {
                cat = EE_AUDIT_N_CLOCK;
                value = e->ts - newer;
            }
        }
        if (cat >= 0 && !au_add_note(n, cap, cat, 1, i, 0, value))
        {
            return FALSE;
        }
    }
    return TRUE;
}

BOOL EeAudit_Notable(const EeAuditElection *el, EeAuditNotes *out)
{
    size_t cap = 0;
    memset(out, 0, sizeof(*out));
    if (el == NULL)
    {
        return FALSE;
    }
    if (!au_notes_election(el, out, &cap) || !au_notes_admin(el, out, &cap))
    {
        EeAudit_FreeNotes(out);
        return FALSE;
    }
    return TRUE;
}

/* -------------------------------------------------------------------------- */
/* Logic and accuracy testing timeline                                        */
/* -------------------------------------------------------------------------- */

BOOL EeAudit_IsTestCopyName(const char *name)
{
    return name != NULL && (au_has_token(name, "test") || au_find_ci(name, "L&A") != NULL ||
                            au_find_ci(name, "logic") != NULL);
}

void EeAudit_FreeTimeline(EeAuditTimeline *t)
{
    uint32_t i;
    if (t == NULL)
    {
        return;
    }
    for (i = 0; i < t->ntests; i++)
    {
        free(t->tests[i].name);
    }
    free(t->tests);
    free(t->periods);
    free(t->pauses);
    free(t->other_copy);
    memset(t, 0, sizeof(*t));
}

/* A list of names (test copies). */
typedef struct AuNames
{
    char **v;
    uint32_t n;
    size_t cap;
} AuNames;

static BOOL au_names_has(const AuNames *s, const char *name)
{
    uint32_t i;
    for (i = 0; i < s->n; i++)
    {
        if (strcmp(s->v[i], name) == 0)
        {
            return TRUE;
        }
    }
    return FALSE;
}

/* Add a copy of @p name unless present. */
static BOOL au_names_add(AuNames *s, const char *name)
{
    if (au_names_has(s, name))
    {
        return TRUE;
    }
    if (!au_grow((void **)&s->v, &s->cap, (size_t)s->n + 1, sizeof(char *)) ||
        (s->v[s->n] = au_strdup_n(name, strlen(name))) == NULL)
    {
        return FALSE;
    }
    s->n++;
    return TRUE;
}

static void au_names_free(AuNames *s)
{
    uint32_t i;
    for (i = 0; i < s->n; i++)
    {
        free(s->v[i]);
    }
    free(s->v);
    memset(s, 0, sizeof(*s));
}

/* The test copy a "Restored election <name> from machine ..." event names, or NULL. */
static char *au_restored_name(const char *d)
{
    const char *nm, *from;
    if (strncmp(d, "Restored election ", 18) != 0)
    {
        return NULL;
    }
    nm = d + 18;
    from = strstr(nm, " from machine");
    return au_strdup_n(nm, from != NULL ? (size_t)(from - nm) : strlen(nm));
}

/* This election's test copies: those restored within two hours after a backup of this
 * election ("Election backup file created: C:\Electionware\May 26, 2026 Joint ...\..."). */
static BOOL au_this_copies(const EeAuditElection *el, AuNames *out)
{
    const EeAuditLog *log = &el->logs[1];
    int64_t backup_ts = 0;
    BOOL backup_this = FALSE;
    uint32_t i;
    for (i = log->nevents; i-- > 0;) /* oldest first */
    {
        const EeAuditCopy *c = NULL;
        const EeAuditEvent *e = EeAudit_MergedEvent(log, i, &c);
        const char *d = EeAudit_Desc(c, e);
        char *name;
        if (strncmp(d, "Election backup file created:", 29) == 0)
        {
            backup_this = el->election != NULL && el->election[0] != '\0' && strstr(d, el->election) != NULL;
            backup_ts = e->ts;
        }
        else if ((name = au_restored_name(d)) != NULL)
        {
            BOOL ok = TRUE;
            if (backup_this && e->ts - backup_ts <= 7200 && EeAudit_IsTestCopyName(name))
            {
                ok = au_names_add(out, name);
            }
            free(name);
            if (!ok)
            {
                return FALSE;
            }
        }
    }
    return TRUE;
}

static int au_cmp_test(const void *pa, const void *pb)
{
    const EeAuditTest *a = (const EeAuditTest *)pa;
    const EeAuditTest *b = (const EeAuditTest *)pb;
    return a->start < b->start ? -1 : (a->start > b->start ? 1 : 0);
}

/* The Admin log's test copies: "Restored election <name> from machine ..." and
 * "Election Opened: <name>", closed by "Election Closed: <name>". A restore and the open
 * that follows it within 10 minutes are one test. */
static BOOL au_admin_tests(const EeAuditLog *log, const AuNames *mine, EeAuditTimeline *t, size_t *cap)
{
    uint32_t i;
    int open_test = -1; /* the test copy open now */
    AuNames others;
    memset(&others, 0, sizeof(others));
    for (i = log->nevents; i-- > 0;)
    {
        const EeAuditCopy *c;
        const EeAuditEvent *e = EeAudit_MergedEvent(log, i, &c);
        const char *d = EeAudit_Desc(c, e);
        const char *nm = NULL;
        size_t nlen = 0;
        int kind = 0;
        if (strncmp(d, "Restored election ", 18) == 0)
        {
            const char *from = strstr(d + 18, " from machine");
            nm = d + 18;
            nlen = from != NULL ? (size_t)(from - nm) : strlen(nm);
            kind = EE_AUDIT_TEST_RESTORED;
        }
        else if (strncmp(d, "Election Opened: ", 17) == 0)
        {
            nm = d + 17;
            nlen = strlen(nm);
            kind = EE_AUDIT_TEST_OPENED;
        }
        else if (strncmp(d, "Election Closed: ", 17) == 0 && open_test >= 0)
        {
            if (t->tests[open_test].name != NULL && strcmp(d + 17, t->tests[open_test].name) == 0)
            {
                t->tests[open_test].end = e->ts;
                open_test = -1;
            }
            continue;
        }
        if (kind == 0)
        {
            continue;
        }
        {
            char *name = au_strdup_n(nm, nlen);
            EeAuditTest *x;
            if (name == NULL)
            {
                return FALSE;
            }
            if (!EeAudit_IsTestCopyName(name) || !au_names_has(mine, name))
            {
                if (EeAudit_IsTestCopyName(name) && !au_names_add(&others, name))
                {
                    free(name);
                    au_names_free(&others);
                    return FALSE;
                }
                free(name);
                if (kind == EE_AUDIT_TEST_OPENED)
                {
                    open_test = -1; /* another election opened */
                }
                continue;
            }
            if (kind == EE_AUDIT_TEST_OPENED && t->ntests > 0)
            {
                EeAuditTest *last = &t->tests[t->ntests - 1];
                if (last->kind == EE_AUDIT_TEST_RESTORED && last->log == 1 && strcmp(last->name, name) == 0 &&
                    e->ts - last->start <= 600)
                {
                    free(name);
                    last->end = e->ts;
                    open_test = (int)(t->ntests - 1);
                    continue;
                }
            }
            if (!au_grow((void **)&t->tests, cap, (size_t)t->ntests + 1, sizeof(EeAuditTest)))
            {
                free(name);
                au_names_free(&others);
                return FALSE;
            }
            x = &t->tests[t->ntests];
            memset(x, 0, sizeof(*x));
            x->start = x->end = e->ts;
            x->kind = kind;
            x->log = 1;
            x->event = i;
            x->name = name;
            open_test = (int)t->ntests;
            t->ntests++;
        }
    }
    t->other_copies = others.n;
    if (others.n > 0)
    {
        t->other_copy = au_strdup_n(others.v[0], strlen(others.v[0]));
    }
    au_names_free(&others);
    return TRUE;
}

/* The Admin log shows the election (not a test copy) closed between @p from and @p to. */
static BOOL au_closed_between(const EeAuditElection *el, int64_t from, int64_t to)
{
    const EeAuditLog *log = &el->logs[1];
    size_t nlen = el->election != NULL ? strlen(el->election) : 0;
    uint32_t i;
    for (i = 0; i < log->nevents; i++)
    {
        const EeAuditCopy *c;
        const EeAuditEvent *e = EeAudit_MergedEvent(log, i, &c);
        const char *d;
        if (e->ts <= from || e->ts >= to)
        {
            continue;
        }
        d = EeAudit_Desc(c, e);
        if (strncmp(d, "Election Closed: ", 17) == 0 && !EeAudit_IsTestCopyName(d + 17) &&
            (nlen == 0 || strncmp(d + 17, el->election, nlen) == 0))
        {
            return TRUE;
        }
    }
    return FALSE;
}

BOOL EeAudit_Timeline(const EeAuditElection *el, const EeAuditAccounting *acct, EeAuditTimeline *out)
{
    size_t cap_tests = 0, cap_periods = 0, cap_pauses = 0;
    uint32_t si, b;
    uint32_t next_test = 0; /* tests that started at or before the current load */
    EeAuditPeriod *cur = NULL;
    int64_t prev_ts = 0;
    uint8_t *era = NULL;
    AuNames mine;
    memset(out, 0, sizeof(*out));
    memset(&mine, 0, sizeof(mine));
    if (el == NULL || acct == NULL)
    {
        return FALSE;
    }
    if ((era = au_election_eras(el, NULL)) == NULL || !au_this_copies(el, &mine))
    {
        goto oom;
    }
    /* tests: the Election log's test sessions, then the Admin log's test copies */
    for (si = 0; si < acct->nsessions; si++)
    {
        const EeAuditSession *s = &acct->sessions[si];
        EeAuditTest *x;
        if (!s->test || s->nbatches == 0 || era[acct->batches[s->first_batch].event] == AU_ERA_OTHER)
        {
            continue;
        }
        if (!au_grow((void **)&out->tests, &cap_tests, (size_t)out->ntests + 1, sizeof(EeAuditTest)))
        {
            goto oom;
        }
        x = &out->tests[out->ntests++];
        memset(x, 0, sizeof(*x));
        x->start = acct->batches[s->first_batch].ts;
        x->end = s->end;
        x->kind = EE_AUDIT_TEST_SESSION;
        x->log = 0;
        x->event = acct->batches[s->first_batch].event;
        x->session = si;
        for (b = s->first_batch; b < s->first_batch + s->nbatches; b++)
        {
            x->batches++;
            x->ballots += acct->batches[b].ballots;
        }
    }
    if (!au_admin_tests(&el->logs[1], &mine, out, &cap_tests))
    {
        goto oom;
    }
    if (out->ntests > 1)
    {
        qsort(out->tests, out->ntests, sizeof(EeAuditTest), au_cmp_test);
    }
    /* counting periods: loads of ballots (not test decks) in load order, split at each test */
    for (b = 0; b < acct->nbatches; b++)
    {
        const EeAuditBatch *x = &acct->batches[b];
        uint32_t nt = next_test;
        if (x->test)
        {
            continue;
        }
        if (era[x->event] == AU_ERA_OTHER)
        {
            out->other_loads++; /* another election's history */
            continue;
        }
        while (nt < out->ntests && out->tests[nt].start <= x->ts)
        {
            nt++;
        }
        if (cur == NULL || nt != next_test)
        {
            if (!au_grow((void **)&out->periods, &cap_periods, (size_t)out->nperiods + 1, sizeof(EeAuditPeriod)))
            {
                goto oom;
            }
            cur = &out->periods[out->nperiods++];
            memset(cur, 0, sizeof(*cur));
            cur->first = x->ts;
            cur->first_event = x->event;
            cur->test_before = nt > 0 ? (int)nt - 1 : -1;
            cur->test_after = -1;
            cur->first_pause = out->npauses;
        }
        else if (x->ts - prev_ts >= EE_AUDIT_PAUSE_SECS)
        {
            EeAuditPause *p;
            if (!au_grow((void **)&out->pauses, &cap_pauses, (size_t)out->npauses + 1, sizeof(EeAuditPause)))
            {
                goto oom;
            }
            p = &out->pauses[out->npauses++];
            p->from = prev_ts;
            p->to = x->ts;
            p->closed = au_closed_between(el, prev_ts, x->ts);
            cur->npauses++;
        }
        next_test = nt;
        cur->last = x->ts;
        cur->last_event = x->event;
        cur->batches++;
        cur->ballots += x->ballots;
        prev_ts = x->ts;
    }
    for (b = 0; b < out->nperiods; b++)
    {
        EeAuditPeriod *p = &out->periods[b];
        uint32_t k;
        for (k = 0; k < out->ntests; k++)
        {
            if (out->tests[k].start >= p->last)
            {
                p->test_after = (int)k;
                break;
            }
        }
    }
    free(era);
    au_names_free(&mine);
    return TRUE;
oom:
    free(era);
    au_names_free(&mine);
    EeAudit_FreeTimeline(out);
    return FALSE;
}

/* -------------------------------------------------------------------------- */
/* The reloadable Events export                                               */
/* -------------------------------------------------------------------------- */

enum
{
    AX_TYPE = 0,
    AX_TIME,
    AX_LOG,
    AX_EVENT,
    AX_MODULE,
    AX_USER,
    AX_DESC,
    AX_COPIES,
    AX_FIRST,
    AX_PAGE,
    AX_COUNTY,
    AX_DATE,
    AX_COUNT
};

static const char *const k_ax_cols[AX_COUNT] = {"Record Type", "Timestamp",   "Log",        "Event",
                                                "Module",      "User Id",     "Description", "Copies",
                                                "First Copy",  "Page",        "County",     "Election Date"};

/* The export being built; ok turns FALSE when memory runs out. */
typedef struct AuOut
{
    char *p;
    size_t len;
    size_t cap;
    BOOL ok;
} AuOut;

static void ax_put(AuOut *o, const char *s, size_t n)
{
    if (!o->ok || n == 0)
    {
        return;
    }
    if (!au_grow((void **)&o->p, &o->cap, o->len + n + 1, 1))
    {
        o->ok = FALSE;
        return;
    }
    memcpy(o->p + o->len, s, n);
    o->len += n;
    o->p[o->len] = '\0';
}

/* One RFC-4180 field: quoted when it holds the delimiter, a quote or a line break. */
static void ax_field(AuOut *o, const char *s, char delim)
{
    const char *q;
    BOOL quote = FALSE;
    if (s == NULL)
    {
        return;
    }
    for (q = s; *q != '\0'; q++)
    {
        if (*q == delim || *q == '"' || *q == '\r' || *q == '\n')
        {
            quote = TRUE;
            break;
        }
    }
    if (!quote)
    {
        ax_put(o, s, strlen(s));
        return;
    }
    ax_put(o, "\"", 1);
    for (q = s; *q != '\0'; q++)
    {
        ax_put(o, q, 1);
        if (*q == '"')
        {
            ax_put(o, "\"", 1);
        }
    }
    ax_put(o, "\"", 1);
}

static void ax_record(AuOut *o, const char *const *f, char delim)
{
    int k;
    for (k = 0; k < AX_COUNT; k++)
    {
        if (k > 0)
        {
            ax_put(o, &delim, 1);
        }
        ax_field(o, f[k], delim);
    }
    ax_put(o, "\r\n", 2);
}

/* A path's file name in UTF-8. */
static void ax_file_name(const wchar_t *path, char *out, size_t cap)
{
    const wchar_t *name = L"";
    out[0] = '\0';
    if (path != NULL)
    {
        const wchar_t *b = wcsrchr(path, L'\\');
        const wchar_t *f = wcsrchr(path, L'/');
        name = b != NULL && (f == NULL || b > f) ? b + 1 : (f != NULL ? f + 1 : path);
    }
    if (WideCharToMultiByte(CP_UTF8, 0, name, -1, out, (int)cap, NULL, NULL) <= 0)
    {
        out[0] = '\0';
    }
}

/* The copies of @p mask among @p n: "1-3,5 of 5". */
static void ax_copies_text(uint64_t mask, uint32_t n, char *out, size_t cap)
{
    uint32_t i = 0;
    size_t len = 0;
    out[0] = '\0';
    while (i < n)
    {
        uint32_t j = i;
        if ((mask & ((uint64_t)1 << i)) == 0)
        {
            i++;
            continue;
        }
        while (j + 1 < n && (mask & ((uint64_t)1 << (j + 1))) != 0)
        {
            j++;
        }
        if (len > 0)
        {
            StringCchCatA(out, cap, ",");
            len = strlen(out);
        }
        if (j > i)
        {
            StringCchPrintfA(out + len, cap - len, "%u-%u", i + 1, j + 1);
        }
        else
        {
            StringCchPrintfA(out + len, cap - len, "%u", i + 1);
        }
        len = strlen(out);
        i = j + 1;
    }
    StringCchPrintfA(out + len, cap - len, " of %u", n);
}

static void ax_copy_record(AuOut *o, const EeAuditCopy *c, int kind, uint32_t number, uint32_t of, char delim)
{
    const char *f[AX_COUNT];
    char t[48] = "", num[32], name[MAX_PATH * 3], pages[16], date[16] = "";
    memset((void *)f, 0, sizeof(f));
    f[AX_TYPE] = "Copy";
    if (c->printed != 0)
    {
        EeAudit_FormatTime(c->printed, t, sizeof(t));
    }
    f[AX_TIME] = t;
    f[AX_LOG] = kind == EE_AUDIT_ADMIN ? "Admin" : "Election";
    f[AX_DESC] = c->election;
    StringCchPrintfA(num, sizeof(num), "%u of %u", number, of);
    f[AX_COPIES] = num;
    ax_file_name(c->path, name, sizeof(name));
    f[AX_FIRST] = name;
    StringCchPrintfA(pages, sizeof(pages), "%u", c->pages);
    f[AX_PAGE] = pages;
    f[AX_COUNTY] = c->customer;
    if (c->election_date != 0)
    {
        StringCchPrintfA(date, sizeof(date), "%02d/%02d/%04d", c->election_date / 100 % 100, c->election_date % 100,
                         c->election_date / 10000);
    }
    f[AX_DATE] = date;
    ax_record(o, f, delim);
}

static void ax_event_record(AuOut *o, const EeAuditCopy *c, const EeAuditEvent *e, int kind, const char *copies,
                            const char *first, char delim)
{
    const char *f[AX_COUNT];
    char t[48], page[16] = "";
    memset((void *)f, 0, sizeof(f));
    f[AX_TYPE] = "Event";
    EeAudit_FormatTime(e->ts, t, sizeof(t));
    f[AX_TIME] = t;
    f[AX_LOG] = kind == EE_AUDIT_ADMIN ? "Admin" : "Election";
    f[AX_EVENT] = EeAudit_Str(c, e->level);
    f[AX_MODULE] = EeAudit_Str(c, e->module);
    f[AX_USER] = EeAudit_Str(c, e->user);
    f[AX_DESC] = EeAudit_Desc(c, e);
    f[AX_COPIES] = copies;
    f[AX_FIRST] = first;
    if (e->page > 0)
    {
        StringCchPrintfA(page, sizeof(page), "%u", e->page);
    }
    f[AX_PAGE] = page;
    ax_record(o, f, delim);
}

#define AX_NAME (MAX_PATH * 3)

BOOL EeAudit_FormatExportUtf8(const EeAuditElection *el,
                              int copy_log,
                              int copy,
                              const EeAuditExportRow *rows,
                              uint32_t nrows,
                              char delim,
                              char **out,
                              size_t *out_len)
{
    AuOut o;
    uint32_t i, k;

    *out = NULL;
    *out_len = 0;
    memset(&o, 0, sizeof(o));
    o.ok = TRUE;
    for (k = 0; k < AX_COUNT; k++)
    {
        if (k > 0)
        {
            ax_put(&o, &delim, 1);
        }
        ax_field(&o, k_ax_cols[k], delim);
    }
    ax_put(&o, "\r\n", 2);
    if (copy >= 0)
    {
        const EeAuditLog *log = &el->logs[copy_log != 0 ? 1 : 0];
        const EeAuditCopy *c;
        char name[AX_NAME];
        if ((uint32_t)copy >= log->ncopies)
        {
            free(o.p);
            return FALSE;
        }
        c = &log->copies[copy];
        ax_copy_record(&o, c, log->kind, 1, 1, delim);
        ax_file_name(c->path, name, sizeof(name));
        for (i = 0; i < nrows && o.ok; i++)
        {
            if (rows[i].index < c->nevents)
            {
                ax_event_record(&o, c, &c->events[rows[i].index], log->kind, "1 of 1", name, delim);
            }
        }
    }
    else
    {
        BOOL used[2] = {FALSE, FALSE};
        int li;
        char *names = (char *)malloc((size_t)2 * EE_AUDIT_MAX_COPIES * AX_NAME); /* each copy's file name */
        if (names == NULL)
        {
            free(o.p);
            return FALSE;
        }
        for (i = 0; i < nrows; i++)
        {
            if (rows[i].log < 2)
            {
                used[rows[i].log] = TRUE;
            }
        }
        for (li = 0; li < 2; li++)
        {
            const EeAuditLog *log = &el->logs[li];
            for (k = 0; used[li] && k < log->ncopies && k < EE_AUDIT_MAX_COPIES; k++)
            {
                ax_copy_record(&o, &log->copies[k], log->kind, k + 1, log->ncopies, delim);
                ax_file_name(log->copies[k].path, names + ((size_t)li * EE_AUDIT_MAX_COPIES + k) * AX_NAME, AX_NAME);
            }
        }
        for (i = 0; i < nrows && o.ok; i++)
        {
            const EeAuditLog *log;
            const EeAuditMerged *m;
            const EeAuditCopy *c;
            const EeAuditEvent *e;
            char cp[256];
            if (rows[i].log >= 2 || rows[i].index >= el->logs[rows[i].log].nevents)
            {
                continue;
            }
            log = &el->logs[rows[i].log];
            m = &log->events[rows[i].index];
            e = EeAudit_MergedEvent(log, rows[i].index, &c);
            ax_copies_text(m->copies, log->ncopies, cp, sizeof(cp));
            ax_event_record(&o, c, e, log->kind, cp,
                            names + ((size_t)rows[i].log * EE_AUDIT_MAX_COPIES + m->copy) * AX_NAME, delim);
        }
        free(names);
    }
    if (!o.ok)
    {
        free(o.p);
        return FALSE;
    }
    *out = o.p;
    *out_len = o.len;
    return TRUE;
}

BOOL EeAudit_IsExportPath(const wchar_t *path)
{
    const wchar_t *dot = path != NULL ? wcsrchr(path, L'.') : NULL;
    return dot != NULL && (_wcsicmp(dot, L".csv") == 0 || _wcsicmp(dot, L".tsv") == 0 || _wcsicmp(dot, L".txt") == 0);
}

/* An export being read back. */
typedef struct AuReload
{
    int col[AX_COUNT]; /* each column's cell, -1 when missing */
    BOOL have_header;
    uint32_t ncopies[2];
    EeAuditCopy *copies[2];
    uint64_t described[2]; /* bit k: copy k's Copy record was read */
    uint64_t record;       /* records read, for messages */
    wchar_t msg[300];      /* what went wrong (without the file name) */
} AuReload;

static const char *ax_cell(const AuReload *rl, const char *const *cells, uint32_t ncells, int k)
{
    int i = rl->col[k];
    return i >= 0 && (uint32_t)i < ncells && cells[i] != NULL ? cells[i] : "";
}

static BOOL ax_uint(const char **pp, uint32_t *out)
{
    const char *p = *pp;
    uint32_t v = 0;
    int d = 0;
    while (*p >= '0' && *p <= '9' && d < 9)
    {
        v = v * 10 + (uint32_t)(*p - '0');
        p++;
        d++;
    }
    if (d == 0)
    {
        return FALSE;
    }
    *pp = p;
    *out = v;
    return TRUE;
}

/* "1-3,5 of 5" -> mask 0x17 of 5. */
static BOOL ax_parse_copies(const char *s, uint64_t *mask, uint32_t *of)
{
    const char *p = s;
    uint64_t m = 0;
    uint32_t n, k;
    while (*p == ' ')
    {
        p++;
    }
    for (;;)
    {
        uint32_t a, b;
        if (!ax_uint(&p, &a))
        {
            return FALSE;
        }
        b = a;
        if (*p == '-')
        {
            p++;
            if (!ax_uint(&p, &b))
            {
                return FALSE;
            }
        }
        if (a < 1 || b < a || b > EE_AUDIT_MAX_COPIES)
        {
            return FALSE;
        }
        for (k = a; k <= b; k++)
        {
            m |= (uint64_t)1 << (k - 1);
        }
        if (*p != ',')
        {
            break;
        }
        p++;
        while (*p == ' ')
        {
            p++;
        }
    }
    if (strncmp(p, " of ", 4) != 0)
    {
        return FALSE;
    }
    p += 4;
    if (!ax_uint(&p, &n))
    {
        return FALSE;
    }
    while (*p == ' ')
    {
        p++;
    }
    if (*p != '\0' || n < 1 || n > EE_AUDIT_MAX_COPIES || (n < 64 && (m >> n) != 0))
    {
        return FALSE;
    }
    *mask = m;
    *of = n;
    return TRUE;
}

/* "11/05/2024 09:25:36PM", "11/5/2024 21:25" or "11/05/2024" (midnight). */
static BOOL ax_parse_time(const char *s, int64_t *out, int *yyyymmdd)
{
    uint32_t v[6] = {0, 0, 0, 0, 0, 0};
    const char *p = s;
    int k;
    while (*p == ' ')
    {
        p++;
    }
    for (k = 0; k < 3; k++)
    {
        if (!ax_uint(&p, &v[k]))
        {
            return FALSE;
        }
        if (k < 2)
        {
            if (*p != '/')
            {
                return FALSE;
            }
            p++;
        }
    }
    while (*p == ' ')
    {
        p++;
    }
    if (*p >= '0' && *p <= '9')
    {
        for (k = 3; k < 6; k++)
        {
            if (!ax_uint(&p, &v[k]))
            {
                return FALSE;
            }
            if (*p != ':' || k == 5)
            {
                break;
            }
            p++;
        }
        while (*p == ' ')
        {
            p++;
        }
        if ((p[0] == 'A' || p[0] == 'a' || p[0] == 'P' || p[0] == 'p') && (p[1] == 'M' || p[1] == 'm'))
        {
            if (v[3] < 1 || v[3] > 12)
            {
                return FALSE;
            }
            v[3] = v[3] % 12 + ((p[0] == 'P' || p[0] == 'p') ? 12 : 0);
            p += 2;
        }
    }
    while (*p == ' ')
    {
        p++;
    }
    if (*p != '\0' || v[0] < 1 || v[0] > 12 || v[1] < 1 || v[1] > 31 || v[2] < 1900 || v[2] > 2200 || v[3] > 23 ||
        v[4] > 59 || v[5] > 59)
    {
        return FALSE;
    }
    if (out != NULL)
    {
        *out = EeAudit_MakeTime((int)v[2], (int)v[0], (int)v[1], (int)v[3], (int)v[4], (int)v[5]);
    }
    if (yyyymmdd != NULL)
    {
        *yyyymmdd = (int)(v[2] * 10000 + v[0] * 100 + v[1]);
    }
    return TRUE;
}

/* Room for log @p li's @p of copies (the first record naming the log sets their number). */
static BOOL ax_log_copies(AuReload *rl, int li, uint32_t of)
{
    uint32_t k;
    if (rl->ncopies[li] == of)
    {
        return TRUE;
    }
    if (rl->ncopies[li] != 0)
    {
        StringCchPrintfW(rl->msg, ARRAYSIZE(rl->msg), L"record %llu gives the %s log %u copies, not %u.",
                         (unsigned long long)rl->record, li == 1 ? L"Admin" : L"Election", of, rl->ncopies[li]);
        return FALSE;
    }
    rl->copies[li] = (EeAuditCopy *)calloc(of, sizeof(EeAuditCopy));
    if (rl->copies[li] == NULL)
    {
        StringCchCopyW(rl->msg, ARRAYSIZE(rl->msg), L"out of memory.");
        return FALSE;
    }
    rl->ncopies[li] = of;
    for (k = 0; k < of; k++)
    {
        rl->copies[li][k].kind = li == 1 ? EE_AUDIT_ADMIN : EE_AUDIT_ELECTION;
    }
    return TRUE;
}

static char *ax_dup(const char *s)
{
    return s[0] != '\0' ? au_strdup_n(s, strlen(s)) : NULL;
}

/* The header: find each column by its title (any order). */
static BOOL ax_header(AuReload *rl, const char *const *cells, uint32_t ncells)
{
    uint32_t i;
    int k;
    for (k = 0; k < AX_COUNT; k++)
    {
        rl->col[k] = -1;
        for (i = 0; i < ncells; i++)
        {
            if (cells[i] != NULL && _stricmp(cells[i], k_ax_cols[k]) == 0)
            {
                rl->col[k] = (int)i;
                break;
            }
        }
    }
    if (rl->col[AX_TYPE] < 0)
    {
        StringCchCopyW(rl->msg, ARRAYSIZE(rl->msg),
                       rl->col[AX_TIME] >= 0 && rl->col[AX_DESC] >= 0 && rl->col[AX_COPIES] >= 0
                           ? L"this is a displayed-view export, which does not keep the copies that hold each "
                             L"event. Export the Events view again, choosing Reloadable."
                           : L"this is not an audit log export from Election Explorer (it has no \"Record Type\" "
                             L"column).");
        return FALSE;
    }
    for (k = 0; k <= AX_COPIES; k++)
    {
        if (rl->col[k] < 0)
        {
            StringCchPrintfW(rl->msg, ARRAYSIZE(rl->msg), L"the \"%S\" column is missing.", k_ax_cols[k]);
            return FALSE;
        }
    }
    rl->have_header = TRUE;
    return TRUE;
}

/* A Copy record: copy k of log li's header fields. */
static void ax_copy_fields(AuReload *rl, EeAuditCopy *c, const char *const *cells, uint32_t ncells)
{
    const char *s;
    int64_t ts;
    int date;
    if (ax_parse_time(ax_cell(rl, cells, ncells, AX_TIME), &ts, NULL))
    {
        c->printed = ts;
    }
    if (ax_parse_time(ax_cell(rl, cells, ncells, AX_DATE), NULL, &date))
    {
        c->election_date = date;
    }
    c->election = ax_dup(ax_cell(rl, cells, ncells, AX_DESC));
    c->customer = ax_dup(ax_cell(rl, cells, ncells, AX_COUNTY));
    c->pages = (uint32_t)strtoul(ax_cell(rl, cells, ncells, AX_PAGE), NULL, 10);
    s = ax_cell(rl, cells, ncells, AX_FIRST);
    if (s[0] != '\0')
    {
        int n = MultiByteToWideChar(CP_UTF8, 0, s, -1, NULL, 0);
        if (n > 0 && (c->path = (wchar_t *)malloc((size_t)n * sizeof(wchar_t))) != NULL)
        {
            MultiByteToWideChar(CP_UTF8, 0, s, -1, c->path, n);
        }
    }
}

static BOOL ax_sink(void *ctx, const char *const *cells, uint32_t ncells)
{
    AuReload *rl = (AuReload *)ctx;
    const char *type, *logname;
    uint64_t mask = 0;
    uint32_t of = 0, k;
    int li;
    rl->record++;
    if (!rl->have_header)
    {
        return ax_header(rl, cells, ncells);
    }
    type = ax_cell(rl, cells, ncells, AX_TYPE);
    if (type[0] == '\0' && ncells <= 1)
    {
        return TRUE; /* a blank line */
    }
    logname = ax_cell(rl, cells, ncells, AX_LOG);
    if (_stricmp(logname, "Election") == 0)
    {
        li = 0;
    }
    else if (_stricmp(logname, "Admin") == 0)
    {
        li = 1;
    }
    else
    {
        StringCchPrintfW(rl->msg, ARRAYSIZE(rl->msg), L"record %llu: the Log is neither Election nor Admin.",
                         (unsigned long long)rl->record);
        return FALSE;
    }
    if (!ax_parse_copies(ax_cell(rl, cells, ncells, AX_COPIES), &mask, &of))
    {
        StringCchPrintfW(rl->msg, ARRAYSIZE(rl->msg), L"record %llu: Copies does not list the copies (\"1-3,5 of 5\").",
                         (unsigned long long)rl->record);
        return FALSE;
    }
    if (!ax_log_copies(rl, li, of))
    {
        return FALSE;
    }
    if (_stricmp(type, "Copy") == 0)
    {
        for (k = 0; k + 1 < of && (mask & ((uint64_t)1 << k)) == 0; k++)
        {
        }
        if (mask != (uint64_t)1 << k || (rl->described[li] & mask) != 0)
        {
            StringCchPrintfW(rl->msg, ARRAYSIZE(rl->msg), L"record %llu: a Copy record must name one copy, once.",
                             (unsigned long long)rl->record);
            return FALSE;
        }
        rl->described[li] |= mask;
        ax_copy_fields(rl, &rl->copies[li][k], cells, ncells);
        return TRUE;
    }
    if (_stricmp(type, "Event") == 0)
    {
        int64_t ts;
        uint32_t page = (uint32_t)strtoul(ax_cell(rl, cells, ncells, AX_PAGE), NULL, 10);
        BOOL first = TRUE;
        if (!ax_parse_time(ax_cell(rl, cells, ncells, AX_TIME), &ts, NULL))
        {
            StringCchPrintfW(rl->msg, ARRAYSIZE(rl->msg), L"record %llu: the Timestamp is not a date and time.",
                             (unsigned long long)rl->record);
            return FALSE;
        }
        for (k = 0; k < of; k++)
        {
            if ((mask & ((uint64_t)1 << k)) == 0)
            {
                continue;
            }
            /* the page is the first holder's: the copy whose strings the merge keeps */
            if (!EeAudit_AddEvent(&rl->copies[li][k], ts, ax_cell(rl, cells, ncells, AX_EVENT),
                                  ax_cell(rl, cells, ncells, AX_DESC), ax_cell(rl, cells, ncells, AX_USER),
                                  ax_cell(rl, cells, ncells, AX_MODULE), first ? page : 0))
            {
                StringCchCopyW(rl->msg, ARRAYSIZE(rl->msg), L"out of memory.");
                return FALSE;
            }
            first = FALSE;
        }
        return TRUE;
    }
    StringCchPrintfW(rl->msg, ARRAYSIZE(rl->msg), L"record %llu: the Record Type is neither Copy nor Event.",
                     (unsigned long long)rl->record);
    return FALSE;
}

EeLoadStatus EeAudit_LoadExport(const wchar_t *path,
                                EeAuditCopy **out,
                                uint32_t *out_count,
                                volatile LONG *cancel_flag,
                                EeLoadProgressFn progress_fn,
                                void *progress_user,
                                wchar_t *err,
                                size_t errcch)
{
    AuReload rl;
    wchar_t csv_err[256];
    EeLoadStatus st;
    EeAuditCopy *arr = NULL;
    uint32_t li, k, n = 0;

    *out = NULL;
    *out_count = 0;
    memset(&rl, 0, sizeof(rl));
    csv_err[0] = L'\0';
    st = EeCsv_ReadSheet(path, ax_sink, &rl, cancel_flag, progress_fn, progress_user, csv_err, ARRAYSIZE(csv_err));
    if (st == EeLoadStatus_Ok && rl.ncopies[0] == 0 && rl.ncopies[1] == 0)
    {
        StringCchCopyW(rl.msg, ARRAYSIZE(rl.msg), rl.have_header ? L"the export holds no copies." : L"the file is empty.");
        st = EeLoadStatus_Error;
    }
    for (li = 0; li < 2 && st == EeLoadStatus_Ok; li++)
    {
        for (k = 0; k < rl.ncopies[li]; k++)
        {
            if ((rl.described[li] & ((uint64_t)1 << k)) == 0)
            {
                StringCchPrintfW(rl.msg, ARRAYSIZE(rl.msg), L"there is no Copy record for copy %u of %u of the %s log.",
                                 k + 1, rl.ncopies[li], li == 1 ? L"Admin" : L"Election");
                st = EeLoadStatus_Error;
                break;
            }
        }
    }
    if (st == EeLoadStatus_Ok)
    {
        arr = (EeAuditCopy *)calloc((size_t)rl.ncopies[0] + rl.ncopies[1] + 1, sizeof(EeAuditCopy));
        if (arr == NULL)
        {
            StringCchCopyW(rl.msg, ARRAYSIZE(rl.msg), L"out of memory.");
            st = EeLoadStatus_Error;
        }
    }
    for (li = 0; li < 2; li++)
    {
        for (k = 0; k < rl.ncopies[li]; k++)
        {
            if (arr != NULL)
            {
                arr[n++] = rl.copies[li][k];
            }
            else
            {
                EeAudit_FreeCopy(&rl.copies[li][k]);
            }
        }
        free(rl.copies[li]);
    }
    if (st != EeLoadStatus_Ok)
    {
        if (st == EeLoadStatus_Error && err != NULL && errcch > 0)
        {
            const wchar_t *name = wcsrchr(path, L'\\');
            name = name != NULL ? name + 1 : path;
            if (rl.msg[0] != L'\0')
            {
                StringCchPrintfW(err, errcch, L"%s: %s", name, rl.msg);
            }
            else if (csv_err[0] != L'\0')
            {
                StringCchPrintfW(err, errcch, L"%s: %s", name, csv_err);
            }
            else
            {
                StringCchPrintfW(err, errcch, L"Could not read %s.", name);
            }
        }
        return st;
    }
    *out = arr;
    *out_count = n;
    return EeLoadStatus_Ok;
}
