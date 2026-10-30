/**
 * @file ee_audit_hart.c
 * @brief Hart Verity "Audit Log Report" PDFs read into audit events, and their ballot
 *        accounting and notable events. See ee_audit.h and docs/audit-log-design.md.
 *
 * The report (Microsoft Reporting Services) prints a header on every page (customer, "Audit
 * Log Report", "<election> | <election ID>", election date, Run Time, Run Date, "Page N of
 * M", "Component: VerityCount 2.7.5", "Election ID:"), then a table: Row | Log Date & Time |
 * Device ID | User | Event | Event Data. Rows are numbered from 1, oldest first. A row's time
 * wraps after the zone's "-" ("2026-03-03T16:19:46-" / "06:00") and its Event Data wraps onto
 * more lines. There is no footer: a page's last row can sit 20 points from its bottom, and a
 * row that does not fit is either continued at the next page's top or printed there again in
 * full (above the table heading).
 */

#include "ee_audit_hart.h"

#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strsafe.h>

/* -------------------------------------------------------------------------- */
/* Small helpers                                                              */
/* -------------------------------------------------------------------------- */

static BOOL hv_grow(void **arr, size_t *cap, size_t need, size_t elem)
{
    size_t n;
    void *p;
    if (need <= *cap)
    {
        return TRUE;
    }
    n = *cap < 16 ? 16 : *cap * 2;
    while (n < need)
    {
        n *= 2;
    }
    p = realloc(*arr, n * elem);
    if (p == NULL)
    {
        return FALSE;
    }
    *arr = p;
    *cap = n;
    return TRUE;
}

static char *hv_strdup_n(const char *s, size_t n)
{
    char *p = (char *)malloc(n + 1);
    if (p != NULL)
    {
        memcpy(p, s, n);
        p[n] = '\0';
    }
    return p;
}

static void hv_set_err(wchar_t *dst, size_t cch, const wchar_t *fmt, const wchar_t *path)
{
    if (dst != NULL && cch > 0)
    {
        const wchar_t *name = wcsrchr(path, L'\\');
        StringCchPrintfW(dst, cch, fmt, name != NULL ? name + 1 : path);
    }
}

/* A growing NUL-terminated string. */
typedef struct HvBuf
{
    char *p;
    size_t len;
    size_t cap;
} HvBuf;

/* Append @p n bytes of @p s, after @p sep when the buffer is not empty (0: none). */
static BOOL hv_buf_add(HvBuf *b, const char *s, size_t n, char sep)
{
    size_t need = b->len + n + 2;
    if (!hv_grow((void **)&b->p, &b->cap, need, 1))
    {
        return FALSE;
    }
    if (b->len > 0 && sep != 0)
    {
        b->p[b->len++] = sep;
    }
    memcpy(b->p + b->len, s, n);
    b->len += n;
    b->p[b->len] = '\0';
    return TRUE;
}

static void hv_buf_clear(HvBuf *b)
{
    b->len = 0;
    if (b->p != NULL)
    {
        b->p[0] = '\0';
    }
}

static const char *hv_buf_str(const HvBuf *b)
{
    return b->p != NULL ? b->p : "";
}

static BOOL hv_is_alpha(char c)
{
    return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z');
}

/* Case-insensitive strstr. */
static const char *hv_find_ci(const char *s, const char *pat)
{
    size_t n = strlen(pat);
    for (; *s != '\0'; s++)
    {
        if (_strnicmp(s, pat, n) == 0)
        {
            return s;
        }
    }
    return NULL;
}

/* Read 1..maxd digits. */
static BOOL hv_num(const char **pp, int maxd, int *out)
{
    const char *p = *pp;
    int v = 0, n = 0;
    while (*p >= '0' && *p <= '9' && n < maxd)
    {
        v = v * 10 + (*p++ - '0');
        n++;
    }
    if (n == 0)
    {
        return FALSE;
    }
    *pp = p;
    *out = v;
    return TRUE;
}

/* "3/3/2026" (and what follows when @p end is given) -> yyyymmdd, or 0. */
static int hv_mdy(const char *s, const char **end)
{
    int m, d, y;
    const char *p = s;
    if (!hv_num(&p, 2, &m) || *p++ != '/' || !hv_num(&p, 2, &d) || *p++ != '/' || !hv_num(&p, 4, &y) ||
        (p[0] >= '0' && p[0] <= '9') || m < 1 || m > 12 || d < 1 || d > 31 || y < 1990 || y > 2100)
    {
        return 0;
    }
    if (end != NULL)
    {
        *end = p;
    }
    else if (*p != '\0')
    {
        return 0;
    }
    return y * 10000 + m * 100 + d;
}

/* "3:03 PM" or "10:42:39 AM" -> seconds into the day, or -1. */
static int hv_clock(const char *s)
{
    const char *p = s;
    int h, mi, se = 0;
    while (*p == ' ')
    {
        p++;
    }
    if (!hv_num(&p, 2, &h) || *p++ != ':' || !hv_num(&p, 2, &mi))
    {
        return -1;
    }
    if (*p == ':' && (p++, !hv_num(&p, 2, &se)))
    {
        return -1;
    }
    while (*p == ' ')
    {
        p++;
    }
    if ((p[0] == 'A' || p[0] == 'a' || p[0] == 'P' || p[0] == 'p') && (p[1] == 'M' || p[1] == 'm'))
    {
        if (h < 1 || h > 12)
        {
            return -1;
        }
        h = h % 12 + ((p[0] == 'P' || p[0] == 'p') ? 12 : 0);
    }
    if (h > 23 || mi > 59 || se > 59)
    {
        return -1;
    }
    return h * 3600 + mi * 60 + se;
}

/* "2026-03-03T15:58:36-06:00": the local date and time (the zone is left out, as the
 * report prints each row in its own zone). */
static BOOL hv_iso(const char *s, int64_t *out)
{
    const char *p = s;
    int y, mo, d, h, mi, se;
    if (!hv_num(&p, 4, &y) || *p++ != '-' || !hv_num(&p, 2, &mo) || *p++ != '-' || !hv_num(&p, 2, &d) ||
        *p++ != 'T' || !hv_num(&p, 2, &h) || *p++ != ':' || !hv_num(&p, 2, &mi) || *p++ != ':' ||
        !hv_num(&p, 2, &se) || y < 1990 || y > 2100 || mo < 1 || mo > 12 || d < 1 || d > 31 || h > 23 || mi > 59 ||
        se > 59)
    {
        return FALSE;
    }
    *out = EeAudit_MakeTime(y, mo, d, h, mi, se);
    return TRUE;
}

/* "3/10/2026 10:42:39 AM" -> seconds, or 0. */
static int64_t hv_mdy_time(const char *s)
{
    const char *p;
    int date = hv_mdy(s, &p), secs;
    if (date == 0 || (secs = hv_clock(p)) < 0)
    {
        return 0;
    }
    return EeAudit_MakeTime(date / 10000, date / 100 % 100, date % 100, 0, 0, 0) + secs;
}

/* -------------------------------------------------------------------------- */
/* Reading the report                                                         */
/* -------------------------------------------------------------------------- */

/* Columns by the left edge of their text (the same in every report seen: Verity 2.4 - 2.7). */
#define HV_X_TIME   45.0f
#define HV_X_DEVICE 135.0f
#define HV_X_USER   195.0f
#define HV_X_EVENT  255.0f
#define HV_X_DATA   400.0f

enum
{
    HV_TIME = 0,
    HV_DEVICE,
    HV_USER,
    HV_EVENT,
    HV_DATA,
    HV_NFIELDS
};

typedef struct HvRun
{
    float x;
    float y;
    const char *t; /* trimmed, NUL-terminated (in HvPage.buf) */
    size_t n;
} HvRun;

typedef struct HvPage
{
    HvRun *runs;
    size_t nruns;
    size_t cap_runs;
    char *buf;
    size_t buf_cap;
} HvPage;

static int hv_cmp_run(const void *pa, const void *pb)
{
    const HvRun *a = (const HvRun *)pa;
    const HvRun *b = (const HvRun *)pb;
    if (a->y > b->y + 0.5f)
    {
        return -1;
    }
    if (a->y < b->y - 0.5f)
    {
        return 1;
    }
    return a->x < b->x ? -1 : (a->x > b->x ? 1 : 0);
}

/* The page's upright runs, trimmed (empty ones and no-break-space runs dropped), top down. */
static BOOL hv_collect(HvPage *pg, const EePdfPageText *pt)
{
    uint32_t i;
    size_t need = 1, pos = 0;
    pg->nruns = 0;
    for (i = 0; i < pt->nruns; i++)
    {
        need += pt->runs[i].text_len + 1;
    }
    if (!hv_grow((void **)&pg->buf, &pg->buf_cap, need, 1) ||
        !hv_grow((void **)&pg->runs, &pg->cap_runs, (size_t)pt->nruns + 1, sizeof(HvRun)))
    {
        return FALSE;
    }
    for (i = 0; i < pt->nruns; i++)
    {
        const EePdfTextRun *r = &pt->runs[i];
        const char *t = pt->text + r->text_off;
        size_t n = r->text_len;
        HvRun *h;
        for (;;)
        {
            if (n > 0 && (t[0] == ' ' || t[0] == '\t'))
            {
                t++;
                n--;
            }
            else if (n > 1 && (unsigned char)t[0] == 0xC2 && (unsigned char)t[1] == 0xA0)
            {
                t += 2;
                n -= 2;
            }
            else
            {
                break;
            }
        }
        for (;;)
        {
            if (n > 0 && (t[n - 1] == ' ' || t[n - 1] == '\t' || t[n - 1] == '\r' || t[n - 1] == '\n'))
            {
                n--;
            }
            else if (n > 1 && (unsigned char)t[n - 2] == 0xC2 && (unsigned char)t[n - 1] == 0xA0)
            {
                n -= 2;
            }
            else
            {
                break;
            }
        }
        if (n == 0 || r->rot != 0)
        {
            continue;
        }
        h = &pg->runs[pg->nruns++];
        memcpy(pg->buf + pos, t, n);
        pg->buf[pos + n] = '\0';
        h->t = pg->buf + pos;
        h->n = n;
        h->x = r->x;
        h->y = r->y;
        pos += n + 1;
    }
    qsort(pg->runs, pg->nruns, sizeof(HvRun), hv_cmp_run);
    return TRUE;
}

static const HvRun *hv_find(const HvPage *pg, const char *text)
{
    size_t i;
    for (i = 0; i < pg->nruns; i++)
    {
        if (strcmp(pg->runs[i].t, text) == 0)
        {
            return &pg->runs[i];
        }
    }
    return NULL;
}

/* The run right of a label on its line (the label's value). */
static const HvRun *hv_value_of(const HvPage *pg, const HvRun *label)
{
    const HvRun *best = NULL;
    size_t i;
    if (label == NULL)
    {
        return NULL;
    }
    for (i = 0; i < pg->nruns; i++)
    {
        const HvRun *r = &pg->runs[i];
        if (r != label && fabsf(r->y - label->y) < 1.5f && r->x > label->x && (best == NULL || r->x < best->x))
        {
            best = r;
        }
    }
    return best;
}

/* The log kind a Component names ("VerityCount 2.7.5"), or 0. */
static int hv_component_kind(const char *comp)
{
    if (strncmp(comp, "VerityCount", 11) == 0)
    {
        return EE_AUDIT_HART_COUNT;
    }
    if (strncmp(comp, "VerityCentral", 13) == 0)
    {
        return EE_AUDIT_HART_CENTRAL;
    }
    if (strncmp(comp, "VerityData", 10) == 0 || strncmp(comp, "VerityBuild", 11) == 0)
    {
        return EE_AUDIT_HART_DATA;
    }
    return 0;
}

/* Page 1's header into @p c. FALSE when it is not a Verity audit log report. */
static BOOL hv_header(EeAuditCopy *c, const HvPage *pg)
{
    const HvRun *comp = hv_find(pg, "Component:");
    const HvRun *v, *top = NULL;
    int run_date = 0, run_secs = -1;
    size_t i;
    if (comp == NULL || hv_find(pg, "Audit Log Report") == NULL || (v = hv_value_of(pg, comp)) == NULL)
    {
        return FALSE;
    }
    c->kind = hv_component_kind(v->t);
    c->component = hv_strdup_n(v->t, v->n);
    if ((v = hv_value_of(pg, hv_find(pg, "Run Date"))) != NULL)
    {
        run_date = hv_mdy(v->t, NULL);
    }
    if ((v = hv_value_of(pg, hv_find(pg, "Run Time"))) != NULL)
    {
        run_secs = hv_clock(v->t);
    }
    if (run_date != 0)
    {
        c->printed = EeAudit_MakeTime(run_date / 10000, run_date / 100 % 100, run_date % 100, 0, 0, 0) +
                     (run_secs > 0 ? run_secs : 0);
    }
    for (i = 0; i < pg->nruns; i++)
    {
        const HvRun *r = &pg->runs[i];
        const char *bar;
        int d;
        if (r->y < comp->y + 2.0f)
        {
            continue;
        }
        if (top == NULL || r->y > top->y + 0.5f)
        {
            top = r;
        }
        if (c->election == NULL && (bar = strstr(r->t, " | ")) != NULL)
        {
            size_t n = (size_t)(bar - r->t);
            while (n > 0 && r->t[n - 1] == ' ')
            {
                n--;
            }
            c->election = hv_strdup_n(r->t, n);
        }
        else if (r->x > 300.0f && (d = hv_mdy(r->t, NULL)) != 0)
        {
            c->election_date = d;
        }
    }
    if (top != NULL && top->x > 250.0f)
    {
        /* "Burnet County, Texas", "PARKER COUNTY, TEXAS" */
        size_t n = top->n;
        const char *comma = strrchr(top->t, ',');
        if (comma != NULL && _stricmp(comma, ", Texas") == 0)
        {
            n = (size_t)(comma - top->t);
        }
        c->customer = hv_strdup_n(top->t, n);
    }
    return TRUE;
}

BOOL AuHart_IsReport(EePdf *pdf)
{
    char title[128];
    EePdfPageText pt;
    HvPage pg;
    BOOL yes = FALSE;
    if (EePdf_GetInfoText(pdf, "Title", title, sizeof(title)) && strstr(title, "AuditLogReport") != NULL &&
        strstr(title, "Verity") != NULL)
    {
        return TRUE;
    }
    if (EePdf_PageCount(pdf) == 0)
    {
        return FALSE;
    }
    EePdf_PageTextInit(&pt);
    memset(&pg, 0, sizeof(pg));
    if (EePdf_ExtractPageText(pdf, 0, &pt) && hv_collect(&pg, &pt))
    {
        const HvRun *comp = hv_find(&pg, "Component:");
        const HvRun *v = hv_value_of(&pg, comp);
        yes = hv_find(&pg, "Audit Log Report") != NULL && v != NULL && strncmp(v->t, "Verity", 6) == 0;
    }
    free(pg.runs);
    free(pg.buf);
    EePdf_PageTextFree(&pt);
    return yes;
}

/* The row being read: it can run onto the next page. */
typedef struct HvRow
{
    BOOL open;
    BOOL skip;    /* a row printed again in full at a page's top: its text is dropped */
    uint32_t row; /* its number */
    uint32_t page;
    HvBuf f[HV_NFIELDS];
} HvRow;

/* Add the open row to @p c as an event. */
static BOOL hv_flush(EeAuditCopy *c, HvRow *row, int64_t *last_ts)
{
    int64_t ts;
    if (!row->open)
    {
        return TRUE;
    }
    row->open = FALSE;
    if (hv_iso(hv_buf_str(&row->f[HV_TIME]), &ts))
    {
        *last_ts = ts;
    }
    else
    {
        ts = *last_ts; /* unreadable: keep it in order */
    }
    return EeAudit_AddEvent(c, ts, hv_buf_str(&row->f[HV_EVENT]), hv_buf_str(&row->f[HV_DATA]),
                            hv_buf_str(&row->f[HV_USER]), hv_buf_str(&row->f[HV_DEVICE]), row->page);
}

static BOOL hv_all_digits(const char *s, size_t n)
{
    size_t i;
    for (i = 0; i < n; i++)
    {
        if (s[i] < '0' || s[i] > '9')
        {
            return FALSE;
        }
    }
    return n > 0 && n < 10;
}

/* One page's table rows. */
static BOOL hv_page_rows(EeAuditCopy *c, const HvPage *pg, uint32_t page_no, HvRow *row, uint32_t *last_row,
                         int64_t *last_ts)
{
    const HvRun *comp = hv_find(pg, "Component:");
    const HvRun *head = hv_find(pg, "Row");
    float top;
    size_t i;
    if (comp != NULL)
    {
        top = comp->y - 5.0f;
    }
    else if (head != NULL)
    {
        top = head->y + 10.0f;
    }
    else
    {
        return TRUE; /* no table on the page */
    }
    for (i = 0; i < pg->nruns; i++)
    {
        const HvRun *r = &pg->runs[i];
        int field;
        char sep = ' ';
        if (r->y >= top || r->y < 3.0f || (head != NULL && fabsf(r->y - head->y) < 0.6f))
        {
            continue; /* the header, or the table heading */
        }
        if (r->x < HV_X_TIME)
        {
            uint32_t no;
            if (!hv_all_digits(r->t, r->n))
            {
                continue;
            }
            no = (uint32_t)strtoul(r->t, NULL, 10);
            if (no == row->row && (row->open || row->skip))
            {
                row->skip = TRUE; /* printed again in full */
                continue;
            }
            if (!hv_flush(c, row, last_ts))
            {
                return FALSE;
            }
            if (no > *last_row + 1)
            {
                c->missing_rows += no - *last_row - 1;
            }
            if (no > *last_row)
            {
                *last_row = no;
            }
            for (field = 0; field < HV_NFIELDS; field++)
            {
                hv_buf_clear(&row->f[field]);
            }
            row->open = TRUE;
            row->skip = FALSE;
            row->row = no;
            row->page = page_no;
            continue;
        }
        if (!row->open || row->skip)
        {
            continue; /* above the first row, or a reprinted row's text */
        }
        field = r->x < HV_X_DEVICE ? HV_TIME
                : r->x < HV_X_USER ? HV_DEVICE
                : r->x < HV_X_EVENT ? HV_USER
                : r->x < HV_X_DATA ? HV_EVENT
                                   : HV_DATA;
        if (field == HV_TIME)
        {
            sep = 0; /* "2026-03-03T16:19:46-" + "06:00" */
        }
        if (!hv_buf_add(&row->f[field], r->t, r->n, sep))
        {
            return FALSE;
        }
    }
    return TRUE;
}

EeLoadStatus AuHart_ReadPdf(EePdf *pdf,
                            const wchar_t *path,
                            EeAuditCopy *out,
                            volatile LONG *cancel_flag,
                            EeLoadProgressFn progress_fn,
                            void *progress_user,
                            wchar_t *err,
                            size_t errcch)
{
    EePdfPageText pt;
    HvPage pg;
    HvRow row;
    uint32_t p, npages, last_row = 0, i;
    int64_t last_ts = 0;
    EeLoadStatus st = EeLoadStatus_Ok;

    npages = EePdf_PageCount(pdf);
    out->pages = npages;
    EePdf_PageTextInit(&pt);
    memset(&pg, 0, sizeof(pg));
    memset(&row, 0, sizeof(row));
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
        if (!EePdf_ExtractPageText(pdf, p, &pt) || !hv_collect(&pg, &pt))
        {
            hv_set_err(err, errcch, L"A page of %s could not be read.", path);
            st = EeLoadStatus_Error;
            break;
        }
        if (p == 0)
        {
            if (!hv_header(out, &pg))
            {
                hv_set_err(err, errcch, L"%s is not a Hart Verity audit log report.", path);
                st = EeLoadStatus_Error;
                break;
            }
            if (out->kind == 0)
            {
                hv_set_err(err, errcch,
                           L"%s is the audit log of a Hart Verity component other than VerityCount, "
                           L"VerityCentral or VerityData.",
                           path);
                st = EeLoadStatus_Error;
                break;
            }
        }
        if (!hv_page_rows(out, &pg, p + 1, &row, &last_row, &last_ts))
        {
            hv_set_err(err, errcch, L"Out of memory reading %s.", path);
            st = EeLoadStatus_Error;
            break;
        }
    }
    if (st == EeLoadStatus_Ok && !hv_flush(out, &row, &last_ts))
    {
        hv_set_err(err, errcch, L"Out of memory reading %s.", path);
        st = EeLoadStatus_Error;
    }
    for (i = 0; i < HV_NFIELDS; i++)
    {
        free(row.f[i].p);
    }
    free(pg.runs);
    free(pg.buf);
    EePdf_PageTextFree(&pt);
    if (st == EeLoadStatus_Ok)
    {
        /* the report is oldest first; copies are kept newest first */
        for (i = 0; i < out->nevents / 2; i++)
        {
            EeAuditEvent t = out->events[i];
            out->events[i] = out->events[out->nevents - 1 - i];
            out->events[out->nevents - 1 - i] = t;
        }
    }
    return st;
}

/* -------------------------------------------------------------------------- */
/* Test tasks                                                                 */
/* -------------------------------------------------------------------------- */

#define HV_MAX_WORDS 16

static char g_hv_words[HV_MAX_WORDS][40];
static int g_hv_nwords = -1; /* -1: not set yet (the default) */

static const wchar_t k_hv_default_words[] = L"LAT, L&A, Test, Training, Logic";

const wchar_t *EeAudit_DefaultTestTaskWords(void)
{
    return k_hv_default_words;
}

void EeAudit_SetTestTaskWords(const wchar_t *words)
{
    char buf[HV_MAX_WORDS * 40];
    char *p, *ctx = NULL;
    if (words == NULL || words[0] == L'\0')
    {
        words = k_hv_default_words;
    }
    if (WideCharToMultiByte(CP_UTF8, 0, words, -1, buf, (int)sizeof(buf), NULL, NULL) == 0)
    {
        buf[0] = '\0';
    }
    g_hv_nwords = 0;
    for (p = strtok_s(buf, ",;", &ctx); p != NULL && g_hv_nwords < HV_MAX_WORDS; p = strtok_s(NULL, ",;", &ctx))
    {
        size_t n;
        while (*p == ' ')
        {
            p++;
        }
        n = strlen(p);
        while (n > 0 && p[n - 1] == ' ')
        {
            n--;
        }
        if (n > 0 && n < sizeof(g_hv_words[0]))
        {
            memcpy(g_hv_words[g_hv_nwords], p, n);
            g_hv_words[g_hv_nwords][n] = '\0';
            g_hv_nwords++;
        }
    }
}

BOOL EeAudit_IsTestTask(const char *name)
{
    int w;
    if (name == NULL)
    {
        return FALSE;
    }
    if (g_hv_nwords < 0)
    {
        EeAudit_SetTestTaskWords(NULL);
    }
    for (w = 0; w < g_hv_nwords; w++)
    {
        const char *word = g_hv_words[w];
        size_t n = strlen(word);
        const char *s;
        for (s = name; *s != '\0'; s++)
        {
            size_t before = (size_t)(s - name), k;
            BOOL prefix = TRUE;
            if (_strnicmp(s, word, n) != 0 || hv_is_alpha(s[n]))
            {
                continue;
            }
            /* at the start, after a non-letter, or after a short prefix ("PreLAT", "PLAT") */
            for (k = 0; k < before; k++)
            {
                prefix = prefix && hv_is_alpha(name[k]);
            }
            if (before == 0 || !hv_is_alpha(s[-1]) || (prefix && before <= 3))
            {
                return TRUE;
            }
        }
    }
    return FALSE;
}

/* -------------------------------------------------------------------------- */
/* Events of a VerityCount log                                                */
/* -------------------------------------------------------------------------- */

/* The task a "Task opened" event names ("Task: 'X'", "The task 'X' was opened"). */
static char *hv_task_name(const char *level, const char *d)
{
    const char *a, *b;
    if (strncmp(level, "Task opened", 11) != 0)
    {
        return NULL;
    }
    if (strncmp(d, "Task: '", 7) == 0)
    {
        a = d + 7;
        b = strrchr(a, '\'');
    }
    else if (strncmp(d, "The task '", 10) == 0)
    {
        a = d + 10;
        b = strstr(a, "' was opened");
    }
    else
    {
        return NULL;
    }
    if (b == NULL || b < a)
    {
        b = a + strlen(a);
    }
    return hv_strdup_n(a, (size_t)(b - a));
}

/* A "vDrive read" or "vDrive saved" event's values. */
typedef struct HvRead
{
    BOOL saved;
    char *place;
    int64_t ballots;
    int64_t time_read; /* 0 when unreadable */
    BOOL state_read;   /* State- Read */
    char *device;      /* vDrive ID */
} HvRead;

static void hv_read_free(HvRead *r)
{
    free(r->place);
    free(r->device);
    memset(r, 0, sizeof(*r));
}

/* The text after @p key up to @p stop (or the end), trimmed of ", " and spaces. */
static char *hv_field(const char *d, const char *key, const char *stop)
{
    const char *a = strstr(d, key), *b;
    size_t n;
    if (a == NULL)
    {
        return NULL;
    }
    a += strlen(key);
    b = stop != NULL ? strstr(a, stop) : NULL;
    n = b != NULL ? (size_t)(b - a) : strlen(a);
    while (n > 0 && (a[n - 1] == ' ' || a[n - 1] == ','))
    {
        n--;
    }
    return hv_strdup_n(a, n);
}

static BOOL hv_parse_read(const char *d, HvRead *out)
{
    const char *p;
    char *s;
    memset(out, 0, sizeof(*out));
    if (strncmp(d, "vDrive read with values: ", 25) == 0)
    {
        out->saved = FALSE;
    }
    else if (strncmp(d, "vDrive saved with values: ", 26) == 0)
    {
        out->saved = TRUE;
    }
    else
    {
        return FALSE;
    }
    out->place = hv_field(d, "Polling Place- '", "', Ballot count");
    if ((p = strstr(d, "Ballot count = ")) == NULL)
    {
        hv_read_free(out);
        return FALSE;
    }
    out->ballots = _strtoi64(p + 15, NULL, 10);
    if ((s = hv_field(d, "Time read- ", ", State-")) != NULL)
    {
        out->time_read = hv_mdy_time(s);
        free(s);
    }
    if ((s = hv_field(d, "State- ", ",")) != NULL)
    {
        out->state_read = strcmp(s, "Read") == 0;
        free(s);
    }
    out->device = hv_field(d, "vDrive ID- ", ", State reason");
    return TRUE;
}

/* vDrive IDs are the same: compared without spaces (a wrapped line can add or lose one). */
static BOOL hv_same_id(const char *a, const char *b)
{
    if (a == NULL || b == NULL || a[0] == '\0' || b[0] == '\0')
    {
        return FALSE;
    }
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

/* -------------------------------------------------------------------------- */
/* Ballot accounting                                                          */
/* -------------------------------------------------------------------------- */

#define HV_SLOT_COUNT   (EE_AUDIT_HART_COUNT - 1)
#define HV_SLOT_CENTRAL (EE_AUDIT_HART_CENTRAL - 1)
#define HV_SLOT_DATA    (EE_AUDIT_HART_DATA - 1)

/* A read of the Count log, before the reads are grouped by task. */
typedef struct HvTaskRead
{
    uint32_t event;
    int64_t ts;
    uint32_t task;
    char *place;
    char *device;
    int64_t ballots;
} HvTaskRead;

/* The session (task) named @p name, added when new. -1 out of memory. */
static int hv_task_session(EeAuditAccounting *a, size_t *cap, const char *name, int64_t ts)
{
    uint32_t i;
    EeAuditSession *s;
    for (i = 0; i < a->nsessions; i++)
    {
        if (strcmp(a->sessions[i].name, name) == 0)
        {
            return (int)i;
        }
    }
    if (!hv_grow((void **)&a->sessions, cap, (size_t)a->nsessions + 1, sizeof(EeAuditSession)))
    {
        return -1;
    }
    s = &a->sessions[a->nsessions];
    memset(s, 0, sizeof(*s));
    s->name = hv_strdup_n(name, strlen(name));
    if (s->name == NULL)
    {
        return -1;
    }
    s->start = s->end = ts;
    s->test = EeAudit_IsTestTask(name);
    return (int)a->nsessions++;
}

/* Count log: tasks and their reads. */
static BOOL hv_count_reads(const EeAuditLog *log, EeAuditAccounting *a, HvTaskRead **reads, uint32_t *nreads)
{
    size_t cap_s = 0, cap_r = 0;
    uint32_t i;
    int task = -1;
    for (i = log->nevents; i-- > 0;) /* oldest first */
    {
        const EeAuditCopy *c;
        const EeAuditEvent *e = EeAudit_MergedEvent(log, i, &c);
        const char *d = EeAudit_Desc(c, e);
        const char *level = EeAudit_Str(c, e->level);
        char *name = hv_task_name(level, d);
        HvRead rd;
        if (name != NULL)
        {
            task = hv_task_session(a, &cap_s, name, e->ts);
            free(name);
            if (task < 0)
            {
                return FALSE;
            }
        }
        else if (strncmp(d, "vDrive read with values: ", 25) == 0 && hv_parse_read(d, &rd))
        {
            if (rd.state_read)
            {
                HvTaskRead *r;
                if (task < 0 && (task = hv_task_session(a, &cap_s, "(no task opened)", e->ts)) < 0)
                {
                    hv_read_free(&rd);
                    return FALSE;
                }
                if (!hv_grow((void **)reads, &cap_r, (size_t)*nreads + 1, sizeof(HvTaskRead)))
                {
                    hv_read_free(&rd);
                    return FALSE;
                }
                r = &(*reads)[(*nreads)++];
                r->event = i;
                r->ts = e->ts;
                r->task = (uint32_t)task;
                r->place = rd.place;
                r->device = rd.device;
                r->ballots = rd.ballots;
                rd.place = rd.device = NULL;
            }
            hv_read_free(&rd);
        }
        else if (strncmp(level, "vDrive rejected", 15) == 0)
        {
            a->rejected++;
        }
        if (task >= 0)
        {
            a->sessions[task].end = e->ts;
        }
    }
    return TRUE;
}

/* The reads as batches, grouped by task in the order first opened. */
static BOOL hv_batches(EeAuditAccounting *a, HvTaskRead *reads, uint32_t nreads)
{
    uint32_t si, i, k;
    int64_t best_last = 0;
    a->batches = (EeAuditBatch *)calloc((size_t)nreads + 1, sizeof(EeAuditBatch));
    if (a->batches == NULL)
    {
        return FALSE;
    }
    a->election_session = -1;
    for (si = 0; si < a->nsessions; si++)
    {
        EeAuditSession *s = &a->sessions[si];
        int64_t last = 0;
        uint32_t id = 0;
        s->first_batch = a->nbatches;
        for (i = 0; i < nreads; i++)
        {
            EeAuditBatch *b;
            if (reads[i].task != si)
            {
                continue;
            }
            b = &a->batches[a->nbatches++];
            b->event = reads[i].event;
            b->id = ++id;
            b->name = reads[i].place;
            b->place = hv_strdup_n(b->name != NULL ? b->name : "", b->name != NULL ? strlen(b->name) : 0);
            b->device = reads[i].device;
            reads[i].place = reads[i].device = NULL;
            b->media = -1;
            b->ballots = reads[i].ballots;
            b->ts = reads[i].ts;
            b->session = si;
            b->status = EE_AUDIT_BATCH_COUNTED;
            b->vote_type = EeAudit_VoteTypeOf(b->place != NULL ? b->place : "");
            b->test = s->test;
            if (b->place == NULL)
            {
                return FALSE;
            }
            if (b->name == NULL)
            {
                b->name = hv_strdup_n("", 0);
            }
        }
        s->nbatches = a->nbatches - s->first_batch;
        /* a vDrive read again into the task replaces its earlier read */
        for (i = s->first_batch; i < a->nbatches; i++)
        {
            for (k = i + 1; k < a->nbatches; k++)
            {
                if (hv_same_id(a->batches[i].device, a->batches[k].device))
                {
                    a->batches[i].status = EE_AUDIT_BATCH_REPLACED;
                    break;
                }
            }
        }
        for (i = s->first_batch; i < a->nbatches; i++)
        {
            const EeAuditBatch *b = &a->batches[i];
            if (b->status != EE_AUDIT_BATCH_COUNTED)
            {
                continue;
            }
            s->counted++;
            s->ballots += b->ballots;
            s->ballots_vt[b->vote_type] += b->ballots;
            last = b->ts;
        }
        /* the election: the official task read into last */
        if (!s->test && s->counted > 0 && (a->election_session < 0 || last >= best_last))
        {
            a->election_session = (int)si;
            best_last = last;
        }
    }
    return TRUE;
}

/* Central log: the vDrives written ("CVRs written to vDrive" then "vDrive closed"). */
static BOOL hv_transfers(const EeAuditElection *el, EeAuditAccounting *a)
{
    const EeAuditLog *log = &el->logs[HV_SLOT_CENTRAL];
    BOOL have_count = el->logs[HV_SLOT_COUNT].ncopies > 0;
    size_t cap = 0;
    uint32_t i, k;
    int open = -1; /* the transfer awaiting its vDrive closed */
    for (i = log->nevents; i-- > 0;)
    {
        const EeAuditCopy *c;
        const EeAuditEvent *e = EeAudit_MergedEvent(log, i, &c);
        const char *d = EeAudit_Desc(c, e);
        const char *level = EeAudit_Str(c, e->level);
        if (strncmp(level, "CVRs written to vDrive", 22) == 0)
        {
            EeAuditTransfer *t;
            const char *p;
            if (!hv_grow((void **)&a->transfers, &cap, (size_t)a->ntransfers + 1, sizeof(EeAuditTransfer)))
            {
                return FALSE;
            }
            t = &a->transfers[a->ntransfers];
            memset(t, 0, sizeof(*t));
            t->event = i;
            t->ts = e->ts;
            t->batches = hv_field(d, "Batch(es): [", "]");
            if ((p = strstr(d, "Total ballots: ")) != NULL)
            {
                t->ballots = _strtoi64(p + 15, NULL, 10);
            }
            t->read_batch = -1;
            t->status = have_count ? EE_AUDIT_XFER_UNREAD : EE_AUDIT_XFER_NO_COUNT;
            open = (int)a->ntransfers++;
        }
        else if (strncmp(level, "vDrive closed", 13) == 0 && open >= 0)
        {
            a->transfers[open].device = hv_field(d, "vDrive Device Data ID: ", NULL);
            open = -1;
        }
    }
    for (k = 0; k < a->ntransfers && have_count; k++)
    {
        EeAuditTransfer *t = &a->transfers[k];
        for (i = 0; i < a->nbatches; i++)
        {
            const EeAuditBatch *b = &a->batches[i];
            /* the last read, an official task's before a test task's */
            if (hv_same_id(t->device, b->device) && (t->read_batch < 0 || !b->test || a->batches[t->read_batch].test))
            {
                t->read_batch = (int)i;
            }
        }
        if (t->read_batch >= 0)
        {
            t->read_ballots = a->batches[t->read_batch].ballots;
            t->status = t->read_ballots == t->ballots ? EE_AUDIT_XFER_MATCHED : EE_AUDIT_XFER_DIFFERS;
        }
    }
    /* a vDrive whose closing was not logged: the first read of its ballot count within two
     * days after it that no other vDrive was matched to */
    for (k = 0; k < a->ntransfers && have_count; k++)
    {
        EeAuditTransfer *t = &a->transfers[k];
        int best = -1;
        if (t->device != NULL || t->read_batch >= 0)
        {
            continue;
        }
        for (i = 0; i < a->nbatches; i++)
        {
            const EeAuditBatch *b = &a->batches[i];
            uint32_t j;
            BOOL used = FALSE;
            if (b->ballots != t->ballots || b->ts < t->ts || b->ts - t->ts > 2 * 86400 ||
                (best >= 0 && a->batches[best].ts <= b->ts))
            {
                continue;
            }
            for (j = 0; j < a->ntransfers && !used; j++)
            {
                used = a->transfers[j].read_batch == (int)i;
            }
            if (!used)
            {
                best = (int)i;
            }
        }
        if (best >= 0)
        {
            t->read_batch = best;
            t->read_ballots = a->batches[best].ballots;
            t->by_count = TRUE;
            t->status = EE_AUDIT_XFER_MATCHED;
        }
    }
    return TRUE;
}

BOOL EeAudit_HartAccounting(const EeAuditElection *el, EeAuditAccounting *out)
{
    HvTaskRead *reads = NULL;
    uint32_t nreads = 0, i;
    BOOL ok;
    memset(out, 0, sizeof(*out));
    out->election_session = -1;
    out->hart = TRUE;
    out->log = HV_SLOT_COUNT;
    if (el == NULL)
    {
        return TRUE;
    }
    ok = hv_count_reads(&el->logs[HV_SLOT_COUNT], out, &reads, &nreads) && hv_batches(out, reads, nreads) &&
         hv_transfers(el, out);
    for (i = 0; i < nreads; i++)
    {
        free(reads[i].place);
        free(reads[i].device);
    }
    free(reads);
    if (!ok)
    {
        EeAudit_FreeAccounting(out);
        return FALSE;
    }
    return TRUE;
}

/* -------------------------------------------------------------------------- */
/* Notable events                                                             */
/* -------------------------------------------------------------------------- */

/* Reports that are not results. */
static const char *const k_hv_not_results[] = {"Device Log",    "Audit Log",     "Scanned Batch",
                                               "vDrive Status", "Precincts Reporting", "Polling Places",
                                               "Configuration", "Device Report", "Batch Summary",
                                               "Registered Voter"};

/* A results report printed or exported, or CVRs exported. */
static BOOL hv_is_results_output(const char *level, const char *d)
{
    const char *name = NULL;
    size_t i;
    if (strncmp(level, "CVRs exported", 13) == 0)
    {
        return TRUE;
    }
    if (strncmp(level, "Print", 5) == 0 && strstr(d, "' printed to '") != NULL)
    {
        name = d;
    }
    else if ((strncmp(level, "Export report", 13) == 0 || strncmp(level, "Report exported", 15) == 0) &&
             strstr(d, "' was exported to") != NULL)
    {
        name = d;
    }
    else if (strncmp(level, "Export data", 11) == 0 && strncmp(d, "Exported '", 10) == 0)
    {
        name = d + 9;
    }
    if (name == NULL)
    {
        return FALSE;
    }
    for (i = 0; i < sizeof(k_hv_not_results) / sizeof(k_hv_not_results[0]); i++)
    {
        const char *at = hv_find_ci(name, k_hv_not_results[i]);
        const char *q = name[0] == '\'' ? strchr(name + 1, '\'') : NULL;
        if (at != NULL && (q == NULL || at < q)) /* in the report's name */
        {
            return FALSE;
        }
    }
    return TRUE;
}

/* An error: Hart's error and exception events, a scanner error, a failed import (its "Import
 * failure reason" rows are details of it). */
static BOOL hv_is_error(const char *level, const char *d)
{
    return strncmp(level, "Error occurred", 14) == 0 || strncmp(level, "Exception occurred", 18) == 0 ||
           strncmp(level, "Scanner error", 13) == 0 ||
           (strncmp(level, "Election data imported", 22) == 0 && hv_find_ci(d, "failed") != NULL);
}

typedef struct HvNotes
{
    EeAuditNotes *n;
    size_t cap;
} HvNotes;

static BOOL hv_note(HvNotes *hn, int category, int log, uint32_t event, uint32_t batches, int64_t value)
{
    EeAuditNote *x;
    if (!hv_grow((void **)&hn->n->notes, &hn->cap, (size_t)hn->n->count + 1, sizeof(EeAuditNote)))
    {
        return FALSE;
    }
    x = &hn->n->notes[hn->n->count++];
    x->category = category;
    x->log = log;
    x->event = event;
    x->batches = batches;
    x->value = value;
    hn->n->per_category[category]++;
    return TRUE;
}

/* The reads of session @p si counted when the merged event @p at happened (older than it). */
static void hv_loaded_at(const EeAuditAccounting *a, uint32_t si, uint32_t at, uint32_t *batches, int64_t *ballots)
{
    const EeAuditSession *s = &a->sessions[si];
    uint32_t i, k;
    *batches = 0;
    *ballots = 0;
    for (i = s->first_batch; i < s->first_batch + s->nbatches; i++)
    {
        const EeAuditBatch *b = &a->batches[i];
        BOOL replaced = FALSE;
        if (b->event <= at)
        {
            continue; /* not read yet */
        }
        for (k = i + 1; k < s->first_batch + s->nbatches && !replaced; k++)
        {
            replaced = a->batches[k].event > at && hv_same_id(b->device, a->batches[k].device);
        }
        if (!replaced)
        {
            (*batches)++;
            *ballots += b->ballots;
        }
    }
}

/* The value a manual vote record event gives: its ballots. */
static int64_t hv_manual_ballots(const char *d)
{
    const char *p = strstr(d, "Number of ballots- ");
    if (p != NULL)
    {
        return _strtoi64(p + 19, NULL, 10);
    }
    p = strstr(d, ", with ");
    return p != NULL ? _strtoi64(p + 7, NULL, 10) : 0;
}

static BOOL hv_notes_count(const EeAuditElection *el, const EeAuditAccounting *a, HvNotes *hn)
{
    const EeAuditLog *log = &el->logs[HV_SLOT_COUNT];
    int d0 = el->election_date;
    int64_t eday = d0 > 0 ? EeAudit_MakeTime(d0 / 10000, d0 / 100 % 100, d0 % 100, 0, 0, 0) : 0;
    int task = -1;
    uint32_t i, k;
    for (i = log->nevents; i-- > 0;) /* oldest first */
    {
        const EeAuditCopy *c;
        const EeAuditEvent *e = EeAudit_MergedEvent(log, i, &c);
        const char *d = EeAudit_Desc(c, e);
        const char *level = EeAudit_Str(c, e->level);
        char *name = hv_task_name(level, d);
        int cat = -1;
        uint32_t batches = 0;
        int64_t value = 0;
        HvRead rd;
        if (name != NULL)
        {
            task = -1;
            for (k = 0; k < a->nsessions; k++)
            {
                if (strcmp(a->sessions[k].name, name) == 0)
                {
                    task = (int)k;
                }
            }
            free(name);
            continue;
        }
        if (hv_is_results_output(level, d))
        {
            if (eday != 0 && e->ts < eday)
            {
                int64_t ballots = 0;
                if (task >= 0)
                {
                    hv_loaded_at(a, (uint32_t)task, i, &batches, &ballots);
                }
                if (task >= 0 && !a->sessions[task].test && ballots > 0)
                {
                    cat = EE_AUDIT_N_RESULTS_EARLY;
                    value = ballots;
                }
                else
                {
                    cat = EE_AUDIT_N_RESULTS_TEST;
                    value = 0;
                }
            }
        }
        else if (hv_parse_read(d, &rd))
        {
            if (!rd.saved && rd.state_read && task >= 0 && !a->sessions[task].test && rd.ballots > 0 && eday != 0 &&
                e->ts < eday - (int64_t)EE_AUDIT_EARLY_COUNT_DAYS * 86400)
            {
                cat = EE_AUDIT_N_LOADED_EARLY;
                value = rd.ballots;
            }
            else if (rd.time_read != 0 && (e->ts - rd.time_read > 600 || rd.time_read - e->ts > 600))
            {
                cat = EE_AUDIT_N_CLOCK;
                value = e->ts - rd.time_read;
            }
            hv_read_free(&rd);
        }
        else if (strncmp(level, "Task deleted", 12) == 0)
        {
            cat = EE_AUDIT_N_DELETED;
        }
        else if (hv_is_error(level, d))
        {
            cat = EE_AUDIT_N_ERRORS;
        }
        else if (strncmp(level, "Manual vote record created", 26) == 0 ||
                 strncmp(level, "Manual vote record deleted", 26) == 0)
        {
            cat = EE_AUDIT_N_MANUAL_VOTES;
            value = hv_manual_ballots(d);
        }
        else if (strstr(level, "reporting status manually overridden") != NULL ||
                 strncmp(level, "Open vDrive override", 20) == 0)
        {
            cat = EE_AUDIT_N_OVERRIDES;
        }
        else if (strncmp(level, "vDrive rejected", 15) == 0)
        {
            cat = EE_AUDIT_N_VDRIVE_REJECTED;
        }
        else if (strncmp(level, "Registered Voters updated", 25) == 0)
        {
            cat = EE_AUDIT_N_REG_VOTERS;
        }
        if (cat >= 0 && !hv_note(hn, cat, HV_SLOT_COUNT, i, batches, value))
        {
            return FALSE;
        }
    }
    return TRUE;
}

/* Central and Data logs: errors, deleted tasks, and (Central) vDrives Count read otherwise. */
static BOOL hv_notes_other(const EeAuditElection *el, const EeAuditAccounting *a, int slot, HvNotes *hn)
{
    const EeAuditLog *log = &el->logs[slot];
    uint32_t i, k;
    for (i = log->nevents; i-- > 0;)
    {
        const EeAuditCopy *c;
        const EeAuditEvent *e = EeAudit_MergedEvent(log, i, &c);
        const char *d = EeAudit_Desc(c, e);
        const char *level = EeAudit_Str(c, e->level);
        int cat = -1;
        int64_t value = 0;
        if (slot == HV_SLOT_CENTRAL && strncmp(level, "CVRs written to vDrive", 22) == 0)
        {
            for (k = 0; k < a->ntransfers; k++)
            {
                const EeAuditTransfer *t = &a->transfers[k];
                if (t->event == i && (t->status == EE_AUDIT_XFER_DIFFERS || t->status == EE_AUDIT_XFER_UNREAD))
                {
                    cat = EE_AUDIT_N_TRANSFER;
                    value = t->status == EE_AUDIT_XFER_DIFFERS ? t->read_ballots - t->ballots : 0;
                }
            }
        }
        else if (strncmp(level, "Task deleted", 12) == 0)
        {
            cat = EE_AUDIT_N_DELETED;
        }
        else if (hv_is_error(level, d))
        {
            cat = EE_AUDIT_N_ERRORS;
        }
        if (cat >= 0 && !hv_note(hn, cat, slot, i, 0, value))
        {
            return FALSE;
        }
    }
    return TRUE;
}

BOOL EeAudit_HartNotable(const EeAuditElection *el, EeAuditNotes *out)
{
    EeAuditAccounting a;
    HvNotes hn;
    BOOL ok;
    memset(out, 0, sizeof(*out));
    if (el == NULL || !EeAudit_HartAccounting(el, &a))
    {
        return FALSE;
    }
    hn.n = out;
    hn.cap = 0;
    ok = hv_notes_count(el, &a, &hn) && hv_notes_other(el, &a, HV_SLOT_CENTRAL, &hn) &&
         hv_notes_other(el, &a, HV_SLOT_DATA, &hn);
    EeAudit_FreeAccounting(&a);
    if (!ok)
    {
        EeAudit_FreeNotes(out);
        return FALSE;
    }
    return TRUE;
}
