/**
 * @file ee_rcv.c
 * @brief Ranked-choice (instant-runoff) tabulation over a loaded CVR table. See ee_cvr.h.
 *
 * A ranked-choice contest is stored as one column per rank, titled
 * "<contest> (Rank 1)", "<contest> (Rank 2)", ... Each cell holds the candidate the voter
 * ranked there, "undervote" (nothing at that rank), "overvote" (several candidates at
 * that rank) or a write-in marker. EeCvr_TabulateRcv runs a single-winner instant runoff
 * with the options San Francisco's official Dominion RCV reports list:
 *   RCV method IRV, Elimination type Single, threshold = continuing ballots per round,
 *   Exclude unresolved write-ins = True, Declare winners by threshold = False (rounds
 *   continue until two candidates remain), Skip overvoted rankings = False (an
 *   overvoted ranking stops the ballot), Assign skipped rankings to exhausted = False
 *   (a skipped ranking is passed over).
 * Validated round-by-round against the official reports (e.g. Nov 2024 Mayor, 14 rounds,
 * and Nov 2020 Supervisor District 1) -- every candidate, continuing, blank, exhausted and
 * overvote figure matches.
 */
#include "ee_cvr.h"

#include <stdlib.h>
#include <string.h>

#define RCV_SKIP (-1) /* a ranking passed over: undervote or an unresolved write-in */
#define RCV_OV   (-2) /* an overvoted ranking */

/* -------------------------------------------------------------------------- */
/* Contest discovery                                                          */
/* -------------------------------------------------------------------------- */

/* Parse a "<base> (Rank N)" column title. On success sets *base_len (characters before
 * " (Rank") and *rank (N >= 1). */
static BOOL rcv_parse_title(const wchar_t *title, size_t *base_len, uint32_t *rank)
{
    static const wchar_t k_Tag[] = L" (Rank ";
    const size_t tag_len = ARRAYSIZE(k_Tag) - 1;
    size_t n;
    size_t i;
    size_t d;
    uint32_t v = 0;

    if (title == NULL)
    {
        return FALSE;
    }
    n = wcslen(title);
    if (n < tag_len + 2 || title[n - 1] != L')')
    {
        return FALSE;
    }
    /* Digits immediately before the closing parenthesis. */
    d = n - 1;
    while (d > 0 && title[d - 1] >= L'0' && title[d - 1] <= L'9')
    {
        d--;
    }
    if (d == n - 1 || n - 1 - d > 4 || d < tag_len)
    {
        return FALSE;
    }
    if (wcsncmp(title + d - tag_len, k_Tag, tag_len) != 0)
    {
        return FALSE;
    }
    for (i = d; i < n - 1; i++)
    {
        v = v * 10u + (uint32_t)(title[i] - L'0');
    }
    if (v == 0)
    {
        return FALSE;
    }
    *base_len = d - tag_len;
    *rank = v;
    return TRUE;
}

/* If @p first_col is the "Rank 1" column of a ranked-choice contest, return its number
 * of rank columns (>= 2), else 0. */
static uint32_t rcv_contest_span(const EeCvrTable *t, uint32_t first_col)
{
    size_t base_len;
    uint32_t rank;
    uint32_t n = 1;

    if (t == NULL || t->col_titles == NULL || first_col < t->frozen_count || first_col >= t->ncols)
    {
        return 0;
    }
    if (!rcv_parse_title(t->col_titles[first_col], &base_len, &rank) || rank != 1)
    {
        return 0;
    }
    while (first_col + n < t->ncols)
    {
        size_t bl;
        uint32_t rk;
        const wchar_t *title = t->col_titles[first_col + n];
        if (!rcv_parse_title(title, &bl, &rk) || rk != n + 1 || bl != base_len ||
            wcsncmp(title, t->col_titles[first_col], base_len) != 0)
        {
            break;
        }
        n++;
    }
    return (n >= 2) ? n : 0;
}

uint32_t EeCvr_FindRcvContests(const EeCvrTable *t, uint32_t *first_cols, uint32_t cap)
{
    uint32_t c;
    uint32_t found = 0;

    if (t == NULL || t->col_titles == NULL)
    {
        return 0;
    }
    for (c = t->frozen_count; c < t->ncols;)
    {
        uint32_t span = rcv_contest_span(t, c);
        if (span == 0)
        {
            c++;
            continue;
        }
        if (first_cols != NULL && found < cap)
        {
            first_cols[found] = c;
        }
        found++;
        c += span;
    }
    return found;
}

/* -------------------------------------------------------------------------- */
/* Tabulation                                                                 */
/* -------------------------------------------------------------------------- */

static wchar_t *rcv_wide_dup(const char *s, size_t n_utf8)
{
    int n = MultiByteToWideChar(CP_UTF8, 0, s, (int)n_utf8, NULL, 0);
    wchar_t *w;
    if (n < 0)
    {
        n = 0;
    }
    w = (wchar_t *)malloc(((size_t)n + 1) * sizeof(wchar_t));
    if (w == NULL)
    {
        return NULL;
    }
    if (n > 0 && MultiByteToWideChar(CP_UTF8, 0, s, (int)n_utf8, w, n) <= 0)
    {
        n = 0;
    }
    w[n] = L'\0';
    return w;
}

/* Classify a cell value. Mirrors the write-in variants ee_cvr.c's tabulation groups
 * (image marker, literal text, ES&S "No image found"). */
static int rcv_classify(const char *v)
{
    if (_stricmp(v, "undervote") == 0)
    {
        return RCV_SKIP;
    }
    if (_stricmp(v, "overvote") == 0)
    {
        return RCV_OV;
    }
    if (_stricmp(v, "[write-in]") == 0 || _stricmp(v, "write-in") == 0 ||
        _stricmp(v, "writein") == 0 || _stricmp(v, "write in") == 0 ||
        _stricmp(v, "no image found") == 0)
    {
        return RCV_SKIP; /* unresolved write-ins are excluded from the tabulation */
    }
    return 0;            /* a candidate */
}

/* Growable int32 array. */
typedef struct RcvVec
{
    int32_t *v;
    size_t n;
    size_t cap;
} RcvVec;

static BOOL rcv_vec_push(RcvVec *a, int32_t x)
{
    if (a->n == a->cap)
    {
        size_t nc = a->cap ? a->cap * 2 : 1024;
        int32_t *nv = (int32_t *)realloc(a->v, nc * sizeof(int32_t));
        if (nv == NULL)
        {
            return FALSE;
        }
        a->v = nv;
        a->cap = nc;
    }
    a->v[a->n++] = x;
    return TRUE;
}

void EeCvr_FreeRcvResult(EeRcvResult *r)
{
    uint32_t i;
    if (r == NULL)
    {
        return;
    }
    free(r->contest);
    if (r->cand != NULL)
    {
        for (i = 0; i < r->ncand; i++)
        {
            free(r->cand[i]);
        }
        free(r->cand);
    }
    free(r->votes);
    free(r->continuing);
    free(r->blanks);
    free(r->exhausted);
    free(r->overvotes);
    free(r->eliminated);
    free(r->elim_tie);
    ZeroMemory(r, sizeof(*r));
}

/* Pick the candidate to eliminate after round @p rnd: fewest votes among hopefuls; a tie
 * is broken by the lower total in the latest earlier round that separates the tied
 * candidates, then by name (the later name goes). *tie is set when more than one
 * candidate shared the lowest total. */
static int32_t rcv_pick_loser(const uint32_t *votes /* [round*ncand+c] */,
                              uint32_t ncand,
                              uint32_t rnd,
                              const BOOL *hopeful,
                              const uint32_t *val_of_cand,
                              const EeCvrTable *t,
                              BOOL *tie)
{
    BOOL *tied = (BOOL *)calloc(ncand, sizeof(BOOL));
    uint32_t c;
    uint32_t low = UINT32_MAX;
    uint32_t ntied = 0;
    int32_t pick = -1;
    uint32_t back;

    *tie = FALSE;
    if (tied == NULL)
    {
        return -1;
    }
    for (c = 0; c < ncand; c++)
    {
        if (hopeful[c] && votes[(size_t)rnd * ncand + c] < low)
        {
            low = votes[(size_t)rnd * ncand + c];
        }
    }
    for (c = 0; c < ncand; c++)
    {
        if (hopeful[c] && votes[(size_t)rnd * ncand + c] == low)
        {
            tied[c] = TRUE;
            ntied++;
        }
    }
    if (ntied > 1)
    {
        *tie = TRUE;
    }
    /* Look back through earlier rounds for one that separates the tied candidates. */
    for (back = rnd; ntied > 1 && back > 0; back--)
    {
        uint32_t r = back - 1;
        uint32_t m = UINT32_MAX;
        for (c = 0; c < ncand; c++)
        {
            if (tied[c] && votes[(size_t)r * ncand + c] < m)
            {
                m = votes[(size_t)r * ncand + c];
            }
        }
        for (c = 0; c < ncand; c++)
        {
            if (tied[c] && votes[(size_t)r * ncand + c] != m)
            {
                tied[c] = FALSE;
                ntied--;
            }
        }
    }
    for (c = 0; c < ncand; c++)
    {
        if (!tied[c])
        {
            continue;
        }
        if (pick < 0 || strcmp(t->val_pool + t->val_off[val_of_cand[c]],
                               t->val_pool + t->val_off[val_of_cand[pick]]) > 0)
        {
            pick = (int32_t)c;
        }
    }
    free(tied);
    return pick;
}

BOOL EeCvr_TabulateRcv(const EeCvrTable *t,
                       uint32_t first_col,
                       const uint32_t *rows,
                       uint32_t nrows,
                       EeRcvResult *out)
{
    uint32_t nranks;
    uint32_t end_col;
    int32_t *vmap = NULL;         /* value id -> candidate index / RCV_SKIP / RCV_OV */
    uint32_t *val_of_cand = NULL; /* candidate index -> value id */
    uint32_t ncand = 0;
    uint32_t cand_cap = 0;
    RcvVec seq;    /* concatenated ballot ranking sequences */
    RcvVec bstart; /* start of each ballot's sequence in seq */
    uint32_t nblank = 0;
    uint32_t *pos = NULL;
    uint8_t *status = NULL; /* 0 continuing, 1 exhausted, 2 overvote */
    BOOL *hopeful = NULL;
    uint32_t nhopeful;
    uint32_t *votes = NULL; /* [round * ncand + c], internal candidate order */
    uint32_t rnd_cap;
    uint32_t nrounds = 0;
    uint32_t *order = NULL; /* finishing position -> internal candidate index */
    uint32_t *rank_of = NULL;
    uint32_t iter_n;
    uint32_t rr;
    uint32_t i;
    uint32_t c;
    size_t b;
    size_t nballots;
    BOOL ok = FALSE;

    if (out == NULL)
    {
        return FALSE;
    }
    ZeroMemory(out, sizeof(*out));
    ZeroMemory(&seq, sizeof(seq));
    ZeroMemory(&bstart, sizeof(bstart));
    nranks = rcv_contest_span(t, first_col);
    if (nranks == 0)
    {
        return FALSE;
    }
    end_col = first_col + nranks;

    vmap = (int32_t *)malloc(((size_t)t->val_count + 1) * sizeof(int32_t));
    if (vmap == NULL)
    {
        goto cleanup;
    }
    for (i = 0; i < t->val_count; i++)
    {
        vmap[i] = INT32_MIN; /* not yet classified */
    }

    /* ---- Collect each ballot's ranking sequence ---- */
    iter_n = (rows != NULL) ? nrows : t->nrows;
    for (rr = 0; rr < iter_n; rr++)
    {
        uint32_t r = (rows != NULL) ? rows[rr] : rr;
        uint32_t lo;
        uint32_t hi;
        uint32_t k;
        BOOL present = FALSE;
        size_t start = seq.n;
        if (r >= t->nrows)
        {
            continue;
        }
        lo = t->row_start[r];
        hi = t->row_start[r + 1];
        for (k = lo; k < hi; k++)
        {
            uint32_t col = t->ent_col[k];
            uint32_t vid;
            int32_t code;
            if (col < first_col)
            {
                continue;
            }
            if (col >= end_col)
            {
                break; /* entries are in ascending column order */
            }
            present = TRUE;
            vid = t->ent_val[k];
            code = vmap[vid];
            if (code == INT32_MIN)
            {
                const char *v = t->val_pool + t->val_off[vid];
                code = rcv_classify(v);
                if (code == 0)
                {
                    if (ncand == cand_cap)
                    {
                        uint32_t nc = cand_cap ? cand_cap * 2 : 16;
                        uint32_t *nv = (uint32_t *)realloc(val_of_cand, nc * sizeof(uint32_t));
                        if (nv == NULL)
                        {
                            goto cleanup;
                        }
                        val_of_cand = nv;
                        cand_cap = nc;
                    }
                    val_of_cand[ncand] = vid;
                    code = (int32_t)ncand++;
                }
                vmap[vid] = code;
            }
            if (code == RCV_SKIP)
            {
                continue;
            }
            if (seq.n > start && seq.v[seq.n - 1] == RCV_OV)
            {
                continue; /* nothing after an overvoted ranking can count */
            }
            if (!rcv_vec_push(&seq, code))
            {
                goto cleanup;
            }
        }
        if (!present)
        {
            continue; /* the contest is not on this ballot (card) */
        }
        if (seq.n == start)
        {
            nblank++;
            continue;
        }
        if (!rcv_vec_push(&bstart, (int32_t)start))
        {
            goto cleanup;
        }
    }
    nballots = bstart.n;
    if (!rcv_vec_push(&bstart, (int32_t)seq.n)) /* sentinel end */
    {
        goto cleanup;
    }

    /* ---- Rounds ---- */
    rnd_cap = ncand + 1;
    pos = (uint32_t *)calloc(nballots ? nballots : 1, sizeof(uint32_t));
    status = (uint8_t *)calloc(nballots ? nballots : 1, 1);
    hopeful = (BOOL *)malloc((ncand ? ncand : 1) * sizeof(BOOL));
    votes = (uint32_t *)calloc((size_t)rnd_cap * (ncand ? ncand : 1), sizeof(uint32_t));
    out->continuing = (uint32_t *)calloc(rnd_cap, sizeof(uint32_t));
    out->blanks = (uint32_t *)calloc(rnd_cap, sizeof(uint32_t));
    out->exhausted = (uint32_t *)calloc(rnd_cap, sizeof(uint32_t));
    out->overvotes = (uint32_t *)calloc(rnd_cap, sizeof(uint32_t));
    out->eliminated = (int32_t *)malloc(rnd_cap * sizeof(int32_t));
    out->elim_tie = (BOOL *)calloc(rnd_cap, sizeof(BOOL));
    if (pos == NULL || status == NULL || hopeful == NULL || votes == NULL ||
        out->continuing == NULL || out->blanks == NULL || out->exhausted == NULL ||
        out->overvotes == NULL || out->eliminated == NULL || out->elim_tie == NULL)
    {
        goto cleanup;
    }
    for (b = 0; b < nballots; b++)
    {
        pos[b] = (uint32_t)bstart.v[b];
    }
    for (c = 0; c < ncand; c++)
    {
        hopeful[c] = TRUE;
    }
    nhopeful = ncand;

    for (;;)
    {
        uint32_t *rv = votes + (size_t)nrounds * ncand;
        uint32_t cont = 0;
        uint32_t nex = 0;
        uint32_t nov = 0;
        uint32_t best = 0;
        for (b = 0; b < nballots; b++)
        {
            uint32_t p;
            uint32_t e;
            if (status[b] != 0)
            {
                nex += (status[b] == 1);
                nov += (status[b] == 2);
                continue;
            }
            p = pos[b];
            e = (uint32_t)bstart.v[b + 1];
            while (p < e && seq.v[p] >= 0 && !hopeful[seq.v[p]])
            {
                p++;
            }
            pos[b] = p;
            if (p == e)
            {
                status[b] = 1;
                nex++;
            }
            else if (seq.v[p] == RCV_OV)
            {
                status[b] = 2;
                nov++;
            }
            else
            {
                rv[seq.v[p]]++;
                cont++;
            }
        }
        out->continuing[nrounds] = cont;
        out->blanks[nrounds] = nblank;
        out->exhausted[nrounds] = nex;
        out->overvotes[nrounds] = nov;
        out->eliminated[nrounds] = -1;
        for (c = 0; c < ncand; c++)
        {
            if (hopeful[c] && rv[c] > best)
            {
                best = rv[c];
            }
        }
        if (out->majority_round == 0 && cont > 0 && (uint64_t)best * 2u > cont)
        {
            out->majority_round = nrounds + 1;
        }
        nrounds++;
        if (nhopeful <= 2 || nrounds >= rnd_cap)
        {
            break;
        }
        {
            BOOL tie = FALSE;
            int32_t loser =
                rcv_pick_loser(votes, ncand, nrounds - 1, hopeful, val_of_cand, t, &tie);
            if (loser < 0)
            {
                goto cleanup;
            }
            hopeful[loser] = FALSE;
            nhopeful--;
            out->eliminated[nrounds - 1] = loser;
            out->elim_tie[nrounds - 1] = tie;
        }
    }

    /* ---- Finishing order: remaining candidates by final votes, then the eliminated
     *      ones latest-first ---- */
    order = (uint32_t *)malloc((ncand ? ncand : 1) * sizeof(uint32_t));
    rank_of = (uint32_t *)malloc((ncand ? ncand : 1) * sizeof(uint32_t));
    if (order == NULL || rank_of == NULL)
    {
        goto cleanup;
    }
    {
        const uint32_t *fv = votes + (size_t)(nrounds - 1) * ncand;
        uint32_t n = 0;
        for (c = 0; c < ncand; c++)
        {
            if (hopeful[c])
            {
                uint32_t j = n++;
                while (j > 0 && fv[order[j - 1]] < fv[c])
                {
                    order[j] = order[j - 1];
                    j--;
                }
                order[j] = c;
            }
        }
        for (i = nrounds; i > 0; i--)
        {
            if (out->eliminated[i - 1] >= 0)
            {
                order[n++] = (uint32_t)out->eliminated[i - 1];
            }
        }
    }
    for (i = 0; i < ncand; i++)
    {
        rank_of[order[i]] = i;
    }

    out->nranks = nranks;
    out->ncand = ncand;
    out->nrounds = nrounds;
    out->winner = (ncand > 0) ? 0 : -1;
    out->cand = (wchar_t **)calloc(ncand ? ncand : 1, sizeof(wchar_t *));
    out->votes = (uint32_t *)calloc((size_t)nrounds * (ncand ? ncand : 1), sizeof(uint32_t));
    {
        size_t base_len = 0;
        uint32_t rk;
        const wchar_t *title = t->col_titles[first_col];
        rcv_parse_title(title, &base_len, &rk);
        out->contest = (wchar_t *)malloc((base_len + 1) * sizeof(wchar_t));
        if (out->contest != NULL)
        {
            memcpy(out->contest, title, base_len * sizeof(wchar_t));
            out->contest[base_len] = L'\0';
        }
    }
    if (out->cand == NULL || out->votes == NULL || out->contest == NULL)
    {
        goto cleanup;
    }
    for (i = 0; i < ncand; i++)
    {
        const char *v = t->val_pool + t->val_off[val_of_cand[order[i]]];
        out->cand[i] = rcv_wide_dup(v, strlen(v));
        if (out->cand[i] == NULL)
        {
            goto cleanup;
        }
    }
    for (i = 0; i < nrounds; i++)
    {
        for (c = 0; c < ncand; c++)
        {
            out->votes[(size_t)i * ncand + rank_of[c]] = votes[(size_t)i * ncand + c];
        }
        if (out->eliminated[i] >= 0)
        {
            out->eliminated[i] = (int32_t)rank_of[out->eliminated[i]];
        }
    }
    ok = TRUE;

cleanup:
    if (!ok)
    {
        EeCvr_FreeRcvResult(out);
    }
    free(vmap);
    free(val_of_cand);
    free(seq.v);
    free(bstart.v);
    free(pos);
    free(status);
    free(hopeful);
    free(votes);
    free(order);
    free(rank_of);
    return ok;
}
