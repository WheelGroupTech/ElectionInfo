/**
 * @file hart_cvr.c
 * @brief Loader for Hart voting-system Cast Vote Records.
 *
 * A Hart CVR export is one or more ZIP files, each containing one XML file per
 * scanned ballot SHEET (the first/only sheet is named `1_<guid>.xml`; later sheets
 * of a multi-sheet ballot are `<guid>.xml`). There is not enough information to link
 * the sheets of one ballot together, so each XML becomes one row.
 *
 * Each XML looks like:
 *   <Cvr><Contests>
 *     <Contest><Name>..</Name><Id>..</Id>
 *       <Options><Option><Name>..</Name><Id>..</Id><Value>1</Value>
 *                 [<WriteInData><OriginalText>..</OriginalText>..</WriteInData>]</Option>..</Options>
 *       [<Undervotes>n</Undervotes>] [<Overvoted />]
 *     </Contest>..
 *   </Contests>
 *   <BatchSequence>..</BatchSequence><SheetNumber>..</SheetNumber>
 *   <PrecinctSplit><Name>..</Name><Id>..</Id></PrecinctSplit>
 *   [<Party><Name>..</Name><Id>..</Id></Party>]
 *   <BatchNumber>..</BatchNumber><CvrGuid>..</CvrGuid><IsBlank>true|false</IsBlank></Cvr>
 *
 * Mapping into the sparse EeCvrTable (shared with the ES&S loader so tabulation,
 * reports, filtering and export work unchanged):
 *  - Frozen key columns, in order: CvrGuid, Sheet Number, Batch Sequence,
 *    Batch Number, Precinct, Party (only when any ballot has one), Is Blank.
 *  - Each contest becomes one column, or several ("vote for N") when a ballot marks
 *    more than one option; the extra columns carry a blank continuation header so
 *    the existing col_group logic sums the race across them. A selected candidate is
 *    its name; a write-in is the marker "Write-in"; an unfilled seat is "undervote";
 *    an over-marked contest fills every seat with "overvote".
 *  - Contests are ordered Federal -> State -> County -> City -> ISD -> Other -> MUD.
 *
 * The XML is flat and simple, so a small hand-written tag scanner is used (no third
 * party); the ZIP is iterated entry-by-entry with the vendored miniz so a multi-GB
 * export is never held in memory at once. Two passes: pass 1 discovers the contest
 * set and each contest's seat count + category; pass 2 fills the rows.
 */

#include "ee_cvr.h"

#include <windows.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strsafe.h>

#include "third_party/miniz/miniz.h"

/* -------------------------------------------------------------------------- */
/* Small string helpers                                                       */
/* -------------------------------------------------------------------------- */

static void hart_set_err(wchar_t *dst, size_t cch, const wchar_t *msg)
{
    if (dst != NULL && cch > 0)
    {
        StringCchCopyW(dst, cch, msg);
    }
}

/* Case-insensitive ASCII substring test (haystack NUL-terminated). */
static BOOL contains_ci(const char *hay, const char *needle)
{
    size_t nl = strlen(needle);
    if (nl == 0)
    {
        return TRUE;
    }
    for (; *hay; hay++)
    {
        size_t k = 0;
        while (k < nl)
        {
            char a = hay[k];
            char b = needle[k];
            if (a >= 'A' && a <= 'Z')
            {
                a = (char)(a - 'A' + 'a');
            }
            if (b >= 'A' && b <= 'Z')
            {
                b = (char)(b - 'A' + 'a');
            }
            if (a != b)
            {
                break;
            }
            k++;
        }
        if (k == nl)
        {
            return TRUE;
        }
    }
    return FALSE;
}

/* -------------------------------------------------------------------------- */
/* Contest category ordering (Federal -> State -> County -> City -> ISD ->     */
/* Other -> MUD). Returns major*1000 + minor; lower sorts first. Cosmetic only */
/* (does not affect tallies).                                                  */
/* -------------------------------------------------------------------------- */

enum
{
    MAJ_FED = 0,
    MAJ_STATE = 1,
    MAJ_COUNTY = 2,
    MAJ_CITY = 3,
    MAJ_ISD = 4,
    MAJ_OTHER = 5,
    MAJ_MUD = 6
};

static int hart_contest_rank(const char *n)
{
    /* Federal */
    if (contains_ci(n, "President"))
        return MAJ_FED * 1000 + 0;
    if (contains_ci(n, "United States Senator") || contains_ci(n, "U.S. Senator") ||
        contains_ci(n, "US Senator"))
        return MAJ_FED * 1000 + 1;
    if (contains_ci(n, "United States Representative") || contains_ci(n, "U.S. Representative") ||
        contains_ci(n, "US Representative") || contains_ci(n, "Congress"))
        return MAJ_FED * 1000 + 2;

    /* State (order per spec). Check specific courts before the generic ones. */
    if (contains_ci(n, "Lieutenant Governor"))
        return MAJ_STATE * 1000 + 1;
    if (contains_ci(n, "Governor"))
        return MAJ_STATE * 1000 + 0;
    if (contains_ci(n, "Attorney General"))
        return MAJ_STATE * 1000 + 2;
    if (contains_ci(n, "Comptroller"))
        return MAJ_STATE * 1000 + 3;
    if (contains_ci(n, "Land Office"))
        return MAJ_STATE * 1000 + 4;
    if (contains_ci(n, "Agricultur"))
        return MAJ_STATE * 1000 + 5;
    if (contains_ci(n, "Railroad Commissioner"))
        return MAJ_STATE * 1000 + 6;
    if (contains_ci(n, "Supreme Court"))
        return MAJ_STATE * 1000 + 7;
    if (contains_ci(n, "Court of Criminal Appeals"))
        return MAJ_STATE * 1000 + 8;
    if (contains_ci(n, "State Board of Education"))
        return MAJ_STATE * 1000 + 9;
    if (contains_ci(n, "State Senator"))
        return MAJ_STATE * 1000 + 10;
    if (contains_ci(n, "State Representative"))
        return MAJ_STATE * 1000 + 11;
    if (contains_ci(n, "Court of Appeals"))
        return MAJ_STATE * 1000 + 12;

    /* County */
    if (contains_ci(n, "District Court") || contains_ci(n, "District Judge") ||
        contains_ci(n, "Judicial District") || contains_ci(n, "Family District Court"))
        return MAJ_COUNTY * 1000 + 0;
    if (contains_ci(n, "County Court at Law") || contains_ci(n, "Probate Court") ||
        contains_ci(n, "County Criminal Court") || contains_ci(n, "County Judge"))
        return MAJ_COUNTY * 1000 + 1;
    if (contains_ci(n, "District Attorney"))
        return MAJ_COUNTY * 1000 + 2;
    if (contains_ci(n, "District Clerk"))
        return MAJ_COUNTY * 1000 + 3;
    if (contains_ci(n, "County Clerk"))
        return MAJ_COUNTY * 1000 + 4;
    if (contains_ci(n, "County Commissioner") || contains_ci(n, "Commissioner, Precinct") ||
        contains_ci(n, "Commissioners Court"))
        return MAJ_COUNTY * 1000 + 5;
    if (contains_ci(n, "Justice of the Peace"))
        return MAJ_COUNTY * 1000 + 6;
    if (contains_ci(n, "Constable"))
        return MAJ_COUNTY * 1000 + 7;
    if (contains_ci(n, "Sheriff"))
        return MAJ_COUNTY * 1000 + 8;
    if (contains_ci(n, "Tax Assessor") || contains_ci(n, "Assessor-Collector") ||
        contains_ci(n, "Assessor Collector"))
        return MAJ_COUNTY * 1000 + 9;
    if (contains_ci(n, "County Treasurer"))
        return MAJ_COUNTY * 1000 + 10;

    /* MUD / water districts (checked before generic "District" catches). */
    if (contains_ci(n, "MUD") || contains_ci(n, "Municipal Utility District") ||
        contains_ci(n, "Water Control") || contains_ci(n, "WCID") ||
        contains_ci(n, "Improvement District") || contains_ci(n, "Water District"))
        return MAJ_MUD * 1000 + 0;

    /* ISD / school */
    if (contains_ci(n, "ISD") || contains_ci(n, "Independent School District") ||
        contains_ci(n, "School District") || contains_ci(n, "Board of Trustees") ||
        contains_ci(n, "Trustee"))
        return MAJ_ISD * 1000 + 0;

    /* City */
    if (contains_ci(n, "City of") || contains_ci(n, "Mayor") || contains_ci(n, "City Council") ||
        contains_ci(n, "Council Member") || contains_ci(n, "Alderman") ||
        contains_ci(n, "Town of") || contains_ci(n, "Village of"))
        return MAJ_CITY * 1000 + 0;

    /* Remaining County catch-all (kept after city/school so "County" bond etc. that
     * are really local still fall through here only when nothing else matched). */
    if (contains_ci(n, "County"))
        return MAJ_COUNTY * 1000 + 20;

    return MAJ_OTHER * 1000 + 0;
}

/* -------------------------------------------------------------------------- */
/* Entity-decoding text extraction                                            */
/* -------------------------------------------------------------------------- */

/* Decode XML entities from [src, src+len) into dst (cap includes the NUL). Returns
 * the decoded length (excluding NUL). */
static size_t xml_decode(char *dst, size_t cap, const char *src, size_t len)
{
    size_t o = 0;
    size_t i = 0;
    if (cap == 0)
    {
        return 0;
    }
    while (i < len && o + 1 < cap)
    {
        char c = src[i];
        if (c == '&')
        {
            if (i + 3 < len && src[i + 1] == 'l' && src[i + 2] == 't' && src[i + 3] == ';')
            {
                dst[o++] = '<';
                i += 4;
                continue;
            }
            if (i + 3 < len && src[i + 1] == 'g' && src[i + 2] == 't' && src[i + 3] == ';')
            {
                dst[o++] = '>';
                i += 4;
                continue;
            }
            if (i + 4 < len && src[i + 1] == 'a' && src[i + 2] == 'm' && src[i + 3] == 'p' &&
                src[i + 4] == ';')
            {
                dst[o++] = '&';
                i += 5;
                continue;
            }
            if (i + 5 < len && src[i + 1] == 'q' && src[i + 2] == 'u' && src[i + 3] == 'o' &&
                src[i + 4] == 't' && src[i + 5] == ';')
            {
                dst[o++] = '"';
                i += 6;
                continue;
            }
            if (i + 5 < len && src[i + 1] == 'a' && src[i + 2] == 'p' && src[i + 3] == 'o' &&
                src[i + 4] == 's' && src[i + 5] == ';')
            {
                dst[o++] = '\'';
                i += 6;
                continue;
            }
            if (i + 2 < len && src[i + 1] == '#')
            {
                /* numeric char ref &#dd; or &#xhh; -> UTF-8 */
                unsigned long cp = 0;
                size_t j = i + 2;
                int hex = 0;
                if (j < len && (src[j] == 'x' || src[j] == 'X'))
                {
                    hex = 1;
                    j++;
                }
                while (j < len && src[j] != ';')
                {
                    char d = src[j];
                    if (hex)
                    {
                        if (d >= '0' && d <= '9')
                            cp = cp * 16 + (unsigned long)(d - '0');
                        else if (d >= 'a' && d <= 'f')
                            cp = cp * 16 + (unsigned long)(d - 'a' + 10);
                        else if (d >= 'A' && d <= 'F')
                            cp = cp * 16 + (unsigned long)(d - 'A' + 10);
                        else
                            break;
                    }
                    else
                    {
                        if (d >= '0' && d <= '9')
                            cp = cp * 10 + (unsigned long)(d - '0');
                        else
                            break;
                    }
                    j++;
                }
                if (j < len && src[j] == ';')
                {
                    /* Encode cp as UTF-8. */
                    if (cp < 0x80 && o + 1 < cap)
                    {
                        dst[o++] = (char)cp;
                    }
                    else if (cp < 0x800 && o + 2 < cap)
                    {
                        dst[o++] = (char)(0xC0 | (cp >> 6));
                        dst[o++] = (char)(0x80 | (cp & 0x3F));
                    }
                    else if (cp < 0x10000 && o + 3 < cap)
                    {
                        dst[o++] = (char)(0xE0 | (cp >> 12));
                        dst[o++] = (char)(0x80 | ((cp >> 6) & 0x3F));
                        dst[o++] = (char)(0x80 | (cp & 0x3F));
                    }
                    i = j + 1;
                    continue;
                }
            }
        }
        dst[o++] = c;
        i++;
    }
    dst[o] = '\0';
    return o;
}

/* Find <tag>content</tag> (leaf); returns content pointer + length via the closing
 * '<', or NULL. @p hay is NUL-terminated; search is bounded by it. */
static const char *tag_content(const char *hay, const char *open_tag, size_t *out_len)
{
    const char *p = strstr(hay, open_tag);
    const char *end;
    if (p == NULL)
    {
        return NULL;
    }
    p += strlen(open_tag);
    end = strchr(p, '<');
    if (end == NULL)
    {
        end = p + strlen(p);
    }
    *out_len = (size_t)(end - p);
    return p;
}

/* Copy a leaf tag's decoded text into a fixed buffer (empty string if absent). */
static void tag_copy(const char *hay, const char *open_tag, char *dst, size_t cap)
{
    size_t len = 0;
    const char *c = tag_content(hay, open_tag, &len);
    if (c == NULL)
    {
        if (cap > 0)
        {
            dst[0] = '\0';
        }
        return;
    }
    xml_decode(dst, cap, c, len);
}

/* -------------------------------------------------------------------------- */
/* Parsed sheet                                                               */
/* -------------------------------------------------------------------------- */

typedef struct
{
    size_t name_off;   /* contest name (offset into arena) */
    int undervotes;
    int overvoted;
    size_t *sel_off;   /* selected option values (offsets into arena) */
    int nsel;
    int cap_sel;
} HartContest;

typedef struct
{
    char guid[80];
    char sheet[24];
    char batchseq[24];
    char batchnum[24];
    char isblank[16];
    char precinct[192];
    char party[192];
    int has_party;

    char *arena; /* decoded strings, NUL-terminated, referenced by offset */
    size_t arena_len;
    size_t arena_cap;

    HartContest *ct;
    int nct;
    int cap_ct;
} HartSheet;

static void sheet_init(HartSheet *s)
{
    ZeroMemory(s, sizeof(*s));
}

static void sheet_free(HartSheet *s)
{
    int i;
    for (i = 0; i < s->cap_ct; i++)
    {
        free(s->ct[i].sel_off);
    }
    free(s->ct);
    free(s->arena);
    ZeroMemory(s, sizeof(*s));
}

/* Append decoded [src,len) to the arena; returns its offset (or (size_t)-1 on OOM). */
static size_t arena_add(HartSheet *s, const char *src, size_t len)
{
    size_t need = s->arena_len + len + 1;
    size_t off;
    if (need > s->arena_cap)
    {
        size_t nc = s->arena_cap ? s->arena_cap : 4096;
        char *nb;
        while (nc < need)
        {
            nc *= 2;
        }
        nb = (char *)realloc(s->arena, nc);
        if (nb == NULL)
        {
            return (size_t)-1;
        }
        s->arena = nb;
        s->arena_cap = nc;
    }
    off = s->arena_len;
    s->arena_len += xml_decode(s->arena + off, len + 1, src, len) + 1;
    return off;
}

/* Parse one Hart CVR XML buffer (NUL-terminated, BOM already skipped) into @p s.
 * Returns FALSE on OOM. */
static BOOL parse_sheet(HartSheet *s, char *xml)
{
    const char *contests = strstr(xml, "<Contests>");
    char *cend_region;
    char *p;

    /* reset (keep allocations) */
    s->arena_len = 0;
    s->nct = 0;
    s->has_party = 0;
    s->guid[0] = s->sheet[0] = s->batchseq[0] = s->batchnum[0] = s->isblank[0] = '\0';
    s->precinct[0] = s->party[0] = '\0';

    if (contests != NULL)
    {
        char *region = (char *)contests + 10; /* after "<Contests>" */
        char *region_end = strstr(region, "</Contests>");
        cend_region = region_end ? region_end : (region + strlen(region));
        p = region;
        for (;;)
        {
            char *c = strstr(p, "<Contest>");
            char *cend;
            char saved;
            HartContest *hc;
            const char *nm;
            size_t nmlen;
            char *opts;
            if (c == NULL || c >= cend_region)
            {
                break;
            }
            cend = strstr(c, "</Contest>");
            if (cend == NULL || cend > cend_region)
            {
                break;
            }
            saved = *cend;
            *cend = '\0'; /* bound searches to this contest block */

            if (s->nct == s->cap_ct)
            {
                int ncap = s->cap_ct ? s->cap_ct * 2 : 32;
                HartContest *ng = (HartContest *)realloc(s->ct, (size_t)ncap * sizeof(HartContest));
                if (ng == NULL)
                {
                    *cend = saved;
                    return FALSE;
                }
                /* zero the new slots so sel_off/cap_sel start clean */
                memset(ng + s->cap_ct, 0, (size_t)(ncap - s->cap_ct) * sizeof(HartContest));
                s->ct = ng;
                s->cap_ct = ncap;
            }
            hc = &s->ct[s->nct];
            hc->nsel = 0;
            hc->undervotes = 0;
            hc->overvoted = (strstr(c, "<Overvoted") != NULL);
            {
                const char *uv = NULL;
                size_t uvlen = 0;
                uv = tag_content(c, "<Undervotes>", &uvlen);
                if (uv != NULL)
                {
                    hc->undervotes = atoi(uv);
                }
            }
            nm = tag_content(c, "<Name>", &nmlen);
            if (nm == NULL)
            {
                nmlen = 0;
                nm = "";
            }
            hc->name_off = arena_add(s, nm, nmlen);
            if (hc->name_off == (size_t)-1)
            {
                *cend = saved;
                return FALSE;
            }

            /* options */
            opts = strstr(c, "<Options>");
            if (opts != NULL)
            {
                char *oend = strstr(opts, "</Options>");
                char *op = opts + 9;
                if (oend == NULL)
                {
                    oend = cend;
                }
                for (;;)
                {
                    char *o = strstr(op, "<Option>");
                    char *o_end;
                    char osav;
                    int is_wi;
                    const char *onm;
                    size_t onmlen;
                    size_t voff;
                    if (o == NULL || o >= oend)
                    {
                        break;
                    }
                    o_end = strstr(o, "</Option>");
                    if (o_end == NULL || o_end > oend)
                    {
                        break;
                    }
                    osav = *o_end;
                    *o_end = '\0';
                    is_wi = (strstr(o, "<WriteInData") != NULL);
                    onm = tag_content(o, "<Name>", &onmlen);
                    if (onm == NULL)
                    {
                        onmlen = 0;
                        onm = "";
                    }
                    if (is_wi || onmlen == 0)
                    {
                        voff = arena_add(s, "Write-in", 8);
                    }
                    else
                    {
                        voff = arena_add(s, onm, onmlen);
                    }
                    *o_end = osav;
                    if (voff == (size_t)-1)
                    {
                        *cend = saved;
                        return FALSE;
                    }
                    if (hc->nsel == hc->cap_sel)
                    {
                        int scap = hc->cap_sel ? hc->cap_sel * 2 : 4;
                        size_t *ns = (size_t *)realloc(hc->sel_off, (size_t)scap * sizeof(size_t));
                        if (ns == NULL)
                        {
                            *cend = saved;
                            return FALSE;
                        }
                        hc->sel_off = ns;
                        hc->cap_sel = scap;
                    }
                    hc->sel_off[hc->nsel++] = voff;
                    op = o_end + 9;
                }
            }
            s->nct++;
            *cend = saved;
            p = cend + 10;
        }
    }

    /* Metadata (unique tag names -> global search is safe). */
    tag_copy(xml, "<CvrGuid>", s->guid, sizeof(s->guid));
    tag_copy(xml, "<SheetNumber>", s->sheet, sizeof(s->sheet));
    tag_copy(xml, "<BatchSequence>", s->batchseq, sizeof(s->batchseq));
    tag_copy(xml, "<BatchNumber>", s->batchnum, sizeof(s->batchnum));
    tag_copy(xml, "<IsBlank>", s->isblank, sizeof(s->isblank));
    {
        const char *ps = strstr(xml, "<PrecinctSplit>");
        if (ps != NULL)
        {
            tag_copy(ps, "<Name>", s->precinct, sizeof(s->precinct));
        }
        else
        {
            s->precinct[0] = '\0';
        }
    }
    {
        const char *pa = strstr(xml, "<Party>");
        if (pa != NULL)
        {
            s->has_party = 1;
            tag_copy(pa, "<Name>", s->party, sizeof(s->party));
        }
    }
    return TRUE;
}

/* -------------------------------------------------------------------------- */
/* Contest dictionary                                                         */
/* -------------------------------------------------------------------------- */

typedef struct
{
    char *name;        /* owned */
    int rank;          /* hart_contest_rank */
    uint32_t first_seen;
    int max_slots;     /* seats (vote for N) */
    int any_nonover;   /* saw at least one non-overvoted ballot */
    uint32_t col_start;/* assigned during finalize */
} ContestInfo;

typedef struct
{
    ContestInfo *info;
    uint32_t n;
    uint32_t cap;
    uint32_t *hash; /* open addressing: slot -> (index+1); 0 = empty */
    uint32_t hash_cap;
    uint32_t seen_counter;
} ContestDict;

static uint32_t fnv1a(const char *s)
{
    uint32_t h = 2166136261u;
    for (; *s; s++)
    {
        h ^= (unsigned char)*s;
        h *= 16777619u;
    }
    return h;
}

static BOOL dict_grow_hash(ContestDict *d, uint32_t newcap)
{
    uint32_t *nh = (uint32_t *)calloc(newcap, sizeof(uint32_t));
    uint32_t i;
    if (nh == NULL)
    {
        return FALSE;
    }
    for (i = 0; i < d->n; i++)
    {
        uint32_t slot = fnv1a(d->info[i].name) & (newcap - 1);
        while (nh[slot] != 0)
        {
            slot = (slot + 1) & (newcap - 1);
        }
        nh[slot] = i + 1;
    }
    free(d->hash);
    d->hash = nh;
    d->hash_cap = newcap;
    return TRUE;
}

/* Find or create a contest entry; returns its index, or (uint32_t)-1 on OOM. */
static uint32_t dict_intern(ContestDict *d, const char *name)
{
    uint32_t slot;
    if (d->hash_cap == 0 && !dict_grow_hash(d, 256))
    {
        return (uint32_t)-1;
    }
    if ((d->n + 1) * 4 >= d->hash_cap * 3 && !dict_grow_hash(d, d->hash_cap * 2))
    {
        return (uint32_t)-1;
    }
    slot = fnv1a(name) & (d->hash_cap - 1);
    while (d->hash[slot] != 0)
    {
        uint32_t idx = d->hash[slot] - 1;
        if (strcmp(d->info[idx].name, name) == 0)
        {
            return idx;
        }
        slot = (slot + 1) & (d->hash_cap - 1);
    }
    if (d->n == d->cap)
    {
        uint32_t nc = d->cap ? d->cap * 2 : 128;
        ContestInfo *ni = (ContestInfo *)realloc(d->info, (size_t)nc * sizeof(ContestInfo));
        if (ni == NULL)
        {
            return (uint32_t)-1;
        }
        d->info = ni;
        d->cap = nc;
    }
    {
        ContestInfo *e = &d->info[d->n];
        size_t len = strlen(name) + 1;
        e->name = (char *)malloc(len);
        if (e->name == NULL)
        {
            return (uint32_t)-1;
        }
        memcpy(e->name, name, len);
        e->rank = hart_contest_rank(name);
        e->first_seen = d->seen_counter++;
        e->max_slots = 1;
        e->any_nonover = 0;
        e->col_start = 0;
    }
    d->hash[slot] = d->n + 1;
    return d->n++;
}

static void dict_free(ContestDict *d)
{
    uint32_t i;
    for (i = 0; i < d->n; i++)
    {
        free(d->info[i].name);
    }
    free(d->info);
    free(d->hash);
    ZeroMemory(d, sizeof(*d));
}

static int __cdecl contest_order_cmp(void *ctx, const void *a, const void *b)
{
    const ContestDict *d = (const ContestDict *)ctx;
    const ContestInfo *x = &d->info[*(const uint32_t *)a];
    const ContestInfo *y = &d->info[*(const uint32_t *)b];
    if (x->rank != y->rank)
    {
        return (x->rank < y->rank) ? -1 : 1;
    }
    return (x->first_seen < y->first_seen) ? -1 : (x->first_seen > y->first_seen ? 1 : 0);
}

/* -------------------------------------------------------------------------- */
/* ZIP iteration                                                              */
/* -------------------------------------------------------------------------- */

typedef BOOL (*HartEntryFn)(void *ctx, char *xml /*NUL-terminated, BOM-skipped*/);

/* Iterate every *.xml entry of one zip, extracting into a reusable growable buffer
 * (*buf/*buf_cap) and invoking @p fn. Returns EeLoadStatus. */
static EeLoadStatus hart_iterate_zip(const wchar_t *path,
                                     char **buf,
                                     size_t *buf_cap,
                                     HartEntryFn fn,
                                     void *ctx,
                                     volatile LONG *cancel_flag,
                                     uint64_t *done_entries,
                                     uint64_t total_entries,
                                     EeLoadProgressFn progress_fn,
                                     void *progress_user,
                                     uint32_t *last_pct,
                                     wchar_t *err,
                                     size_t errcch)
{
    FILE *fp = NULL;
    mz_zip_archive zip;
    mz_uint i, count;
    __int64 fsize;
    EeLoadStatus status = EeLoadStatus_Ok;

    if (_wfopen_s(&fp, path, L"rb") != 0 || fp == NULL)
    {
        hart_set_err(err, errcch, L"Could not open the CVR zip file.");
        return EeLoadStatus_Error;
    }
    if (_fseeki64(fp, 0, SEEK_END) != 0 || (fsize = _ftelli64(fp)) <= 0)
    {
        fclose(fp);
        hart_set_err(err, errcch, L"Could not read the CVR zip file.");
        return EeLoadStatus_Error;
    }
    _fseeki64(fp, 0, SEEK_SET);

    mz_zip_zero_struct(&zip);
    if (!mz_zip_reader_init_cfile(&zip, fp, (mz_uint64)fsize, 0))
    {
        fclose(fp);
        hart_set_err(err, errcch, L"The CVR zip file could not be opened.");
        return EeLoadStatus_Error;
    }

    count = mz_zip_reader_get_num_files(&zip);
    for (i = 0; i < count; i++)
    {
        mz_zip_archive_file_stat st;
        char *xml;
        size_t need;
        if (!mz_zip_reader_file_stat(&zip, i, &st))
        {
            continue;
        }
        if (mz_zip_reader_is_file_a_directory(&zip, i))
        {
            continue;
        }
        {
            size_t nlen = strlen(st.m_filename);
            if (nlen < 4 || _stricmp(st.m_filename + (nlen - 4), ".xml") != 0)
            {
                continue;
            }
        }
        need = (size_t)st.m_uncomp_size + 1;
        if (need > *buf_cap)
        {
            size_t nc = *buf_cap ? *buf_cap : 65536;
            char *nb;
            while (nc < need)
            {
                nc *= 2;
            }
            nb = (char *)realloc(*buf, nc);
            if (nb == NULL)
            {
                status = EeLoadStatus_Error;
                hart_set_err(err, errcch, L"Out of memory reading the CVR.");
                break;
            }
            *buf = nb;
            *buf_cap = nc;
        }
        if (!mz_zip_reader_extract_to_mem(&zip, i, *buf, (size_t)st.m_uncomp_size, 0))
        {
            continue; /* skip an unreadable entry */
        }
        (*buf)[st.m_uncomp_size] = '\0';
        xml = *buf;
        if ((mz_uint64)st.m_uncomp_size >= 3 && (unsigned char)xml[0] == 0xEF &&
            (unsigned char)xml[1] == 0xBB && (unsigned char)xml[2] == 0xBF)
        {
            xml += 3; /* UTF-8 BOM */
        }
        if (!fn(ctx, xml))
        {
            status = EeLoadStatus_Error;
            hart_set_err(err, errcch, L"Out of memory building the CVR table.");
            break;
        }

        (*done_entries)++;
        if (((*done_entries) & 0x3FF) == 0)
        {
            if (cancel_flag != NULL && *cancel_flag != 0)
            {
                status = EeLoadStatus_Cancelled;
                break;
            }
            if (progress_fn != NULL && total_entries > 0)
            {
                uint32_t pct = (uint32_t)((*done_entries * 99ull) / total_entries);
                if (pct > 99u)
                {
                    pct = 99u;
                }
                if (pct != *last_pct)
                {
                    EeLoadProgress pr;
                    *last_pct = pct;
                    pr.percent = pct;
                    pr.rows_loaded = (uint32_t)*done_entries;
                    pr.bytes_read = *done_entries;
                    pr.bytes_total = total_entries;
                    if (!progress_fn(&pr, progress_user) && cancel_flag != NULL)
                    {
                        InterlockedExchange(cancel_flag, 1);
                    }
                }
            }
        }
    }

    mz_zip_reader_end(&zip);
    fclose(fp);
    return status;
}

/* Count *.xml entries across all zips (for progress). */
static uint64_t hart_count_entries(const wchar_t *const *paths, int count)
{
    uint64_t total = 0;
    int f;
    for (f = 0; f < count; f++)
    {
        FILE *fp = NULL;
        mz_zip_archive zip;
        __int64 fsize;
        mz_uint i, n;
        if (_wfopen_s(&fp, paths[f], L"rb") != 0 || fp == NULL)
        {
            continue;
        }
        if (_fseeki64(fp, 0, SEEK_END) != 0 || (fsize = _ftelli64(fp)) <= 0)
        {
            fclose(fp);
            continue;
        }
        _fseeki64(fp, 0, SEEK_SET);
        mz_zip_zero_struct(&zip);
        if (mz_zip_reader_init_cfile(&zip, fp, (mz_uint64)fsize, 0))
        {
            n = mz_zip_reader_get_num_files(&zip);
            for (i = 0; i < n; i++)
            {
                mz_zip_archive_file_stat st;
                if (mz_zip_reader_file_stat(&zip, i, &st) &&
                    !mz_zip_reader_is_file_a_directory(&zip, i))
                {
                    size_t nlen = strlen(st.m_filename);
                    if (nlen >= 4 && _stricmp(st.m_filename + (nlen - 4), ".xml") == 0)
                    {
                        total++;
                    }
                }
            }
            mz_zip_reader_end(&zip);
        }
        fclose(fp);
    }
    return total;
}

/* -------------------------------------------------------------------------- */
/* Two-pass load                                                              */
/* -------------------------------------------------------------------------- */

typedef struct
{
    ContestDict *dict;
    HartSheet *sheet;
    int global_has_party;
} Pass1Ctx;

static BOOL pass1_entry(void *vctx, char *xml)
{
    Pass1Ctx *p = (Pass1Ctx *)vctx;
    int i;
    if (!parse_sheet(p->sheet, xml))
    {
        return FALSE;
    }
    if (p->sheet->has_party)
    {
        p->global_has_party = 1;
    }
    for (i = 0; i < p->sheet->nct; i++)
    {
        HartContest *hc = &p->sheet->ct[i];
        const char *name = p->sheet->arena + hc->name_off;
        uint32_t idx = dict_intern(p->dict, name);
        ContestInfo *ci;
        if (idx == (uint32_t)-1)
        {
            return FALSE;
        }
        ci = &p->dict->info[idx];
        /* Seat count (vote-for-N) comes only from non-overvoted ballots: an
         * overvoted ballot marks MORE than the seat count, so it must not inflate N
         * (e.g. a vote-for-1 marked twice is 1 overvote, not 2). A contest seen only
         * overvoted keeps the default of 1 seat. */
        if (!hc->overvoted)
        {
            int slots = hc->nsel + hc->undervotes;
            if (slots < 1)
            {
                slots = 1;
            }
            if (slots > ci->max_slots)
            {
                ci->max_slots = slots;
            }
            ci->any_nonover = 1;
        }
    }
    return TRUE;
}

typedef struct
{
    ContestDict *dict;
    HartSheet *sheet;
    EeCvrTable *table;
    const char **cells; /* ncols */
    uint32_t ncols;
    uint32_t frozen;
    int party_col;      /* -1 if none */
    int isblank_col;
    int precinct_col;
} Pass2Ctx;

static BOOL pass2_entry(void *vctx, char *xml)
{
    Pass2Ctx *p = (Pass2Ctx *)vctx;
    uint32_t c;
    int i;
    if (!parse_sheet(p->sheet, xml))
    {
        return FALSE;
    }
    for (c = 0; c < p->ncols; c++)
    {
        p->cells[c] = "";
    }
    p->cells[0] = p->sheet->guid;
    p->cells[1] = p->sheet->sheet;
    p->cells[2] = p->sheet->batchseq;
    p->cells[3] = p->sheet->batchnum;
    p->cells[p->precinct_col] = p->sheet->precinct;
    if (p->party_col >= 0)
    {
        p->cells[p->party_col] = p->sheet->party;
    }
    p->cells[p->isblank_col] = p->sheet->isblank;

    for (i = 0; i < p->sheet->nct; i++)
    {
        HartContest *hc = &p->sheet->ct[i];
        const char *name = p->sheet->arena + hc->name_off;
        uint32_t idx = dict_intern(p->dict, name); /* already present */
        ContestInfo *ci;
        uint32_t base;
        int n, slot = 0, k;
        if (idx == (uint32_t)-1)
        {
            return FALSE;
        }
        ci = &p->dict->info[idx];
        base = ci->col_start;
        n = ci->max_slots;
        if (hc->overvoted)
        {
            for (k = 0; k < n; k++)
            {
                p->cells[base + (uint32_t)k] = "overvote";
            }
            continue;
        }
        for (k = 0; k < hc->nsel && slot < n; k++)
        {
            p->cells[base + (uint32_t)slot] = p->sheet->arena + hc->sel_off[k];
            slot++;
        }
        for (k = 0; k < hc->undervotes && slot < n; k++)
        {
            p->cells[base + (uint32_t)slot] = "undervote";
            slot++;
        }
    }
    return EeCvr_BuildAppendRow(p->table, p->cells, p->ncols);
}

EeLoadStatus EeCvr_LoadFromHartZips(const wchar_t *const *paths,
                                    int count,
                                    EeCvrTable *out,
                                    volatile LONG *cancel_flag,
                                    EeLoadProgressFn progress_fn,
                                    void *progress_user,
                                    wchar_t *error_message,
                                    size_t error_cch)
{
    ContestDict dict;
    HartSheet sheet;
    char *buf = NULL;
    size_t buf_cap = 0;
    uint64_t total, done = 0;
    uint32_t last_pct = 101;
    EeLoadStatus s = EeLoadStatus_Ok;
    int f;
    uint32_t frozen, ncols, i;
    uint32_t *order = NULL;
    const char **header = NULL;
    Pass1Ctx p1;
    Pass2Ctx p2;

    if (paths == NULL || out == NULL || count <= 0)
    {
        hart_set_err(error_message, error_cch, L"Invalid arguments.");
        return EeLoadStatus_Error;
    }
    EeCvr_Clear(out);
    ZeroMemory(&dict, sizeof(dict));
    sheet_init(&sheet);

    /* progress spans both passes */
    total = hart_count_entries(paths, count) * 2ull;

    /* ---- Pass 1: discover contests / seat counts / party presence ---- */
    p1.dict = &dict;
    p1.sheet = &sheet;
    p1.global_has_party = 0;
    for (f = 0; f < count && s == EeLoadStatus_Ok; f++)
    {
        s = hart_iterate_zip(paths[f], &buf, &buf_cap, pass1_entry, &p1, cancel_flag, &done, total,
                             progress_fn, progress_user, &last_pct, error_message, error_cch);
    }
    if (s != EeLoadStatus_Ok)
    {
        goto cleanup;
    }
    if (dict.n == 0)
    {
        hart_set_err(error_message, error_cch, L"No Cast Vote Records were found in the zip.");
        s = EeLoadStatus_Error;
        goto cleanup;
    }

    /* ---- Build the ordered column layout ---- */
    frozen = p1.global_has_party ? 7u : 6u;
    order = (uint32_t *)malloc((size_t)dict.n * sizeof(uint32_t));
    if (order == NULL)
    {
        s = EeLoadStatus_Error;
        goto cleanup;
    }
    for (i = 0; i < dict.n; i++)
    {
        order[i] = i;
    }
    qsort_s(order, dict.n, sizeof(uint32_t), contest_order_cmp, &dict);

    ncols = frozen;
    for (i = 0; i < dict.n; i++)
    {
        dict.info[order[i]].col_start = ncols;
        ncols += (uint32_t)dict.info[order[i]].max_slots;
    }

    header = (const char **)calloc(ncols, sizeof(char *));
    if (header == NULL)
    {
        s = EeLoadStatus_Error;
        goto cleanup;
    }
    header[0] = "CvrGuid";
    header[1] = "Sheet Number";
    header[2] = "Batch Sequence";
    header[3] = "Batch Number";
    header[4] = "Precinct";
    if (p1.global_has_party)
    {
        header[5] = "Party";
        header[6] = "Is Blank";
    }
    else
    {
        header[5] = "Is Blank";
    }
    for (i = frozen; i < ncols; i++)
    {
        header[i] = ""; /* continuation columns default to blank */
    }
    for (i = 0; i < dict.n; i++)
    {
        ContestInfo *ci = &dict.info[order[i]];
        header[ci->col_start] = ci->name; /* first column titled; rest stay "" */
    }

    if (!EeCvr_BuildBegin(out, header, ncols, frozen))
    {
        hart_set_err(error_message, error_cch, L"Out of memory building the CVR table.");
        s = EeLoadStatus_Error;
        goto cleanup;
    }

    /* ---- Pass 2: fill rows ---- */
    p2.dict = &dict;
    p2.sheet = &sheet;
    p2.table = out;
    p2.ncols = ncols;
    p2.frozen = frozen;
    p2.precinct_col = 4;
    p2.party_col = p1.global_has_party ? 5 : -1;
    p2.isblank_col = p1.global_has_party ? 6 : 5;
    p2.cells = (const char **)malloc((size_t)ncols * sizeof(char *));
    if (p2.cells == NULL)
    {
        s = EeLoadStatus_Error;
        goto cleanup;
    }
    for (f = 0; f < count && s == EeLoadStatus_Ok; f++)
    {
        s = hart_iterate_zip(paths[f], &buf, &buf_cap, pass2_entry, &p2, cancel_flag, &done, total,
                             progress_fn, progress_user, &last_pct, error_message, error_cch);
    }
    free((void *)p2.cells);

    if (s != EeLoadStatus_Ok)
    {
        goto cleanup;
    }
    if (out->nrows == 0)
    {
        hart_set_err(error_message, error_cch, L"No Cast Vote Records were found in the zip.");
        s = EeLoadStatus_Error;
    }

cleanup:
    if (s != EeLoadStatus_Ok)
    {
        EeCvr_Clear(out);
    }
    free(order);
    free(header);
    free(buf);
    sheet_free(&sheet);
    dict_free(&dict);
    return s;
}
