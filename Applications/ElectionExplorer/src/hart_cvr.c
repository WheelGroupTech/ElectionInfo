/**
 * @file hart_cvr.c
 * @brief Loader for Hart voting-system Cast Vote Records (ZIP/XML and PDF reports).
 *
 * Hart publishes CVRs in two forms; either or both can be loaded:
 *  - ZIP: one XML file per ballot sheet (described below);
 *  - PDF: the "CVR Report" (Microsoft Reporting Services rendering, document title
 *    "Count_CvrReport"), one record per ballot sheet, each starting a new page and
 *    continuing onto further pages when long. Each page carries a header block
 *    (Precinct, Party, Polling Place, Voting Type, Device Type, Device Serial,
 *    Device Data Id, Cvr Id, Central Batch Id) above a two-column "Contest Title" /
 *    "Option" table. An Option cell is a selection, "Write-in", "Overvote", or
 *    "Undervotes: N". The PDF has no Sheet Number, Batch Sequence or Is Blank, but
 *    adds the device and polling-place fields the XML lacks.
 *
 * Load modes (EeCvr_LoadFromHartFiles):
 *  - ZIPs only: as before;
 *  - PDFs only: the votes and header fields come from the PDF records (some counties
 *    publish only the PDF);
 *  - ZIPs + PDFs: the votes come from the ZIPs; the PDFs supply Voting Type, Polling
 *    Place, Device Type, Device Serial and Device Data Id, matched by Cvr Id.
 * A PDF is accepted only if its first page has the "CVR Report" title, a Cvr Id label
 * and the Contest Title / Option table header, so other vendors' PDFs are rejected with
 * a clear message. Counties may redact the other header fields (Burnet County removes
 * Polling Place, Voting Type and the device fields, and sometimes Party and Central
 * Batch Id): a PDF field that is blank on every record gets no column. A vote-for-N
 * contest is printed as one row per seat with the contest title repeated; rows with
 * the same title on one sheet are merged into one contest.
 *
 * A Hart CVR ZIP export is one or more ZIP files, each containing one XML file per
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
 *    Batch Number, Precinct, Party (only when any ballot has one), [Voting Type,
 *    Polling Place, Device Type, Device Serial, Device Data Id -- when PDFs are
 *    loaded], Is Blank. A PDF-only load has no Sheet Number, Batch Sequence or Is
 *    Blank column.
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
#include "pdf_reader.h"

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
    /* PDF-only header fields (blank for a ZIP sheet unless filled from a PDF). */
    char vtype[64];
    char pplace[256];
    char dtype[64];
    char dserial[64];
    char ddata[96];

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
    s->vtype[0] = s->pplace[0] = s->dtype[0] = s->dserial[0] = s->ddata[0] = '\0';

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

/* Append raw (already decoded) text [src,len) to the arena; returns its offset or
 * (size_t)-1 on OOM. Used for PDF text, which needs no entity decoding. */
static size_t arena_add_raw(HartSheet *s, const char *src, size_t len)
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
    if (len > 0)
    {
        memcpy(s->arena + off, src, len);
    }
    s->arena[off + len] = '\0';
    s->arena_len += len + 1;
    return off;
}

/* Start a new contest named [name,len) on @p s; returns its index or -1 on OOM. */
static int sheet_add_contest(HartSheet *s, const char *name, size_t len)
{
    HartContest *hc;
    if (s->nct == s->cap_ct)
    {
        int ncap = s->cap_ct ? s->cap_ct * 2 : 32;
        HartContest *ng = (HartContest *)realloc(s->ct, (size_t)ncap * sizeof(HartContest));
        if (ng == NULL)
        {
            return -1;
        }
        memset(ng + s->cap_ct, 0, (size_t)(ncap - s->cap_ct) * sizeof(HartContest));
        s->ct = ng;
        s->cap_ct = ncap;
    }
    hc = &s->ct[s->nct];
    hc->nsel = 0;
    hc->undervotes = 0;
    hc->overvoted = 0;
    hc->name_off = arena_add_raw(s, name, len);
    if (hc->name_off == (size_t)-1)
    {
        return -1;
    }
    return s->nct++;
}

/* Index of the contest on @p s named exactly [name,len), or -1. */
static int sheet_find_contest(const HartSheet *s, const char *name, size_t len)
{
    int i;
    for (i = 0; i < s->nct; i++)
    {
        const char *nm = s->arena + s->ct[i].name_off;
        if (strlen(nm) == len && memcmp(nm, name, len) == 0)
        {
            return i;
        }
    }
    return -1;
}

/* Add selection text [v,len) to contest @p idx. FALSE on OOM. */
static BOOL sheet_add_selection(HartSheet *s, int idx, const char *v, size_t len)
{
    HartContest *hc = &s->ct[idx];
    size_t off = arena_add_raw(s, v, len);
    if (off == (size_t)-1)
    {
        return FALSE;
    }
    if (hc->nsel == hc->cap_sel)
    {
        int scap = hc->cap_sel ? hc->cap_sel * 2 : 4;
        size_t *ns = (size_t *)realloc(hc->sel_off, (size_t)scap * sizeof(size_t));
        if (ns == NULL)
        {
            return FALSE;
        }
        hc->sel_off = ns;
        hc->cap_sel = scap;
    }
    hc->sel_off[hc->nsel++] = off;
    return TRUE;
}

/* Party ballot name -> short prefix ("REP"/"DEM"/"LIB"/"GRN"), or NULL. In a
 * primary each party's copy of a contest is a distinct race, so we prefix the
 * contest name (ES&S-style "REP United States Senator") to keep them separate. */
static const char *party_abbr(const char *party_name)
{
    if (party_name == NULL || party_name[0] == '\0')
    {
        return NULL;
    }
    if (contains_ci(party_name, "Republican"))
        return "REP";
    if (contains_ci(party_name, "Democratic") || contains_ci(party_name, "Democrat"))
        return "DEM";
    if (contains_ci(party_name, "Libertarian"))
        return "LIB";
    if (contains_ci(party_name, "Green"))
        return "GRN";
    return NULL;
}

/* Build the effective contest name (party-prefixed for a primary) into @p out. */
static void contest_display_name(char *out, size_t cap, const char *party_name, const char *base)
{
    const char *ab = party_abbr(party_name);
    if (ab != NULL)
    {
        StringCchPrintfA(out, cap, "%s %s", ab, base);
    }
    else
    {
        StringCchCopyA(out, cap, base);
    }
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

/* Case-insensitive "natural" compare: runs of digits compare by numeric value (so
 * "District 6" < "District 26" < "District 33" and "Precinct 3486" < "Precinct 4095"),
 * everything else compares by lowercased byte. Leading zeros in a digit run are ignored
 * for the value but a longer run of significant digits is the larger number. */
static int natural_cmp_ci(const char *a, const char *b)
{
    for (;;)
    {
        unsigned char ca = (unsigned char)*a;
        unsigned char cb = (unsigned char)*b;
        if (ca == 0 || cb == 0)
        {
            return (ca == cb) ? 0 : (ca == 0 ? -1 : 1);
        }
        if (ca >= '0' && ca <= '9' && cb >= '0' && cb <= '9')
        {
            const char *ea;
            const char *eb;
            size_t la;
            size_t lb;
            while (*a == '0')
            {
                a++;
            }
            while (*b == '0')
            {
                b++;
            }
            for (ea = a; *ea >= '0' && *ea <= '9'; ea++)
            {
            }
            for (eb = b; *eb >= '0' && *eb <= '9'; eb++)
            {
            }
            la = (size_t)(ea - a);
            lb = (size_t)(eb - b);
            if (la != lb)
            {
                return (la < lb) ? -1 : 1; /* more significant digits => larger number */
            }
            for (; a < ea; a++, b++)
            {
                if (*a != *b)
                {
                    return ((unsigned char)*a < (unsigned char)*b) ? -1 : 1;
                }
            }
            b = eb; /* a == ea already; both runs have equal value, continue past them */
            continue;
        }
        {
            unsigned char lca = (ca >= 'A' && ca <= 'Z') ? (unsigned char)(ca + 32) : ca;
            unsigned char lcb = (cb >= 'A' && cb <= 'Z') ? (unsigned char)(cb + 32) : cb;
            if (lca != lcb)
            {
                return (lca < lcb) ? -1 : 1;
            }
        }
        a++;
        b++;
    }
}

static int __cdecl contest_order_cmp(void *ctx, const void *a, const void *b)
{
    const ContestDict *d = (const ContestDict *)ctx;
    const ContestInfo *x = &d->info[*(const uint32_t *)a];
    const ContestInfo *y = &d->info[*(const uint32_t *)b];
    int c;
    if (x->rank != y->rank)
    {
        return (x->rank < y->rank) ? -1 : 1;
    }
    /* Same office category: sort by contest name so races that differ only by a trailing
     * number (US Rep District 6/26/33, Precinct Chair Precinct 3486/4095, ...) come out
     * in numeric order instead of the arbitrary order Hart wrote them. */
    c = natural_cmp_ci(x->name, y->name);
    if (c != 0)
    {
        return c;
    }
    return (x->first_seen < y->first_seen) ? -1 : (x->first_seen > y->first_seen ? 1 : 0);
}

/* -------------------------------------------------------------------------- */
/* Progress                                                                   */
/* -------------------------------------------------------------------------- */

/* Shared progress/cancel state across every pass and file of one load. Work units are
 * zip entries and PDF pages. */
typedef struct HartProgress
{
    volatile LONG *cancel_flag;
    EeLoadProgressFn fn;
    void *user;
    uint64_t done;
    uint64_t total;
    uint32_t last_pct;
    const uint32_t *rows; /* ballot records so far, or NULL during a scan pass */
} HartProgress;

/* Count one unit of work; every 1024 units check cancel and report. Returns FALSE when
 * the load should stop (cancelled). */
static BOOL hart_tick(HartProgress *pg)
{
    pg->done++;
    if ((pg->done & 0x3FF) != 0)
    {
        return TRUE;
    }
    if (pg->cancel_flag != NULL && *pg->cancel_flag != 0)
    {
        return FALSE;
    }
    if (pg->fn != NULL && pg->total > 0)
    {
        uint32_t pct = (uint32_t)((pg->done * 99ull) / pg->total);
        if (pct > 99u)
        {
            pct = 99u;
        }
        if (pct != pg->last_pct)
        {
            EeLoadProgress pr;
            pg->last_pct = pct;
            pr.percent = pct;
            pr.rows_loaded = (pg->rows != NULL) ? *pg->rows : 0u;
            pr.bytes_read = pg->done;
            pr.bytes_total = pg->total;
            /* A discovery pass (rows == NULL) has no rows yet -- flag it so the UI can
             * show "Scanning ballots...". */
            pr.scanning = (pg->rows == NULL) ? 1 : 0;
            if (!pg->fn(&pr, pg->user) && pg->cancel_flag != NULL)
            {
                InterlockedExchange(pg->cancel_flag, 1);
            }
        }
    }
    return TRUE;
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
                                     HartProgress *pg,
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
        if (!hart_tick(pg))
        {
            status = EeLoadStatus_Cancelled;
            break;
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
/* String map (interning + Cvr Id lookup for the ZIP+PDF merge)               */
/* -------------------------------------------------------------------------- */

/* Open-addressing map from a NUL-terminated string (stored once in a pool) to a
 * uint32 value. Strings are addressed by pool offset (the pool may move). */
typedef struct StrMap
{
    char *pool;
    size_t pool_len;
    size_t pool_cap;
    uint32_t *slot_off; /* slot -> pool offset + 1 (0 = empty) */
    uint32_t *slot_val;
    uint32_t cap;
    uint32_t n;
} StrMap;

static void strmap_free(StrMap *m)
{
    free(m->pool);
    free(m->slot_off);
    free(m->slot_val);
    ZeroMemory(m, sizeof(*m));
}

static BOOL strmap_rehash(StrMap *m, uint32_t ncap)
{
    uint32_t *no = (uint32_t *)calloc(ncap, sizeof(uint32_t));
    uint32_t *nv = (uint32_t *)calloc(ncap, sizeof(uint32_t));
    uint32_t i;
    if (no == NULL || nv == NULL)
    {
        free(no);
        free(nv);
        return FALSE;
    }
    for (i = 0; i < m->cap; i++)
    {
        if (m->slot_off[i] != 0)
        {
            uint32_t s = fnv1a(m->pool + (m->slot_off[i] - 1)) & (ncap - 1);
            while (no[s] != 0)
            {
                s = (s + 1) & (ncap - 1);
            }
            no[s] = m->slot_off[i];
            nv[s] = m->slot_val[i];
        }
    }
    free(m->slot_off);
    free(m->slot_val);
    m->slot_off = no;
    m->slot_val = nv;
    m->cap = ncap;
    return TRUE;
}

/* Find @p key; returns its slot or (uint32_t)-1. */
static uint32_t strmap_find(const StrMap *m, const char *key)
{
    uint32_t s;
    if (m->cap == 0)
    {
        return (uint32_t)-1;
    }
    s = fnv1a(key) & (m->cap - 1);
    while (m->slot_off[s] != 0)
    {
        if (strcmp(m->pool + (m->slot_off[s] - 1), key) == 0)
        {
            return s;
        }
        s = (s + 1) & (m->cap - 1);
    }
    return (uint32_t)-1;
}

/* Insert @p key (or find it). *out_off receives its pool offset; when new, its value
 * is set to @p val. Returns FALSE on OOM. */
static BOOL strmap_put(StrMap *m, const char *key, uint32_t val, uint32_t *out_off)
{
    uint32_t s;
    size_t len;
    if (m->cap == 0 || (m->n + 1) * 4 >= m->cap * 3)
    {
        if (!strmap_rehash(m, m->cap ? m->cap * 2 : 1024))
        {
            return FALSE;
        }
    }
    s = fnv1a(key) & (m->cap - 1);
    while (m->slot_off[s] != 0)
    {
        if (strcmp(m->pool + (m->slot_off[s] - 1), key) == 0)
        {
            *out_off = m->slot_off[s] - 1;
            return TRUE;
        }
        s = (s + 1) & (m->cap - 1);
    }
    len = strlen(key) + 1;
    if (m->pool_len + len > 0xFFFFFFF0u)
    {
        return FALSE;
    }
    if (m->pool_len + len > m->pool_cap)
    {
        size_t nc = m->pool_cap ? m->pool_cap * 2 : 65536;
        char *np;
        while (nc < m->pool_len + len)
        {
            nc *= 2;
        }
        np = (char *)realloc(m->pool, nc);
        if (np == NULL)
        {
            return FALSE;
        }
        m->pool = np;
        m->pool_cap = nc;
    }
    memcpy(m->pool + m->pool_len, key, len);
    m->slot_off[s] = (uint32_t)m->pool_len + 1;
    m->slot_val[s] = val;
    *out_off = (uint32_t)m->pool_len;
    m->pool_len += len;
    m->n++;
    return TRUE;
}

/* PDF header fields per Cvr Id, for decorating ZIP rows. */
enum
{
    META_VTYPE = 0,
    META_PPLACE,
    META_DTYPE,
    META_DSERIAL,
    META_DDATA,
    META_COUNT
};

typedef struct HartMeta
{
    StrMap vals;   /* interned field values */
    StrMap guids;  /* lowercase Cvr Id -> entry index */
    uint32_t *ent; /* META_COUNT value offsets per entry */
    uint32_t n;
    uint32_t cap;
    unsigned present; /* (1u << META_*) for fields non-blank on any record */
} HartMeta;

static void meta_free(HartMeta *m)
{
    strmap_free(&m->vals);
    strmap_free(&m->guids);
    free(m->ent);
    ZeroMemory(m, sizeof(*m));
}

static void ascii_lower(char *s)
{
    for (; *s; s++)
    {
        if (*s >= 'A' && *s <= 'Z')
        {
            *s = (char)(*s - 'A' + 'a');
        }
    }
}

static BOOL meta_add(HartMeta *m, const HartSheet *s)
{
    const char *f[META_COUNT];
    uint32_t off, k;
    uint32_t *e;
    char key[80];
    if (s->guid[0] == '\0')
    {
        return TRUE;
    }
    StringCchCopyA(key, ARRAYSIZE(key), s->guid);
    ascii_lower(key);
    if (strmap_find(&m->guids, key) != (uint32_t)-1)
    {
        return TRUE; /* first occurrence wins */
    }
    if (m->n == m->cap)
    {
        uint32_t nc = m->cap ? m->cap * 2 : 4096;
        uint32_t *ne = (uint32_t *)realloc(m->ent, (size_t)nc * META_COUNT * sizeof(uint32_t));
        if (ne == NULL)
        {
            return FALSE;
        }
        m->ent = ne;
        m->cap = nc;
    }
    f[META_VTYPE] = s->vtype;
    f[META_PPLACE] = s->pplace;
    f[META_DTYPE] = s->dtype;
    f[META_DSERIAL] = s->dserial;
    f[META_DDATA] = s->ddata;
    e = &m->ent[(size_t)m->n * META_COUNT];
    for (k = 0; k < META_COUNT; k++)
    {
        if (f[k][0] != '\0')
        {
            m->present |= 1u << k;
        }
        if (!strmap_put(&m->vals, f[k], 0, &e[k]))
        {
            return FALSE;
        }
    }
    if (!strmap_put(&m->guids, key, m->n, &off))
    {
        return FALSE;
    }
    m->n++;
    return TRUE;
}

/* Fill @p s's PDF fields from the entry for its Cvr Id. Returns TRUE if found. */
static BOOL meta_apply(const HartMeta *m, HartSheet *s)
{
    char key[80];
    uint32_t slot;
    const uint32_t *e;
    StringCchCopyA(key, ARRAYSIZE(key), s->guid);
    ascii_lower(key);
    slot = strmap_find(&m->guids, key);
    if (slot == (uint32_t)-1)
    {
        s->vtype[0] = s->pplace[0] = s->dtype[0] = s->dserial[0] = s->ddata[0] = '\0';
        return FALSE;
    }
    e = &m->ent[(size_t)m->guids.slot_val[slot] * META_COUNT];
    StringCchCopyA(s->vtype, ARRAYSIZE(s->vtype), m->vals.pool + e[META_VTYPE]);
    StringCchCopyA(s->pplace, ARRAYSIZE(s->pplace), m->vals.pool + e[META_PPLACE]);
    StringCchCopyA(s->dtype, ARRAYSIZE(s->dtype), m->vals.pool + e[META_DTYPE]);
    StringCchCopyA(s->dserial, ARRAYSIZE(s->dserial), m->vals.pool + e[META_DSERIAL]);
    StringCchCopyA(s->ddata, ARRAYSIZE(s->ddata), m->vals.pool + e[META_DDATA]);
    return TRUE;
}

/* -------------------------------------------------------------------------- */
/* Hart PDF report parsing                                                    */
/* -------------------------------------------------------------------------- */

/* One table/header cell: the text of all runs drawn inside one clip rectangle. */
typedef struct PdfCell
{
    float x0, y0, x1, y1;
    size_t off; /* text in HartPdfCtx.text */
    size_t len;
} PdfCell;

typedef struct HartPdfCtx
{
    EePdf *pdf;
    EePdfPageText pt;
    PdfCell *cells;
    uint32_t ncells;
    uint32_t cap_cells;
    char *text;
    size_t text_len;
    size_t text_cap;
    uint32_t *order; /* scratch index array */
    uint32_t cap_order;
} HartPdfCtx;

static void pdfctx_free(HartPdfCtx *c)
{
    EePdf_Close(c->pdf);
    EePdf_PageTextFree(&c->pt);
    free(c->cells);
    free(c->text);
    free(c->order);
    ZeroMemory(c, sizeof(*c));
}

static BOOL cell_text_append(HartPdfCtx *c, const char *s, size_t n)
{
    if (c->text_len + n + 1 > c->text_cap)
    {
        size_t nc = c->text_cap ? c->text_cap * 2 : 8192;
        char *nt;
        while (nc < c->text_len + n + 1)
        {
            nc *= 2;
        }
        nt = (char *)realloc(c->text, nc);
        if (nt == NULL)
        {
            return FALSE;
        }
        c->text = nt;
        c->text_cap = nc;
    }
    memcpy(c->text + c->text_len, s, n);
    c->text_len += n;
    c->text[c->text_len] = '\0';
    return TRUE;
}

/* Group the page's runs into cells: consecutive runs inside the same clip form one
 * cell whose text is their concatenation (a wrapped contest title is two runs). */
static BOOL pdf_build_cells(HartPdfCtx *c)
{
    uint32_t i;
    c->ncells = 0;
    c->text_len = 0;
    for (i = 0; i < c->pt.nruns; i++)
    {
        const EePdfTextRun *r = &c->pt.runs[i];
        PdfCell *cell;
        BOOL join = FALSE;
        if (i > 0 && r->clip_id != 0 && r->clip_id == c->pt.runs[i - 1].clip_id && c->ncells > 0)
        {
            join = TRUE;
        }
        if (!join)
        {
            if (c->ncells == c->cap_cells)
            {
                uint32_t nc = c->cap_cells ? c->cap_cells * 2 : 256;
                PdfCell *ncl = (PdfCell *)realloc(c->cells, (size_t)nc * sizeof(PdfCell));
                if (ncl == NULL)
                {
                    return FALSE;
                }
                c->cells = ncl;
                c->cap_cells = nc;
            }
            cell = &c->cells[c->ncells++];
            if (r->has_clip)
            {
                cell->x0 = r->clip_x0;
                cell->y0 = r->clip_y0;
                cell->x1 = r->clip_x1;
                cell->y1 = r->clip_y1;
            }
            else
            {
                cell->x0 = cell->x1 = r->x;
                cell->y0 = cell->y1 = r->y;
            }
            cell->off = c->text_len;
            cell->len = 0;
        }
        cell = &c->cells[c->ncells - 1];
        if (!cell_text_append(c, c->pt.text + r->text_off, r->text_len))
        {
            return FALSE;
        }
        cell->len += r->text_len;
    }
    return TRUE;
}

/* Trimmed view of a cell's text. */
static const char *cell_trim(const HartPdfCtx *c, const PdfCell *cell, size_t *len)
{
    const char *s = c->text + cell->off;
    size_t n = cell->len;
    while (n > 0 && (*s == ' ' || *s == '\t'))
    {
        s++;
        n--;
    }
    while (n > 0 && (s[n - 1] == ' ' || s[n - 1] == '\t'))
    {
        n--;
    }
    *len = n;
    return s;
}

static BOOL text_is(const char *s, size_t n, const char *lit)
{
    size_t l = strlen(lit);
    return n == l && memcmp(s, lit, l) == 0;
}

/* If [s,n) starts with @p label (e.g. "Cvr Id:"), copy the trimmed remainder into
 * @p dst and return TRUE. */
static BOOL take_label(const char *s, size_t n, const char *label, char *dst, size_t cap)
{
    size_t l = strlen(label);
    if (n < l || memcmp(s, label, l) != 0)
    {
        return FALSE;
    }
    s += l;
    n -= l;
    while (n > 0 && *s == ' ')
    {
        s++;
        n--;
    }
    while (n > 0 && s[n - 1] == ' ')
    {
        n--;
    }
    if (n >= cap)
    {
        n = cap - 1;
    }
    memcpy(dst, s, n);
    dst[n] = '\0';
    return TRUE;
}

/* "3156 - 008" -> "3156-008": the PDF spaces the precinct split separator, the XML
 * does not; normalize so both sources key the same precinct. */
static void normalize_precinct(char *p)
{
    char *w = p;
    const char *r = p;
    while (*r)
    {
        if (r[0] == ' ' && r[1] == '-' && r[2] == ' ')
        {
            *w++ = '-';
            r += 3;
            continue;
        }
        *w++ = *r++;
    }
    *w = '\0';
}

/* Parsed header of one PDF page. */
typedef struct HartPdfPage
{
    int is_cvr_page;   /* has the Contest Title / Option table header */
    int has_labels;    /* has the "CVR Report" title and a Cvr Id label (the fields that
                        * survive county redaction) */
    char guid[80];
    char precinct[192];
    char party[192];
    char batch[24];
    char vtype[64];
    char pplace[256];
    char dtype[64];
    char dserial[64];
    char ddata[96];
    int title_cell;    /* index of "Contest Title" header cell */
    int option_cell;   /* index of "Option" header cell */
} HartPdfPage;

static void pdf_parse_header(const HartPdfCtx *c, HartPdfPage *pg)
{
    uint32_t i;
    int found = 0;
    ZeroMemory(pg, sizeof(*pg));
    pg->title_cell = pg->option_cell = -1;
    for (i = 0; i < c->ncells; i++)
    {
        size_t n;
        const char *s = cell_trim(c, &c->cells[i], &n);
        if (pg->title_cell < 0 && text_is(s, n, "Contest Title"))
        {
            pg->title_cell = (int)i;
        }
        else if (pg->option_cell < 0 && text_is(s, n, "Option"))
        {
            pg->option_cell = (int)i;
        }
    }
    pg->is_cvr_page = (pg->title_cell >= 0 && pg->option_cell >= 0);
    for (i = 0; i < c->ncells; i++)
    {
        const PdfCell *cell = &c->cells[i];
        size_t n;
        const char *s;
        /* header block = above the table header (when there is one) */
        if (pg->is_cvr_page && cell->y1 <= c->cells[pg->title_cell].y1)
        {
            continue;
        }
        s = cell_trim(c, cell, &n);
        if (text_is(s, n, "CVR Report"))
            found |= 2;
        else if (take_label(s, n, "Cvr Id:", pg->guid, sizeof(pg->guid)))
            found |= 1;
        else if (take_label(s, n, "Device Serial:", pg->dserial, sizeof(pg->dserial)))
            ;
        else if (take_label(s, n, "Device Data Id:", pg->ddata, sizeof(pg->ddata)))
            ;
        else if (take_label(s, n, "Central Batch Id:", pg->batch, sizeof(pg->batch)))
            ;
        else if (take_label(s, n, "Precinct:", pg->precinct, sizeof(pg->precinct)))
            normalize_precinct(pg->precinct);
        else if (take_label(s, n, "Party:", pg->party, sizeof(pg->party)))
            ;
        else if (take_label(s, n, "Polling Place:", pg->pplace, sizeof(pg->pplace)))
            ;
        else if (take_label(s, n, "Voting Type:", pg->vtype, sizeof(pg->vtype)))
            ;
        else if (take_label(s, n, "Device Type:", pg->dtype, sizeof(pg->dtype)))
            ;
    }
    pg->has_labels = (found == 3);
    ascii_lower(pg->guid);
}

static int __cdecl cell_y_desc_cmp(void *ctx, const void *a, const void *b)
{
    const PdfCell *cells = (const PdfCell *)ctx;
    float ya = cells[*(const uint32_t *)a].y1;
    float yb = cells[*(const uint32_t *)b].y1;
    if (ya != yb)
    {
        return (ya > yb) ? -1 : 1;
    }
    return (*(const uint32_t *)a < *(const uint32_t *)b) ? -1 : 1;
}

/* Interpret one Option cell's text into contest @p idx. */
static BOOL add_option_text(HartSheet *s, int idx, const char *v, size_t n)
{
    HartContest *hc = &s->ct[idx];
    if (n >= 11 && memcmp(v, "Undervotes:", 11) == 0)
    {
        hc->undervotes += atoi(v + 11) > 0 ? atoi(v + 11) : 1;
        return TRUE;
    }
    if ((n == 8 && memcmp(v, "Overvote", 8) == 0) || (n >= 10 && memcmp(v, "Overvotes:", 10) == 0))
    {
        hc->overvoted = 1;
        return TRUE;
    }
    if (n >= 8 && _strnicmp(v, "Write-in", 8) == 0)
    {
        return sheet_add_selection(s, idx, "Write-in", 8);
    }
    if (n == 0)
    {
        return TRUE;
    }
    return sheet_add_selection(s, idx, v, n);
}

/* Add the page's table rows (contests + options) to @p s. Option cells are matched to
 * the title cell whose vertical extent contains their centre (a wrapped title is a
 * taller cell); an option with no title on this page continues the previous contest. */
static BOOL pdf_add_rows(HartPdfCtx *c, const HartPdfPage *pg, HartSheet *s)
{
    const PdfCell *th = &c->cells[pg->title_cell];
    const PdfCell *oh = &c->cells[pg->option_cell];
    float split = oh->x0; /* left of the Option column = title column */
    uint32_t i, nt = 0, no = 0;
    uint32_t *titles, *opts;
    int *title_idx = NULL;
    BOOL ok = TRUE;

    if (c->cap_order < c->ncells * 2 + 2)
    {
        uint32_t nc = c->ncells * 2 + 64;
        uint32_t *nb = (uint32_t *)realloc(c->order, (size_t)nc * sizeof(uint32_t));
        if (nb == NULL)
        {
            return FALSE;
        }
        c->order = nb;
        c->cap_order = nc;
    }
    titles = c->order;
    opts = c->order + c->ncells + 1;
    for (i = 0; i < c->ncells; i++)
    {
        const PdfCell *cell = &c->cells[i];
        float cx = (cell->x0 + cell->x1) * 0.5f;
        if ((int)i == pg->title_cell || (int)i == pg->option_cell)
        {
            continue;
        }
        if (cell->y1 > th->y0 + 0.5f) /* not below the table header */
        {
            continue;
        }
        if (cx < split)
        {
            titles[nt++] = i;
        }
        else if (cx < oh->x1 + 0.5f)
        {
            opts[no++] = i;
        }
    }
    qsort_s(titles, nt, sizeof(uint32_t), cell_y_desc_cmp, c->cells);
    qsort_s(opts, no, sizeof(uint32_t), cell_y_desc_cmp, c->cells);

    if (nt > 0)
    {
        title_idx = (int *)malloc((size_t)nt * sizeof(int));
        if (title_idx == NULL)
        {
            return FALSE;
        }
    }
    for (i = 0; i < nt && ok; i++)
    {
        size_t n;
        const char *t = cell_trim(c, &c->cells[titles[i]], &n);
        /* The PDF prints a vote-for-N contest as one row per seat with the title
         * repeated: rows with the same title on one sheet are the same contest. */
        title_idx[i] = sheet_find_contest(s, t, n);
        if (title_idx[i] < 0)
        {
            title_idx[i] = sheet_add_contest(s, t, n);
        }
        ok = (title_idx[i] >= 0);
    }
    for (i = 0; i < no && ok; i++)
    {
        const PdfCell *oc = &c->cells[opts[i]];
        float cy = (oc->y0 + oc->y1) * 0.5f;
        int target = -1;
        uint32_t k;
        size_t n;
        const char *v;
        for (k = 0; k < nt; k++)
        {
            const PdfCell *tc = &c->cells[titles[k]];
            if (cy >= tc->y0 - 0.5f && cy <= tc->y1 + 0.5f)
            {
                target = title_idx[k];
                break;
            }
        }
        if (target < 0)
        {
            /* nearest title above, else the last contest so far (continued page) */
            for (k = 0; k < nt; k++)
            {
                if (c->cells[titles[k]].y0 >= cy)
                {
                    target = title_idx[k];
                }
            }
            if (target < 0 && s->nct > 0)
            {
                target = s->nct - 1 - (int)nt;
                if (target < 0)
                {
                    target = -1;
                }
            }
        }
        if (target < 0)
        {
            continue;
        }
        v = cell_trim(c, oc, &n);
        ok = add_option_text(s, target, v, n);
    }
    free(title_idx);
    return ok;
}

/* Start a new sheet record from a page header. */
static void pdf_begin_sheet(HartSheet *s, const HartPdfPage *pg)
{
    s->arena_len = 0;
    s->nct = 0;
    s->sheet[0] = s->batchseq[0] = s->isblank[0] = '\0';
    StringCchCopyA(s->guid, ARRAYSIZE(s->guid), pg->guid);
    StringCchCopyA(s->batchnum, ARRAYSIZE(s->batchnum), pg->batch);
    StringCchCopyA(s->precinct, ARRAYSIZE(s->precinct), pg->precinct);
    StringCchCopyA(s->party, ARRAYSIZE(s->party), pg->party);
    s->has_party = (pg->party[0] != '\0');
    StringCchCopyA(s->vtype, ARRAYSIZE(s->vtype), pg->vtype);
    StringCchCopyA(s->pplace, ARRAYSIZE(s->pplace), pg->pplace);
    StringCchCopyA(s->dtype, ARRAYSIZE(s->dtype), pg->dtype);
    StringCchCopyA(s->dserial, ARRAYSIZE(s->dserial), pg->dserial);
    StringCchCopyA(s->ddata, ARRAYSIZE(s->ddata), pg->ddata);
}

/* Last path component, for messages. */
static const wchar_t *path_leaf(const wchar_t *path)
{
    const wchar_t *leaf = path;
    const wchar_t *p;
    for (p = path; *p; p++)
    {
        if (*p == L'\\' || *p == L'/')
        {
            leaf = p + 1;
        }
    }
    return leaf;
}

static void set_pdf_err(wchar_t *err, size_t cch, const wchar_t *path, const wchar_t *what)
{
    if (err != NULL && cch > 0)
    {
        StringCchPrintfW(err, cch, L"%s: %s", path_leaf(path), what);
    }
}

/* Open @p path and verify page 1 is a Hart CVR Report page. On success the context
 * holds the open document. */
static BOOL hart_pdf_open(HartPdfCtx *c, const wchar_t *path, wchar_t *err, size_t errcch)
{
    wchar_t perr[256] = L"";
    HartPdfPage pg;
    ZeroMemory(c, sizeof(*c));
    EePdf_PageTextInit(&c->pt);
    if (!EePdf_Open(path, &c->pdf, perr, ARRAYSIZE(perr)))
    {
        set_pdf_err(err, errcch, path, perr);
        return FALSE;
    }
    if (EePdf_PageCount(c->pdf) == 0 || !EePdf_ExtractPageText(c->pdf, 0, &c->pt) ||
        !pdf_build_cells(c))
    {
        set_pdf_err(err, errcch, path, L"the first page could not be read.");
        pdfctx_free(c);
        return FALSE;
    }
    pdf_parse_header(c, &pg);
    if (!pg.is_cvr_page || !pg.has_labels || pg.guid[0] == '\0')
    {
        set_pdf_err(err, errcch, path,
                    L"not a Hart Cast Vote Record report (expected the Hart \"CVR Report\" "
                    L"with a Cvr Id and Contest Title / Option columns).");
        pdfctx_free(c);
        return FALSE;
    }
    return TRUE;
}

BOOL EeCvr_IsHartCvrPdf(const wchar_t *path, wchar_t *error_message, size_t error_cch)
{
    HartPdfCtx c;
    if (path == NULL)
    {
        hart_set_err(error_message, error_cch, L"Invalid arguments.");
        return FALSE;
    }
    if (!hart_pdf_open(&c, path, error_message, error_cch))
    {
        return FALSE;
    }
    pdfctx_free(&c);
    return TRUE;
}

typedef BOOL (*HartSheetFn)(void *ctx, HartSheet *sheet);

/* Walk every page of a Hart CVR Report PDF, assembling consecutive pages with the
 * same Cvr Id into one sheet record, and call @p fn per record. When @p rows_only is
 * FALSE (metadata scan) the contest table is skipped. */
static EeLoadStatus hart_iterate_pdf(const wchar_t *path,
                                     HartSheet *sheet,
                                     HartSheetFn fn,
                                     void *ctx,
                                     BOOL want_rows,
                                     HartProgress *pg,
                                     wchar_t *err,
                                     size_t errcch)
{
    HartPdfCtx c;
    uint32_t i, n;
    BOOL open_rec = FALSE;
    EeLoadStatus status = EeLoadStatus_Ok;

    if (!hart_pdf_open(&c, path, err, errcch))
    {
        return EeLoadStatus_Error;
    }
    n = EePdf_PageCount(c.pdf);
    for (i = 0; i < n; i++)
    {
        HartPdfPage hp;
        if (i > 0 && (!EePdf_ExtractPageText(c.pdf, i, &c.pt) || !pdf_build_cells(&c)))
        {
            wchar_t msg[96];
            StringCchPrintfW(msg, ARRAYSIZE(msg), L"page %u could not be read.", i + 1);
            set_pdf_err(err, errcch, path, msg);
            status = EeLoadStatus_Error;
            break;
        }
        pdf_parse_header(&c, &hp);
        if (hp.is_cvr_page && hp.guid[0] == '\0')
        {
            /* Never drop or mis-attribute a ballot sheet silently: a table page with no
             * readable Cvr Id means the layout is not understood. */
            wchar_t msg[96];
            StringCchPrintfW(msg, ARRAYSIZE(msg), L"page %u has no readable Cvr Id.", i + 1);
            set_pdf_err(err, errcch, path, msg);
            status = EeLoadStatus_Error;
            break;
        }
        if (hp.is_cvr_page)
        {
            if (!open_rec || strcmp(hp.guid, sheet->guid) != 0)
            {
                if (open_rec && !fn(ctx, sheet))
                {
                    status = EeLoadStatus_Error;
                    hart_set_err(err, errcch, L"Out of memory building the CVR table.");
                    break;
                }
                pdf_begin_sheet(sheet, &hp);
                open_rec = TRUE;
            }
            if (want_rows && !pdf_add_rows(&c, &hp, sheet))
            {
                status = EeLoadStatus_Error;
                hart_set_err(err, errcch, L"Out of memory reading the CVR PDF.");
                break;
            }
        }
        if (!hart_tick(pg))
        {
            status = EeLoadStatus_Cancelled;
            break;
        }
    }
    if (status == EeLoadStatus_Ok && open_rec && !fn(ctx, sheet))
    {
        status = EeLoadStatus_Error;
        hart_set_err(err, errcch, L"Out of memory building the CVR table.");
    }
    pdfctx_free(&c);
    return status;
}

/* -------------------------------------------------------------------------- */
/* Column layout                                                              */
/* -------------------------------------------------------------------------- */

enum
{
    HK_GUID = 0,
    HK_SHEET,
    HK_BSEQ,
    HK_BNUM,
    HK_PRECINCT,
    HK_PARTY,
    HK_VTYPE,
    HK_PPLACE,
    HK_DTYPE,
    HK_DSERIAL,
    HK_DDATA,
    HK_ISBLANK,
    HK_COUNT
};

static const char *const k_HartKeyNames[HK_COUNT] = {
    "CvrGuid",       "Sheet Number",  "Batch Sequence", "Batch Number",
    "Precinct",      "Party",         "Voting Type",    "Polling Place",
    "Device Type",   "Device Serial", "Device Data Id", "Is Blank"};

static const char *hart_key_value(const HartSheet *s, int key)
{
    switch (key)
    {
        case HK_GUID: return s->guid;
        case HK_SHEET: return s->sheet;
        case HK_BSEQ: return s->batchseq;
        case HK_BNUM: return s->batchnum;
        case HK_PRECINCT: return s->precinct;
        case HK_PARTY: return s->party;
        case HK_VTYPE: return s->vtype;
        case HK_PPLACE: return s->pplace;
        case HK_DTYPE: return s->dtype;
        case HK_DSERIAL: return s->dserial;
        case HK_DDATA: return s->ddata;
        case HK_ISBLANK: return s->isblank;
        default: return "";
    }
}

/* Choose the frozen key columns for a load. @p present has (1u << HK_*) set for each
 * PDF-sourced field that is non-blank on at least one record: a field blank on every
 * record (redacted by the county, or never filled) gets no column. The ZIP's own
 * columns, including its Batch Number, are always kept. */
static uint32_t hart_key_layout(int *keys, BOOL has_zip, BOOL has_pdf, BOOL has_party,
                                unsigned present)
{
    static const int k_PdfKeys[] = {HK_VTYPE, HK_PPLACE, HK_DTYPE, HK_DSERIAL, HK_DDATA};
    uint32_t n = 0, k;
    keys[n++] = HK_GUID;
    if (has_zip)
    {
        keys[n++] = HK_SHEET;
        keys[n++] = HK_BSEQ;
    }
    if (has_zip || (present & (1u << HK_BNUM)))
    {
        keys[n++] = HK_BNUM;
    }
    keys[n++] = HK_PRECINCT;
    if (has_party)
    {
        keys[n++] = HK_PARTY;
    }
    for (k = 0; has_pdf && k < ARRAYSIZE(k_PdfKeys); k++)
    {
        if (present & (1u << k_PdfKeys[k]))
        {
            keys[n++] = k_PdfKeys[k];
        }
    }
    if (has_zip)
    {
        keys[n++] = HK_ISBLANK;
    }
    return n;
}

/* -------------------------------------------------------------------------- */
/* Two-pass load                                                              */
/* -------------------------------------------------------------------------- */

typedef struct
{
    ContestDict *dict;
    HartSheet *sheet;
    int global_has_party;
    const HartMeta *meta; /* ZIP+PDF: count Cvr Ids found in the PDFs */
    uint64_t meta_hits;
    unsigned present; /* (1u << HK_*) for PDF-sourced fields seen non-blank */
} Pass1Ctx;

static BOOL pass1_sheet(void *vctx, HartSheet *sheet)
{
    Pass1Ctx *p = (Pass1Ctx *)vctx;
    int i;
    if (sheet->has_party)
    {
        p->global_has_party = 1;
    }
    {
        static const int k_Fields[] = {HK_BNUM, HK_VTYPE, HK_PPLACE, HK_DTYPE, HK_DSERIAL,
                                       HK_DDATA};
        uint32_t k;
        for (k = 0; k < ARRAYSIZE(k_Fields); k++)
        {
            if (hart_key_value(sheet, k_Fields[k])[0] != '\0')
            {
                p->present |= 1u << k_Fields[k];
            }
        }
    }
    if (p->meta != NULL)
    {
        char key[80];
        StringCchCopyA(key, ARRAYSIZE(key), sheet->guid);
        ascii_lower(key);
        if (strmap_find(&p->meta->guids, key) != (uint32_t)-1)
        {
            p->meta_hits++;
        }
    }
    for (i = 0; i < sheet->nct; i++)
    {
        HartContest *hc = &sheet->ct[i];
        char name[384];
        uint32_t idx;
        ContestInfo *ci;
        contest_display_name(name, sizeof(name), sheet->party, sheet->arena + hc->name_off);
        idx = dict_intern(p->dict, name);
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

static BOOL pass1_entry(void *vctx, char *xml)
{
    Pass1Ctx *p = (Pass1Ctx *)vctx;
    if (!parse_sheet(p->sheet, xml))
    {
        return FALSE;
    }
    return pass1_sheet(vctx, p->sheet);
}

typedef struct
{
    ContestDict *dict;
    HartSheet *sheet;
    EeCvrTable *table;
    const char **cells; /* ncols */
    uint32_t ncols;
    const int *keys;    /* frozen key column ids, nkeys of them */
    uint32_t nkeys;
    const HartMeta *meta; /* ZIP+PDF: decorate rows by Cvr Id */
} Pass2Ctx;

static BOOL pass2_sheet(void *vctx, HartSheet *sheet)
{
    Pass2Ctx *p = (Pass2Ctx *)vctx;
    uint32_t c;
    int i;
    if (p->meta != NULL)
    {
        meta_apply(p->meta, sheet);
    }
    for (c = 0; c < p->ncols; c++)
    {
        p->cells[c] = "";
    }
    for (c = 0; c < p->nkeys; c++)
    {
        p->cells[c] = hart_key_value(sheet, p->keys[c]);
    }

    for (i = 0; i < sheet->nct; i++)
    {
        HartContest *hc = &sheet->ct[i];
        char name[384];
        uint32_t idx;
        ContestInfo *ci;
        uint32_t base;
        int n, slot = 0, k;
        contest_display_name(name, sizeof(name), sheet->party, sheet->arena + hc->name_off);
        idx = dict_intern(p->dict, name); /* already present */
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
            p->cells[base + (uint32_t)slot] = sheet->arena + hc->sel_off[k];
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

static BOOL pass2_entry(void *vctx, char *xml)
{
    Pass2Ctx *p = (Pass2Ctx *)vctx;
    if (!parse_sheet(p->sheet, xml))
    {
        return FALSE;
    }
    return pass2_sheet(vctx, p->sheet);
}

/* Metadata scan of the PDFs (ZIP+PDF mode). */
static BOOL meta_sheet(void *vctx, HartSheet *sheet)
{
    return meta_add((HartMeta *)vctx, sheet);
}

static BOOL path_has_ext_w(const wchar_t *path, const wchar_t *ext)
{
    size_t n = wcslen(path);
    size_t e = wcslen(ext);
    return n >= e && _wcsicmp(path + (n - e), ext) == 0;
}

EeLoadStatus EeCvr_LoadFromHartFiles(const wchar_t *const *paths,
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
    HartMeta meta;
    HartProgress pg;
    char *buf = NULL;
    size_t buf_cap = 0;
    EeLoadStatus s = EeLoadStatus_Ok;
    int f, nzip = 0, npdf = 0;
    uint32_t frozen, ncols, i;
    uint32_t *order = NULL;
    const char **header = NULL;
    const wchar_t **zips = NULL;
    const wchar_t **pdfs = NULL;
    int keys[HK_COUNT];
    uint64_t pdf_pages = 0;
    Pass1Ctx p1;
    Pass2Ctx p2;

    if (paths == NULL || out == NULL || count <= 0)
    {
        hart_set_err(error_message, error_cch, L"Invalid arguments.");
        return EeLoadStatus_Error;
    }
    EeCvr_Clear(out);
    ZeroMemory(&dict, sizeof(dict));
    ZeroMemory(&meta, sizeof(meta));
    ZeroMemory(&pg, sizeof(pg));
    ZeroMemory(&p2, sizeof(p2));
    sheet_init(&sheet);

    zips = (const wchar_t **)calloc((size_t)count, sizeof(wchar_t *));
    pdfs = (const wchar_t **)calloc((size_t)count, sizeof(wchar_t *));
    if (zips == NULL || pdfs == NULL)
    {
        hart_set_err(error_message, error_cch, L"Out of memory.");
        s = EeLoadStatus_Error;
        goto cleanup;
    }
    for (f = 0; f < count; f++)
    {
        if (path_has_ext_w(paths[f], L".zip"))
        {
            zips[nzip++] = paths[f];
        }
        else if (path_has_ext_w(paths[f], L".pdf"))
        {
            pdfs[npdf++] = paths[f];
        }
        else
        {
            set_pdf_err(error_message, error_cch, paths[f],
                        L"Hart Cast Vote Records must be .zip or .pdf files.");
            s = EeLoadStatus_Error;
            goto cleanup;
        }
    }

    /* Validate every PDF up front (rejects other vendors' PDFs before any work) and
     * size the progress bar. */
    for (f = 0; f < npdf; f++)
    {
        HartPdfCtx c;
        if (!hart_pdf_open(&c, pdfs[f], error_message, error_cch))
        {
            s = EeLoadStatus_Error;
            goto cleanup;
        }
        pdf_pages += EePdf_PageCount(c.pdf);
        pdfctx_free(&c);
    }

    pg.cancel_flag = cancel_flag;
    pg.fn = progress_fn;
    pg.user = progress_user;
    pg.last_pct = 101;
    pg.total = hart_count_entries(zips, nzip) * 2ull + pdf_pages * (nzip > 0 ? 1ull : 2ull);

    /* ---- ZIP+PDF: index the PDFs' header fields by Cvr Id ---- */
    if (nzip > 0 && npdf > 0)
    {
        pg.rows = NULL;
        for (f = 0; f < npdf && s == EeLoadStatus_Ok; f++)
        {
            s = hart_iterate_pdf(pdfs[f], &sheet, meta_sheet, &meta, FALSE, &pg, error_message,
                                 error_cch);
        }
        if (s != EeLoadStatus_Ok)
        {
            goto cleanup;
        }
    }

    /* ---- Pass 1: discover contests / seat counts / party presence ---- */
    ZeroMemory(&p1, sizeof(p1));
    p1.dict = &dict;
    p1.sheet = &sheet;
    p1.meta = (nzip > 0 && npdf > 0) ? &meta : NULL;
    pg.rows = NULL;
    if (nzip > 0)
    {
        for (f = 0; f < nzip && s == EeLoadStatus_Ok; f++)
        {
            s = hart_iterate_zip(zips[f], &buf, &buf_cap, pass1_entry, &p1, &pg, error_message,
                                 error_cch);
        }
    }
    else
    {
        for (f = 0; f < npdf && s == EeLoadStatus_Ok; f++)
        {
            s = hart_iterate_pdf(pdfs[f], &sheet, pass1_sheet, &p1, TRUE, &pg, error_message,
                                 error_cch);
        }
    }
    if (s != EeLoadStatus_Ok)
    {
        goto cleanup;
    }
    if (dict.n == 0)
    {
        hart_set_err(error_message, error_cch, L"No Cast Vote Records were found.");
        s = EeLoadStatus_Error;
        goto cleanup;
    }
    if (p1.meta != NULL && p1.meta_hits == 0)
    {
        hart_set_err(error_message, error_cch,
                     L"The PDF reports do not match the CVR zip files (no Cvr Id in common).");
        s = EeLoadStatus_Error;
        goto cleanup;
    }

    /* ---- Build the ordered column layout ---- */
    if (nzip > 0 && npdf > 0)
    {
        /* PDF fields come from the metadata index; map META_* bits to HK_* bits. */
        static const int k_MetaKey[META_COUNT] = {HK_VTYPE, HK_PPLACE, HK_DTYPE, HK_DSERIAL,
                                                  HK_DDATA};
        uint32_t k;
        p1.present = 0;
        for (k = 0; k < META_COUNT; k++)
        {
            if (meta.present & (1u << k))
            {
                p1.present |= 1u << k_MetaKey[k];
            }
        }
    }
    frozen = hart_key_layout(keys, nzip > 0, npdf > 0, p1.global_has_party != 0, p1.present);
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
    for (i = 0; i < frozen; i++)
    {
        header[i] = k_HartKeyNames[keys[i]];
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
    p2.keys = keys;
    p2.nkeys = frozen;
    p2.meta = (nzip > 0 && npdf > 0) ? &meta : NULL;
    p2.cells = (const char **)malloc((size_t)ncols * sizeof(char *));
    if (p2.cells == NULL)
    {
        s = EeLoadStatus_Error;
        goto cleanup;
    }
    pg.rows = &out->nrows;
    if (nzip > 0)
    {
        for (f = 0; f < nzip && s == EeLoadStatus_Ok; f++)
        {
            s = hart_iterate_zip(zips[f], &buf, &buf_cap, pass2_entry, &p2, &pg, error_message,
                                 error_cch);
        }
    }
    else
    {
        for (f = 0; f < npdf && s == EeLoadStatus_Ok; f++)
        {
            s = hart_iterate_pdf(pdfs[f], &sheet, pass2_sheet, &p2, TRUE, &pg, error_message,
                                 error_cch);
        }
    }

    if (s != EeLoadStatus_Ok)
    {
        goto cleanup;
    }
    if (out->nrows == 0)
    {
        hart_set_err(error_message, error_cch, L"No Cast Vote Records were found.");
        s = EeLoadStatus_Error;
    }

cleanup:
    if (s != EeLoadStatus_Ok)
    {
        EeCvr_Clear(out);
    }
    free((void *)p2.cells);
    free(order);
    free(header);
    free(buf);
    free((void *)zips);
    free((void *)pdfs);
    sheet_free(&sheet);
    dict_free(&dict);
    meta_free(&meta);
    return s;
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
    return EeCvr_LoadFromHartFiles(paths, count, out, cancel_flag, progress_fn, progress_user,
                                   error_message, error_cch);
}
