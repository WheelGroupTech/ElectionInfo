/**
 * @file voter_table.c
 * @brief Voter roster storage, header mapping, CSV/TSV parse, sort.
 */

#include "voter_table.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strsafe.h>
#include <intsafe.h>

/* -------------------------------------------------------------------------- */
/* UTF-8 pool                                                                 */
/* -------------------------------------------------------------------------- */

static BOOL pool_reserve(EeVoterTable *table, size_t extra)
{
    size_t need;
    size_t new_cap;
    char *new_pool;

    if (table->pool_len > SIZE_MAX - extra)
    {
        return FALSE;
    }
    need = table->pool_len + extra;
    if (need <= table->pool_cap)
    {
        return TRUE;
    }

    new_cap = table->pool_cap ? table->pool_cap : (size_t)1 << 20; /* 1 MiB */
    while (new_cap < need)
    {
        if (new_cap > SIZE_MAX / 2)
        {
            new_cap = need;
            break;
        }
        new_cap *= 2;
    }

    new_pool = (char *)realloc(table->pool, new_cap);
    if (new_pool == NULL)
    {
        return FALSE;
    }
    table->pool = new_pool;
    table->pool_cap = new_cap;
    if (table->pool_len == 0)
    {
        /* Offset 0 is always the empty string. */
        table->pool[0] = '\0';
        table->pool_len = 1;
    }
    return TRUE;
}

static BOOL pool_add(EeVoterTable *table, const char *text, size_t len, uint32_t *out_ofs)
{
    size_t need;

    if (text == NULL || len == 0)
    {
        *out_ofs = 0;
        return TRUE;
    }

    if (table->pool_len == 0)
    {
        if (!pool_reserve(table, 1))
        {
            return FALSE;
        }
    }

    if (len > UINT32_MAX - table->pool_len - 1)
    {
        return FALSE;
    }

    need = len + 1;
    if (!pool_reserve(table, need))
    {
        return FALSE;
    }

    *out_ofs = (uint32_t)table->pool_len;
    memcpy(table->pool + table->pool_len, text, len);
    table->pool_len += len;
    table->pool[table->pool_len++] = '\0';
    return TRUE;
}

/* -------------------------------------------------------------------------- */
/* Rows                                                                       */
/* -------------------------------------------------------------------------- */

static BOOL ensure_row_capacity(EeVoterTable *table, uint32_t min_rows)
{
    uint32_t new_cap;
    uint32_t *new_cells;
    uint32_t *new_index;
    size_t bytes;

    if (min_rows <= table->row_cap)
    {
        return TRUE;
    }

    new_cap = table->row_cap ? table->row_cap : 4096u;
    while (new_cap < min_rows)
    {
        if (new_cap > UINT32_MAX / 2u)
        {
            new_cap = min_rows;
            break;
        }
        new_cap *= 2u;
    }

    {
        ULONGLONG cell_count;
        ULONGLONG cell_bytes;
        if (FAILED(ULongLongMult(new_cap, table->column_count, &cell_count)))
        {
            return FALSE;
        }
        if (FAILED(ULongLongMult(cell_count, sizeof(uint32_t), &cell_bytes)))
        {
            return FALSE;
        }
        if (cell_bytes > SIZE_MAX)
        {
            return FALSE;
        }
        bytes = (size_t)cell_bytes;
    }

    new_cells = (uint32_t *)realloc(table->cells, bytes);
    if (new_cells == NULL)
    {
        return FALSE;
    }
    table->cells = new_cells;

    new_index = (uint32_t *)realloc(table->view_index, (size_t)new_cap * sizeof(uint32_t));
    if (new_index == NULL)
    {
        return FALSE;
    }
    table->view_index = new_index;
    table->row_cap = new_cap;
    return TRUE;
}

/* -------------------------------------------------------------------------- */
/* Public lifecycle                                                           */
/* -------------------------------------------------------------------------- */

void EeVoterTable_Init(EeVoterTable *table)
{
    uint32_t i;

    if (table == NULL)
    {
        return;
    }
    ZeroMemory(table, sizeof(*table));
    table->sort_column = -1;
    table->sort_ascending = TRUE;
    table->delimiter = ',';
    for (i = 0; i < EE_MAX_COLUMNS; i++)
    {
        table->column_titles[i] = NULL;
    }
}

void EeVoterTable_Clear(EeVoterTable *table)
{
    uint32_t i;

    if (table == NULL)
    {
        return;
    }
    for (i = 0; i < EE_MAX_COLUMNS; i++)
    {
        free(table->column_titles[i]);
        table->column_titles[i] = NULL;
    }
    free(table->pool);
    free(table->cells);
    free(table->view_index);
    EeVoterTable_Init(table);
}

const char *EeVoterTable_GetCellUtf8(const EeVoterTable *table,
                                     uint32_t physical_row,
                                     uint32_t column)
{
    uint32_t ofs;
    size_t index;

    if (table == NULL || table->pool == NULL || physical_row >= table->row_count ||
        column >= table->column_count)
    {
        return "";
    }
    index = (size_t)physical_row * (size_t)table->column_count + (size_t)column;
    ofs = table->cells[index];
    if (ofs >= table->pool_len)
    {
        return "";
    }
    return table->pool + ofs;
}

const char *EeVoterTable_GetViewCellUtf8(const EeVoterTable *table,
                                         uint32_t view_row,
                                         uint32_t column)
{
    uint32_t physical;

    if (table == NULL || view_row >= table->row_count || table->view_index == NULL)
    {
        return "";
    }
    physical = table->view_index[view_row];
    return EeVoterTable_GetCellUtf8(table, physical, column);
}

void EeVoterTable_GetViewCellW(const EeVoterTable *table,
                               uint32_t view_row,
                               uint32_t column,
                               wchar_t *buffer,
                               size_t buffer_cch)
{
    const char *utf8;
    int n;

    if (buffer == NULL || buffer_cch == 0)
    {
        return;
    }
    buffer[0] = L'\0';
    utf8 = EeVoterTable_GetViewCellUtf8(table, view_row, column);
    if (utf8[0] == '\0')
    {
        return;
    }
    n = MultiByteToWideChar(CP_UTF8, 0, utf8, -1, buffer, (int)buffer_cch);
    if (n == 0)
    {
        /* Fallback for legacy single-byte roster encodings stored as-is. */
        MultiByteToWideChar(CP_ACP, 0, utf8, -1, buffer, (int)buffer_cch);
    }
}

/* -------------------------------------------------------------------------- */
/* Clipboard / copy text                                                      */
/* -------------------------------------------------------------------------- */

typedef struct Utf8Buf
{
    char *data;
    size_t len;
    size_t cap;
} Utf8Buf;

static BOOL utf8buf_reserve(Utf8Buf *buf, size_t extra)
{
    size_t need;
    size_t new_cap;
    char *p;

    if (FAILED(SizeTAdd(buf->len, extra, &need)))
    {
        return FALSE;
    }
    if (FAILED(SizeTAdd(need, 1, &need)))
    {
        return FALSE;
    }
    if (need <= buf->cap)
    {
        return TRUE;
    }

    new_cap = buf->cap ? buf->cap : 4096;
    while (new_cap < need)
    {
        if (new_cap > SIZE_MAX / 2)
        {
            new_cap = need;
            break;
        }
        new_cap *= 2;
    }

    p = (char *)realloc(buf->data, new_cap);
    if (p == NULL)
    {
        return FALSE;
    }
    buf->data = p;
    buf->cap = new_cap;
    return TRUE;
}

static BOOL utf8buf_append(Utf8Buf *buf, const char *s, size_t n)
{
    if (n == 0)
    {
        if (buf->data == NULL)
        {
            if (!utf8buf_reserve(buf, 0))
            {
                return FALSE;
            }
            buf->data[0] = '\0';
        }
        return TRUE;
    }
    if (!utf8buf_reserve(buf, n))
    {
        return FALSE;
    }
    memcpy(buf->data + buf->len, s, n);
    buf->len += n;
    buf->data[buf->len] = '\0';
    return TRUE;
}

static BOOL utf8buf_append_field(Utf8Buf *buf, const char *s, char delim)
{
    const char *p;
    BOOL quote = FALSE;

    if (s == NULL)
    {
        s = "";
    }
    for (p = s; *p != '\0'; p++)
    {
        if (*p == delim || *p == '"' || *p == '\n' || *p == '\r')
        {
            quote = TRUE;
            break;
        }
    }
    if (!quote)
    {
        return utf8buf_append(buf, s, strlen(s));
    }
    if (!utf8buf_append(buf, "\"", 1))
    {
        return FALSE;
    }
    for (p = s; *p != '\0'; p++)
    {
        if (*p == '"')
        {
            if (!utf8buf_append(buf, "\"\"", 2))
            {
                return FALSE;
            }
        }
        else if (!utf8buf_append(buf, p, 1))
        {
            return FALSE;
        }
    }
    return utf8buf_append(buf, "\"", 1);
}

BOOL EeVoterTable_FormatCopyUtf8(const EeVoterTable *table,
                                 const uint32_t *view_rows,
                                 uint32_t n_rows,
                                 BOOL prepend_normalized,
                                 char **out_text,
                                 size_t *out_len)
{
    Utf8Buf buf;
    uint32_t r;
    char delim;
    uint32_t start_col;

    ZeroMemory(&buf, sizeof(buf));
    if (out_text == NULL)
    {
        return FALSE;
    }
    *out_text = NULL;
    if (out_len != NULL)
    {
        *out_len = 0;
    }
    if (table == NULL || (n_rows > 0 && view_rows == NULL))
    {
        return FALSE;
    }

    delim = table->delimiter != '\0' ? table->delimiter : ',';
    start_col = prepend_normalized ? 0u : (uint32_t)EE_FROZEN_COLUMN_COUNT;

    for (r = 0; r < n_rows; r++)
    {
        uint32_t c;
        BOOL first = TRUE;
        for (c = start_col; c < table->column_count; c++)
        {
            const char *cell = EeVoterTable_GetViewCellUtf8(table, view_rows[r], c);
            if (!first)
            {
                char d[1];
                d[0] = delim;
                if (!utf8buf_append(&buf, d, 1))
                {
                    goto fail;
                }
            }
            first = FALSE;
            if (!utf8buf_append_field(&buf, cell, delim))
            {
                goto fail;
            }
        }
        if (!utf8buf_append(&buf, "\r\n", 2))
        {
            goto fail;
        }
    }

    if (buf.data == NULL)
    {
        buf.data = (char *)malloc(1);
        if (buf.data == NULL)
        {
            return FALSE;
        }
        buf.data[0] = '\0';
        buf.len = 0;
    }

    *out_text = buf.data;
    if (out_len != NULL)
    {
        *out_len = buf.len;
    }
    return TRUE;

fail:
    free(buf.data);
    return FALSE;
}

/* -------------------------------------------------------------------------- */
/* Header normalize / field roles                                             */
/* -------------------------------------------------------------------------- */

typedef enum FieldRole
{
    Role_None = 0,
    Role_Vuid,
    Role_OtherId,
    Role_FullName,
    Role_NamePrefix,
    Role_FirstName,
    Role_MiddleName,
    Role_LastName,
    Role_NameSuffix
} FieldRole;

static void normalize_header(const char *in, char *out, size_t out_cch)
{
    size_t o = 0;
    const unsigned char *p = (const unsigned char *)in;

    if (out_cch == 0)
    {
        return;
    }
    while (*p && o + 1 < out_cch)
    {
        unsigned char c = *p++;
        if (c >= 'a' && c <= 'z')
        {
            c = (unsigned char)(c - 'a' + 'A');
        }
        if ((c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9'))
        {
            out[o++] = (char)c;
        }
    }
    out[o] = '\0';
}

static FieldRole classify_field(const char *norm)
{
    if (norm[0] == '\0')
    {
        return Role_None;
    }

    /* Voter ID */
    if (strcmp(norm, "VUID") == 0 || strcmp(norm, "VUIDNO") == 0 ||
        strcmp(norm, "SOSVOTERID") == 0 || strcmp(norm, "BARCODEVUID") == 0 ||
        strcmp(norm, "SOSVOTERIDNUMBER") == 0)
    {
        return Role_Vuid;
    }

    /* Other / county IDs (not used as primary VUID unless no VUID). */
    if (strcmp(norm, "ID") == 0 || strcmp(norm, "IDNUMBER") == 0 ||
        strcmp(norm, "LEGACYVOTERID") == 0 || strcmp(norm, "COUNTYID") == 0)
    {
        return Role_OtherId;
    }

    /* Full name */
    if (strcmp(norm, "NAME") == 0 || strcmp(norm, "FULLNAME") == 0 ||
        strcmp(norm, "FULLNAM") == 0 || strcmp(norm, "CONTACTNAME") == 0 ||
        strcmp(norm, "VOTERNAME") == 0)
    {
        return Role_FullName;
    }

    if (strcmp(norm, "NAMPFX") == 0 || strcmp(norm, "NAMEPREFIX") == 0 ||
        strcmp(norm, "PREFIX") == 0)
    {
        return Role_NamePrefix;
    }
    if (strcmp(norm, "FSTNAM") == 0 || strcmp(norm, "FIRSTNAME") == 0 ||
        strcmp(norm, "FIRST") == 0 || strcmp(norm, "FNAME") == 0 || strcmp(norm, "GIVENNAME") == 0)
    {
        return Role_FirstName;
    }
    if (strcmp(norm, "MIDNAM") == 0 || strcmp(norm, "MIDDLENAME") == 0 ||
        strcmp(norm, "MIDDLE") == 0 || strcmp(norm, "MNAME") == 0)
    {
        return Role_MiddleName;
    }
    if (strcmp(norm, "LSTNAM") == 0 || strcmp(norm, "LASTNAME") == 0 || strcmp(norm, "LAST") == 0 ||
        strcmp(norm, "LNAME") == 0 || strcmp(norm, "SURNAME") == 0)
    {
        return Role_LastName;
    }
    if (strcmp(norm, "NAMESUFFIX") == 0 || strcmp(norm, "NAMESUFFIXCODE") == 0 ||
        strcmp(norm, "SUFFIX") == 0 || strcmp(norm, "NSUFFIX") == 0)
    {
        return Role_NameSuffix;
    }

    return Role_None;
}

static BOOL utf8_to_wide_dup(const char *utf8, wchar_t **out_w)
{
    int n;
    wchar_t *w;

    *out_w = NULL;
    if (utf8 == NULL)
    {
        return FALSE;
    }
    n = MultiByteToWideChar(CP_UTF8, 0, utf8, -1, NULL, 0);
    if (n <= 0)
    {
        n = MultiByteToWideChar(CP_ACP, 0, utf8, -1, NULL, 0);
        if (n <= 0)
        {
            return FALSE;
        }
        w = (wchar_t *)malloc((size_t)n * sizeof(wchar_t));
        if (w == NULL)
        {
            return FALSE;
        }
        MultiByteToWideChar(CP_ACP, 0, utf8, -1, w, n);
        *out_w = w;
        return TRUE;
    }
    w = (wchar_t *)malloc((size_t)n * sizeof(wchar_t));
    if (w == NULL)
    {
        return FALSE;
    }
    MultiByteToWideChar(CP_UTF8, 0, utf8, -1, w, n);
    *out_w = w;
    return TRUE;
}

/* -------------------------------------------------------------------------- */
/* Line / field parsing                                                       */
/* -------------------------------------------------------------------------- */

typedef struct FieldList
{
    char **items;
    size_t *lengths;
    size_t count;
    size_t cap;
} FieldList;

static void field_list_free(FieldList *fl)
{
    size_t i;
    if (fl == NULL)
    {
        return;
    }
    if (fl->items != NULL)
    {
        for (i = 0; i < fl->count; i++)
        {
            free(fl->items[i]);
        }
    }
    free(fl->items);
    free(fl->lengths);
    ZeroMemory(fl, sizeof(*fl));
}

static BOOL field_list_push(FieldList *fl, const char *s, size_t len)
{
    char *copy;
    size_t new_cap;
    char **new_items;
    size_t *new_lengths;

    if (fl->count == fl->cap)
    {
        new_cap = fl->cap ? fl->cap * 2 : 32;
        new_items = (char **)realloc(fl->items, new_cap * sizeof(char *));
        new_lengths = (size_t *)realloc(fl->lengths, new_cap * sizeof(size_t));
        if (new_items == NULL || new_lengths == NULL)
        {
            free(new_items);
            free(new_lengths);
            return FALSE;
        }
        fl->items = new_items;
        fl->lengths = new_lengths;
        fl->cap = new_cap;
    }

    copy = (char *)malloc(len + 1);
    if (copy == NULL)
    {
        return FALSE;
    }
    if (len > 0)
    {
        memcpy(copy, s, len);
    }
    copy[len] = '\0';
    fl->items[fl->count] = copy;
    fl->lengths[fl->count] = len;
    fl->count++;
    return TRUE;
}

static void trim_spaces(const char **start, size_t *len)
{
    const char *s = *start;
    size_t n = *len;
    while (n > 0 && (*s == ' ' || *s == '\t' || *s == '\r'))
    {
        s++;
        n--;
    }
    while (n > 0 && (s[n - 1] == ' ' || s[n - 1] == '\t' || s[n - 1] == '\r'))
    {
        n--;
    }
    *start = s;
    *len = n;
}

/**
 * Parse one delimited line into fields. Handles CSV quotes ("" escapes).
 * @p line must not contain the newline character.
 */
static BOOL parse_delimited_line(const char *line, size_t line_len, char delim, FieldList *out)
{
    size_t i = 0;
    field_list_free(out);

    while (i <= line_len)
    {
        const char *field_start;
        size_t field_len;
        char *built = NULL;
        size_t built_len = 0;
        size_t built_cap = 0;
        BOOL quoted = FALSE;

        if (i < line_len && line[i] == '"')
        {
            quoted = TRUE;
            i++;
            while (i < line_len)
            {
                if (line[i] == '"')
                {
                    if (i + 1 < line_len && line[i + 1] == '"')
                    {
                        if (built_len + 1 >= built_cap)
                        {
                            size_t nc = built_cap ? built_cap * 2 : 64;
                            char *nb = (char *)realloc(built, nc);
                            if (nb == NULL)
                            {
                                free(built);
                                field_list_free(out);
                                return FALSE;
                            }
                            built = nb;
                            built_cap = nc;
                        }
                        built[built_len++] = '"';
                        i += 2;
                    }
                    else
                    {
                        i++;
                        break;
                    }
                }
                else
                {
                    if (built_len + 1 >= built_cap)
                    {
                        size_t nc = built_cap ? built_cap * 2 : 64;
                        char *nb = (char *)realloc(built, nc);
                        if (nb == NULL)
                        {
                            free(built);
                            field_list_free(out);
                            return FALSE;
                        }
                        built = nb;
                        built_cap = nc;
                    }
                    built[built_len++] = line[i++];
                }
            }
            /* Skip trailing spaces before delimiter */
            while (i < line_len && (line[i] == ' ' || line[i] == '\t'))
            {
                i++;
            }
            if (i < line_len && line[i] == delim)
            {
                i++;
            }
            else if (i < line_len && (line[i] == '\r' || line[i] == '\n'))
            {
                i = line_len;
            }
            field_start = built ? built : "";
            field_len = built_len;
            if (!field_list_push(out, field_start, field_len))
            {
                free(built);
                field_list_free(out);
                return FALSE;
            }
            free(built);
            if (i >= line_len)
            {
                /* Trailing delimiter yields empty field once more if last was delim */
                break;
            }
            continue;
        }

        field_start = line + i;
        while (i < line_len && line[i] != delim && line[i] != '\r' && line[i] != '\n')
        {
            i++;
        }
        field_len = (size_t)((line + i) - field_start);
        trim_spaces(&field_start, &field_len);
        if (!field_list_push(out, field_start, field_len))
        {
            field_list_free(out);
            return FALSE;
        }
        if (i < line_len && line[i] == delim)
        {
            i++;
            if (i == line_len)
            {
                /* trailing delimiter */
                if (!field_list_push(out, "", 0))
                {
                    field_list_free(out);
                    return FALSE;
                }
            }
        }
        else
        {
            break;
        }
        (void)quoted;
    }
    return TRUE;
}

static char detect_delimiter(const wchar_t *path, const char *header_line)
{
    const wchar_t *ext = wcsrchr(path, L'.');
    size_t commas = 0;
    size_t tabs = 0;
    const char *p;

    if (ext != NULL)
    {
        if (_wcsicmp(ext, L".txt") == 0)
        {
            return '\t';
        }
        if (_wcsicmp(ext, L".csv") == 0)
        {
            return ',';
        }
    }

    for (p = header_line; *p; p++)
    {
        if (*p == ',')
        {
            commas++;
        }
        else if (*p == '\t')
        {
            tabs++;
        }
    }
    return (tabs > commas) ? '\t' : ',';
}

/* -------------------------------------------------------------------------- */
/* Name composition                                                           */
/* -------------------------------------------------------------------------- */

static BOOL append_name_part(char *buf, size_t cap, size_t *len, const char *part)
{
    size_t part_len;
    if (part == NULL || part[0] == '\0')
    {
        return TRUE;
    }
    part_len = strlen(part);
    if (*len > 0)
    {
        if (*len + 1 >= cap)
        {
            return FALSE;
        }
        buf[(*len)++] = ' ';
    }
    if (*len + part_len >= cap)
    {
        return FALSE;
    }
    memcpy(buf + *len, part, part_len);
    *len += part_len;
    buf[*len] = '\0';
    return TRUE;
}

static BOOL compose_name(const FieldList *fields,
                         const FieldRole *roles,
                         size_t role_count,
                         int full_idx,
                         int pre_idx,
                         int first_idx,
                         int mid_idx,
                         int last_idx,
                         int suf_idx,
                         char *out,
                         size_t out_cap)
{
    size_t len = 0;
    out[0] = '\0';

    if (full_idx >= 0 && (size_t)full_idx < fields->count && fields->items[full_idx][0] != '\0')
    {
        if (FAILED(StringCchCopyA(out, out_cap, fields->items[full_idx])))
        {
            return FALSE;
        }
        return TRUE;
    }

    (void)roles;
    (void)role_count;

    if (pre_idx >= 0 && (size_t)pre_idx < fields->count)
    {
        append_name_part(out, out_cap, &len, fields->items[pre_idx]);
    }
    if (first_idx >= 0 && (size_t)first_idx < fields->count)
    {
        append_name_part(out, out_cap, &len, fields->items[first_idx]);
    }
    if (mid_idx >= 0 && (size_t)mid_idx < fields->count)
    {
        append_name_part(out, out_cap, &len, fields->items[mid_idx]);
    }
    if (last_idx >= 0 && (size_t)last_idx < fields->count)
    {
        append_name_part(out, out_cap, &len, fields->items[last_idx]);
    }
    if (suf_idx >= 0 && (size_t)suf_idx < fields->count)
    {
        append_name_part(out, out_cap, &len, fields->items[suf_idx]);
    }
    return TRUE;
}

/* -------------------------------------------------------------------------- */
/* Sort                                                                       */
/* -------------------------------------------------------------------------- */

typedef struct SortCtx
{
    EeVoterTable *table;
    uint32_t column;
    BOOL ascending;
} SortCtx;

static int sort_cmp(void *context, const void *a, const void *b)
{
    const SortCtx *ctx = (const SortCtx *)context;
    uint32_t ra = *(const uint32_t *)a;
    uint32_t rb = *(const uint32_t *)b;
    const char *sa = EeVoterTable_GetCellUtf8(ctx->table, ra, ctx->column);
    const char *sb = EeVoterTable_GetCellUtf8(ctx->table, rb, ctx->column);
    int cmp = _stricmp(sa, sb);
    if (cmp == 0)
    {
        if (ra < rb)
        {
            return -1;
        }
        if (ra > rb)
        {
            return 1;
        }
        return 0;
    }
    return ctx->ascending ? cmp : -cmp;
}

BOOL EeVoterTable_SortByColumn(EeVoterTable *table, uint32_t column)
{
    SortCtx ctx;
    uint32_t i;

    if (table == NULL || column >= table->column_count || table->row_count == 0)
    {
        return FALSE;
    }

    if (table->sort_column == (int)column)
    {
        table->sort_ascending = !table->sort_ascending;
    }
    else
    {
        table->sort_column = (int)column;
        table->sort_ascending = TRUE;
    }

    for (i = 0; i < table->row_count; i++)
    {
        table->view_index[i] = i;
    }

    ctx.table = table;
    ctx.column = column;
    ctx.ascending = table->sort_ascending;
    qsort_s(table->view_index, table->row_count, sizeof(uint32_t), sort_cmp, &ctx);
    return TRUE;
}

/* -------------------------------------------------------------------------- */
/* Load                                                                       */
/* -------------------------------------------------------------------------- */

static void set_error(wchar_t *error_message, size_t error_cch, const wchar_t *msg)
{
    if (error_message != NULL && error_cch > 0)
    {
        StringCchCopyW(error_message, error_cch, msg);
    }
}

static BOOL report_progress(EeLoadProgressFn fn,
                            void *user,
                            uint32_t percent,
                            uint32_t rows,
                            uint64_t read,
                            uint64_t total)
{
    EeLoadProgress p;
    if (fn == NULL)
    {
        return TRUE;
    }
    p.percent = percent > 100u ? 100u : percent;
    p.rows_loaded = rows;
    p.bytes_read = read;
    p.bytes_total = total;
    return fn(&p, user);
}

static BOOL cancelled(volatile LONG *flag)
{
    return flag != NULL && InterlockedCompareExchange(flag, 0, 0) != 0;
}

EeLoadStatus EeVoterTable_LoadFromFile(const wchar_t *path,
                                       EeVoterTable *out_table,
                                       volatile LONG *cancel_flag,
                                       EeLoadProgressFn progress_fn,
                                       void *progress_user,
                                       wchar_t *error_message,
                                       size_t error_cch)
{
    FILE *fp = NULL;
    char *read_buf = NULL;
    char *line = NULL;
    size_t line_len = 0;
    size_t line_cap = 0;
    size_t read_n;
    BOOL in_crlf = FALSE;
    FieldList header_fields;
    FieldList row_fields;
    FieldRole roles[EE_MAX_COLUMNS];
    char delim = ',';
    uint64_t file_size = 0;
    uint64_t bytes_read = 0;
    uint32_t last_percent = 0;
    int vuid_idx = -1;
    int other_id_idx = -1;
    int full_idx = -1;
    int pre_idx = -1;
    int first_idx = -1;
    int mid_idx = -1;
    int last_idx = -1;
    int suf_idx = -1;
    uint32_t src_col_count = 0;
    uint32_t display_cols = 0;
    size_t i;
    errno_t err;
    LARGE_INTEGER li;
    HANDLE hfile;

    ZeroMemory(&header_fields, sizeof(header_fields));
    ZeroMemory(&row_fields, sizeof(row_fields));
    ZeroMemory(roles, sizeof(roles));

    if (path == NULL || out_table == NULL)
    {
        set_error(error_message, error_cch, L"Invalid load arguments.");
        return EeLoadStatus_Error;
    }

    EeVoterTable_Clear(out_table);

    hfile = CreateFileW(path,
                        GENERIC_READ,
                        FILE_SHARE_READ,
                        NULL,
                        OPEN_EXISTING,
                        FILE_ATTRIBUTE_NORMAL,
                        NULL);
    if (hfile != INVALID_HANDLE_VALUE)
    {
        if (GetFileSizeEx(hfile, &li))
        {
            file_size = (uint64_t)li.QuadPart;
        }
        CloseHandle(hfile);
    }

    err = _wfopen_s(&fp, path, L"rb");
    if (err != 0 || fp == NULL)
    {
        set_error(error_message, error_cch, L"Could not open the voter list file.");
        return EeLoadStatus_Error;
    }

    read_buf = (char *)malloc(1024 * 1024);
    if (read_buf == NULL)
    {
        fclose(fp);
        set_error(error_message, error_cch, L"Out of memory.");
        return EeLoadStatus_Error;
    }

    /* Read file assembling lines. First non-empty line is header. */
    {
        BOOL got_header = FALSE;
        BOOL done = FALSE;

        while (!done)
        {
            if (cancelled(cancel_flag))
            {
                free(read_buf);
                free(line);
                field_list_free(&header_fields);
                field_list_free(&row_fields);
                fclose(fp);
                EeVoterTable_Clear(out_table);
                set_error(error_message, error_cch, L"Load cancelled.");
                return EeLoadStatus_Cancelled;
            }

            read_n = fread(read_buf, 1, 1024 * 1024, fp);
            if (read_n == 0)
            {
                if (ferror(fp))
                {
                    free(read_buf);
                    free(line);
                    field_list_free(&header_fields);
                    field_list_free(&row_fields);
                    fclose(fp);
                    EeVoterTable_Clear(out_table);
                    set_error(error_message, error_cch, L"Error reading file.");
                    return EeLoadStatus_Error;
                }
                /* EOF: flush last line if any */
                if (line_len > 0 || got_header)
                {
                    /* process remaining as final line below via sentinel */
                }
                done = TRUE;
                if (line_len == 0 && got_header)
                {
                    break;
                }
                if (line_len == 0 && !got_header)
                {
                    free(read_buf);
                    free(line);
                    fclose(fp);
                    set_error(error_message, error_cch, L"The file is empty.");
                    return EeLoadStatus_Error;
                }
                /* Fall through to process final line once */
                read_n = 0;
            }
            else
            {
                bytes_read += read_n;
            }

            size_t pos = 0;
            while (pos < read_n || (done && line_len > 0))
            {
                BOOL line_complete = FALSE;

                if (!done)
                {
                    while (pos < read_n)
                    {
                        char c = read_buf[pos++];
                        if (c == '\n')
                        {
                            if (in_crlf)
                            {
                                in_crlf = FALSE;
                            }
                            line_complete = TRUE;
                            break;
                        }
                        if (c == '\r')
                        {
                            in_crlf = TRUE;
                            line_complete = TRUE;
                            break;
                        }
                        in_crlf = FALSE;
                        if (line_len + 1 >= line_cap)
                        {
                            size_t nc = line_cap ? line_cap * 2 : 4096;
                            char *nl = (char *)realloc(line, nc);
                            if (nl == NULL)
                            {
                                free(read_buf);
                                free(line);
                                field_list_free(&header_fields);
                                field_list_free(&row_fields);
                                fclose(fp);
                                EeVoterTable_Clear(out_table);
                                set_error(error_message, error_cch, L"Out of memory.");
                                return EeLoadStatus_Error;
                            }
                            line = nl;
                            line_cap = nc;
                        }
                        line[line_len++] = c;
                    }
                }
                else
                {
                    line_complete = TRUE;
                }

                if (!line_complete)
                {
                    break;
                }

                /* Skip UTF-8 BOM on first line */
                {
                    size_t start = 0;
                    if (!got_header && line_len >= 3 && (unsigned char)line[0] == 0xEF &&
                        (unsigned char)line[1] == 0xBB && (unsigned char)line[2] == 0xBF)
                    {
                        start = 3;
                    }

                    if (line_len - start == 0)
                    {
                        line_len = 0;
                        if (done)
                        {
                            break;
                        }
                        continue;
                    }

                    line[line_len] = '\0';

                    if (!got_header)
                    {
                        delim = detect_delimiter(path, line + start);
                        if (!parse_delimited_line(line + start,
                                                  line_len - start,
                                                  delim,
                                                  &header_fields))
                        {
                            free(read_buf);
                            free(line);
                            field_list_free(&header_fields);
                            fclose(fp);
                            set_error(error_message, error_cch, L"Failed to parse header row.");
                            return EeLoadStatus_Error;
                        }
                        if (header_fields.count == 0 ||
                            header_fields.count > EE_MAX_COLUMNS - EE_FROZEN_COLUMN_COUNT)
                        {
                            free(read_buf);
                            free(line);
                            field_list_free(&header_fields);
                            fclose(fp);
                            set_error(error_message,
                                      error_cch,
                                      L"Unsupported column count in voter list.");
                            return EeLoadStatus_Error;
                        }

                        src_col_count = (uint32_t)header_fields.count;
                        out_table->delimiter = delim;
                        for (i = 0; i < header_fields.count; i++)
                        {
                            char norm[128];
                            normalize_header(header_fields.items[i], norm, sizeof(norm));
                            roles[i] = classify_field(norm);
                            switch (roles[i])
                            {
                                case Role_Vuid:
                                    if (vuid_idx < 0)
                                    {
                                        vuid_idx = (int)i;
                                    }
                                    break;
                                case Role_OtherId:
                                    if (other_id_idx < 0)
                                    {
                                        other_id_idx = (int)i;
                                    }
                                    break;
                                case Role_FullName:
                                    if (full_idx < 0)
                                    {
                                        full_idx = (int)i;
                                    }
                                    break;
                                case Role_NamePrefix:
                                    if (pre_idx < 0)
                                    {
                                        pre_idx = (int)i;
                                    }
                                    break;
                                case Role_FirstName:
                                    if (first_idx < 0)
                                    {
                                        first_idx = (int)i;
                                    }
                                    break;
                                case Role_MiddleName:
                                    if (mid_idx < 0)
                                    {
                                        mid_idx = (int)i;
                                    }
                                    break;
                                case Role_LastName:
                                    if (last_idx < 0)
                                    {
                                        last_idx = (int)i;
                                    }
                                    break;
                                case Role_NameSuffix:
                                    if (suf_idx < 0)
                                    {
                                        suf_idx = (int)i;
                                    }
                                    break;
                                default:
                                    break;
                            }
                        }
                        if (vuid_idx < 0)
                        {
                            vuid_idx = other_id_idx;
                        }

                        /* Display columns: Voter ID, Name, then all source columns. */
                        display_cols = EE_FROZEN_COLUMN_COUNT + src_col_count;
                        out_table->column_count = display_cols;
                        if (!utf8_to_wide_dup("Voter ID", &out_table->column_titles[0]) ||
                            !utf8_to_wide_dup("Name", &out_table->column_titles[1]))
                        {
                            free(read_buf);
                            free(line);
                            field_list_free(&header_fields);
                            fclose(fp);
                            EeVoterTable_Clear(out_table);
                            set_error(error_message, error_cch, L"Out of memory.");
                            return EeLoadStatus_Error;
                        }
                        for (i = 0; i < header_fields.count; i++)
                        {
                            if (!utf8_to_wide_dup(
                                    header_fields.items[i],
                                    &out_table->column_titles[EE_FROZEN_COLUMN_COUNT + i]))
                            {
                                free(read_buf);
                                free(line);
                                field_list_free(&header_fields);
                                fclose(fp);
                                EeVoterTable_Clear(out_table);
                                set_error(error_message, error_cch, L"Out of memory.");
                                return EeLoadStatus_Error;
                            }
                        }

                        if (!pool_reserve(out_table, 1))
                        {
                            free(read_buf);
                            free(line);
                            field_list_free(&header_fields);
                            fclose(fp);
                            EeVoterTable_Clear(out_table);
                            set_error(error_message, error_cch, L"Out of memory.");
                            return EeLoadStatus_Error;
                        }

                        got_header = TRUE;
                        line_len = 0;
                        if (done)
                        {
                            break;
                        }
                        continue;
                    }

                    /* Data row */
                    if (!parse_delimited_line(line + start, line_len - start, delim, &row_fields))
                    {
                        free(read_buf);
                        free(line);
                        field_list_free(&header_fields);
                        field_list_free(&row_fields);
                        fclose(fp);
                        EeVoterTable_Clear(out_table);
                        set_error(error_message, error_cch, L"Failed to parse a data row.");
                        return EeLoadStatus_Error;
                    }

                    if (!ensure_row_capacity(out_table, out_table->row_count + 1))
                    {
                        free(read_buf);
                        free(line);
                        field_list_free(&header_fields);
                        field_list_free(&row_fields);
                        fclose(fp);
                        EeVoterTable_Clear(out_table);
                        set_error(error_message, error_cch, L"Out of memory loading rows.");
                        return EeLoadStatus_Error;
                    }

                    {
                        uint32_t row = out_table->row_count;
                        uint32_t *cell =
                            out_table->cells + (size_t)row * (size_t)out_table->column_count;
                        char name_buf[512];
                        const char *vuid_text = "";
                        uint32_t c;

                        if (vuid_idx >= 0 && (size_t)vuid_idx < row_fields.count)
                        {
                            vuid_text = row_fields.items[vuid_idx];
                        }
                        if (!pool_add(out_table, vuid_text, strlen(vuid_text), &cell[0]))
                        {
                            free(read_buf);
                            free(line);
                            field_list_free(&header_fields);
                            field_list_free(&row_fields);
                            fclose(fp);
                            EeVoterTable_Clear(out_table);
                            set_error(error_message, error_cch, L"Out of memory.");
                            return EeLoadStatus_Error;
                        }

                        if (!compose_name(&row_fields,
                                          roles,
                                          header_fields.count,
                                          full_idx,
                                          pre_idx,
                                          first_idx,
                                          mid_idx,
                                          last_idx,
                                          suf_idx,
                                          name_buf,
                                          sizeof(name_buf)))
                        {
                            name_buf[0] = '\0';
                        }
                        if (!pool_add(out_table, name_buf, strlen(name_buf), &cell[1]))
                        {
                            free(read_buf);
                            free(line);
                            field_list_free(&header_fields);
                            field_list_free(&row_fields);
                            fclose(fp);
                            EeVoterTable_Clear(out_table);
                            set_error(error_message, error_cch, L"Out of memory.");
                            return EeLoadStatus_Error;
                        }

                        for (c = 0; c < src_col_count; c++)
                        {
                            const char *t = "";
                            size_t tlen = 0;
                            if (c < row_fields.count)
                            {
                                t = row_fields.items[c];
                                tlen = row_fields.lengths[c];
                            }
                            if (!pool_add(out_table, t, tlen, &cell[EE_FROZEN_COLUMN_COUNT + c]))
                            {
                                free(read_buf);
                                free(line);
                                field_list_free(&header_fields);
                                field_list_free(&row_fields);
                                fclose(fp);
                                EeVoterTable_Clear(out_table);
                                set_error(error_message, error_cch, L"Out of memory.");
                                return EeLoadStatus_Error;
                            }
                        }

                        out_table->view_index[row] = row;
                        out_table->row_count++;
                    }

                    line_len = 0;

                    if (file_size > 0)
                    {
                        uint32_t pct = (uint32_t)((bytes_read * 99ull) /
                                                  file_size); /* leave 100 for UI ready */
                        if (pct > 99u)
                        {
                            pct = 99u;
                        }
                        if (pct != last_percent || (out_table->row_count % 5000u) == 0u)
                        {
                            last_percent = pct;
                            if (!report_progress(progress_fn,
                                                 progress_user,
                                                 pct,
                                                 out_table->row_count,
                                                 bytes_read,
                                                 file_size))
                            {
                                if (cancel_flag != NULL)
                                {
                                    InterlockedExchange(cancel_flag, 1);
                                }
                            }
                        }
                    }
                    else if ((out_table->row_count % 2000u) == 0u)
                    {
                        if (!report_progress(progress_fn,
                                             progress_user,
                                             50,
                                             out_table->row_count,
                                             bytes_read,
                                             file_size))
                        {
                            if (cancel_flag != NULL)
                            {
                                InterlockedExchange(cancel_flag, 1);
                            }
                        }
                    }

                    if (cancelled(cancel_flag))
                    {
                        free(read_buf);
                        free(line);
                        field_list_free(&header_fields);
                        field_list_free(&row_fields);
                        fclose(fp);
                        EeVoterTable_Clear(out_table);
                        set_error(error_message, error_cch, L"Load cancelled.");
                        return EeLoadStatus_Cancelled;
                    }
                }

                if (done)
                {
                    break;
                }
            }

            if (done)
            {
                break;
            }
        }
    }

    free(read_buf);
    free(line);
    field_list_free(&header_fields);
    field_list_free(&row_fields);
    fclose(fp);

    if (out_table->column_count == 0)
    {
        EeVoterTable_Clear(out_table);
        set_error(error_message, error_cch, L"No header row found.");
        return EeLoadStatus_Error;
    }

    /* Progress 99%: data parsed; UI will push 100% when grid is ready. */
    report_progress(progress_fn, progress_user, 99, out_table->row_count, bytes_read, file_size);

    if (cancelled(cancel_flag))
    {
        EeVoterTable_Clear(out_table);
        set_error(error_message, error_cch, L"Load cancelled.");
        return EeLoadStatus_Cancelled;
    }

    return EeLoadStatus_Ok;
}
