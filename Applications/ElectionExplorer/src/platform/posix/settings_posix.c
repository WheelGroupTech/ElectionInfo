/**
 * @file settings_posix.c
 * @brief macOS / Linux persistence of user options. See settings.h; src/settings.c is the
 *        Windows (registry) version and this file keeps its value names.
 *
 * A registry subkey maps to a small UTF-8 text file of "Name=value" lines: the leading
 * "Software\" is dropped, the rest becomes a path under the user's configuration folder
 * ($XDG_CONFIG_HOME or ~/.config on Linux, ~/Library/Application Support on macOS), and
 * ".ini" is added. EE_SETTINGS_SUBKEY is therefore ~/.config/WheelGroupTech/ElectionExplorer.ini
 * on Linux.
 */

#include "settings.h"

#include <errno.h>
#include <sys/stat.h>

static const char k_ValZoom[] = "ZoomPercent";
static const char k_ValMapEngine[] = "MapEngine";
static const char k_ValCopyPrepend[] = "CopyPrependNormalized";
static const char k_ValNameSurnameFirst[] = "NameSurnameFirst";
static const char k_ValCvrMergeWriteins[] = "CvrMergeWriteins";
static const char k_ValCvrTabPartyFirst[] = "CvrTabPartyFirst";
static const char k_ValResultsShowVoteTypes[] = "ResultsShowVoteTypes";
static const char k_ValAuditTestWords[] = "AuditTestTaskWords";
static const char k_ValWinListX[] = "WindowListX";
static const char k_ValWinListY[] = "WindowListY";
static const char k_ValWinListW[] = "WindowListWidth";
static const char k_ValWinListH[] = "WindowListHeight";
_Static_assert(EE_WINLIST_NCOLS <= 10, "a column-width value name holds one digit");

/* Most lines a settings file may hold; anything after them is ignored. */
#define SET_MAX_LINES 64

typedef struct SetFile
{
    char *name[SET_MAX_LINES];
    char *value[SET_MAX_LINES];
    int n;
} SetFile;

void EeSettings_Defaults(EeSettings *out)
{
    int i;

    if (out == NULL)
    {
        return;
    }
    out->zoom_percent = 100;
    out->map_engine = 0; /* EeMap_Google */
    out->copy_prepend_normalized = TRUE;
    out->name_surname_first = TRUE;
    out->cvr_merge_writeins = TRUE;
    out->cvr_tab_party_first = EE_TAB_PARTY_REP;
    out->results_show_vote_types = TRUE;
    out->audit_test_words[0] = L'\0';
    out->winlist_x = 0;
    out->winlist_y = 0;
    out->winlist_w = 0;
    out->winlist_h = 0;
    for (i = 0; i < EE_WINLIST_NCOLS; i++)
    {
        out->winlist_cols[i] = 0;
    }
}

/* The configuration folder, or NULL. */
static const char *config_root(char *buf, size_t cb)
{
    const char *home = getenv("HOME");
#if defined(__APPLE__)
    if (home == NULL || home[0] == '\0')
    {
        return NULL;
    }
    snprintf(buf, cb, "%s/Library/Application Support", home);
#else
    const char *xdg = getenv("XDG_CONFIG_HOME");
    if (xdg != NULL && xdg[0] == '/')
    {
        snprintf(buf, cb, "%s", xdg);
    }
    else if (home != NULL && home[0] != '\0')
    {
        snprintf(buf, cb, "%s/.config", home);
    }
    else
    {
        return NULL;
    }
#endif
    return buf;
}

/* File path for @p subkey; with @p make_dirs, creates the folders on the way. */
static BOOL settings_path(const wchar_t *subkey, BOOL make_dirs, char *out, size_t cb)
{
    char root[1024];
    char *rel;
    size_t i, n;
    int len;

    if (subkey == NULL || config_root(root, sizeof(root)) == NULL)
    {
        return FALSE;
    }
    if (_wcsnicmp(subkey, L"Software\\", 9) == 0)
    {
        subkey += 9;
    }
    len = WideCharToMultiByte(CP_UTF8, 0, subkey, -1, NULL, 0, NULL, NULL);
    if (len <= 1 || (rel = (char *)malloc((size_t)len)) == NULL)
    {
        return FALSE;
    }
    WideCharToMultiByte(CP_UTF8, 0, subkey, -1, rel, len, NULL, NULL);
    for (i = 0; rel[i] != '\0'; i++)
    {
        if (rel[i] == '\\')
        {
            rel[i] = '/';
        }
        else if (rel[i] == '/' || (rel[i] == '.' && (i == 0 || rel[i - 1] == '/')))
        {
            rel[i] = '_'; /* a key name never climbs out of the folder */
        }
    }
    n = (size_t)snprintf(out, cb, "%s/%s.ini", root, rel);
    free(rel);
    if (n >= cb)
    {
        return FALSE;
    }
    if (make_dirs)
    {
        for (i = strlen(root) + 1; out[i] != '\0'; i++)
        {
            if (out[i] == '/')
            {
                out[i] = '\0';
                if (mkdir(out, 0700) != 0 && errno != EEXIST)
                {
                    out[i] = '/';
                    return FALSE;
                }
                out[i] = '/';
            }
        }
    }
    return TRUE;
}

static void set_free(SetFile *f)
{
    int i;
    for (i = 0; i < f->n; i++)
    {
        free(f->name[i]);
        free(f->value[i]);
    }
    f->n = 0;
}

/* Read the file for @p subkey; FALSE if it does not exist. */
static BOOL set_read(const wchar_t *subkey, SetFile *f)
{
    char path[2048];
    char line[1024];
    FILE *fp;

    f->n = 0;
    if (!settings_path(subkey, FALSE, path, sizeof(path)) || (fp = fopen(path, "r")) == NULL)
    {
        return FALSE;
    }
    while (f->n < SET_MAX_LINES && fgets(line, sizeof(line), fp) != NULL)
    {
        char *eq = strchr(line, '=');
        size_t n = strlen(line);
        while (n > 0 && (line[n - 1] == '\n' || line[n - 1] == '\r'))
        {
            line[--n] = '\0';
        }
        if (eq == NULL || eq == line)
        {
            continue;
        }
        *eq = '\0';
        f->name[f->n] = strdup(line);
        f->value[f->n] = strdup(eq + 1);
        if (f->name[f->n] == NULL || f->value[f->n] == NULL)
        {
            free(f->name[f->n]);
            free(f->value[f->n]);
            break;
        }
        f->n++;
    }
    fclose(fp);
    return TRUE;
}

static const char *set_get(const SetFile *f, const char *name)
{
    int i;
    for (i = 0; i < f->n; i++)
    {
        if (strcmp(f->name[i], name) == 0)
        {
            return f->value[i];
        }
    }
    return NULL;
}

/* Read one integer into *value; leave it unchanged if absent or not a number. */
static void read_int(const SetFile *f, const char *name, int *value)
{
    const char *v = set_get(f, name);
    char *end;
    long n;
    if (v == NULL || v[0] == '\0')
    {
        return;
    }
    errno = 0;
    n = strtol(v, &end, 10);
    if (errno == 0 && *end == '\0' && n >= INT_MIN && n <= INT_MAX)
    {
        *value = (int)n;
    }
}

static void read_bool(const SetFile *f, const char *name, BOOL *value)
{
    int v = *value ? 1 : 0;
    read_int(f, name, &v);
    *value = (v != 0);
}

BOOL EeSettings_LoadFrom(const wchar_t *subkey, EeSettings *out)
{
    SetFile f;
    const char *words;
    int i;

    if (subkey == NULL || out == NULL)
    {
        return FALSE;
    }
    EeSettings_Defaults(out);
    if (!set_read(subkey, &f))
    {
        return FALSE;
    }
    read_int(&f, k_ValZoom, &out->zoom_percent);
    read_int(&f, k_ValMapEngine, &out->map_engine);
    read_bool(&f, k_ValCopyPrepend, &out->copy_prepend_normalized);
    read_bool(&f, k_ValNameSurnameFirst, &out->name_surname_first);
    read_bool(&f, k_ValCvrMergeWriteins, &out->cvr_merge_writeins);
    read_int(&f, k_ValCvrTabPartyFirst, &out->cvr_tab_party_first);
    if (out->cvr_tab_party_first != EE_TAB_PARTY_DEM)
    {
        out->cvr_tab_party_first = EE_TAB_PARTY_REP;
    }
    read_bool(&f, k_ValResultsShowVoteTypes, &out->results_show_vote_types);
    words = set_get(&f, k_ValAuditTestWords);
    if (words != NULL)
    {
        wchar_t tmp[ARRAYSIZE(out->audit_test_words)];
        int n = MultiByteToWideChar(CP_UTF8, 0, words, -1, tmp, (int)ARRAYSIZE(tmp));
        if (n > 0)
        {
            memcpy(out->audit_test_words, tmp, (size_t)n * sizeof(wchar_t));
        }
    }
    read_int(&f, k_ValWinListX, &out->winlist_x);
    read_int(&f, k_ValWinListY, &out->winlist_y);
    read_int(&f, k_ValWinListW, &out->winlist_w);
    read_int(&f, k_ValWinListH, &out->winlist_h);
    for (i = 0; i < EE_WINLIST_NCOLS; i++)
    {
        char name[32];
        snprintf(name, sizeof(name), "WindowListCol%d", i);
        read_int(&f, name, &out->winlist_cols[i]);
        if (out->winlist_cols[i] < 0)
        {
            out->winlist_cols[i] = 0;
        }
    }
    set_free(&f);
    return TRUE;
}

BOOL EeSettings_SaveTo(const wchar_t *subkey, const EeSettings *in)
{
    char path[2048];
    char tmp_path[2100];
    char words[512];
    FILE *fp;
    int i;
    BOOL ok = TRUE;

    if (subkey == NULL || in == NULL || !settings_path(subkey, TRUE, path, sizeof(path)))
    {
        return FALSE;
    }
    if (WideCharToMultiByte(CP_UTF8,
                            0,
                            in->audit_test_words,
                            -1,
                            words,
                            (int)sizeof(words),
                            NULL,
                            NULL) <= 0)
    {
        words[0] = '\0';
    }
    for (i = 0; words[i] != '\0'; i++)
    {
        if (words[i] == '\r' || words[i] == '\n')
        {
            words[i] = ' '; /* one value per line */
        }
    }
    /* Write a temporary file and rename it over the old one, so a crash never leaves half a file. */
    snprintf(tmp_path, sizeof(tmp_path), "%s.tmp", path);
    fp = fopen(tmp_path, "w");
    if (fp == NULL)
    {
        return FALSE;
    }
    ok &= fprintf(fp, "%s=%d\n", k_ValZoom, in->zoom_percent) > 0;
    ok &= fprintf(fp, "%s=%d\n", k_ValMapEngine, in->map_engine) > 0;
    ok &= fprintf(fp, "%s=%d\n", k_ValCopyPrepend, in->copy_prepend_normalized ? 1 : 0) > 0;
    ok &= fprintf(fp, "%s=%d\n", k_ValNameSurnameFirst, in->name_surname_first ? 1 : 0) > 0;
    ok &= fprintf(fp, "%s=%d\n", k_ValCvrMergeWriteins, in->cvr_merge_writeins ? 1 : 0) > 0;
    ok &= fprintf(fp, "%s=%d\n", k_ValCvrTabPartyFirst, in->cvr_tab_party_first) > 0;
    ok &=
        fprintf(fp, "%s=%d\n", k_ValResultsShowVoteTypes, in->results_show_vote_types ? 1 : 0) > 0;
    ok &= fprintf(fp, "%s=%s\n", k_ValAuditTestWords, words) > 0;
    ok &= fprintf(fp, "%s=%d\n", k_ValWinListX, in->winlist_x) > 0;
    ok &= fprintf(fp, "%s=%d\n", k_ValWinListY, in->winlist_y) > 0;
    ok &= fprintf(fp, "%s=%d\n", k_ValWinListW, in->winlist_w) > 0;
    ok &= fprintf(fp, "%s=%d\n", k_ValWinListH, in->winlist_h) > 0;
    for (i = 0; i < EE_WINLIST_NCOLS; i++)
    {
        ok &= fprintf(fp, "WindowListCol%d=%d\n", i, in->winlist_cols[i]) > 0;
    }
    ok &= fclose(fp) == 0;
    if (!ok || rename(tmp_path, path) != 0)
    {
        remove(tmp_path);
        return FALSE;
    }
    return TRUE;
}

BOOL EeSettings_Load(EeSettings *out)
{
    return EeSettings_LoadFrom(EE_SETTINGS_SUBKEY, out);
}

BOOL EeSettings_Save(const EeSettings *in)
{
    return EeSettings_SaveTo(EE_SETTINGS_SUBKEY, in);
}

LONG RegDeleteKeyW(HKEY root, const wchar_t *subkey)
{
    char path[2048];
    if (root != HKEY_CURRENT_USER || !settings_path(subkey, FALSE, path, sizeof(path)))
    {
        return ERROR_INVALID_PARAMETER;
    }
    if (remove(path) != 0)
    {
        return errno == ENOENT ? ERROR_FILE_NOT_FOUND : ERROR_ACCESS_DENIED;
    }
    return ERROR_SUCCESS;
}
