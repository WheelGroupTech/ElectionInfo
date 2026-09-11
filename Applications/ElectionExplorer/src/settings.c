/**
 * @file settings.c
 * @brief Registry-backed persistence of user options. See settings.h.
 *
 * All values are stored as REG_DWORD under a single per-user key. Value names
 * are kept stable across versions so an upgrade preserves the user's options.
 * The key path carries no version segment for the same reason.
 */

#include "settings.h"

#pragma comment(lib, "advapi32.lib")

/* Registry value names. */
static const wchar_t k_ValZoom[] = L"ZoomPercent";
static const wchar_t k_ValMapEngine[] = L"MapEngine";
static const wchar_t k_ValCopyPrepend[] = L"CopyPrependNormalized";
static const wchar_t k_ValNameSurnameFirst[] = L"NameSurnameFirst";

void EeSettings_Defaults(EeSettings *out)
{
    if (out == NULL)
    {
        return;
    }
    out->zoom_percent = 100;
    out->map_engine = 0; /* EeMap_Google */
    out->copy_prepend_normalized = TRUE;
    out->name_surname_first = TRUE;
}

/* Read one REG_DWORD into *value; leave it unchanged if the value is absent or
 * not a DWORD. */
static void read_dword(HKEY key, const wchar_t *name, int *value)
{
    DWORD data = 0;
    DWORD cb = sizeof(data);
    DWORD type = 0;

    if (RegQueryValueExW(key, name, NULL, &type, (BYTE *)&data, &cb) == ERROR_SUCCESS &&
        type == REG_DWORD && cb == sizeof(data))
    {
        *value = (int)data;
    }
}

/* Write one REG_DWORD; return FALSE on failure. */
static BOOL write_dword(HKEY key, const wchar_t *name, int value)
{
    DWORD data = (DWORD)value;

    return RegSetValueExW(key, name, 0, REG_DWORD, (const BYTE *)&data, sizeof(data)) ==
           ERROR_SUCCESS;
}

BOOL EeSettings_LoadFrom(const wchar_t *subkey, EeSettings *out)
{
    HKEY key = NULL;
    int prepend;
    int surname;

    if (subkey == NULL || out == NULL)
    {
        return FALSE;
    }
    EeSettings_Defaults(out);
    if (RegOpenKeyExW(HKEY_CURRENT_USER, subkey, 0, KEY_QUERY_VALUE, &key) != ERROR_SUCCESS)
    {
        return FALSE;
    }
    read_dword(key, k_ValZoom, &out->zoom_percent);
    read_dword(key, k_ValMapEngine, &out->map_engine);
    prepend = out->copy_prepend_normalized ? 1 : 0;
    read_dword(key, k_ValCopyPrepend, &prepend);
    out->copy_prepend_normalized = (prepend != 0);
    surname = out->name_surname_first ? 1 : 0;
    read_dword(key, k_ValNameSurnameFirst, &surname);
    out->name_surname_first = (surname != 0);
    RegCloseKey(key);
    return TRUE;
}

BOOL EeSettings_SaveTo(const wchar_t *subkey, const EeSettings *in)
{
    HKEY key = NULL;
    BOOL ok = TRUE;

    if (subkey == NULL || in == NULL)
    {
        return FALSE;
    }
    if (RegCreateKeyExW(HKEY_CURRENT_USER,
                        subkey,
                        0,
                        NULL,
                        REG_OPTION_NON_VOLATILE,
                        KEY_SET_VALUE,
                        NULL,
                        &key,
                        NULL) != ERROR_SUCCESS)
    {
        return FALSE;
    }
    ok &= write_dword(key, k_ValZoom, in->zoom_percent);
    ok &= write_dword(key, k_ValMapEngine, in->map_engine);
    ok &= write_dword(key, k_ValCopyPrepend, in->copy_prepend_normalized ? 1 : 0);
    ok &= write_dword(key, k_ValNameSurnameFirst, in->name_surname_first ? 1 : 0);
    RegCloseKey(key);
    return ok;
}

BOOL EeSettings_Load(EeSettings *out)
{
    return EeSettings_LoadFrom(EE_SETTINGS_SUBKEY, out);
}

BOOL EeSettings_Save(const EeSettings *in)
{
    return EeSettings_SaveTo(EE_SETTINGS_SUBKEY, in);
}
