/**
 * @file settings.h
 * @brief Persisted user options (zoom, map engine, copy/name formatting).
 *
 * Options are stored per user in the registry under
 * HKEY_CURRENT_USER\Software\WheelGroupTech\ElectionExplorer, following the
 * Microsoft guidance for Win32 desktop application settings (per-user options
 * live under HKCU, not HKLM, so no elevation is required). For an MSIX /
 * Microsoft Store package these HKCU writes are transparently virtualized by
 * the packaging runtime, so the same registry APIs work unmodified.
 */
#pragma once

#include <windows.h>

#ifdef __cplusplus
extern "C"
{
#endif

    /** Registry subkey (under HKEY_CURRENT_USER) holding user options. */
#define EE_SETTINGS_SUBKEY L"Software\\WheelGroupTech\\ElectionExplorer"

    /** User-configurable options persisted between runs. */
    typedef struct EeSettings
    {
        int zoom_percent;             /**< Grid zoom, percent (e.g. 100). */
        int map_engine;               /**< Map provider (EeMapEngine value). */
        BOOL copy_prepend_normalized; /**< Prepend normalized fields on copy. */
        BOOL name_surname_first;      /**< Display names surname-first. */
    } EeSettings;

    /**
     * Fill @p out with the built-in default options.
     * @param out Receives defaults. No-op if NULL.
     */
    void EeSettings_Defaults(EeSettings *out);

    /**
     * Load options from the default registry location.
     *
     * Missing or malformed values fall back to their defaults, so @p out is
     * always fully populated on return.
     *
     * @param out Receives the loaded options. Must be non-NULL.
     * @return TRUE if the settings key existed and was read; FALSE if it was
     *         absent or could not be opened (defaults were used).
     */
    BOOL EeSettings_Load(EeSettings *out);

    /**
     * Save options to the default registry location, creating the key if
     * needed.
     *
     * @param in Options to persist. Must be non-NULL.
     * @return TRUE if every value was written; FALSE if the key could not be
     *         created or a value could not be written.
     */
    BOOL EeSettings_Save(const EeSettings *in);

    /**
     * Load options from an explicit HKEY_CURRENT_USER subkey (for testing).
     * @param subkey Registry subkey under HKEY_CURRENT_USER. Must be non-NULL.
     * @param out    Receives options; missing values use defaults. Non-NULL.
     * @return TRUE if the key existed and was read; FALSE otherwise.
     */
    BOOL EeSettings_LoadFrom(const wchar_t *subkey, EeSettings *out);

    /**
     * Save options to an explicit HKEY_CURRENT_USER subkey (for testing).
     * @param subkey Registry subkey under HKEY_CURRENT_USER. Must be non-NULL.
     * @param in     Options to persist. Must be non-NULL.
     * @return TRUE if every value was written; FALSE otherwise.
     */
    BOOL EeSettings_SaveTo(const wchar_t *subkey, const EeSettings *in);

#ifdef __cplusplus
}
#endif
