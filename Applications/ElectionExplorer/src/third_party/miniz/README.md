# miniz (vendored)

Third-party public-domain / MIT library used by ElectionExplorer's XLSX importer
for ZIP archive reading and DEFLATE inflate (an `.xlsx` is a ZIP of XML parts).

- **Version:** 3.0.2 (amalgamated single-source release)
- **Source:** https://github.com/richgel999/miniz/releases/download/3.0.2/miniz-3.0.2.zip
- **Files:** `miniz.c`, `miniz.h` (self-contained; defines `MINIZ_EXPORT`
  inline, no separate `miniz_export.h`), `LICENSE`
- **License:** MIT-style / public domain (see `LICENSE`) — kept verbatim.
- **Vendored:** 2026-09-17. Sign-off recorded in `docs/xlsx-import-design.md`.

## Local modifications

None. Keep it pristine so it can be re-updated by dropping in a newer release.
ElectionExplorer-specific configuration (e.g. `MINIZ_NO_*` macros) is applied via
the build / an app wrapper, not by editing these files.

## Updating

Download the newer release zip, replace `miniz.c` / `miniz.h` / `LICENSE`, update
the version + URL above, and rebuild.
