/**
 * @file ee_audit_hart.h
 * @brief Hart Verity "Audit Log Report" PDFs: the reader ee_audit.c calls (internal to the
 *        audit engine; the public API is ee_audit.h).
 */
#pragma once

#include "ee_audit.h"
#include "pdf_reader.h"

#ifdef __cplusplus
extern "C"
{
#endif

    /** TRUE when @p pdf is a Hart Verity audit log report (its Title or its first page). */
    BOOL AuHart_IsReport(EePdf *pdf);

    /**
     * Read a Hart Verity audit log report into @p out (initialized, its path set): header,
     * then every numbered row as an event, kept newest first. On failure @p out keeps what was
     * read; the caller frees it.
     */
    EeLoadStatus AuHart_ReadPdf(EePdf *pdf,
                                const wchar_t *path,
                                EeAuditCopy *out,
                                volatile LONG *cancel_flag,
                                EeLoadProgressFn progress_fn,
                                void *progress_user,
                                wchar_t *err,
                                size_t errcch);

#ifdef __cplusplus
}
#endif
