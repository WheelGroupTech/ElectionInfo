/**
 * @file ocr_win.c
 * @brief Windows.Media.Ocr wrapper (C ABI of the WinRT headers + WIC). See ocr_win.h.
 *
 * Pipeline per image: WIC decodes (or wraps raw pixels) -> optional WIC scaler ->
 * WIC format converter to 8bpp gray -> IWICBitmap -> ISoftwareBitmapNativeFactory ->
 * SoftwareBitmap -> OcrEngine.RecognizeAsync (polled to completion on this thread) ->
 * lines/words with bounding boxes, converted to UTF-8.
 *
 * The SDK only declares the WinRT interface IDs (EXTERN_C const IID ...), so the ones
 * used here are defined locally from the header MIDL_INTERFACE uuids.
 */

#define COBJMACROS
#include "ocr_win.h"

#include <roapi.h>
#include <winstring.h>
#include <wincodec.h>
#include <asyncinfo.h>
#include <windows.graphics.imaging.interop.h>
#include <windows.media.ocr.h>

#include <stdlib.h>
#include <string.h>
#include <strsafe.h>

/* Interface IDs (from the Windows SDK 10.0.26100 headers). */
static const IID k_IID_OcrEngineStatics = {0x5bffa85a,
                                           0x3384,
                                           0x3540,
                                           {0x99, 0x40, 0x69, 0x91, 0x20, 0xd4, 0x28, 0xa8}};
static const IID k_IID_SoftwareBitmap = {0x689e0708,
                                         0x7eef,
                                         0x483f,
                                         {0x96, 0x3f, 0xda, 0x93, 0x88, 0x18, 0xe0, 0x73}};
static const IID k_IID_SoftwareBitmapNativeFactory =
    {0xc3c181ec, 0x2914, 0x4791, {0xaf, 0x02, 0x02, 0xd2, 0x24, 0xa1, 0x0b, 0x43}};
static const CLSID k_CLSID_SoftwareBitmapNativeFactory =
    {0x84e65691, 0x8602, 0x4a84, {0xbe, 0x46, 0x70, 0x8b, 0xe9, 0xcd, 0x4b, 0x74}};
static const IID k_IID_AsyncInfo = {0x00000036,
                                    0x0000,
                                    0x0000,
                                    {0xc0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x46}};
static const CLSID k_CLSID_WICImagingFactory = {0xcacaf262,
                                                0x9370,
                                                0x4615,
                                                {0xa1, 0x3b, 0x9f, 0x55, 0x39, 0xda, 0x4c, 0x0a}};
static const IID k_IID_WICImagingFactory = {0xec5ec8a9,
                                            0xc395,
                                            0x4314,
                                            {0x9c, 0x77, 0x54, 0xd7, 0xa9, 0x35, 0xff, 0x70}};
static const GUID k_PixelFormat8bppGray = {0x6fddc324,
                                           0x4e03,
                                           0x4bfe,
                                           {0xb1, 0x85, 0x3d, 0x77, 0x76, 0x8d, 0xc9, 0x08}};
static const GUID k_PixelFormat24bppRGB = {0x6fddc324,
                                           0x4e03,
                                           0x4bfe,
                                           {0xb1, 0x85, 0x3d, 0x77, 0x76, 0x8d, 0xc9, 0x0d}};

struct EeOcr
{
    BOOL ro_init; /* this object initialized the Windows Runtime on the thread */
    IWICImagingFactory *wic;
    ISoftwareBitmapNativeFactory *sbf;
    __x_ABI_CWindows_CMedia_COcr_CIOcrEngine *engine;
    UINT32 max_dim;
};

#define RELEASE(p)                                                                                 \
    do                                                                                             \
    {                                                                                              \
        if ((p) != NULL)                                                                           \
        {                                                                                          \
            (p)->lpVtbl->Release(p);                                                               \
            (p) = NULL;                                                                            \
        }                                                                                          \
    } while (0)

static void set_err(wchar_t *err, size_t cch, const wchar_t *msg, HRESULT hr)
{
    if (err != NULL && cch > 0)
    {
        if (hr != S_OK)
        {
            StringCchPrintfW(err, cch, L"%s (0x%08lX)", msg, (unsigned long)hr);
        }
        else
        {
            StringCchCopyW(err, cch, msg);
        }
    }
}

void EeOcr_TextInit(EeOcrText *t)
{
    if (t != NULL)
    {
        ZeroMemory(t, sizeof(*t));
    }
}

void EeOcr_TextFree(EeOcrText *t)
{
    if (t != NULL)
    {
        free(t->words);
        free(t->lines);
        free(t->text);
        ZeroMemory(t, sizeof(*t));
    }
}

void EeOcr_Destroy(EeOcr *ocr)
{
    if (ocr == NULL)
    {
        return;
    }
    RELEASE(ocr->engine);
    RELEASE(ocr->sbf);
    RELEASE(ocr->wic);
    if (ocr->ro_init)
    {
        RoUninitialize();
    }
    free(ocr);
}

BOOL EeOcr_Create(EeOcr **out, wchar_t *err, size_t errcch)
{
    EeOcr *ocr;
    HRESULT hr;
    HSTRING_HEADER hdr;
    HSTRING cls = NULL;
    __x_ABI_CWindows_CMedia_COcr_CIOcrEngineStatics *statics = NULL;
    static const wchar_t k_Class[] = L"Windows.Media.Ocr.OcrEngine";

    if (out == NULL)
    {
        return FALSE;
    }
    *out = NULL;
    ocr = (EeOcr *)calloc(1, sizeof(EeOcr));
    if (ocr == NULL)
    {
        set_err(err, errcch, L"Out of memory.", S_OK);
        return FALSE;
    }
    hr = RoInitialize(RO_INIT_MULTITHREADED);
    if (hr == S_OK || hr == S_FALSE)
    {
        ocr->ro_init = TRUE;           /* balanced by RoUninitialize in destroy */
    }
    else if (hr != RPC_E_CHANGED_MODE) /* an STA thread is fine to use as is */
    {
        set_err(err, errcch, L"Windows Runtime could not be initialized", hr);
        EeOcr_Destroy(ocr);
        return FALSE;
    }
    hr = CoCreateInstance(&k_CLSID_WICImagingFactory,
                          NULL,
                          CLSCTX_INPROC_SERVER,
                          &k_IID_WICImagingFactory,
                          (void **)&ocr->wic);
    if (FAILED(hr))
    {
        set_err(err, errcch, L"Windows Imaging Component is not available", hr);
        EeOcr_Destroy(ocr);
        return FALSE;
    }
    hr = CoCreateInstance(&k_CLSID_SoftwareBitmapNativeFactory,
                          NULL,
                          CLSCTX_INPROC_SERVER,
                          &k_IID_SoftwareBitmapNativeFactory,
                          (void **)&ocr->sbf);
    if (FAILED(hr))
    {
        set_err(err, errcch, L"Windows OCR is not available (no bitmap factory)", hr);
        EeOcr_Destroy(ocr);
        return FALSE;
    }
    hr = WindowsCreateStringReference(k_Class, (UINT32)(ARRAYSIZE(k_Class) - 1), &hdr, &cls);
    if (SUCCEEDED(hr))
    {
        hr = RoGetActivationFactory(cls, &k_IID_OcrEngineStatics, (void **)&statics);
    }
    if (FAILED(hr) || statics == NULL)
    {
        set_err(err, errcch, L"Windows OCR is not available on this version of Windows", hr);
        EeOcr_Destroy(ocr);
        return FALSE;
    }
    if (FAILED(statics->lpVtbl->get_MaxImageDimension(statics, &ocr->max_dim)) || ocr->max_dim == 0)
    {
        ocr->max_dim = 2600;
    }
    hr = statics->lpVtbl->TryCreateFromUserProfileLanguages(statics, &ocr->engine);
    RELEASE(statics);
    if (FAILED(hr) || ocr->engine == NULL)
    {
        set_err(err,
                errcch,
                L"Windows OCR isn't available: no OCR-capable language is installed. Add one "
                L"(e.g. English) in Settings > Time & language > Language & region.",
                S_OK);
        EeOcr_Destroy(ocr);
        return FALSE;
    }
    *out = ocr;
    return TRUE;
}

/* ---- result conversion ------------------------------------------------------------ */

static BOOL text_append(EeOcrText *t, const char *s, size_t n)
{
    if (t->text_len + n + 1 > t->text_cap)
    {
        size_t nc = t->text_cap ? t->text_cap * 2 : 4096;
        char *np;
        while (nc < t->text_len + n + 1)
        {
            nc *= 2;
        }
        np = (char *)realloc(t->text, nc);
        if (np == NULL)
        {
            return FALSE;
        }
        t->text = np;
        t->text_cap = nc;
    }
    memcpy(t->text + t->text_len, s, n);
    t->text_len += n;
    t->text[t->text_len] = '\0';
    return TRUE;
}

static BOOL add_word(EeOcrText *t, HSTRING hs, const __x_ABI_CWindows_CFoundation_CRect *r)
{
    UINT32 wl = 0;
    const wchar_t *w = WindowsGetStringRawBuffer(hs, &wl);
    EeOcrWord *wd;
    char buf[512];
    int n = 0;
    if (t->nwords == t->cap_words)
    {
        uint32_t nc = t->cap_words ? t->cap_words * 2 : 256;
        EeOcrWord *nw = (EeOcrWord *)realloc(t->words, (size_t)nc * sizeof(EeOcrWord));
        if (nw == NULL)
        {
            return FALSE;
        }
        t->words = nw;
        t->cap_words = nc;
    }
    if (w != NULL && wl > 0)
    {
        n = WideCharToMultiByte(CP_UTF8, 0, w, (int)wl, buf, (int)sizeof(buf), NULL, NULL);
        if (n < 0)
        {
            n = 0;
        }
    }
    wd = &t->words[t->nwords];
    wd->x0 = r->X;
    wd->y0 = r->Y;
    wd->x1 = r->X + r->Width;
    wd->y1 = r->Y + r->Height;
    wd->text_off = (uint32_t)t->text_len;
    wd->text_len = (uint32_t)n;
    wd->line = t->nlines - 1;
    if (!text_append(t, buf, (size_t)n))
    {
        return FALSE;
    }
    t->lines[t->nlines - 1].nwords++;
    t->nwords++;
    return TRUE;
}

static BOOL collect_result(__x_ABI_CWindows_CMedia_COcr_CIOcrResult *res, EeOcrText *out)
{
    __FIVectorView_1_Windows__CMedia__COcr__COcrLine *lines = NULL;
    UINT32 nl = 0, i;
    BOOL ok = TRUE;
    if (FAILED(res->lpVtbl->get_Lines(res, &lines)) || lines == NULL)
    {
        return FALSE;
    }
    lines->lpVtbl->get_Size(lines, &nl);
    for (i = 0; i < nl && ok; i++)
    {
        __x_ABI_CWindows_CMedia_COcr_CIOcrLine *line = NULL;
        __FIVectorView_1_Windows__CMedia__COcr__COcrWord *words = NULL;
        UINT32 nw = 0, k;
        if (FAILED(lines->lpVtbl->GetAt(lines, i, &line)) || line == NULL)
        {
            continue;
        }
        if (out->nlines == out->cap_lines)
        {
            uint32_t nc = out->cap_lines ? out->cap_lines * 2 : 64;
            EeOcrLine *nln = (EeOcrLine *)realloc(out->lines, (size_t)nc * sizeof(EeOcrLine));
            if (nln == NULL)
            {
                RELEASE(line);
                ok = FALSE;
                break;
            }
            out->lines = nln;
            out->cap_lines = nc;
        }
        out->lines[out->nlines].first_word = out->nwords;
        out->lines[out->nlines].nwords = 0;
        out->nlines++;
        if (SUCCEEDED(line->lpVtbl->get_Words(line, &words)) && words != NULL)
        {
            words->lpVtbl->get_Size(words, &nw);
            for (k = 0; k < nw && ok; k++)
            {
                __x_ABI_CWindows_CMedia_COcr_CIOcrWord *word = NULL;
                __x_ABI_CWindows_CFoundation_CRect rect;
                HSTRING hs = NULL;
                if (FAILED(words->lpVtbl->GetAt(words, k, &word)) || word == NULL)
                {
                    continue;
                }
                if (SUCCEEDED(word->lpVtbl->get_BoundingRect(word, &rect)) &&
                    SUCCEEDED(word->lpVtbl->get_Text(word, &hs)))
                {
                    ok = add_word(out, hs, &rect);
                }
                if (hs != NULL)
                {
                    WindowsDeleteString(hs);
                }
                RELEASE(word);
            }
            RELEASE(words);
        }
        RELEASE(line);
    }
    RELEASE(lines);
    return ok;
}

/* ---- recognition ---------------------------------------------------------------- */

/* Recognize a WIC bitmap source: scale, convert to gray, run OCR. */
static BOOL recognize_source(EeOcr *ocr,
                             IWICBitmapSource *src,
                             double scale,
                             EeOcrText *out,
                             wchar_t *err,
                             size_t errcch)
{
    HRESULT hr;
    UINT w = 0, h = 0, tw, th;
    IWICBitmapScaler *scaler = NULL;
    IWICFormatConverter *conv = NULL;
    IWICBitmap *bmp = NULL;
    __x_ABI_CWindows_CGraphics_CImaging_CISoftwareBitmap *sb = NULL;
    __FIAsyncOperation_1_Windows__CMedia__COcr__COcrResult *op = NULL;
    IAsyncInfo *info = NULL;
    __x_ABI_CWindows_CMedia_COcr_CIOcrResult *res = NULL;
    IWICBitmapSource *cur = src;
    BOOL ok = FALSE;

    out->nwords = out->nlines = 0;
    out->text_len = 0;
    if (out->text != NULL)
    {
        out->text[0] = '\0';
    }
    hr = IWICBitmapSource_GetSize(src, &w, &h);
    if (FAILED(hr) || w == 0 || h == 0)
    {
        set_err(err, errcch, L"The page image could not be read", hr);
        return FALSE;
    }
    if (scale <= 0.0)
    {
        scale = 1.0;
    }
    tw = (UINT)(w * scale + 0.5);
    th = (UINT)(h * scale + 0.5);
    if (tw > ocr->max_dim || th > ocr->max_dim)
    {
        double f = (double)ocr->max_dim / (double)((tw > th) ? tw : th);
        tw = (UINT)(tw * f);
        th = (UINT)(th * f);
    }
    if (tw != w || th != h)
    {
        hr = IWICImagingFactory_CreateBitmapScaler(ocr->wic, &scaler);
        if (SUCCEEDED(hr))
        {
            hr = IWICBitmapScaler_Initialize(scaler, src, tw, th, WICBitmapInterpolationModeFant);
        }
        if (FAILED(hr))
        {
            set_err(err, errcch, L"The page image could not be scaled", hr);
            goto done;
        }
        cur = (IWICBitmapSource *)scaler;
    }
    hr = IWICImagingFactory_CreateFormatConverter(ocr->wic, &conv);
    if (SUCCEEDED(hr))
    {
        hr = IWICFormatConverter_Initialize(conv,
                                            cur,
                                            &k_PixelFormat8bppGray,
                                            WICBitmapDitherTypeNone,
                                            NULL,
                                            0.0,
                                            WICBitmapPaletteTypeCustom);
    }
    if (SUCCEEDED(hr))
    {
        hr = IWICImagingFactory_CreateBitmapFromSource(ocr->wic,
                                                       (IWICBitmapSource *)conv,
                                                       WICBitmapCacheOnLoad,
                                                       &bmp);
    }
    if (SUCCEEDED(hr))
    {
        hr = ocr->sbf->lpVtbl->CreateFromWICBitmap(ocr->sbf,
                                                   bmp,
                                                   TRUE,
                                                   &k_IID_SoftwareBitmap,
                                                   (void **)&sb);
    }
    if (FAILED(hr) || sb == NULL)
    {
        set_err(err, errcch, L"The page image could not be prepared for OCR", hr);
        goto done;
    }
    hr = ocr->engine->lpVtbl->RecognizeAsync(ocr->engine, sb, &op);
    if (SUCCEEDED(hr))
    {
        hr = op->lpVtbl->QueryInterface(op, &k_IID_AsyncInfo, (void **)&info);
    }
    if (FAILED(hr))
    {
        set_err(err, errcch, L"Windows OCR could not start", hr);
        goto done;
    }
    for (;;)
    {
        AsyncStatus st = Started;
        hr = info->lpVtbl->get_Status(info, &st);
        if (FAILED(hr) || st == Error || st == Canceled)
        {
            HRESULT ec = S_OK;
            info->lpVtbl->get_ErrorCode(info, &ec);
            set_err(err, errcch, L"Windows OCR failed", FAILED(hr) ? hr : ec);
            goto done;
        }
        if (st == Completed)
        {
            break;
        }
        Sleep(1);
    }
    hr = op->lpVtbl->GetResults(op, &res);
    if (FAILED(hr) || res == NULL)
    {
        set_err(err, errcch, L"Windows OCR returned no result", hr);
        goto done;
    }
    out->width = tw;
    out->height = th;
    ok = collect_result(res, out);
    if (!ok)
    {
        set_err(err, errcch, L"Out of memory reading the OCR result.", S_OK);
    }
done:
    RELEASE(res);
    RELEASE(info);
    RELEASE(op);
    RELEASE(sb);
    RELEASE(bmp);
    RELEASE(conv);
    RELEASE(scaler);
    return ok;
}

BOOL EeOcr_RecognizeEncoded(EeOcr *ocr,
                            const void *data,
                            size_t len,
                            double scale,
                            EeOcrText *out,
                            wchar_t *err,
                            size_t errcch)
{
    HRESULT hr;
    IWICStream *stream = NULL;
    IWICBitmapDecoder *dec = NULL;
    IWICBitmapFrameDecode *frame = NULL;
    BOOL ok = FALSE;
    if (ocr == NULL || data == NULL || len == 0 || len > 0xFFFFFFFFu || out == NULL)
    {
        set_err(err, errcch, L"Invalid arguments.", S_OK);
        return FALSE;
    }
    hr = IWICImagingFactory_CreateStream(ocr->wic, &stream);
    if (SUCCEEDED(hr))
    {
        hr = IWICStream_InitializeFromMemory(stream, (BYTE *)data, (DWORD)len);
    }
    if (SUCCEEDED(hr))
    {
        hr = IWICImagingFactory_CreateDecoderFromStream(ocr->wic,
                                                        (IStream *)stream,
                                                        NULL,
                                                        WICDecodeMetadataCacheOnDemand,
                                                        &dec);
    }
    if (SUCCEEDED(hr))
    {
        hr = IWICBitmapDecoder_GetFrame(dec, 0, &frame);
    }
    if (FAILED(hr))
    {
        set_err(err, errcch, L"The page image could not be decoded", hr);
    }
    else
    {
        ok = recognize_source(ocr, (IWICBitmapSource *)frame, scale, out, err, errcch);
    }
    RELEASE(frame);
    RELEASE(dec);
    RELEASE(stream);
    return ok;
}

BOOL EeOcr_RecognizePixels(EeOcr *ocr,
                           const void *pixels,
                           uint32_t width,
                           uint32_t height,
                           uint32_t stride,
                           int channels,
                           double scale,
                           EeOcrText *out,
                           wchar_t *err,
                           size_t errcch)
{
    HRESULT hr;
    IWICBitmap *bmp = NULL;
    uint64_t size = (uint64_t)stride * height;
    BOOL ok;
    if (ocr == NULL || pixels == NULL || width == 0 || height == 0 || out == NULL ||
        (channels != 1 && channels != 3) || stride < width * (uint32_t)channels ||
        size > 0xFFFFFFFFu)
    {
        set_err(err, errcch, L"Invalid arguments.", S_OK);
        return FALSE;
    }
    hr = IWICImagingFactory_CreateBitmapFromMemory(ocr->wic,
                                                   width,
                                                   height,
                                                   (channels == 1) ? &k_PixelFormat8bppGray
                                                                   : &k_PixelFormat24bppRGB,
                                                   stride,
                                                   (UINT)size,
                                                   (BYTE *)pixels,
                                                   &bmp);
    if (FAILED(hr))
    {
        set_err(err, errcch, L"The page image could not be read", hr);
        return FALSE;
    }
    ok = recognize_source(ocr, (IWICBitmapSource *)bmp, scale, out, err, errcch);
    RELEASE(bmp);
    return ok;
}
