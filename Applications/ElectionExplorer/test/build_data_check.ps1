# Build test\data_check.c (the loader harness used by test\backup_data.py) into
# Applications\Build\win-x64-release\ElectionExplorer\data_check\data_check.exe.
# Run from any directory; needs Visual Studio with the C++ x64 tools.
$ErrorActionPreference = "Stop"
$app = Split-Path -Parent $PSScriptRoot
$src = Join-Path $app "src"
$out = Join-Path (Split-Path -Parent $app) "Build\win-x64-release\ElectionExplorer\data_check"
$vs = & "${env:ProgramFiles(x86)}\Microsoft Visual Studio\Installer\vswhere.exe" -latest -property installationPath
Import-Module (Join-Path $vs "Common7\Tools\Microsoft.VisualStudio.DevShell.dll")
Enter-VsDevShell -VsInstallPath $vs -SkipAutomaticLocation -DevCmdArguments "-arch=x64 -no_logo" | Out-Null
New-Item -ItemType Directory -Force $out | Out-Null
Push-Location $out
try {
    $files = @("$PSScriptRoot\data_check.c") + (@(
            "voter_table.c", "filter.c", "settings.c", "xlsx.c", "xls.c", "csv_sheet.c", "voter_roster.c",
            "ee_cvr.c", "hart_cvr.c", "hart_hvs.c", "hart_ocr.c", "dominion_cvr.c", "ee_rcv.c", "pdf_reader.c",
            "ocr_win.c", "ee_results.c", "ee_results_pdf.c", "ee_results_dom.c", "ee_results_ocr.c",
            "ee_glyph_ocr.c", "ee_audit.c", "ee_audit_hart.c", "third_party\miniz\miniz.c") | ForEach-Object { Join-Path $src $_ })
    & cl /nologo /O2 /W4 /std:c11 /TC /utf-8 /DWIN32_LEAN_AND_MEAN /DUNICODE /D_UNICODE /DWINVER=0x0A00 `
        /D_WIN32_WINNT=0x0A00 /I $src $files /Fe:data_check.exe /link /SUBSYSTEM:CONSOLE `
        user32.lib advapi32.lib gdi32.lib ole32.lib runtimeobject.lib 2>&1 |
        Where-Object { $_ -match "error|warning C" -and $_ -notmatch "miniz" }
    if ($LASTEXITCODE -ne 0) { throw "data_check build failed" }
    Write-Output (Join-Path $out "data_check.exe")
}
finally {
    Pop-Location
}
