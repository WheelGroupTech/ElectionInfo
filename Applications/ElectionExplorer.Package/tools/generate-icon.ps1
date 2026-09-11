# Regenerates the application icon (res/app.ico) from the logo source PNG.
#
# Produces a multi-resolution, transparent .ico (16/24/32/48/64/128/256) using a
# SQUARE CROP of the (portrait) logo: transparent margins are trimmed, the
# content is center-cropped to a square, then inset slightly so the art doesn't
# touch the icon edge.
#
# Requires ImageMagick 7+ ("magick" on PATH). Run from anywhere:
#   pwsh -File tools\generate-icon.ps1

param(
    [string]$SourcePng = "$PSScriptRoot\logo-source.png",
    [string]$OutIco    = "$PSScriptRoot\..\..\ElectionExplorer\res\app.ico"
)

$ErrorActionPreference = "Stop"

if (-not (Get-Command magick -ErrorAction SilentlyContinue)) {
    throw "ImageMagick ('magick') not found on PATH. Install ImageMagick 7+."
}
if (-not (Test-Path $SourcePng)) {
    throw "Source image not found: $SourcePng"
}

$tmp = Join-Path ([System.IO.Path]::GetTempPath()) ("app_icon_" + [guid]::NewGuid().ToString("N"))
New-Item -ItemType Directory -Force -Path $tmp | Out-Null
$trim   = Join-Path $tmp "trim.png"
$square = Join-Path $tmp "square.png"
$master = Join-Path $tmp "master256.png"

try {
    # 1. Trim fully-transparent margins down to the logo content.
    & magick $SourcePng -trim +repage $trim
    $dims = (& magick identify -format "%w %h" $trim) -split ' '
    $side = [Math]::Min([int]$dims[0], [int]$dims[1])

    # 2. Center-crop the (portrait) content to a square.
    & magick $trim -background none -gravity center -extent "${side}x${side}" +repage $square

    # 3. Inset ~9% so the art doesn't touch the edges, on a 256x256 canvas.
    & magick $square -resize 232x232 -background none -gravity center -extent 256x256 $master

    # 4. Emit a multi-resolution .ico.
    & magick $master -define icon:auto-resize=256,128,64,48,32,24,16 $OutIco

    Write-Host "Wrote $OutIco"
    & magick identify $OutIco
}
finally {
    Remove-Item -Recurse -Force $tmp -ErrorAction SilentlyContinue
}
