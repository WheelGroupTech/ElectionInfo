# Generates MSIX Store logo assets from the app logo.
#
# Default source: logo-source.png (in this folder). It is a high-resolution
# rendering of the app logo. If it has a real alpha channel, assets are made on
# a transparent canvas; if it is opaque (solid background), the background color
# is sampled from a corner, the logo's content box is cropped out of the empty
# margins, and assets are rendered on that same solid background.
#
# Output: ..\Images\*.png  (referenced by Package.appxmanifest)
#
# Usage (from a normal PowerShell prompt):
#   pwsh -File tools\generate-assets.ps1
#   pwsh -File tools\generate-assets.ps1 -SourcePng path\to\other-logo.png

param(
    [string]$SourcePng = "$PSScriptRoot\logo-source.png",
    [string]$SourceIco = "$PSScriptRoot\..\..\ElectionExplorer\res\app.ico",
    [string]$OutDir    = "$PSScriptRoot\..\Images"
)

Add-Type -AssemblyName System.Drawing

New-Item -ItemType Directory -Force -Path $OutDir | Out-Null

# Load the source (prefer the high-res PNG; fall back to the app icon).
if (Test-Path $SourcePng) {
    $orig = [System.Drawing.Bitmap]::new($SourcePng)
} else {
    $icon = New-Object System.Drawing.Icon($SourceIco, 256, 256)
    $orig = $icon.ToBitmap()
}

# Detect transparency by scanning the alpha channel on a grid.
$hasAlpha = $false
for ($y = 0; $y -lt $orig.Height -and -not $hasAlpha; $y += 8) {
    for ($x = 0; $x -lt $orig.Width; $x += 8) {
        if ($orig.GetPixel($x, $y).A -lt 250) { $hasAlpha = $true; break }
    }
}

# Background color: transparent if the source has alpha, else the top-left pixel
# (a solid-background logo). MSIX assets on a solid background read cleanly and
# match the tile BackgroundColor in the manifest.
if ($hasAlpha) {
    $bg = [System.Drawing.Color]::Transparent
    Write-Host "Source has alpha; assets on a transparent canvas."
} else {
    $bg = $orig.GetPixel(0, 0)
    Write-Host ("Source is opaque; background sampled as #{0:X2}{1:X2}{2:X2}." -f $bg.R, $bg.G, $bg.B)
}

# Content bounding box: tightest rectangle around pixels that differ from the
# background (alpha for transparent sources; color distance for opaque ones).
function Get-ContentBounds([System.Drawing.Bitmap]$bmp, [bool]$alpha, [System.Drawing.Color]$back) {
    $minX = $bmp.Width; $minY = $bmp.Height; $maxX = -1; $maxY = -1
    $tol = 24
    for ($y = 0; $y -lt $bmp.Height; $y += 2) {
        for ($x = 0; $x -lt $bmp.Width; $x += 2) {
            $c = $bmp.GetPixel($x, $y)
            $isContent = if ($alpha) {
                $c.A -gt 16
            } else {
                ([Math]::Abs($c.R - $back.R) + [Math]::Abs($c.G - $back.G) + [Math]::Abs($c.B - $back.B)) -gt $tol
            }
            if ($isContent) {
                if ($x -lt $minX) { $minX = $x }
                if ($y -lt $minY) { $minY = $y }
                if ($x -gt $maxX) { $maxX = $x }
                if ($y -gt $maxY) { $maxY = $y }
            }
        }
    }
    if ($maxX -lt 0) { return [System.Drawing.Rectangle]::new(0, 0, $bmp.Width, $bmp.Height) }
    # Pad a little and clamp to the bitmap.
    $pad = 6
    $minX = [Math]::Max(0, $minX - $pad); $minY = [Math]::Max(0, $minY - $pad)
    $maxX = [Math]::Min($bmp.Width - 1, $maxX + $pad); $maxY = [Math]::Min($bmp.Height - 1, $maxY + $pad)
    return [System.Drawing.Rectangle]::new($minX, $minY, ($maxX - $minX + 1), ($maxY - $minY + 1))
}

$bounds = Get-ContentBounds $orig $hasAlpha $bg
$src = $orig.Clone($bounds, $orig.PixelFormat)
$orig.Dispose()
Write-Host ("Cropped logo content to {0}x{1}." -f $src.Width, $src.Height)

# One asset: fill the canvas with the background, then draw the cropped logo
# centered and scaled to occupy $fill of the smaller dimension, preserving the
# logo's aspect ratio.
function New-Asset([string]$name, [int]$w, [int]$h, [double]$fill) {
    $bmp = New-Object System.Drawing.Bitmap($w, $h)
    $g = [System.Drawing.Graphics]::FromImage($bmp)
    $g.InterpolationMode = [System.Drawing.Drawing2D.InterpolationMode]::HighQualityBicubic
    $g.SmoothingMode     = [System.Drawing.Drawing2D.SmoothingMode]::HighQuality
    $g.PixelOffsetMode   = [System.Drawing.Drawing2D.PixelOffsetMode]::HighQuality
    $g.Clear($bg)

    # Fit the source into a box of ($fill * min(w,h)) preserving aspect ratio.
    $box = [Math]::Min($w, $h) * $fill
    $scale = [Math]::Min($box / $src.Width, $box / $src.Height)
    $dw = [int][Math]::Round($src.Width * $scale)
    $dh = [int][Math]::Round($src.Height * $scale)
    $x = [int](($w - $dw) / 2)
    $y = [int](($h - $dh) / 2)
    $g.DrawImage($src, $x, $y, $dw, $dh)
    $g.Dispose()

    $path = Join-Path $OutDir $name
    $bmp.Save($path, [System.Drawing.Imaging.ImageFormat]::Png)
    $bmp.Dispose()
    Write-Host "  $name  (${w}x${h})"
}

Write-Host "Generating MSIX assets into $OutDir"

# Tiles: logo centered with padding.
New-Asset "Square150x150Logo.png" 150 150 0.85
New-Asset "Square310x310Logo.png" 310 310 0.85
New-Asset "Square71x71Logo.png"   71  71  0.85
New-Asset "Wide310x150Logo.png"   310 150 0.80
New-Asset "SplashScreen.png"      620 300 0.60

# StoreLogo + small square: fuller.
New-Asset "StoreLogo.png"         50  50  0.92
New-Asset "Square44x44Logo.png"   44  44  0.92

# Square44 target sizes for crisp taskbar / Start icons (fill the frame).
# Source is large, so target sizes up to 48 are downscales (sharp). No 256
# target size: it would exceed the 200 KB small-logo asset cap.
New-Asset "Square44x44Logo.targetsize-16.png"                  16 16 1.0
New-Asset "Square44x44Logo.targetsize-24.png"                  24 24 1.0
New-Asset "Square44x44Logo.targetsize-32.png"                  32 32 1.0
New-Asset "Square44x44Logo.targetsize-48.png"                  48 48 1.0
New-Asset "Square44x44Logo.targetsize-24_altform-unplated.png" 24 24 1.0
New-Asset "Square44x44Logo.targetsize-48_altform-unplated.png" 48 48 1.0

$src.Dispose()
Write-Host "Done."
