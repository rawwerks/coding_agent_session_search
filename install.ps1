Param(
  [string]$Version = "v0.1.0",
  [string]$Dest = "$HOME/.local/bin",
  [string]$Owner = "coding-agent-search",
  [string]$Repo = "coding-agent-search",
  [string]$Checksum = "",
  [string]$ChecksumUrl = "",
  [string]$ArtifactUrl = "",
  [switch]$EasyMode,
  [switch]$Verify
)

$ErrorActionPreference = "Stop"
$os = "windows"
$arch = if ([Environment]::Is64BitProcess) { "x86_64" } else { "x86" }
$zip = "coding-agent-search-$Version-$os-$arch.zip"
$url = if ($ArtifactUrl) { $ArtifactUrl } else { "https://github.com/$Owner/$Repo/releases/download/$Version/$zip" }
$tmp = New-TemporaryFile | Split-Path

Write-Host "Downloading $url"
Invoke-WebRequest -Uri $url -OutFile "$tmp/$zip"

$checksumToUse = $Checksum
if (-not $checksumToUse) {
  if (-not $ChecksumUrl) { $ChecksumUrl = "$url.sha256" }
  Write-Host "Fetching checksum from $ChecksumUrl"
  try { $checksumToUse = (Invoke-WebRequest -Uri $ChecksumUrl -UseBasicParsing).Content.Trim() } catch { Write-Error "Checksum file not found; refusing to install."; exit 1 }
}

$hash = Get-FileHash "$tmp/$zip" -Algorithm SHA256
if ($hash.Hash.ToLower() -ne $checksumToUse.ToLower()) { Write-Error "Checksum mismatch"; exit 1 }

Add-Type -AssemblyName System.IO.Compression.FileSystem
[System.IO.Compression.ZipFile]::ExtractToDirectory("$tmp/$zip", $tmp)

New-Item -ItemType Directory -Force -Path $Dest | Out-Null
Copy-Item "$tmp/coding-agent-search.exe" "$Dest" -Force

Write-Host "Installed to $Dest/coding-agent-search.exe"
$path = [Environment]::GetEnvironmentVariable("PATH", "User")
if (-not $path.Contains($Dest)) {
  if ($EasyMode) {
    [Environment]::SetEnvironmentVariable("PATH", "$path;$Dest", "User")
    Write-Host "Added $Dest to PATH (User)"
  } else {
    Write-Host "Add $Dest to PATH to use coding-agent-search"
  }
}

if ($Verify) {
  & "$Dest/coding-agent-search.exe" --version | Write-Host
}
