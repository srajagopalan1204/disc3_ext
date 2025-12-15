Param(
  [Parameter(Mandatory=$true)][string]$Config,
  [Parameter(Mandatory=$true)][string]$Saamm,
  [Parameter(Mandatory=$true)][string]$PricingRoot,
  [Parameter(Mandatory=$true)][string]$Out,
  [switch]$DryRun,
  [switch]$WriteFinal
)

if (-not ($DryRun -xor $WriteFinal)) {
  Write-Host "Specify exactly one of -DryRun or -WriteFinal" -ForegroundColor Yellow
  exit 1
}

$lines = Get-Content $Config | Where-Object { $_ -and -not $_.StartsWith("#") }
foreach ($line in $lines) {
  $pricing = Join-Path $PricingRoot "Pricing_$($line)_Price_sheet.xlsx"
  & ./scripts/run_line.ps1 -Line $line -Saamm $Saamm -Pricing $pricing -Out $Out @PSBoundParameters
}
