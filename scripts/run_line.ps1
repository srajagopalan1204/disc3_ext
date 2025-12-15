Param(
  [Parameter(Mandatory=$true)][string]$Line,
  [Parameter(Mandatory=$true)][string]$Saamm,
  [Parameter(Mandatory=$true)][string]$Pricing,
  [Parameter(Mandatory=$true)][string]$Out,
  [switch]$DryRun,
  [switch]$WriteFinal
)

if (-not ($DryRun -xor $WriteFinal)) {
  Write-Host "Specify exactly one of -DryRun or -WriteFinal" -ForegroundColor Yellow
  exit 1
}

$py = "python"
$mode = if ($DryRun) { "dryrun" } else { "writefinal" }
$cmd = "$py ./src/build_desc3.py --line $Line --saamm `"$Saamm`" --pricing `"$Pricing`" --out `"$Out`" --mode $mode"

Write-Host "Running: $cmd"
Invoke-Expression $cmd
