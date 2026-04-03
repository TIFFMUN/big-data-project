###############################################################################
# cost-check.ps1 — Check your current AWS spending (Windows PowerShell)
#
# Usage: powershell -ExecutionPolicy Bypass -File cost-check.ps1
###############################################################################

$ErrorActionPreference = "SilentlyContinue"
$ProjectDir = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)

# Source .env if it exists
$envFile = Join-Path $ProjectDir "terraform\.env"
if (Test-Path $envFile) {
    Get-Content $envFile | ForEach-Object {
        if ($_ -match '^\s*export\s+(\w+)=["'']?(.+?)["'']?\s*$') {
            [System.Environment]::SetEnvironmentVariable($Matches[1], $Matches[2], "Process")
        } elseif ($_ -match '^\s*(\w+)=["'']?(.+?)["'']?\s*$' -and $_ -notmatch '^\s*#') {
            [System.Environment]::SetEnvironmentVariable($Matches[1], $Matches[2], "Process")
        }
    }
}

Write-Host ""
Write-Host "=============================================" -ForegroundColor White
Write-Host "  AWS Cost Dashboard" -ForegroundColor White
Write-Host "=============================================" -ForegroundColor White

$today = (Get-Date).ToString("yyyy-MM-dd")
$monthStart = (Get-Date -Day 1).ToString("yyyy-MM-dd")
$tomorrow = (Get-Date).AddDays(1).ToString("yyyy-MM-dd")

# This month's cost
Write-Host ""
Write-Host "This month's cost ($monthStart to $today):" -ForegroundColor Cyan
try {
    $costJson = aws ce get-cost-and-usage `
        --time-period "Start=$monthStart,End=$tomorrow" `
        --granularity MONTHLY `
        --metrics "UnblendedCost" `
        --output json 2>$null | ConvertFrom-Json
    $amount = [math]::Round([double]$costJson.ResultsByTime[0].Total.UnblendedCost.Amount, 2)
    $unit = $costJson.ResultsByTime[0].Total.UnblendedCost.Unit
    Write-Host "  `$${amount} ${unit}" -ForegroundColor Yellow
} catch {
    Write-Host "  Cost Explorer not available yet (takes 24hrs to activate)" -ForegroundColor Yellow
}

# Cost by service
Write-Host ""
Write-Host "Cost by service (this month):" -ForegroundColor Cyan
try {
    $serviceJson = aws ce get-cost-and-usage `
        --time-period "Start=$monthStart,End=$tomorrow" `
        --granularity MONTHLY `
        --metrics "UnblendedCost" `
        --group-by "Type=DIMENSION,Key=SERVICE" `
        --output json 2>$null | ConvertFrom-Json
    foreach ($group in $serviceJson.ResultsByTime[0].Groups) {
        $svcCost = [math]::Round([double]$group.Metrics.UnblendedCost.Amount, 4)
        if ($svcCost -gt 0) {
            $svcName = $group.Keys[0]
            Write-Host ("  {0,-45} `${1}" -f $svcName, $svcCost)
        }
    }
} catch {
    Write-Host "  Not available yet" -ForegroundColor Yellow
}

# Running EC2 instances
Write-Host ""
Write-Host "Running EC2 instances:" -ForegroundColor Cyan
try {
    aws ec2 describe-instances `
        --filters "Name=instance-state-name,Values=running" "Name=tag:Project,Values=big-data-project" `
        --query "Reservations[].Instances[].[InstanceId,InstanceType,PublicIpAddress]" `
        --output table
} catch {
    Write-Host "  None found"
}

# Active EMR clusters
Write-Host ""
Write-Host "Active EMR clusters:" -ForegroundColor Cyan
try {
    aws emr list-clusters --active --query "Clusters[].{ID:Id,Name:Name,State:Status.State}" --output table
} catch {
    Write-Host "  None found"
}

Write-Host ""
Write-Host "============================================="
Write-Host ""

