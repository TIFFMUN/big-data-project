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

# S3 Storage Costs
Write-Host ""
Write-Host "S3 Storage (data retained):" -ForegroundColor Cyan

try {
    # Get bucket name from terraform output
    Push-Location (Join-Path $ProjectDir "terraform")
    $bucketName = terraform output -raw s3_bucket_name 2>$null
    Pop-Location

    if ([string]::IsNullOrEmpty($bucketName)) {
        # Fallback: find bucket by name pattern
        $buckets = aws s3api list-buckets --query "Buckets[].Name" --output text 2>$null
        $bucketName = $buckets -split "`t" | Where-Object { $_ -like "*bigdata*" } | Select-Object -First 1
    }

    if (-not [string]::IsNullOrEmpty($bucketName)) {
        Write-Host "  Bucket: $bucketName"

        # Try CloudWatch metrics first (more accurate)
        $twoDaysAgo = (Get-Date).AddDays(-2).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ss")
        $now = (Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ss")

        $metricsJson = aws cloudwatch get-metric-statistics `
            --namespace AWS/S3 `
            --metric-name BucketSizeBytes `
            --dimensions "Name=BucketName,Value=$bucketName" "Name=StorageType,Value=StandardStorage" `
            --start-time $twoDaysAgo `
            --end-time $now `
            --period 86400 `
            --statistics Average `
            --output json 2>$null | ConvertFrom-Json

        $sizeBytes = $null
        if ($metricsJson.Datapoints.Count -gt 0) {
            $sizeBytes = $metricsJson.Datapoints[-1].Average
        }

        if ($sizeBytes -and $sizeBytes -gt 0) {
            $sizeGB = [math]::Round($sizeBytes / 1GB, 2)
            $costMonth = [math]::Round($sizeGB * 0.023, 2)

            Write-Host "  📦 Size: $sizeGB GB" -ForegroundColor White
            Write-Host "  💰 Estimated cost: `$$costMonth/month (at `$0.023/GB)" -ForegroundColor Green
        } else {
            # Fallback: calculate from S3 API
            Write-Host "  Calculating bucket size..."
            $s3Objects = aws s3 ls "s3://$bucketName" --recursive --summarize 2>$null
            $totalSizeLine = $s3Objects | Select-String "Total Size:"

            if ($totalSizeLine) {
                $totalSize = [long]($totalSizeLine -replace '.*Total Size:\s*', '')
                $sizeGB = [math]::Round($totalSize / 1GB, 2)
                $costMonth = [math]::Round($sizeGB * 0.023, 2)

                Write-Host "  📦 Size: $sizeGB GB" -ForegroundColor White
                Write-Host "  💰 Estimated cost: `$$costMonth/month (at `$0.023/GB)" -ForegroundColor Green
            } else {
                Write-Host "  Bucket is empty or data not available yet" -ForegroundColor Yellow
            }
        }

        # Show object count by prefix
        Write-Host ""
        Write-Host "  Objects by prefix:"
        foreach ($prefix in @("raw", "processed", "scripts", "athena-results", "emr-logs")) {
            $count = (aws s3 ls "s3://$bucketName/$prefix/" --recursive 2>$null | Measure-Object).Count
            if ($count -gt 0) {
                Write-Host "    /$prefix/: $count files"
            }
        }
    } else {
        Write-Host "  No bucket found" -ForegroundColor Yellow
    }
} catch {
    Write-Host "  Unable to retrieve S3 storage info" -ForegroundColor Yellow
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

