###############################################################################
# teardown.ps1 — Stop everything that costs money (Windows PowerShell)
#
# What it does:
#   1. Stops Airflow containers (local Docker)
#   2. Terminates any active EMR clusters
#   3. Terminates the EC2 Airflow instance
#   4. Cleans up local files (.env)
#
# What it KEEPS (all free / pennies):
#   - S3 bucket + your data
#   - IAM roles & policies
#   - Security groups
#   - Glue catalog & crawler definition
#   - Athena workgroup
#
# Usage:
#   powershell -ExecutionPolicy Bypass -File teardown.ps1
###############################################################################

$ErrorActionPreference = "Stop"
$ProjectDir = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
$TfDir = Join-Path $ProjectDir "terraform"
$AirflowDir = Join-Path $ProjectDir "airflow"

function Info($msg)  { Write-Host "[INFO]  $msg" -ForegroundColor Cyan }
function Ok($msg)    { Write-Host "[OK]    $msg" -ForegroundColor Green }
function Warn($msg)  { Write-Host "[WARN]  $msg" -ForegroundColor Yellow }
function Err($msg)   { Write-Host "[ERROR] $msg" -ForegroundColor Red }

Write-Host ""
Write-Host "============================================="
Write-Host "  Big Data Project - Teardown" -ForegroundColor Red
Write-Host "============================================="
Write-Host ""
Warn "This will stop all PAID resources (EC2 + EMR) and Airflow."
Write-Host "  Free resources (S3, IAM, security groups, Glue, Athena) will be kept."
Write-Host ""
$confirm = Read-Host "  Are you sure? [y/N]"

if ($confirm -notin @("y","Y")) {
    Info "Aborted."
    exit 0
}

# Load credentials from .env if present
$envFile = Join-Path $TfDir ".env"
if (Test-Path $envFile) {
    Get-Content $envFile | ForEach-Object {
        if ($_ -match '^\s*export\s+(\w+)=["'']?(.+?)["'']?\s*$') {
            [System.Environment]::SetEnvironmentVariable($Matches[1], $Matches[2], "Process")
        } elseif ($_ -match '^\s*(\w+)=["'']?(.+?)["'']?\s*$' -and $_ -notmatch '^\s*#') {
            [System.Environment]::SetEnvironmentVariable($Matches[1], $Matches[2], "Process")
        }
    }
}

###############################################################################
# Step 1 - Stop Airflow
###############################################################################
Write-Host ""
Info "Stopping Airflow containers..."

Set-Location $AirflowDir
try {
    docker compose down -v 2>$null
    Ok "Airflow stopped and volumes removed."
} catch {
    Ok "Airflow was not running."
}

###############################################################################
# Step 2 - Terminate all active EMR clusters
###############################################################################
Write-Host ""
Info "Checking for active EMR clusters..."

try {
    $clusters = aws emr list-clusters --active --query "Clusters[].Id" --output text 2>$null
    if ($clusters) {
        $clusterIds = $clusters -split "`t"
        $clusterIds | ForEach-Object {
            Info "Terminating EMR cluster: $_"
            aws emr terminate-clusters --cluster-ids $_ 2>$null
        }
        Ok "All active EMR clusters terminating."
    } else {
        Ok "No active EMR clusters found."
    }
} catch {
    Ok "No active EMR clusters found."
}

###############################################################################
# Step 3 - Terminate EC2 Airflow instance
###############################################################################
Write-Host ""
Info "Terminating EC2 Airflow instance..."

try {
    $instanceId = aws ec2 describe-instances `
        --filters "Name=tag:Name,Values=*airflow*" "Name=instance-state-name,Values=running,stopped,pending" `
        --query "Reservations[].Instances[].InstanceId" --output text 2>$null
    if ($instanceId) {
        aws ec2 terminate-instances --instance-ids $instanceId 2>$null | Out-Null
        Ok "Terminated EC2 instance(s): $instanceId"
    } else {
        Ok "No Airflow EC2 instance found."
    }
} catch {
    Ok "No Airflow EC2 instance found."
}

###############################################################################
# Step 4 - Clean up local files
###############################################################################
Write-Host ""
Info "Cleaning up local files..."

$envToRemove = Join-Path $AirflowDir ".env"
if (Test-Path $envToRemove) {
    Remove-Item $envToRemove -Force
    Ok "Removed $envToRemove"
}

Write-Host ""
Write-Host "============================================="
Write-Host "  Teardown complete!" -ForegroundColor Green
Write-Host "============================================="
Write-Host ""
Write-Host "  Stopped:   EC2, EMR, Airflow containers"
Write-Host "  Kept:      S3 bucket, IAM, security groups, Glue, Athena (all free)"
Write-Host ""

