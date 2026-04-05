###############################################################################
# setup.ps1 — One script to deploy EVERYTHING (Windows PowerShell)
#
# What it does:
#   1. Checks prerequisites (aws cli, terraform, docker)
#   2. Prompts for AWS credentials (if not already set)
#   3. Prompts for a unique S3 bucket name
#   4. Deploys infrastructure with Terraform
#   5. Uploads PySpark script to S3
#   6. Generates the Airflow .env file
#   7. Starts Airflow locally with Docker Compose
#   8. Prints the Airflow URL + next steps
#
# Usage:
#   powershell -ExecutionPolicy Bypass -File setup.ps1
###############################################################################

$ErrorActionPreference = "Stop"
$ProjectDir = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
$TfDir = Join-Path $ProjectDir "terraform"
$AirflowDir = Join-Path $ProjectDir "airflow"
$ScriptsDir = Join-Path $ProjectDir "scripts"

function Info($msg)  { Write-Host "[INFO]  $msg" -ForegroundColor Cyan }
function Ok($msg)    { Write-Host "[OK]    $msg" -ForegroundColor Green }
function Warn($msg)  { Write-Host "[WARN]  $msg" -ForegroundColor Yellow }
function Err($msg)   { Write-Host "[ERROR] $msg" -ForegroundColor Red }

function Test-S3BucketExists($bucketName) {
    $awsCmd = Get-Command aws -ErrorAction SilentlyContinue
    if (-not $awsCmd) {
        throw "AWS CLI not found in PATH."
    }

    $stdoutFile = [System.IO.Path]::GetTempFileName()
    $stderrFile = [System.IO.Path]::GetTempFileName()

    try {
        $proc = Start-Process -FilePath $awsCmd.Source `
            -ArgumentList @("s3api", "head-bucket", "--bucket", $bucketName) `
            -NoNewWindow -PassThru -Wait `
            -RedirectStandardOutput $stdoutFile `
            -RedirectStandardError $stderrFile

        $stderr = (Get-Content $stderrFile -Raw -ErrorAction SilentlyContinue)

        if ($proc.ExitCode -eq 0) {
            return @{ Exists = $true; Forbidden = $false; ErrorMessage = "" }
        }

        if ($stderr -match "Forbidden" -or $stderr -match "\b403\b") {
            return @{ Exists = $false; Forbidden = $true; ErrorMessage = $stderr.Trim() }
        }

        return @{ Exists = $false; Forbidden = $false; ErrorMessage = $stderr.Trim() }
    }
    finally {
        Remove-Item $stdoutFile, $stderrFile -Force -ErrorAction SilentlyContinue
    }
}

Write-Host ""
Write-Host "============================================="
Write-Host "  Big Data Project - Setup"
Write-Host "============================================="
Write-Host ""

###############################################################################
# Step 1 - Check prerequisites
###############################################################################
Info "Checking prerequisites..."

$missing = @()
if (-not (Get-Command aws -ErrorAction SilentlyContinue))       { $missing += "aws cli   -> https://docs.aws.amazon.com/cli/latest/userguide/install-cliv2.html" }
if (-not (Get-Command terraform -ErrorAction SilentlyContinue)) { $missing += "terraform -> https://developer.hashicorp.com/terraform/install" }
if (-not (Get-Command docker -ErrorAction SilentlyContinue))    { $missing += "docker    -> https://docs.docker.com/get-docker/" }

if ($missing.Count -gt 0) {
    Err "Missing tools:"
    $missing | ForEach-Object { Write-Host "       * $_" }
    exit 1
}

# Check Docker is running.
# `docker info` can emit warnings on stderr in some environments (e.g., WSL2),
# which PowerShell may treat as terminating errors with $ErrorActionPreference=Stop.
# Using --format avoids that noisy output while still proving the daemon is reachable.
try {
    $dockerServerVersion = docker info --format "{{.ServerVersion}}" 2>$null
    if (-not $dockerServerVersion) { throw "Docker daemon did not return a version." }
} catch {
    Err "Docker is not running. Start Docker Desktop first."
    exit 1
}

Ok "All prerequisites found."

###############################################################################
# Step 2 - AWS credentials
###############################################################################
Write-Host ""
Info "Checking AWS credentials..."

# Auto-source terraform/.env if it exists
$envFile = Join-Path $TfDir ".env"
if (Test-Path $envFile) {
    Info "Found $envFile - loading credentials..."
    Get-Content $envFile | ForEach-Object {
        if ($_ -match '^\s*export\s+(\w+)=["'']?(.+?)["'']?\s*$') {
            [System.Environment]::SetEnvironmentVariable($Matches[1], $Matches[2], "Process")
        } elseif ($_ -match '^\s*(\w+)=["'']?(.+?)["'']?\s*$' -and $_ -notmatch '^\s*#') {
            [System.Environment]::SetEnvironmentVariable($Matches[1], $Matches[2], "Process")
        }
    }
}

$callerIdentity = aws sts get-caller-identity 2>$null
if ($LASTEXITCODE -eq 0) {
    $accountId = ($callerIdentity | ConvertFrom-Json).Account
    Ok "AWS credentials OK - account: $accountId"
} else {
    Warn "No valid AWS credentials found."
    Write-Host ""
    Write-Host "  Options:"
    Write-Host "    1) Enter Access Key / Secret Key now"
    Write-Host "    2) I'll set them up myself (exit)"
    Write-Host ""
    $choice = Read-Host "  Choice [1/2]"

    if ($choice -eq "1") {
        $accessKey = Read-Host "  AWS_ACCESS_KEY_ID"
        $secretKey = Read-Host "  AWS_SECRET_ACCESS_KEY"
        $region = Read-Host "  AWS_DEFAULT_REGION [us-east-1]"
        if (-not $region) { $region = "us-east-1" }

        $env:AWS_ACCESS_KEY_ID = $accessKey
        $env:AWS_SECRET_ACCESS_KEY = $secretKey
        $env:AWS_DEFAULT_REGION = $region

        $callerIdentity = aws sts get-caller-identity 2>$null
        if ($LASTEXITCODE -ne 0) {
            Err "Credentials are invalid. Check your keys and try again."
            exit 1
        }
        Ok "Credentials verified."
    } else {
        Info "Run 'aws configure' or create terraform/.env, then re-run this script."
        exit 0
    }
}

$awsRegion = if ($env:AWS_DEFAULT_REGION) { $env:AWS_DEFAULT_REGION } else { "us-east-1" }

###############################################################################
# Step 3 - Prompt for config
###############################################################################
Write-Host ""
Info "Configuration"
Write-Host ""

$defaultBucket = "bigdata-$($env:USERNAME.ToLower() -replace '[^a-z0-9]','')-$(Get-Date -UFormat %s -Millisecond 0)"
$bucketName = Read-Host "  S3 bucket name (must be globally unique) [$defaultBucket]"
if (-not $bucketName) { $bucketName = $defaultBucket }

Write-Host ""
Ok "Config:"
Write-Host "       Bucket:   $bucketName"
Write-Host "       Region:   $awsRegion"

Write-Host ""
$confirm = Read-Host "  Continue? [Y/n]"
if (-not $confirm) { $confirm = "Y" }
if ($confirm -notin @("Y","y")) {
    Info "Aborted."
    exit 0
}

###############################################################################
# Step 4 - Terraform deploy
###############################################################################
Write-Host ""
Info "Deploying infrastructure with Terraform..."
Write-Host ""

Set-Location $TfDir

@"
aws_region  = "$awsRegion"
project     = "bigdata-project"
bucket_name = "$bucketName"

key_name     = ""
allowed_cidr = "0.0.0.0/0"

airflow_instance_type    = "t3.micro"
emr_master_instance_type = "m5.xlarge"
emr_core_instance_type   = "m5.xlarge"
emr_task_instance_type   = "m5.xlarge"
emr_idle_timeout         = 900

monthly_budget_limit = "150"
alert_emails         = []

tags = {
  Project     = "big-data-project"
  ManagedBy   = "terraform"
  Environment = "dev"
}
"@ | Set-Content -Path "terraform.tfvars"

Ok "Created terraform.tfvars"

Info "terraform init..."
terraform init -input=false
if ($LASTEXITCODE -ne 0) { Err "terraform init failed"; exit 1 }

# If the S3 bucket already exists (e.g. preserved from a previous teardown),
# import it into Terraform state so it won't try to recreate it.
$bucketCheck = Test-S3BucketExists -bucketName $bucketName
if ($bucketCheck.Forbidden) {
    Err "S3 bucket '$bucketName' already exists but is not accessible to this account. Choose a different bucket name."
    exit 1
}

if ($bucketCheck.Exists) {
    Info "S3 bucket '$bucketName' already exists - importing into Terraform state..."
    terraform import -var-file="terraform.tfvars" module.s3.aws_s3_bucket.data_lake $bucketName 2>$null
    terraform import -var-file="terraform.tfvars" module.s3.aws_s3_bucket_versioning.data_lake $bucketName 2>$null
    terraform import -var-file="terraform.tfvars" module.s3.aws_s3_bucket_public_access_block.data_lake $bucketName 2>$null
    Ok "Existing S3 bucket imported - your data is safe."
}

Info "terraform plan..."
terraform plan -input=false -var-file="terraform.tfvars"
if ($LASTEXITCODE -ne 0) { Err "terraform plan failed"; exit 1 }

Write-Host ""
$applyConfirm = Read-Host "  Apply this plan? [Y/n]"
if (-not $applyConfirm) { $applyConfirm = "Y" }
if ($applyConfirm -notin @("Y","y")) {
    Info "Aborted. Run 'cd terraform; terraform apply' when ready."
    exit 0
}

Info "terraform apply..."
terraform apply -auto-approve -input=false -var-file="terraform.tfvars"
if ($LASTEXITCODE -ne 0) { Err "terraform apply failed"; exit 1 }

Write-Host ""
Ok "Infrastructure deployed!"

$s3Bucket = terraform output -raw s3_bucket_name
$airflowIp = terraform output -raw airflow_public_ip 2>$null

###############################################################################
# Step 5 - Upload PySpark script to S3
###############################################################################
Write-Host ""
Info "Uploading PySpark script to S3..."

aws s3 cp "$ScriptsDir\ingest.py" "s3://$s3Bucket/scripts/ingest.py"
Ok "Uploaded ingest.py -> s3://$s3Bucket/scripts/"


###############################################################################
# Step 6 - Generate Airflow .env
###############################################################################
Write-Host ""
Info "Generating Airflow .env file..."

Set-Location $AirflowDir

$envContent = @"
AIRFLOW__CORE__EXECUTOR=LocalExecutor
AIRFLOW__CORE__LOAD_EXAMPLES=False
AIRFLOW__CORE__FERNET_KEY=ZmDfcTF7_60GrrY167zsiPd67pEvs0aGOv2oasOM1Pg=
AIRFLOW__WEBSERVER__SECRET_KEY=supersecretkey
AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:airflow@postgres:5432/airflow
AWS_DEFAULT_REGION=$awsRegion
S3_BUCKET=$s3Bucket
"@

if ($env:AWS_ACCESS_KEY_ID) {
    $envContent += "`nAWS_ACCESS_KEY_ID=$($env:AWS_ACCESS_KEY_ID)"
    $envContent += "`nAWS_SECRET_ACCESS_KEY=$($env:AWS_SECRET_ACCESS_KEY)"
}

$envContent | Set-Content -Path ".env" -NoNewline
Ok "Created airflow/.env"

###############################################################################
# Step 7 - Start Airflow with Docker Compose
###############################################################################
Write-Host ""
Info "Starting Airflow with Docker Compose..."
Write-Host ""

docker compose up -d --build

Write-Host ""
Ok "Airflow is starting up!"

###############################################################################
# Step 8 - Summary
###############################################################################
Write-Host ""
Write-Host "============================================="
Write-Host "  Setup complete!" -ForegroundColor Green
Write-Host "============================================="
Write-Host ""
Write-Host "  Airflow UI:   http://localhost:8080"
Write-Host "  Login:        admin / admin"
Write-Host ""
Write-Host "  S3 Bucket:    $s3Bucket"
if ($airflowIp) {
    Write-Host "  EC2 Airflow:  http://${airflowIp}:8080 (if using EC2)"
}
Write-Host ""
Write-Host "  Next steps:"
Write-Host "    1. Open http://localhost:8080"
Write-Host "    2. Unpause the 'big_data_pipeline' DAG"
Write-Host "    3. Click 'Trigger DAG' to run the pipeline"
Write-Host ""
Write-Host "  To tear everything down:"
Write-Host "    .\teardown.ps1"
Write-Host ""

