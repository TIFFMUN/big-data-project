#!/usr/bin/env bash
###############################################################################
# setup.sh — One script to deploy EVERYTHING
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
#   chmod +x setup.sh
#   ./setup.sh
#
# To tear everything down:
#   ./teardown.sh
###############################################################################
set -euo pipefail

# ── Colours ────────────────────────────────────────────────────────────────
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m' # No colour

info()  { echo -e "${CYAN}[INFO]${NC}  $1"; }
ok()    { echo -e "${GREEN}[OK]${NC}    $1"; }
warn()  { echo -e "${YELLOW}[WARN]${NC}  $1"; }
err()   { echo -e "${RED}[ERROR]${NC} $1"; }

PROJECT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
TF_DIR="$PROJECT_DIR/terraform"
AIRFLOW_DIR="$PROJECT_DIR/airflow"
SCRIPTS_DIR="$PROJECT_DIR/scripts"

###############################################################################
# Step 1 — Check prerequisites
###############################################################################
echo ""
echo "============================================="
echo "  Big Data Project — Setup"
echo "============================================="
echo ""

info "Checking prerequisites..."

MISSING=()
command -v aws       >/dev/null 2>&1 || MISSING+=("aws cli   → https://docs.aws.amazon.com/cli/latest/userguide/install-cliv2.html")
command -v terraform >/dev/null 2>&1 || MISSING+=("terraform → https://developer.hashicorp.com/terraform/install")
command -v docker    >/dev/null 2>&1 || MISSING+=("docker    → https://docs.docker.com/get-docker/")

if [ ${#MISSING[@]} -gt 0 ]; then
  err "Missing tools:"
  for m in "${MISSING[@]}"; do
    echo "       • $m"
  done
  exit 1
fi

# Check Docker is running
if ! docker info >/dev/null 2>&1; then
  err "Docker is not running. Start Docker Desktop first."
  exit 1
fi

ok "All prerequisites found."

###############################################################################
# Step 2 — AWS credentials
###############################################################################
echo ""
info "Checking AWS credentials..."

# Auto-source .env if it exists
for ENV_FILE in "$TF_DIR/.env" "$PROJECT_DIR/.env"; do
  if [ -f "$ENV_FILE" ]; then
    info "Found $ENV_FILE — loading credentials..."
    source "$ENV_FILE"
    break
  fi
done

if aws sts get-caller-identity >/dev/null 2>&1; then
  ACCOUNT_ID=$(aws sts get-caller-identity --query "Account" --output text)
  ok "AWS credentials OK — account: $ACCOUNT_ID"
else
  warn "No valid AWS credentials found."
  echo ""
  echo "  Options:"
  echo "    1) Enter Access Key / Secret Key now"
  echo "    2) I'll set them up myself (exit)"
  echo ""
  echo "  Tip: you can also create terraform/.env with:"
  echo "    export AWS_ACCESS_KEY_ID=\"...\""
  echo "    export AWS_SECRET_ACCESS_KEY=\"...\""
  echo ""
  read -rp "  Choice [1/2]: " AUTH_CHOICE

  if [ "$AUTH_CHOICE" = "1" ]; then
    read -rp "  AWS_ACCESS_KEY_ID: " AWS_ACCESS_KEY_ID
    read -rsp "  AWS_SECRET_ACCESS_KEY: " AWS_SECRET_ACCESS_KEY
    echo ""
    read -rp "  AWS_DEFAULT_REGION [us-east-1]: " AWS_DEFAULT_REGION
    AWS_DEFAULT_REGION="${AWS_DEFAULT_REGION:-us-east-1}"

    export AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_DEFAULT_REGION

    if ! aws sts get-caller-identity >/dev/null 2>&1; then
      err "Credentials are invalid. Check your keys and try again."
      exit 1
    fi
    ok "Credentials verified."
  else
    info "Create terraform/.env or run 'aws configure', then re-run this script."
    exit 0
  fi
fi

AWS_REGION="${AWS_DEFAULT_REGION:-${AWS_REGION:-us-east-1}}"

###############################################################################
# Step 3 — Prompt for config
###############################################################################
echo ""
info "Configuration"
echo ""

# Bucket name
DEFAULT_BUCKET="bigdata-$(whoami | tr '[:upper:]' '[:lower:]' | tr -cd 'a-z0-9')-$(date +%s)"
read -rp "  S3 bucket name (must be globally unique) [$DEFAULT_BUCKET]: " BUCKET_NAME
BUCKET_NAME="${BUCKET_NAME:-$DEFAULT_BUCKET}"

echo ""
ok "Config:"
echo "       Bucket:   $BUCKET_NAME"
echo "       Region:   $AWS_REGION"

echo ""
read -rp "  Continue? [Y/n]: " CONFIRM
CONFIRM="${CONFIRM:-Y}"
if [[ ! "$CONFIRM" =~ ^[Yy]$ ]]; then
  info "Aborted."
  exit 0
fi

###############################################################################
# Step 4 — Terraform deploy
###############################################################################
echo ""
info "Deploying infrastructure with Terraform..."
echo ""

cd "$TF_DIR"

# Create terraform.tfvars from example
cat > terraform.tfvars <<EOF
aws_region  = "$AWS_REGION"
project     = "bigdata-project"
bucket_name = "$BUCKET_NAME"

key_name     = ""
allowed_cidr = "0.0.0.0/0"

airflow_instance_type    = "t3.micro"
emr_master_instance_type = "m5.xlarge"
emr_core_instance_type   = "m5.xlarge"
emr_task_instance_type   = "m5.xlarge"
emr_idle_timeout         = 900

tags = {
  Project     = "big-data-project"
  ManagedBy   = "terraform"
  Environment = "dev"
}
EOF

ok "Created terraform.tfvars"

info "terraform init..."
terraform init -input=false

# If the S3 bucket already exists (e.g. preserved from a previous teardown),
# import it into Terraform state so it won't try to recreate it.
if aws s3api head-bucket --bucket "$BUCKET_NAME" 2>/dev/null; then
  info "S3 bucket '$BUCKET_NAME' already exists — importing into Terraform state..."
  terraform import -var-file="terraform.tfvars" module.s3.aws_s3_bucket.data_lake "$BUCKET_NAME" 2>/dev/null || true
  terraform import -var-file="terraform.tfvars" module.s3.aws_s3_bucket_versioning.data_lake "$BUCKET_NAME" 2>/dev/null || true
  terraform import -var-file="terraform.tfvars" module.s3.aws_s3_bucket_public_access_block.data_lake "$BUCKET_NAME" 2>/dev/null || true
  ok "Existing S3 bucket imported — your data is safe."
fi

info "terraform plan..."
terraform plan -input=false -var-file="terraform.tfvars"

echo ""
read -rp "  Apply this plan? [Y/n]: " APPLY_CONFIRM
APPLY_CONFIRM="${APPLY_CONFIRM:-Y}"
if [[ ! "$APPLY_CONFIRM" =~ ^[Yy]$ ]]; then
  info "Aborted. Run 'cd terraform && terraform apply' when ready."
  exit 0
fi

info "terraform apply..."
terraform apply -auto-approve -input=false -var-file="terraform.tfvars"

echo ""
ok "Infrastructure deployed!"

# Capture outputs
S3_BUCKET=$(terraform output -raw s3_bucket_name)
AIRFLOW_IP=$(terraform output -raw airflow_public_ip 2>/dev/null || echo "")
SUBNET_ID=$(aws ec2 describe-subnets \
  --filters "Name=vpc-id,Values=$(aws ec2 describe-vpcs --filters "Name=is-default,Values=true" --query "Vpcs[0].VpcId" --output text)" \
  --query "Subnets[0].SubnetId" --output text 2>/dev/null || echo "")

###############################################################################
# Step 5 — Upload PySpark script to S3
###############################################################################
echo ""
info "Uploading PySpark script to S3..."

aws s3 cp "$SCRIPTS_DIR/ingest.py" "s3://${S3_BUCKET}/scripts/ingest.py"
ok "Uploaded ingest.py → s3://${S3_BUCKET}/scripts/"


###############################################################################
# Step 6 — Generate Airflow .env
###############################################################################
echo ""
info "Generating Airflow .env file..."

cd "$AIRFLOW_DIR"

cat > .env <<EOF
# Auto-generated by setup.sh — $(date)

# Airflow core
AIRFLOW__CORE__EXECUTOR=LocalExecutor
AIRFLOW__CORE__LOAD_EXAMPLES=False
AIRFLOW__CORE__FERNET_KEY=ZmDfcTF7_60GrrY167zsiPd67pEvs0aGOv2oasOM1Pg=
AIRFLOW__WEBSERVER__SECRET_KEY=supersecretkey

# Postgres
AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:airflow@postgres:5432/airflow

# AWS
AWS_DEFAULT_REGION=${AWS_REGION}
S3_BUCKET=${S3_BUCKET}
SUBNET_ID=${SUBNET_ID}
KEY_NAME=
EOF

# If user entered credentials manually, pass them to Airflow containers too
if [ -n "${AWS_ACCESS_KEY_ID:-}" ]; then
  cat >> .env <<EOF
AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID}
AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY}
EOF
fi

ok "Created airflow/.env"

###############################################################################
# Step 7 — Start Airflow with Docker Compose
###############################################################################
echo ""
info "Starting Airflow with Docker Compose..."
echo ""

docker compose up -d --build

echo ""
ok "Airflow is starting up!"

###############################################################################
# Step 8 — Summary
###############################################################################
echo ""
echo "============================================="
echo -e "  ${GREEN}✅ Setup complete!${NC}"
echo "============================================="
echo ""
echo "  Airflow UI:   http://localhost:8080"
echo "  Login:        admin / admin"
echo ""
echo "  S3 Bucket:    $S3_BUCKET"
if [ -n "$AIRFLOW_IP" ]; then
echo "  EC2 Airflow:  http://${AIRFLOW_IP}:8080 (if using EC2)"
fi
echo ""
echo "  Next steps:"
echo "    1. Open http://localhost:8080"
echo "    2. Unpause the 'big_data_pipeline' DAG"
echo "    3. Click 'Trigger DAG' to run the pipeline"
echo ""
echo "  To tear everything down:"
echo "    ./teardown.sh"
echo ""

