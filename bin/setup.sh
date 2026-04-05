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

# Check if Airflow .env already has a bucket configured
AIRFLOW_BUCKET=""
if [ -f "$AIRFLOW_DIR/.env" ]; then
  AIRFLOW_BUCKET=$(grep "^S3_BUCKET=" "$AIRFLOW_DIR/.env" 2>/dev/null | cut -d= -f2)
  if [ -n "$AIRFLOW_BUCKET" ]; then
    info "Found bucket in airflow/.env: $AIRFLOW_BUCKET"
  fi
fi

# Check for existing buckets from this project
EXISTING_BUCKETS=$(aws s3api list-buckets --query "Buckets[?starts_with(Name, 'bigdata-')].Name" --output text 2>/dev/null || echo "")

if [ -n "$EXISTING_BUCKETS" ]; then
  echo "  Found existing bucket(s) from this project:"
  for bucket in $EXISTING_BUCKETS; do
    if [ "$bucket" = "$AIRFLOW_BUCKET" ]; then
      echo "    - $bucket (configured in Airflow) ⭐"
    else
      echo "    - $bucket"
    fi
  done
  echo ""

  # Suggest Airflow bucket as default if it exists
  if [ -n "$AIRFLOW_BUCKET" ]; then
    read -rp "  Reuse bucket? [$AIRFLOW_BUCKET]: " REUSE_BUCKET
    REUSE_BUCKET="${REUSE_BUCKET:-$AIRFLOW_BUCKET}"
  else
    read -rp "  Reuse existing bucket? Enter bucket name or leave blank for new: " REUSE_BUCKET
  fi

  if [ -n "$REUSE_BUCKET" ]; then
    BUCKET_NAME="$REUSE_BUCKET"
    ok "Reusing existing bucket: $BUCKET_NAME"
  fi
fi

if [ -z "$BUCKET_NAME" ]; then
  # Bucket name
  DEFAULT_BUCKET="bigdata-$(whoami | tr '[:upper:]' '[:lower:]' | tr -cd 'a-z0-9')-$(date +%s)"
  read -rp "  S3 bucket name (must be globally unique) [$DEFAULT_BUCKET]: " BUCKET_NAME
  BUCKET_NAME="${BUCKET_NAME:-$DEFAULT_BUCKET}"
fi

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
# Step 4 — Create/check SSH key pair
###############################################################################
echo ""
info "Checking SSH key pair for EC2 access..."

KEY_NAME="bigdata-project-airflow"
SSH_KEY="$HOME/.ssh/${KEY_NAME}.pem"

# Check if key pair exists in AWS
if aws ec2 describe-key-pairs --key-names "$KEY_NAME" >/dev/null 2>&1; then
  ok "AWS key pair '$KEY_NAME' exists"
  if [ -f "$SSH_KEY" ]; then
    ok "Local SSH key found: $SSH_KEY"
  else
    warn "Local SSH key not found at $SSH_KEY"
    warn "You may need to recreate the key pair if you lost the private key."
    read -rp "  Delete and recreate key pair? [y/N]: " RECREATE_KEY
    if [[ "$RECREATE_KEY" =~ ^[Yy]$ ]]; then
      aws ec2 delete-key-pair --key-name "$KEY_NAME"
      info "Creating new key pair..."
      aws ec2 create-key-pair --key-name "$KEY_NAME" --query 'KeyMaterial' --output text > "$SSH_KEY"
      chmod 400 "$SSH_KEY"
      ok "Created new key pair: $SSH_KEY"
    fi
  fi
else
  info "Creating AWS key pair '$KEY_NAME'..."
  aws ec2 create-key-pair --key-name "$KEY_NAME" --query 'KeyMaterial' --output text > "$SSH_KEY"
  chmod 400 "$SSH_KEY"
  ok "Created key pair: $SSH_KEY"
fi

###############################################################################
# Step 5 — Terraform deploy
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

key_name     = "$KEY_NAME"
allowed_cidr = "0.0.0.0/0"

airflow_instance_type    = "t3.small"
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

# Check for existing Glue resources and import them
GLUE_DB_NAME="bigdata_project_db"
if aws glue get-database --name "$GLUE_DB_NAME" 2>/dev/null >/dev/null; then
  info "Glue database '$GLUE_DB_NAME' already exists — importing..."
  terraform import -var-file="terraform.tfvars" module.glue.aws_glue_catalog_database.data_lake "${ACCOUNT_ID}:${GLUE_DB_NAME}" 2>/dev/null || true
fi

GLUE_CRAWLER_NAME="bigdata-project-processed-crawler"
if aws glue get-crawler --name "$GLUE_CRAWLER_NAME" 2>/dev/null >/dev/null; then
  info "Glue crawler '$GLUE_CRAWLER_NAME' already exists — importing..."
  terraform import -var-file="terraform.tfvars" module.glue.aws_glue_crawler.processed "$GLUE_CRAWLER_NAME" 2>/dev/null || true
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
# Step 6 — Upload PySpark script to S3
###############################################################################
echo ""
info "Uploading PySpark script to S3..."

aws s3 cp "$SCRIPTS_DIR/ingest.py" "s3://${S3_BUCKET}/scripts/ingest.py"
ok "Uploaded ingest.py → s3://${S3_BUCKET}/scripts/"


###############################################################################
# Step 7 — Generate Airflow .env
###############################################################################
echo ""
info "Generating Airflow .env file..."

cd "$AIRFLOW_DIR"

# Check if .env already exists and has a different bucket
if [ -f ".env" ]; then
  EXISTING_BUCKET=$(grep "^S3_BUCKET=" .env 2>/dev/null | cut -d= -f2)
  if [ -n "$EXISTING_BUCKET" ] && [ "$EXISTING_BUCKET" != "$S3_BUCKET" ]; then
    warn "Airflow .env has different bucket: $EXISTING_BUCKET"
    warn "Updating to match Terraform: $S3_BUCKET"
  fi
fi

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

ok "Created airflow/.env with bucket: $S3_BUCKET"

###############################################################################
# Step 8 — Deploy Airflow to EC2
###############################################################################
echo ""
info "Deploying Airflow to EC2 instance..."
echo ""

# Check if we have an EC2 instance
if [ -z "$AIRFLOW_IP" ]; then
  warn "No EC2 Airflow instance found in Terraform outputs."
  warn "Skipping cloud deployment."
else
  if [ ! -f "$SSH_KEY" ]; then
    warn "SSH key not found at $SSH_KEY"
    warn "Skipping EC2 deployment. To deploy later, run:"
    warn "  ./bin/deploy-airflow-to-ec2.sh $SSH_KEY"
  else
    info "SSH key found: $SSH_KEY"
    info "Deploying to EC2 at $AIRFLOW_IP..."

    # Wait for instance to be ready
    info "Waiting for EC2 instance to be ready (this may take 2-3 minutes)..."
    sleep 30

    # Test SSH connection with retries
    MAX_RETRIES=10
    RETRY_COUNT=0
    while [ $RETRY_COUNT -lt $MAX_RETRIES ]; do
      if ssh -i "$SSH_KEY" -o StrictHostKeyChecking=no -o ConnectTimeout=10 ec2-user@"$AIRFLOW_IP" "echo 'SSH ready'" >/dev/null 2>&1; then
        ok "SSH connection successful!"
        break
      else
        RETRY_COUNT=$((RETRY_COUNT + 1))
        if [ $RETRY_COUNT -lt $MAX_RETRIES ]; then
          info "SSH not ready yet, retrying in 10 seconds... ($RETRY_COUNT/$MAX_RETRIES)"
          sleep 10
        else
          warn "Could not establish SSH connection after $MAX_RETRIES attempts."
          warn "The instance may still be booting. To deploy later, run:"
          warn "  ./bin/deploy-airflow-to-ec2.sh $SSH_KEY"
          break
        fi
      fi
    done

    # If SSH is ready, deploy
    if [ $RETRY_COUNT -lt $MAX_RETRIES ]; then
      # Create directories on EC2
      info "Creating directories on EC2..."
      ssh -i "$SSH_KEY" ec2-user@"$AIRFLOW_IP" "mkdir -p ~/big-data-project/airflow/{dags,logs,plugins} ~/big-data-project/scripts"

      # Sync Airflow files
      info "Syncing Airflow files to EC2..."
      rsync -avz --progress -e "ssh -i $SSH_KEY" \
        --exclude 'logs/' --exclude '__pycache__/' --exclude '*.pyc' --exclude '.env' \
        "$AIRFLOW_DIR/" ec2-user@"$AIRFLOW_IP":~/big-data-project/airflow/

      # Sync scripts
      info "Syncing scripts to EC2..."
      rsync -avz --progress -e "ssh -i $SSH_KEY" \
        --exclude '__pycache__/' --exclude '*.pyc' \
        "$SCRIPTS_DIR/" ec2-user@"$AIRFLOW_IP":~/big-data-project/scripts/

      # Create .env on EC2
      info "Creating .env file on EC2..."
      ssh -i "$SSH_KEY" ec2-user@"$AIRFLOW_IP" "cat > ~/big-data-project/airflow/.env" <<ENVEOF
# Auto-generated by setup.sh — $(date)

# Airflow core
AIRFLOW_UID=50000
AIRFLOW__CORE__EXECUTOR=LocalExecutor
AIRFLOW__CORE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:airflow@postgres:5432/airflow
AIRFLOW__CORE__FERNET_KEY=ZmDfcTF7_60GrrY167zsiPd67pEvs0aGOv2oasOM1Pg=
AIRFLOW__WEBSERVER__SECRET_KEY=supersecretkey
AIRFLOW__CORE__LOAD_EXAMPLES=False

# AWS
AWS_DEFAULT_REGION=${AWS_REGION}
AWS_REGION=${AWS_REGION}
S3_BUCKET=${S3_BUCKET}

# Dataset
DATASET_SUBDIR=airline_data
ENVEOF

      # Start Docker Compose on EC2
      info "Starting Airflow on EC2..."
      ssh -i "$SSH_KEY" ec2-user@"$AIRFLOW_IP" << 'ENDSSH'
cd ~/big-data-project/airflow
docker compose down 2>/dev/null || true
docker compose up -d --build
echo "Waiting for Airflow to start..."
sleep 45
docker compose ps
ENDSSH

      ok "Airflow deployed to EC2 successfully!"
      DEPLOY_SUCCESS=true
    fi
  fi
fi

###############################################################################
# Step 9 — Summary
###############################################################################
echo ""
echo "============================================="
echo -e "  ${GREEN}✅ Setup complete!${NC}"
echo "============================================="
echo ""

if [ -n "$AIRFLOW_IP" ] && [ "${DEPLOY_SUCCESS:-false}" = "true" ]; then
  echo "  Airflow UI:   http://${AIRFLOW_IP}:8080"
  echo "  Login:        admin / admin"
  echo ""
  echo "  S3 Bucket:    $S3_BUCKET"
  echo ""
  echo "  SSH Access:   ssh -i $SSH_KEY ec2-user@$AIRFLOW_IP"
  echo ""
  echo "  Next steps:"
  echo "    1. Open http://${AIRFLOW_IP}:8080"
  echo "    2. Unpause the 'big_data_pipeline' DAG"
  echo "    3. Click 'Trigger DAG' to run the pipeline"
  echo ""
  echo "  To update DAGs after changes:"
  echo "    ./bin/deploy-airflow-to-ec2.sh"
  echo ""
  echo "  To stop Airflow (save costs):"
  echo "    ./bin/stop-airflow.sh"
  echo ""
  echo "  To restart Airflow:"
  echo "    ./bin/start-airflow.sh"
else
  echo "  S3 Bucket:    $S3_BUCKET"
  if [ -n "$AIRFLOW_IP" ]; then
    echo "  EC2 IP:       $AIRFLOW_IP"
  fi
  echo ""
  echo "  Airflow was not deployed to EC2 in this run."
  echo "  To deploy manually:"
  echo "    ./bin/deploy-airflow-to-ec2.sh"
fi
echo ""
echo "  To tear everything down:"
echo "    ./bin/teardown.sh"
echo ""

