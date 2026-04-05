#!/usr/bin/env bash
###############################################################################
# Deploy Airflow to EC2
#
# This script deploys your Airflow setup to the EC2 instance created by
# Terraform. It syncs your DAGs, creates the .env file, and starts Docker
# Compose.
#
# Usage:
#   ./bin/deploy-airflow-to-ec2.sh [key_path]
#
# Prerequisites:
#   - Terraform has been applied and EC2 instance is running
#   - SSH key is available (default: ~/.ssh/id_rsa)
###############################################################################

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

function log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

function log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

function log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Get script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
TERRAFORM_DIR="${PROJECT_ROOT}/terraform"

# Check if terraform directory exists
if [[ ! -d "${TERRAFORM_DIR}" ]]; then
    log_error "Terraform directory not found at ${TERRAFORM_DIR}"
    exit 1
fi

cd "${TERRAFORM_DIR}"

# Get EC2 public IP from Terraform output
log_info "Getting EC2 instance IP from Terraform..."
EC2_IP=$(terraform output -raw airflow_public_ip 2>/dev/null)
if [[ -z "${EC2_IP}" ]]; then
    log_error "Could not get EC2 IP from Terraform. Have you run 'terraform apply'?"
    exit 1
fi
log_info "EC2 IP: ${EC2_IP}"

# Get S3 bucket name from Terraform output
log_info "Getting S3 bucket name from Terraform..."
S3_BUCKET=$(terraform output -raw s3_bucket_name 2>/dev/null)
if [[ -z "${S3_BUCKET}" ]]; then
    log_error "Could not get S3 bucket name from Terraform."
    exit 1
fi
log_info "S3 Bucket: ${S3_BUCKET}"

# SSH key path
SSH_KEY="${1:-$HOME/.ssh/bigdata-project-airflow.pem}"
if [[ ! -f "${SSH_KEY}" ]]; then
    log_error "SSH key not found at ${SSH_KEY}"
    log_info "Usage: $0 [path_to_ssh_key]"
    log_info "Default key: ~/.ssh/bigdata-project-airflow.pem"
    exit 1
fi

# AWS region
AWS_REGION="${AWS_REGION:-$(grep 'aws_region' terraform.tfvars 2>/dev/null | cut -d'=' -f2 | tr -d ' "' || echo 'us-east-1')}"

# Test SSH connection
log_info "Testing SSH connection..."
if ! ssh -i "${SSH_KEY}" -o StrictHostKeyChecking=no -o ConnectTimeout=10 ec2-user@"${EC2_IP}" "echo 'SSH connection successful'" &>/dev/null; then
    log_error "Cannot SSH to ${EC2_IP}. Check your security group allows SSH from your IP."
    exit 1
fi
log_info "SSH connection successful!"

# Create project directory on EC2 if it doesn't exist
log_info "Creating project directory on EC2..."
ssh -i "${SSH_KEY}" ec2-user@"${EC2_IP}" "mkdir -p ~/big-data-project/airflow/{dags,logs,plugins} ~/big-data-project/scripts"

# Sync airflow files (excluding local volumes and Python cache)
log_info "Syncing Airflow files to EC2..."
rsync -avz --progress \
    -e "ssh -i ${SSH_KEY}" \
    --exclude 'logs/' \
    --exclude '__pycache__/' \
    --exclude '*.pyc' \
    --exclude '.env' \
    "${PROJECT_ROOT}/airflow/" \
    ec2-user@"${EC2_IP}":~/big-data-project/airflow/

# Sync scripts directory
log_info "Syncing scripts to EC2..."
rsync -avz --progress \
    -e "ssh -i ${SSH_KEY}" \
    --exclude '__pycache__/' \
    --exclude '*.pyc' \
    "${PROJECT_ROOT}/scripts/" \
    ec2-user@"${EC2_IP}":~/big-data-project/scripts/

# Create .env file on EC2
log_info "Creating .env file on EC2..."
ssh -i "${SSH_KEY}" ec2-user@"${EC2_IP}" "cat > ~/big-data-project/airflow/.env" <<EOF
# Airflow Core
AIRFLOW_UID=50000
AIRFLOW__CORE__EXECUTOR=LocalExecutor
AIRFLOW__CORE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:airflow@postgres:5432/airflow
AIRFLOW__CORE__FERNET_KEY=$(python3 -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())" 2>/dev/null || echo "81HqDtbqAywKSOumSha3BhWNOdQ26slT6K0YaZeZyPs=")
AIRFLOW__CORE__LOAD_EXAMPLES=False
AIRFLOW__WEBSERVER__SECRET_KEY=$(openssl rand -hex 32)

# AWS Configuration
AWS_DEFAULT_REGION=${AWS_REGION}
AWS_REGION=${AWS_REGION}
S3_BUCKET=${S3_BUCKET}

# Kaggle Credentials (optional - for landing raw data)
# KAGGLE_USERNAME=your_username
# KAGGLE_KEY=your_api_key

# Dataset Configuration
DATASET_SUBDIR=airline_data
EOF

log_info ".env file created successfully!"

# Start Docker Compose
log_info "Starting Airflow with Docker Compose..."
ssh -i "${SSH_KEY}" ec2-user@"${EC2_IP}" << 'ENDSSH'
cd ~/big-data-project/airflow
docker compose down || true
docker compose up -d --build
echo "Waiting for Airflow to start..."
sleep 30
docker compose ps
ENDSSH

log_info ""
log_info "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
log_info "${GREEN}✓ Airflow deployed successfully!${NC}"
log_info ""
log_info "Airflow UI:  http://${EC2_IP}:8080"
log_info "Username:    admin"
log_info "Password:    admin"
log_info ""
log_info "S3 Bucket:   ${S3_BUCKET}"
log_info ""
log_info "To view logs:"
log_info "  ssh -i ${SSH_KEY} ec2-user@${EC2_IP}"
log_info "  cd ~/big-data-project/airflow"
log_info "  docker compose logs -f"
log_info "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
