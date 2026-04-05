#!/usr/bin/env bash
###############################################################################
# Stop Airflow on EC2 (to save costs when not in use)
#
# This script stops Docker Compose on the EC2 instance to reduce costs.
# The EC2 instance keeps running but Airflow containers are stopped.
#
# Usage:
#   ./bin/stop-airflow.sh [key_path]
###############################################################################

set -e

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

function log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

function log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
TERRAFORM_DIR="${PROJECT_ROOT}/terraform"

cd "${TERRAFORM_DIR}"

# Get EC2 IP
log_info "Getting EC2 instance IP..."
EC2_IP=$(terraform output -raw airflow_public_ip 2>/dev/null)
if [[ -z "${EC2_IP}" ]]; then
    log_error "Could not get EC2 IP. Have you run terraform apply?"
    exit 1
fi

# SSH key
SSH_KEY="${1:-$HOME/.ssh/bigdata-project-airflow.pem}"
if [[ ! -f "${SSH_KEY}" ]]; then
    log_error "SSH key not found at ${SSH_KEY}"
    log_info "Default key: ~/.ssh/bigdata-project-airflow.pem"
    exit 1
fi

log_info "Stopping Airflow on ${EC2_IP}..."
ssh -i "${SSH_KEY}" ec2-user@"${EC2_IP}" << 'ENDSSH'
cd ~/big-data-project/airflow
docker compose down
echo "Airflow stopped"
ENDSSH

log_info "${GREEN}✓ Airflow stopped successfully!${NC}"
log_info "The EC2 instance is still running. To fully stop costs, use:"
log_info "  aws ec2 stop-instances --instance-ids \$(cd terraform && terraform output -raw airflow_instance_id)"
