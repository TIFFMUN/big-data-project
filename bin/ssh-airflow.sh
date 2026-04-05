#!/usr/bin/env bash
###############################################################################
# SSH into Airflow EC2 instance
#
# Usage:
#   ./bin/ssh-airflow.sh
###############################################################################

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
TERRAFORM_DIR="${PROJECT_ROOT}/terraform"

cd "${TERRAFORM_DIR}"

# Get EC2 IP
EC2_IP=$(terraform output -raw airflow_public_ip 2>/dev/null)
if [[ -z "${EC2_IP}" ]]; then
    echo "ERROR: Could not get EC2 IP from Terraform"
    exit 1
fi

SSH_KEY="$HOME/.ssh/bigdata-project-airflow.pem"

echo "Connecting to ec2-user@${EC2_IP}..."
ssh -i "${SSH_KEY}" -o StrictHostKeyChecking=no ec2-user@"${EC2_IP}"
