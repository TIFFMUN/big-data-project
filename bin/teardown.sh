#!/usr/bin/env bash
###############################################################################
# teardown.sh — Stop everything that costs money
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
#   chmod +x teardown.sh
#   ./teardown.sh
###############################################################################
set -euo pipefail

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m'

info()  { echo -e "${CYAN}[INFO]${NC}  $1"; }
ok()    { echo -e "${GREEN}[OK]${NC}    $1"; }
warn()  { echo -e "${YELLOW}[WARN]${NC}  $1"; }
err()   { echo -e "${RED}[ERROR]${NC} $1"; }

PROJECT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
TF_DIR="$PROJECT_DIR/terraform"
AIRFLOW_DIR="$PROJECT_DIR/airflow"

echo ""
echo "============================================="
echo -e "  ${RED}Big Data Project — Teardown${NC}"
echo "============================================="
echo ""
warn "This will stop all PAID resources (EC2 + EMR) and Airflow."
echo "  Free resources (S3, IAM, security groups, Glue, Athena) will be kept."
echo ""
read -rp "  Are you sure? [y/N]: " CONFIRM

if [[ ! "$CONFIRM" =~ ^[Yy]$ ]]; then
  info "Aborted."
  exit 0
fi

###############################################################################
# Step 1 — Stop Airflow
###############################################################################
echo ""
info "Stopping Airflow containers..."

cd "$AIRFLOW_DIR"
if docker compose ps -q >/dev/null 2>&1; then
  docker compose down -v
  ok "Airflow stopped and volumes removed."
else
  ok "Airflow was not running."
fi

###############################################################################
# Step 2 — Terminate all active EMR clusters
###############################################################################
echo ""
info "Checking for active EMR clusters..."

# Source credentials
for ENV_FILE in "$TF_DIR/.env" "$PROJECT_DIR/.env"; do
  if [ -f "$ENV_FILE" ]; then
    source "$ENV_FILE"
    break
  fi
done

CLUSTER_IDS=$(aws emr list-clusters --active --query "Clusters[].Id" --output text 2>/dev/null || echo "")
if [ -n "$CLUSTER_IDS" ]; then
  for CID in $CLUSTER_IDS; do
    info "Terminating EMR cluster: $CID"
    aws emr terminate-clusters --cluster-ids "$CID" 2>/dev/null || true
  done
  ok "All active EMR clusters terminating."
else
  ok "No active EMR clusters found."
fi

###############################################################################
# Step 3 — Terminate EC2 Airflow instance
###############################################################################
echo ""
info "Terminating EC2 Airflow instance..."

INSTANCE_ID=$(aws ec2 describe-instances \
  --filters "Name=tag:Name,Values=*airflow*" "Name=instance-state-name,Values=running,stopped,pending" \
  --query "Reservations[].Instances[].InstanceId" --output text 2>/dev/null || echo "")

if [ -n "$INSTANCE_ID" ]; then
  aws ec2 terminate-instances --instance-ids $INSTANCE_ID >/dev/null 2>&1 || true
  ok "Terminated EC2 instance(s): $INSTANCE_ID"
else
  ok "No Airflow EC2 instance found."
fi

###############################################################################
# Step 4 — Clean up local files
###############################################################################
echo ""
info "Cleaning up local files..."

[ -f "$AIRFLOW_DIR/.env" ] && rm "$AIRFLOW_DIR/.env" && ok "Removed airflow/.env"

echo ""
echo "============================================="
echo -e "  ${GREEN}✅ Teardown complete!${NC}"
echo "============================================="
echo ""
echo "  Stopped:   EC2, EMR, Airflow containers"
echo "  Kept:      S3 bucket, IAM, security groups, Glue, Athena (all free)"
echo ""
