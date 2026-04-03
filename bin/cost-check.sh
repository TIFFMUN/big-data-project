#!/usr/bin/env bash
###############################################################################
# cost-check.sh — Check your current AWS spending
#
# Shows:
#   - Today's cost so far
#   - This month's cost so far
#   - Cost breakdown by service
#   - Running EC2/EMR instances (what's costing you right now)
#
# Usage: ./cost-check.sh
###############################################################################
set -euo pipefail

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
BOLD='\033[1m'
NC='\033[0m'

PROJECT_DIR="$(cd "$(dirname "$0")/.." && pwd)"

# Source .env if it exists
for ENV_FILE in "$PROJECT_DIR/terraform/.env" "$PROJECT_DIR/.env"; do
  if [ -f "$ENV_FILE" ]; then
    source "$ENV_FILE"
    break
  fi
done

echo ""
echo -e "${BOLD}=============================================${NC}"
echo -e "${BOLD}  AWS Cost Dashboard${NC}"
echo -e "${BOLD}=============================================${NC}"

# ── Today's date math ─────────────────────────────────────────────────────
TODAY=$(date -u +%Y-%m-%d)
YESTERDAY=$(date -u -v-1d +%Y-%m-%d 2>/dev/null || date -u -d "yesterday" +%Y-%m-%d)
MONTH_START=$(date -u +%Y-%m-01)
TOMORROW=$(date -u -v+1d +%Y-%m-%d 2>/dev/null || date -u -d "tomorrow" +%Y-%m-%d)

# ── This month's total cost ──────────────────────────────────────────────
echo ""
echo -e "${CYAN}This month's cost (${MONTH_START} to ${TODAY}):${NC}"
aws ce get-cost-and-usage \
  --time-period "Start=${MONTH_START},End=${TOMORROW}" \
  --granularity MONTHLY \
  --metrics "UnblendedCost" \
  --query 'ResultsByTime[0].Total.UnblendedCost.[Amount,Unit]' \
  --output text 2>/dev/null | awk '{printf "  💰 $%.2f %s\n", $1, $2}' \
  || echo -e "  ${YELLOW}Cost Explorer not available yet (takes 24hrs to activate)${NC}"

# ── Cost breakdown by service ────────────────────────────────────────────
echo ""
echo -e "${CYAN}Cost by service (this month):${NC}"
aws ce get-cost-and-usage \
  --time-period "Start=${MONTH_START},End=${TOMORROW}" \
  --granularity MONTHLY \
  --metrics "UnblendedCost" \
  --group-by Type=DIMENSION,Key=SERVICE \
  --query 'ResultsByTime[0].Groups[?Metrics.UnblendedCost.Amount > `0`].{Service: Keys[0], Cost: Metrics.UnblendedCost.Amount}' \
  --output table 2>/dev/null \
  || echo -e "  ${YELLOW}Not available yet${NC}"

# ── Running EC2 instances ────────────────────────────────────────────────
echo ""
echo -e "${CYAN}Running EC2 instances (costing you right now):${NC}"
aws ec2 describe-instances \
  --filters "Name=instance-state-name,Values=running" "Name=tag:Project,Values=big-data-project" \
  --query 'Reservations[].Instances[].[InstanceId,InstanceType,PublicIpAddress,LaunchTime]' \
  --output table 2>/dev/null \
  || echo "  None found"

# ── EMR clusters ─────────────────────────────────────────────────────────
echo ""
echo -e "${CYAN}Active EMR clusters:${NC}"
aws emr list-clusters \
  --active \
  --query 'Clusters[].{ID: Id, Name: Name, State: Status.State}' \
  --output table 2>/dev/null \
  || echo "  None found"

# ── Estimated hourly burn rate ───────────────────────────────────────────
echo ""
echo -e "${CYAN}Estimated burn rate (while everything is running):${NC}"

EC2_COUNT=$(aws ec2 describe-instances \
  --filters "Name=instance-state-name,Values=running" "Name=tag:Project,Values=big-data-project" \
  --query 'length(Reservations[].Instances[])' \
  --output text 2>/dev/null || echo "0")

EMR_ACTIVE=$(aws emr list-clusters --active --query 'length(Clusters)' --output text 2>/dev/null || echo "0")

echo "  EC2 instances running: $EC2_COUNT"
echo "  EMR clusters active:   $EMR_ACTIVE"

if [ "$EMR_ACTIVE" -gt 0 ] 2>/dev/null; then
  echo -e "  ${RED}~\$0.77/hr (EMR) + ~\$0.01/hr (EC2 t3.micro) = ~\$0.78/hr${NC}"
  echo -e "  ${YELLOW}⚠ Run ./teardown.sh when done to stop charges!${NC}"
elif [ "$EC2_COUNT" -gt 0 ] 2>/dev/null; then
  echo -e "  ${GREEN}~\$0.01/hr (EC2 only — EMR is off)${NC}"
else
  echo -e "  ${GREEN}\$0.00/hr — nothing running!${NC}"
fi

echo ""
echo "============================================="
echo ""

