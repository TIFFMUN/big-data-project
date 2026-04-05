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

# ── S3 Storage Costs ─────────────────────────────────────────────────────
echo ""
echo -e "${CYAN}S3 Storage (data retained):${NC}"

# Get bucket name from terraform output or environment
BUCKET_NAME=$(cd "$PROJECT_DIR/terraform" && terraform output -raw s3_bucket_name 2>/dev/null || echo "")

if [ -z "$BUCKET_NAME" ]; then
  # Try to find bucket by tag
  BUCKET_NAME=$(aws s3api list-buckets --query 'Buckets[].Name' --output text 2>/dev/null | tr '\t' '\n' | grep -i bigdata | head -1 || echo "")
fi

if [ -n "$BUCKET_NAME" ]; then
  echo "  Bucket: $BUCKET_NAME"

  # Calculate total size using CloudWatch metrics (more accurate)
  BUCKET_SIZE_BYTES=$(aws cloudwatch get-metric-statistics \
    --namespace AWS/S3 \
    --metric-name BucketSizeBytes \
    --dimensions Name=BucketName,Value="$BUCKET_NAME" Name=StorageType,Value=StandardStorage \
    --start-time "$(date -u -v-2d +%Y-%m-%dT%H:%M:%S 2>/dev/null || date -u -d '2 days ago' +%Y-%m-%dT%H:%M:%S)" \
    --end-time "$(date -u +%Y-%m-%dT%H:%M:%S)" \
    --period 86400 \
    --statistics Average \
    --query 'Datapoints[-1].Average' \
    --output text 2>/dev/null)

  if [ "$BUCKET_SIZE_BYTES" != "None" ] && [ -n "$BUCKET_SIZE_BYTES" ] && [ "$BUCKET_SIZE_BYTES" != "null" ]; then
    # Convert bytes to GB
    SIZE_GB=$(echo "scale=2; $BUCKET_SIZE_BYTES / 1024 / 1024 / 1024" | bc)
    COST_MONTH=$(echo "scale=2; $SIZE_GB * 0.023" | bc)

    echo -e "  📦 Size: ${BOLD}${SIZE_GB} GB${NC}"
    echo -e "  💰 Estimated cost: ${GREEN}\$${COST_MONTH}/month${NC} (at \$0.023/GB)"
  else
    # Fallback: use S3 API to calculate (slower but works immediately)
    echo "  Calculating bucket size..."
    TOTAL_SIZE=$(aws s3 ls s3://"$BUCKET_NAME" --recursive --summarize 2>/dev/null | grep "Total Size" | awk '{print $3}')

    if [ -n "$TOTAL_SIZE" ] && [ "$TOTAL_SIZE" -gt 0 ] 2>/dev/null; then
      SIZE_GB=$(echo "scale=2; $TOTAL_SIZE / 1024 / 1024 / 1024" | bc)
      COST_MONTH=$(echo "scale=2; $SIZE_GB * 0.023" | bc)

      echo -e "  📦 Size: ${BOLD}${SIZE_GB} GB${NC}"
      echo -e "  💰 Estimated cost: ${GREEN}\$${COST_MONTH}/month${NC} (at \$0.023/GB)"
    else
      echo -e "  ${YELLOW}Bucket is empty or data not available yet${NC}"
    fi
  fi

  # Show object count by prefix
  echo ""
  echo "  Objects by prefix:"
  for prefix in "raw" "processed" "scripts" "athena-results" "emr-logs"; do
    COUNT=$(aws s3 ls s3://"$BUCKET_NAME"/"$prefix"/ --recursive 2>/dev/null | wc -l | xargs)
    if [ "$COUNT" -gt 0 ] 2>/dev/null; then
      echo "    /$prefix/: $COUNT files"
    fi
  done
else
  echo -e "  ${YELLOW}No bucket found${NC}"
fi

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

