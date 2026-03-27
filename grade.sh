#!/usr/bin/env bash
set -euo pipefail

###############################################################################
# grade.sh — Top-level grader runner
#
# Runs the chain analysis graders and reports overall results.
###############################################################################

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
GRADER_DIR="$SCRIPT_DIR/grader"

# Colors
if [[ -t 1 ]]; then
  RED='\033[0;31m'
  GREEN='\033[0;32m'
  BOLD='\033[1m'
  NC='\033[0m'
else
  RED=''
  GREEN=''
  BOLD=''
  NC=''
fi

analysis_pass=0
reports_pass=0
docs_pass=0
overall=0

echo ""
echo -e "${BOLD}============================================================${NC}"
echo -e "${BOLD}         Sherlock — Chain Analysis Grading Suite             ${NC}"
echo -e "${BOLD}============================================================${NC}"

# --- Analysis grading ---
echo ""
echo -e "${BOLD}[1/3] Running analysis grader...${NC}"
echo ""

if "$GRADER_DIR/grade_analysis.sh"; then
  analysis_pass=1
  echo -e "${GREEN}Analysis grader: PASSED${NC}"
else
  echo -e "${RED}Analysis grader: FAILED${NC}"
fi

# --- Reports grading ---
echo ""
echo -e "${BOLD}[2/3] Running reports grader...${NC}"
echo ""

if "$GRADER_DIR/grade_reports.sh"; then
  reports_pass=1
  echo -e "${GREEN}Reports grader: PASSED${NC}"
else
  echo -e "${RED}Reports grader: FAILED${NC}"
fi

# --- Documentation grading ---
echo ""
echo -e "${BOLD}[3/3] Running documentation grader...${NC}"
echo ""

if "$GRADER_DIR/grade_docs.sh"; then
  docs_pass=1
  echo -e "${GREEN}Documentation grader: PASSED${NC}"
else
  echo -e "${RED}Documentation grader: FAILED${NC}"
fi

# --- Overall ---
echo ""
echo -e "${BOLD}============================================================${NC}"
echo -e "${BOLD}                     Overall Results                        ${NC}"
echo -e "${BOLD}============================================================${NC}"
echo ""

if [[ $analysis_pass -eq 1 ]]; then
  echo -e "  Analysis:      ${GREEN}PASS${NC}"
else
  echo -e "  Analysis:      ${RED}FAIL${NC}"
fi

if [[ $reports_pass -eq 1 ]]; then
  echo -e "  Reports:       ${GREEN}PASS${NC}"
else
  echo -e "  Reports:       ${RED}FAIL${NC}"
fi

if [[ $docs_pass -eq 1 ]]; then
  echo -e "  Documentation: ${GREEN}PASS${NC}"
else
  echo -e "  Documentation: ${RED}FAIL${NC}"
fi

echo ""

if [[ $analysis_pass -eq 1 && $reports_pass -eq 1 && $docs_pass -eq 1 ]]; then
  echo -e "  ${GREEN}${BOLD}ALL GRADERS PASSED${NC}"
  echo ""
  exit 0
else
  echo -e "  ${RED}${BOLD}SOME GRADERS FAILED${NC}"
  echo ""
  exit 1
fi
