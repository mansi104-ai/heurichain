#!/usr/bin/env bash
set -euo pipefail

###############################################################################
# grader/grade_docs.sh — Documentation validation
#
# Checks APPROACH.md and demo.md for completeness.
###############################################################################

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/lib/common.sh"

REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$REPO_ROOT"

print_header "Chain Analysis — Documentation Validation"

# ---------------------------------------------------------------------------
# APPROACH.md checks
# ---------------------------------------------------------------------------
print_section "APPROACH.md"

if [[ ! -f "APPROACH.md" ]]; then
  print_fail "APPROACH.md exists"
  print_summary
  exit 1
fi
print_pass "APPROACH.md exists"

# Check file size > 500 bytes
approach_size=$(wc -c < "APPROACH.md" | tr -d ' ')
if [[ "$approach_size" -gt 500 ]]; then
  print_pass "APPROACH.md is substantial (${approach_size} bytes)"
else
  print_fail "APPROACH.md size check" "Expected > 500 bytes, got ${approach_size} bytes"
fi

# Check for at least 5 heuristic section headings
# Look for markdown headings that reference heuristic-related terms
heuristic_ids=("cioh" "change.detection" "address.reuse" "coinjoin" "consolidation" "self.transfer" "peeling.chain" "op.return" "round.number" "fee.rate.anomaly" "script.type")

heuristic_sections=0
for pattern in "${heuristic_ids[@]}"; do
  if grep -qiE "(^#+.*${pattern}|${pattern})" "APPROACH.md"; then
    heuristic_sections=$((heuristic_sections + 1))
  fi
done

if [[ $heuristic_sections -ge 5 ]]; then
  print_pass "APPROACH.md references at least 5 heuristics ($heuristic_sections found)"
else
  print_fail "APPROACH.md heuristic coverage" "Expected references to at least 5 heuristics, found $heuristic_sections"
fi

# Check for mandatory heuristics
if grep -qiE "cioh|common.input.ownership" "APPROACH.md"; then
  print_pass "APPROACH.md covers CIOH"
else
  print_fail "APPROACH.md covers CIOH" "No mention of CIOH or Common Input Ownership"
fi

if grep -qiE "change.detection" "APPROACH.md"; then
  print_pass "APPROACH.md covers Change Detection"
else
  print_fail "APPROACH.md covers Change Detection"
fi

# ---------------------------------------------------------------------------
# demo.md checks
# ---------------------------------------------------------------------------
print_section "demo.md"

if [[ ! -f "demo.md" ]]; then
  print_fail "demo.md exists"
  print_summary
  exit 1
fi
print_pass "demo.md exists"

demo_content=$(cat "demo.md")

# Check it does NOT contain the placeholder URL
if echo "$demo_content" | grep -qF "https://example.com/demo-video-link"; then
  print_fail "demo.md has real video link" "Still contains placeholder URL"
else
  print_pass "demo.md does not contain placeholder URL"
fi

# Check URL matches YouTube/Loom/Google Drive pattern
if echo "$demo_content" | grep -qiE "(youtube\.com|youtu\.be|loom\.com|drive\.google\.com|vimeo\.com)"; then
  print_pass "demo.md contains valid video hosting URL"
else
  print_fail "demo.md video URL" "Expected YouTube, Loom, Google Drive, or Vimeo link"
fi

print_summary
[[ $FAIL_COUNT -eq 0 ]]
