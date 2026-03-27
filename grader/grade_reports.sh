#!/usr/bin/env bash
set -euo pipefail

###############################################################################
# grader/grade_reports.sh — Markdown report reproducibility validation
#
# Checks that Markdown reports exist in out/ and are reproducible.
###############################################################################

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/lib/common.sh"

REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$REPO_ROOT"

FIXTURES_DIR="$REPO_ROOT/fixtures"
XOR_FILE="$FIXTURES_DIR/xor.dat"

print_header "Chain Analysis — Markdown Report Validation"

# Step 1: Check out/ directory exists with committed Markdown reports
if [[ ! -d "out" ]]; then
  print_fail "out/ directory exists" "Directory not found — did you commit your output files?"
  print_summary
  exit 1
fi

committed_mds=()
for f in out/*.md; do
  [[ -f "$f" ]] || continue
  committed_mds+=("$f")
done

if [[ ${#committed_mds[@]} -eq 0 ]]; then
  print_fail "Committed Markdown reports exist" "No .md files found in out/"
  print_summary
  exit 1
fi
print_pass "Committed Markdown reports exist (${#committed_mds[@]} files)"

# Step 2: Check each committed report is > 1KB
for md_file in "${committed_mds[@]}"; do
  md_base=$(basename "$md_file")
  md_size=$(wc -c < "$md_file" | tr -d ' ')
  if [[ "$md_size" -gt 1024 ]]; then
    print_pass "$md_base size OK (${md_size} bytes)"
  else
    print_fail "$md_base size check" "Expected > 1024 bytes, got ${md_size} bytes"
  fi
done

# Step 3: Check expected files exist (one per blk fixture)
blk_files=()
for blk in "$FIXTURES_DIR"/blk*.dat; do
  [[ -f "$blk" ]] || continue
  blk_files+=("$blk")
done

for blk_file in "${blk_files[@]}"; do
  blk_base=$(basename "$blk_file")
  blk_stem="${blk_base%.dat}"
  expected_md="out/${blk_stem}.md"
  if [[ -f "$expected_md" ]]; then
    print_pass "Report exists for $blk_base: ${blk_stem}.md"
  else
    print_fail "Report exists for $blk_base" "Expected $expected_md"
  fi
done

# Step 4: Content validation — check for expected sections
for md_file in "${committed_mds[@]}"; do
  md_base=$(basename "$md_file")
  for section in "Summary" "Block" "Heuristic" "Fee Rate"; do
    if grep -qi "$section" "$md_file"; then
      print_pass "$md_base: contains '$section' section"
    else
      print_fail "$md_base: contains '$section' section"
    fi
  done
done

# Step 5: Save committed reports to temp directory for reproducibility check
TMPDIR_COMMITTED=$(mktemp -d)
for md_file in "${committed_mds[@]}"; do
  cp "$md_file" "$TMPDIR_COMMITTED/"
done

# Step 6: Re-run CLI to generate fresh outputs
if [[ ! -f "$XOR_FILE" ]]; then
  print_fail "xor.dat exists" "File not found: $XOR_FILE"
  rm -rf "$TMPDIR_COMMITTED"
  print_summary
  exit 1
fi

if [[ ${#blk_files[@]} -eq 0 ]]; then
  print_fail "Block fixtures found" "No blk*.dat files in $FIXTURES_DIR (run setup.sh first)"
  rm -rf "$TMPDIR_COMMITTED"
  print_summary
  exit 1
fi

# Remove existing outputs and regenerate
rm -rf out/

for blk_file in "${blk_files[@]}"; do
  blk_base=$(basename "$blk_file")
  rev_base="${blk_base/blk/rev}"
  rev_file="$FIXTURES_DIR/$rev_base"

  if [[ ! -f "$rev_file" ]]; then
    print_skip "Regenerate for $blk_base (no rev file)"
    continue
  fi

  print_section "Regenerating outputs for $blk_base"
  run_cli --block "$blk_file" "$rev_file" "$XOR_FILE"

  if [[ $CLI_EXIT -ne 0 ]]; then
    print_fail "Regeneration for $blk_base" "cli.sh exited with code $CLI_EXIT"
  else
    print_pass "Regeneration for $blk_base completed"
  fi
done

# Step 7: Compare committed vs fresh Markdown reports
print_section "Comparing committed vs fresh reports"

for committed_md in "$TMPDIR_COMMITTED"/*.md; do
  [[ -f "$committed_md" ]] || continue
  md_base=$(basename "$committed_md")
  fresh_md="out/$md_base"

  if [[ ! -f "$fresh_md" ]]; then
    print_fail "$md_base reproduced" "Fresh report not generated"
    continue
  fi

  committed_size=$(wc -c < "$committed_md" | tr -d ' ')
  fresh_size=$(wc -c < "$fresh_md" | tr -d ' ')

  if [[ "$committed_size" -eq 0 ]]; then
    print_fail "$md_base size comparison" "Committed report is 0 bytes"
    continue
  fi

  # Size within 5% tolerance (Markdown is deterministic text, tighter than PDF)
  tolerance=$((committed_size * 5 / 100))
  # Minimum tolerance of 100 bytes to allow minor formatting differences
  if [[ $tolerance -lt 100 ]]; then
    tolerance=100
  fi
  diff_size=$((fresh_size - committed_size))
  # Absolute value
  if [[ $diff_size -lt 0 ]]; then
    diff_size=$((-diff_size))
  fi

  if [[ $diff_size -le $tolerance ]]; then
    print_pass "$md_base size within tolerance (committed: ${committed_size}, fresh: ${fresh_size})"
  else
    print_fail "$md_base size comparison" "Sizes differ beyond tolerance: committed=${committed_size}, fresh=${fresh_size}"
  fi
done

# Check for fresh reports that were not committed
for fresh_md in out/*.md; do
  [[ -f "$fresh_md" ]] || continue
  md_base=$(basename "$fresh_md")
  if [[ ! -f "$TMPDIR_COMMITTED/$md_base" ]]; then
    print_fail "$md_base was committed" "Fresh report exists but was not in committed out/"
  fi
done

# Cleanup
rm -rf "$TMPDIR_COMMITTED"

# Restore committed outputs (copy from git)
git checkout -- out/ 2>/dev/null || true

print_summary
[[ $FAIL_COUNT -eq 0 ]]
