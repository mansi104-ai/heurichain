#!/usr/bin/env bash
set -euo pipefail

###############################################################################
# grader/grade_analysis.sh — JSON output validation for chain analysis
#
# For each block file fixture triple (blk/rev/xor):
#   1. Run ./cli.sh --block with 180s timeout
#   2. Validate per-block-file JSON schema and consistency
###############################################################################

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/lib/common.sh"

check_jq

REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$REPO_ROOT"

FIXTURES_DIR="$REPO_ROOT/fixtures"
XOR_FILE="$FIXTURES_DIR/xor.dat"

print_header "Chain Analysis — JSON Output Validation"

# Check xor.dat exists
if [[ ! -f "$XOR_FILE" ]]; then
  print_fail "xor.dat exists" "File not found: $XOR_FILE"
  print_summary
  exit 1
fi

# Discover block fixture pairs (blk*.dat / rev*.dat)
blk_files=()
for blk in "$FIXTURES_DIR"/blk*.dat; do
  [[ -f "$blk" ]] || continue
  blk_files+=("$blk")
done

if [[ ${#blk_files[@]} -eq 0 ]]; then
  print_fail "Block fixtures found" "No blk*.dat files in $FIXTURES_DIR (run setup.sh first)"
  print_summary
  exit 1
fi

print_pass "Block fixtures found (${#blk_files[@]} blk files)"

for blk_file in "${blk_files[@]}"; do
  blk_base=$(basename "$blk_file")
  blk_stem="${blk_base%.dat}"
  # Derive rev file: blk04330.dat -> rev04330.dat
  rev_base="${blk_base/blk/rev}"
  rev_file="$FIXTURES_DIR/$rev_base"

  print_section "Testing $blk_base"

  if [[ ! -f "$rev_file" ]]; then
    print_fail "Undo file exists" "Expected $rev_base but not found"
    continue
  fi

  # Clean out/ before running
  rm -rf out/

  # Run CLI
  run_cli --block "$blk_file" "$rev_file" "$XOR_FILE"

  if [[ $CLI_EXIT -ne 0 ]]; then
    print_fail "cli.sh exits 0" "Exit code: $CLI_EXIT, stderr: $CLI_STDERR"
    continue
  fi
  print_pass "cli.sh exits 0"

  # Check expected output file exists
  json_file="out/${blk_stem}.json"
  if [[ ! -f "$json_file" ]]; then
    print_fail "JSON output exists: ${blk_stem}.json" "File not found: $json_file"
    continue
  fi
  print_pass "JSON output exists: ${blk_stem}.json"

  json_base="${blk_stem}.json"

  # Check valid JSON
  if ! jq empty "$json_file" 2>/dev/null; then
    print_fail "$json_base is valid JSON"
    continue
  fi
  print_pass "$json_base is valid JSON"

  # ---------------------------------------------------------------------------
  # Single-pass jq extraction: read the large file ONCE, produce a small object
  # ---------------------------------------------------------------------------
  check_data=$(jq -c '{
    top: {
      ok: .ok,
      mode: (.mode // null),
      file_exists: ((.file // null) != null),
      block_count: .block_count,
      block_count_type: (.block_count | type),
      blocks_type: (.blocks | type),
      blocks_len: (.blocks | length),
      summary_exists: ((.analysis_summary // null) != null)
    },
    blocks: [.blocks | to_entries[] | .key as $idx | .value | {
      idx: $idx,
      block_hash: (.block_hash // ""),
      block_hash_valid: ((.block_hash // "") | test("^[0-9a-f]{64}$")),
      tx_count: .tx_count,
      tx_count_type: (.tx_count | type),
      summary_exists: ((.analysis_summary // null) != null),
      heuristic_count: ((.analysis_summary.heuristics_applied // []) | length),
      has_cioh: ((.analysis_summary.heuristics_applied // []) | index("cioh") != null),
      has_change: ((.analysis_summary.heuristics_applied // []) | index("change_detection") != null),
      fee_valid: ((.analysis_summary.fee_rate_stats // null) |
        if . == null then false
        elif .min_sat_vb == null or .max_sat_vb == null or .median_sat_vb == null or .mean_sat_vb == null then false
        elif .min_sat_vb < 0 or .max_sat_vb < 0 or .median_sat_vb < 0 or .mean_sat_vb < 0 then false
        elif .min_sat_vb > .median_sat_vb then false
        elif .median_sat_vb > .max_sat_vb then false
        else true end),
      flagged: (.analysis_summary.flagged_transactions // null),
      flagged_valid: (
        .tx_count as $tc |
        .analysis_summary.flagged_transactions |
        if . == null or $tc == null then null
        elif type != "number" then false
        elif . < 0 then false
        elif . > $tc then false
        else true end),
      tx_array_len: (if $idx == 0 then ((.transactions // []) | length) else null end),
      tx_array_type: (if $idx == 0 then ((.transactions // null) | type) else null end)
    }],
    agg: {
      sum_tx: ([.blocks[].tx_count] | add),
      file_total: (.analysis_summary.total_transactions_analyzed // null),
      sum_flagged: ([.blocks[].analysis_summary.flagged_transactions] | add),
      file_flagged: (.analysis_summary.flagged_transactions // null),
      file_fee_valid: ((.analysis_summary.fee_rate_stats // null) |
        if . == null then false
        elif .min_sat_vb == null or .max_sat_vb == null or .median_sat_vb == null or .mean_sat_vb == null then false
        elif .min_sat_vb < 0 or .max_sat_vb < 0 or .median_sat_vb < 0 or .mean_sat_vb < 0 then false
        elif .min_sat_vb > .median_sat_vb then false
        elif .median_sat_vb > .max_sat_vb then false
        else true end),
      file_heuristic_count: ((.analysis_summary.heuristics_applied // []) | length)
    }
  }' "$json_file" 2>/dev/null)

  # Helper to query the small check_data object
  chk() { echo "$check_data" | jq -r "$1" 2>/dev/null; }

  # -----------------------------------------------------------------------
  # Top-level field validation
  # -----------------------------------------------------------------------
  if [[ "$(chk '.top.ok')" == "true" ]]; then
    print_pass "$json_base: ok == true"
  else
    print_fail "$json_base: ok == true" "Expected: true, Got: $(chk '.top.ok')"
  fi

  local_mode=$(chk '.top.mode')
  if [[ "$local_mode" == "chain_analysis" ]]; then
    print_pass "$json_base: mode == chain_analysis"
  else
    print_fail "$json_base: mode == chain_analysis" "Expected: chain_analysis, Got: $local_mode"
  fi

  if [[ "$(chk '.top.file_exists')" == "true" ]]; then
    print_pass "$json_base: file field exists"
  else
    print_fail "$json_base: file field exists" "Field .file is missing or null"
  fi

  if [[ "$(chk '.top.block_count_type')" == "number" ]]; then
    print_pass "$json_base: block_count is number"
  else
    print_fail "$json_base: block_count is number" "Expected type: number, Got: $(chk '.top.block_count_type')"
  fi

  if [[ "$(chk '.top.blocks_type')" == "array" ]]; then
    print_pass "$json_base: blocks is array"
  else
    print_fail "$json_base: blocks is array" "Expected type: array, Got: $(chk '.top.blocks_type')"
  fi

  if [[ "$(chk '.top.summary_exists')" == "true" ]]; then
    print_pass "$json_base: file-level analysis_summary exists"
  else
    print_fail "$json_base: file-level analysis_summary exists" "Field .analysis_summary is missing or null"
  fi

  # Verify block_count == blocks array length
  block_count=$(chk '.top.block_count')
  blocks_len=$(chk '.top.blocks_len')
  if [[ "$block_count" != "null" && "$blocks_len" != "null" && "$block_count" == "$blocks_len" ]]; then
    print_pass "$json_base: block_count ($block_count) == blocks length"
  else
    print_fail "$json_base: block_count == blocks length" "block_count=$block_count, blocks length=$blocks_len"
  fi

  # -----------------------------------------------------------------------
  # Per-block validation
  # -----------------------------------------------------------------------
  if [[ "$blocks_len" == "null" || "$blocks_len" == "0" ]]; then
    print_fail "$json_base: blocks array is non-empty"
    continue
  fi

  for block_idx in $(seq 0 $((blocks_len - 1))); do
    block_prefix="$json_base: blocks[$block_idx]"

    # block_hash is hex64
    if [[ "$(chk ".blocks[$block_idx].block_hash_valid")" == "true" ]]; then
      print_pass "$block_prefix: block_hash is hex64"
    else
      print_fail "$block_prefix: block_hash is hex64" "Value '$(chk ".blocks[$block_idx].block_hash")' does not match regex: $HEX64_REGEX"
    fi

    # tx_count is number
    if [[ "$(chk ".blocks[$block_idx].tx_count_type")" == "number" ]]; then
      print_pass "$block_prefix: tx_count is number"
    else
      print_fail "$block_prefix: tx_count is number" "Expected type: number, Got: $(chk ".blocks[$block_idx].tx_count_type")"
    fi

    # analysis_summary exists
    if [[ "$(chk ".blocks[$block_idx].summary_exists")" == "true" ]]; then
      print_pass "$block_prefix: analysis_summary exists"
    else
      print_fail "$block_prefix: analysis_summary exists" "Field .analysis_summary is missing or null"
    fi

    # Validate transactions array only for the first block
    if [[ "$block_idx" -eq 0 ]]; then
      tx_array_type=$(chk ".blocks[0].tx_array_type")
      if [[ "$tx_array_type" == "array" ]]; then
        print_pass "$block_prefix: transactions is array"
      else
        print_fail "$block_prefix: transactions is array" "Expected type: array, Got: $tx_array_type"
      fi

      tx_count=$(chk ".blocks[0].tx_count")
      tx_array_len=$(chk ".blocks[0].tx_array_len")
      if [[ "$tx_count" != "null" && "$tx_array_len" != "null" && "$tx_count" == "$tx_array_len" ]]; then
        print_pass "$block_prefix: transactions length ($tx_array_len) == tx_count ($tx_count)"
      else
        print_fail "$block_prefix: transactions length == tx_count" "tx_count=$tx_count, array length=$tx_array_len"
      fi
    fi

    # Check heuristics_applied has at least 5 distinct IDs
    heuristic_count=$(chk ".blocks[$block_idx].heuristic_count")
    if [[ "$heuristic_count" -ge 5 ]]; then
      print_pass "$block_prefix: at least 5 heuristics applied ($heuristic_count)"
    else
      print_fail "$block_prefix: at least 5 heuristics applied" "Found $heuristic_count"
    fi

    # Check cioh and change_detection are in heuristics_applied
    if [[ "$(chk ".blocks[$block_idx].has_cioh")" == "true" ]]; then
      print_pass "$block_prefix: cioh in heuristics_applied"
    else
      print_fail "$block_prefix: cioh in heuristics_applied"
    fi
    if [[ "$(chk ".blocks[$block_idx].has_change")" == "true" ]]; then
      print_pass "$block_prefix: change_detection in heuristics_applied"
    else
      print_fail "$block_prefix: change_detection in heuristics_applied"
    fi

    # Validate fee_rate_stats consistency
    if [[ "$(chk ".blocks[$block_idx].fee_valid")" == "true" ]]; then
      print_pass "$block_prefix: fee_rate_stats consistent (min <= median <= max, non-negative)"
    else
      print_fail "$block_prefix: fee_rate_stats consistency"
    fi

    # Check flagged_transactions is a non-negative integer <= tx_count
    flagged_valid=$(chk ".blocks[$block_idx].flagged_valid")
    reported_flagged=$(chk ".blocks[$block_idx].flagged")
    tx_count=$(chk ".blocks[$block_idx].tx_count")
    if [[ "$flagged_valid" == "null" ]]; then
      print_fail "$block_prefix: flagged_transactions" "Field missing or null"
    elif [[ "$flagged_valid" == "true" ]]; then
      print_pass "$block_prefix: flagged_transactions ($reported_flagged) is valid (0 <= n <= tx_count)"
    else
      print_fail "$block_prefix: flagged_transactions" "Expected 0 <= n <= $tx_count, got $reported_flagged"
    fi
  done

  # -----------------------------------------------------------------------
  # File-level aggregation checks
  # -----------------------------------------------------------------------
  print_section "File-level aggregation for $blk_base"

  # total_transactions_analyzed == sum of per-block tx_counts
  sum_tx=$(chk '.agg.sum_tx')
  file_total=$(chk '.agg.file_total')
  if [[ "$sum_tx" != "null" && "$file_total" != "null" && "$sum_tx" == "$file_total" ]]; then
    print_pass "$json_base: file-level total_transactions_analyzed ($file_total) == sum of block tx_counts"
  else
    print_fail "$json_base: file-level total_transactions_analyzed" "Expected $sum_tx, got $file_total"
  fi

  # flagged_transactions == sum of per-block flagged
  sum_flagged=$(chk '.agg.sum_flagged')
  file_flagged=$(chk '.agg.file_flagged')
  if [[ "$sum_flagged" != "null" && "$file_flagged" != "null" && "$sum_flagged" == "$file_flagged" ]]; then
    print_pass "$json_base: file-level flagged_transactions ($file_flagged) == sum of per-block flagged"
  else
    print_fail "$json_base: file-level flagged_transactions" "Expected $sum_flagged, got $file_flagged"
  fi

  # File-level fee_rate_stats consistency
  if [[ "$(chk '.agg.file_fee_valid')" == "true" ]]; then
    print_pass "$json_base: file-level fee_rate_stats consistent"
  else
    print_fail "$json_base: file-level fee_rate_stats consistency"
  fi

  # File-level heuristics_applied has at least 5
  file_heuristic_count=$(chk '.agg.file_heuristic_count')
  if [[ "$file_heuristic_count" -ge 5 ]]; then
    print_pass "$json_base: file-level at least 5 heuristics applied ($file_heuristic_count)"
  else
    print_fail "$json_base: file-level at least 5 heuristics applied" "Found $file_heuristic_count"
  fi

done

# Restore committed outputs (reports, etc.) so subsequent graders find them
git checkout -- out/ 2>/dev/null || true

print_summary
[[ $FAIL_COUNT -eq 0 ]]
