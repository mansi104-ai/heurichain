"""
CLI entry point for the Bitcoin chain analysis engine.
Usage: python3 src/cli.py --block <blk.dat> <rev.dat> <xor.dat>
"""

import sys
import os
import json
import argparse
import traceback

# Allow imports from project root
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.parser.block_parser import (
    parse_blocks_from_file,
    parse_undo_from_file,
    attach_undo_data,
)
from src.analyzer import analyze_file
from src.reporter import generate_report


def error_exit(code: str, message: str):
    print(json.dumps({"ok": False, "error": {"code": code, "message": message}}))
    sys.exit(1)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--block", nargs=3, metavar=("BLK", "REV", "XOR"), required=True)
    args = parser.parse_args()

    blk_path, rev_path, xor_path = args.block

    if not os.path.isfile(blk_path):
        error_exit("FILE_NOT_FOUND", f"Block file not found: {blk_path}")

    if not os.path.isfile(rev_path):
        error_exit("FILE_NOT_FOUND", f"Undo file not found: {rev_path}")

    # xor.dat may not exist (older Bitcoin Core)
    if not os.path.isfile(xor_path):
        xor_path = ""

    try:
        blocks = parse_blocks_from_file(blk_path, xor_path)
    except Exception as e:
        error_exit("PARSE_ERROR", f"Failed to parse block file: {e}\n{traceback.format_exc()}")

    if not blocks:
        error_exit("NO_BLOCKS", f"No valid blocks found in {blk_path}")

    try:
        undo_blocks = parse_undo_from_file(rev_path, xor_path, blocks=blocks)
        attach_undo_data(blocks, undo_blocks)
    except Exception as e:
        error_exit("UNDO_PARSE_ERROR", f"Failed to parse undo file: {e}\n{traceback.format_exc()}")

    try:
        analysis = analyze_file(blk_path, blocks)
    except Exception as e:
        error_exit("ANALYSIS_ERROR", f"Analysis failed: {e}\n{traceback.format_exc()}")

    # Write outputs
    os.makedirs("out", exist_ok=True)
    blk_stem = os.path.splitext(os.path.basename(blk_path))[0]
    json_path = os.path.join("out", f"{blk_stem}.json")
    md_path   = os.path.join("out", f"{blk_stem}.md")

    try:
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(analysis, f, indent=2)
    except Exception as e:
        error_exit("WRITE_ERROR", f"Failed to write JSON: {e}")

    try:
        report = generate_report(analysis)
        with open(md_path, "w", encoding="utf-8") as f:
            f.write(report)
    except Exception as e:
        error_exit("WRITE_ERROR", f"Failed to write Markdown report: {e}")

    print(json.dumps({"ok": True, "json": json_path, "report": md_path,
                      "blocks": analysis["block_count"],
                      "transactions": analysis["analysis_summary"]["total_transactions_analyzed"]}))
    sys.exit(0)


if __name__ == "__main__":
    main()
