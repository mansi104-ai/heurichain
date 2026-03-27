"""
Chain analysis engine.
Applies heuristics to all blocks, computes statistics, and produces the JSON schema.
"""

import statistics
from collections import Counter
from typing import Any, Dict, List, Optional, Tuple

from src.heuristics.heuristics import HEURISTIC_IDS, apply_all
from src.parser.block_parser import Block, Transaction

MAX_MONEY = 2_100_000_000_000_000
MAX_REASONABLE_FEE_RATE = 10_000.0
STANDARD_OUTPUT_TYPES = {"p2pkh", "p2sh", "p2wpkh", "p2wsh", "p2tr"}


def _detected_heuristic_ids(heuristics_result: Dict[str, Any]) -> List[str]:
    return [hid for hid, payload in heuristics_result.items() if payload.get("detected", False)]


def _confidence_weight(confidence: str) -> int:
    if confidence == "high":
        return 3
    if confidence == "medium":
        return 2
    if confidence == "low":
        return 1
    return 1


def _signal_score(heuristics_result: Dict[str, Any]) -> int:
    score = 0
    for payload in heuristics_result.values():
        if not payload.get("detected", False):
            continue
        score += _confidence_weight(str(payload.get("confidence", "low")))
    return score


def _has_high_confidence_signal(heuristics_result: Dict[str, Any]) -> bool:
    for payload in heuristics_result.values():
        if payload.get("detected", False) and payload.get("confidence") == "high":
            return True
    return False


def _classify_transaction(tx: Transaction, heuristics_result: Dict[str, Any]) -> str:
    """
    Map heuristic results to a transaction classification.
    """

    def detected(key: str) -> bool:
        return heuristics_result.get(key, {}).get("detected", False)

    if tx.is_coinbase:
        return "unknown"

    if detected("coinjoin"):
        return "coinjoin"

    consolidation = heuristics_result.get("consolidation", {})
    if consolidation.get("detected") and consolidation.get("input_count", 0) >= 5:
        return "consolidation"

    if detected("self_transfer"):
        return "self_transfer"

    if len(tx.outputs) >= 4 and detected("change_detection") and detected("cioh"):
        return "batch_payment"

    detected_ids = _detected_heuristic_ids(heuristics_result)
    if not detected_ids:
        return "unknown"

    meaningful = [hid for hid in detected_ids if hid not in {"cioh"}]
    if not meaningful:
        return "unknown"

    strong = [hid for hid in meaningful if hid not in {"round_number_payment"}]
    if strong or len(meaningful) >= 2:
        return "simple_payment"
    return "unknown"


def _tx_fee_rate(tx: Transaction) -> Optional[float]:
    """
    Compute exact fee rate (sat/vB) when undo/prevout values are attached.
    """
    if tx.is_coinbase or tx.vsize <= 0:
        return None
    if any(inp.prevout_value is None for inp in tx.inputs):
        return None

    total_in = sum(int(inp.prevout_value or 0) for inp in tx.inputs)
    total_out = sum(o.value for o in tx.outputs)
    if total_in <= 0 or total_in > MAX_MONEY:
        return None

    fee = total_in - total_out
    if fee < 0:
        return None

    rate = fee / tx.vsize
    if rate < 0 or rate > MAX_REASONABLE_FEE_RATE:
        return None
    return rate


def _script_type_distribution(txs: List[Transaction]) -> Dict[str, int]:
    dist: Counter = Counter()
    for tx in txs:
        for out in tx.outputs:
            dist[out.script_type] += 1
    for key in ("p2wpkh", "p2tr", "p2sh", "p2pkh", "p2wsh", "op_return", "unknown"):
        if key not in dist:
            dist[key] = 0
    return dict(dist)


def _fee_rate_stats(fee_rates: List[float]) -> Dict[str, float]:
    if not fee_rates:
        return {"min_sat_vb": 0.0, "max_sat_vb": 0.0, "median_sat_vb": 0.0, "mean_sat_vb": 0.0}
    return {
        "min_sat_vb": round(min(fee_rates), 2),
        "max_sat_vb": round(max(fee_rates), 2),
        "median_sat_vb": round(statistics.median(fee_rates), 2),
        "mean_sat_vb": round(statistics.mean(fee_rates), 2),
    }


def _heuristic_counts_empty() -> Dict[str, int]:
    return {hid: 0 for hid in HEURISTIC_IDS}


def _merge_heuristic_counts(dst: Dict[str, int], src: Dict[str, int]) -> None:
    for hid, count in src.items():
        dst[hid] = dst.get(hid, 0) + count


def analyze_block(block: Block, include_transactions: bool = True) -> Tuple[Dict[str, Any], List[float]]:
    """
    Analyze a single block. Returns (block_json, fee_rates).
    """
    txs = block.transactions
    tx_count = len(txs)
    seen_output_scripts: set = set()
    tx_results: List[Dict[str, Any]] = []
    flagged = 0
    fee_rates: List[float] = []
    detected_signal_total = 0
    high_confidence_txs = 0
    heuristic_trigger_counts = _heuristic_counts_empty()

    for tx in txs:
        heuristic_result = apply_all(tx, block_output_scripts=seen_output_scripts)
        detected_ids = _detected_heuristic_ids(heuristic_result)
        detected_count = len(detected_ids)
        classification = _classify_transaction(tx, heuristic_result)
        signal_score = _signal_score(heuristic_result)

        for out in tx.outputs:
            if out.script and out.script_type in STANDARD_OUTPUT_TYPES:
                seen_output_scripts.add(bytes(out.script))

        if detected_count > 0:
            flagged += 1
            detected_signal_total += detected_count
            if _has_high_confidence_signal(heuristic_result):
                high_confidence_txs += 1

        for hid in detected_ids:
            heuristic_trigger_counts[hid] = heuristic_trigger_counts.get(hid, 0) + 1

        fee_rate = _tx_fee_rate(tx)
        if fee_rate is not None:
            fee_rates.append(round(fee_rate, 2))

        if include_transactions:
            tx_results.append(
                {
                    "txid": tx.txid,
                    "heuristics": heuristic_result,
                    "classification": classification,
                    "detected_heuristics": detected_ids,
                    "signal_score": signal_score,
                }
            )

    summary = {
        "total_transactions_analyzed": tx_count,
        "heuristics_applied": HEURISTIC_IDS,
        "flagged_transactions": flagged,
        "flagged_ratio_pct": round((flagged * 100.0 / tx_count), 2) if tx_count else 0.0,
        "heuristic_trigger_counts": heuristic_trigger_counts,
        "avg_detected_heuristics": round((detected_signal_total / tx_count), 2) if tx_count else 0.0,
        "high_confidence_transactions": high_confidence_txs,
        "script_type_distribution": _script_type_distribution(txs),
        "fee_rate_stats": _fee_rate_stats(fee_rates),
    }

    block_result: Dict[str, Any] = {
        "block_hash": block.header.block_hash,
        "block_height": block.height,
        "timestamp": block.header.timestamp,
        "tx_count": tx_count,
        "analysis_summary": summary,
    }
    if include_transactions:
        block_result["transactions"] = tx_results
    return block_result, fee_rates


def analyze_file(blk_path: str, blocks: List[Block]) -> Dict[str, Any]:
    """
    Analyze all blocks from a blk*.dat file and produce top-level schema output.
    """
    import os

    filename = os.path.basename(blk_path)
    block_results: List[Dict[str, Any]] = []
    all_fee_rates: List[float] = []
    total_txs = 0
    total_flagged = 0
    total_detected_signals = 0
    high_confidence_txs = 0
    all_script_dist: Counter = Counter()
    all_heuristic_counts = _heuristic_counts_empty()

    for idx, block in enumerate(blocks):
        include_transactions = idx == 0
        block_result, fee_rates = analyze_block(block, include_transactions=include_transactions)
        block_results.append(block_result)
        all_fee_rates.extend(fee_rates)

        block_summary = block_result["analysis_summary"]
        total_txs += block_result["tx_count"]
        total_flagged += block_summary["flagged_transactions"]
        high_confidence_txs += block_summary.get("high_confidence_transactions", 0)
        total_detected_signals += sum(block_summary["heuristic_trigger_counts"].values())

        _merge_heuristic_counts(all_heuristic_counts, block_summary["heuristic_trigger_counts"])
        for key, value in block_summary["script_type_distribution"].items():
            all_script_dist[key] += value

    for key in ("p2wpkh", "p2tr", "p2sh", "p2pkh", "p2wsh", "op_return", "unknown"):
        if key not in all_script_dist:
            all_script_dist[key] = 0

    file_summary = {
        "total_transactions_analyzed": total_txs,
        "heuristics_applied": HEURISTIC_IDS,
        "flagged_transactions": total_flagged,
        "flagged_ratio_pct": round((total_flagged * 100.0 / total_txs), 2) if total_txs else 0.0,
        "heuristic_trigger_counts": all_heuristic_counts,
        "avg_detected_heuristics": round((total_detected_signals / total_txs), 2) if total_txs else 0.0,
        "high_confidence_transactions": high_confidence_txs,
        "script_type_distribution": dict(all_script_dist),
        "fee_rate_stats": _fee_rate_stats(all_fee_rates),
    }

    return {
        "ok": True,
        "mode": "chain_analysis",
        "file": filename,
        "block_count": len(block_results),
        "analysis_summary": file_summary,
        "blocks": block_results,
    }
