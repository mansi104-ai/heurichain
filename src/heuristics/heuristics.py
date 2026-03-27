"""
Chain analysis heuristics.
Each heuristic takes a Transaction (and optional block context) and returns a result dict.
"""

from __future__ import annotations

from collections import Counter
from typing import Any, Dict, List, Optional, TYPE_CHECKING

from src.common.primitives import DUST_THRESHOLD

if TYPE_CHECKING:
    from src.parser.block_parser import Transaction

BTC = 100_000_000
STANDARD_OUTPUT_TYPES = {"p2pkh", "p2sh", "p2wpkh", "p2wsh", "p2tr"}
IGNORED_OUTPUT_TYPES = {"op_return", "unknown"}

ROUND_AMOUNTS = {
    int(0.001 * BTC),
    int(0.01 * BTC),
    int(0.1 * BTC),
    int(0.5 * BTC),
    int(1 * BTC),
    int(5 * BTC),
    int(10 * BTC),
    int(50 * BTC),
    int(100 * BTC),
}


def _input_script_types(tx: "Transaction") -> List[str]:
    from src.parser.block_parser import classify_input_script

    script_types: List[str] = []
    for inp in tx.inputs:
        if inp.prevout_script_type:
            script_types.append(inp.prevout_script_type)
        else:
            script_types.append(classify_input_script(inp.script, inp.witness))
    return script_types


def _output_script_types(tx: "Transaction") -> List[str]:
    return [o.script_type for o in tx.outputs]


def _is_exact_round_btc(value: int) -> bool:
    return value in ROUND_AMOUNTS


def _is_round(value: int) -> bool:
    if value < DUST_THRESHOLD:
        return False
    if _is_exact_round_btc(value):
        return True
    # Keep "round number" strict enough to reduce false positives:
    # 0.001 BTC granularity and above.
    return value >= 100_000 and value % 100_000 == 0


def heuristic_cioh(tx: "Transaction", **kwargs) -> Dict[str, Any]:
    """
    If a tx has >1 inputs, they likely belong to the same entity.
    Coinbase transactions are excluded.
    """
    if tx.is_coinbase:
        return {"detected": False, "reason": "coinbase"}

    input_count = len(tx.inputs)
    detected = input_count > 1
    result: Dict[str, Any] = {"detected": detected, "input_count": input_count}
    if detected:
        if input_count >= 8:
            result["confidence"] = "high"
        elif input_count >= 4:
            result["confidence"] = "medium"
        else:
            result["confidence"] = "low"
    return result


def heuristic_change_detection(tx: "Transaction", **kwargs) -> Dict[str, Any]:
    """
    Identify the likely change output.
    Methods (in priority order):
      1. script_type_match: output script type matches dominant input type
      2. round_number: one non-round output among round outputs
      3. value_position: in 2-output tx, significantly larger output likely change
    """
    if tx.is_coinbase or len(tx.outputs) < 2:
        return {"detected": False}

    in_types = [t for t in _input_script_types(tx) if t in STANDARD_OUTPUT_TYPES]
    out_types = _output_script_types(tx)
    out_values = [o.value for o in tx.outputs]

    dominant_in = Counter(in_types).most_common(1)[0][0] if in_types else None
    if dominant_in:
        matches = [
            i
            for i, script_type in enumerate(out_types)
            if script_type == dominant_in and out_values[i] > DUST_THRESHOLD
        ]
        non_matches = [
            i
            for i, script_type in enumerate(out_types)
            if script_type in STANDARD_OUTPUT_TYPES and script_type != dominant_in
        ]
        if len(matches) == 1 and len(non_matches) >= 1:
            return {
                "detected": True,
                "likely_change_index": matches[0],
                "method": "script_type_match",
                "confidence": "high",
            }

    round_mask = [_is_round(v) for v in out_values]
    non_round = [i for i, is_round in enumerate(round_mask) if not is_round and out_values[i] > DUST_THRESHOLD]
    round_like = [i for i, is_round in enumerate(round_mask) if is_round]
    if len(tx.outputs) <= 4 and len(non_round) == 1 and len(round_like) >= 1:
        return {
            "detected": True,
            "likely_change_index": non_round[0],
            "method": "round_number",
            "confidence": "medium",
        }

    if len(tx.outputs) == 2:
        smaller_idx = 0 if out_values[0] <= out_values[1] else 1
        larger_idx = 1 - smaller_idx
        smaller = out_values[smaller_idx]
        larger = out_values[larger_idx]
        if smaller > DUST_THRESHOLD:
            ratio = larger / smaller
            if ratio >= 2.5 and out_types[smaller_idx] not in IGNORED_OUTPUT_TYPES:
                return {
                    "detected": True,
                    "likely_change_index": larger_idx,
                    "method": "value_position",
                    "value_ratio": round(ratio, 2),
                    "confidence": "low",
                }

    return {"detected": False}


def heuristic_coinjoin(tx: "Transaction", **kwargs) -> Dict[str, Any]:
    """
    CoinJoin: multiple equal-value outputs + many inputs.
    """
    if tx.is_coinbase or len(tx.inputs) < 3 or len(tx.outputs) < 3:
        return {"detected": False}

    out_values = [o.value for o in tx.outputs if o.value > 0]
    value_counts = Counter(out_values)
    max_count = max(value_counts.values()) if value_counts else 0
    equal_share = (max_count / len(out_values)) if out_values else 0.0

    if max_count >= 3 and len(tx.inputs) >= max_count and equal_share >= 0.30:
        dominant_value = value_counts.most_common(1)[0][0]
        return {
            "detected": True,
            "equal_output_count": max_count,
            "equal_output_share": round(equal_share, 2),
            "equal_output_value_sat": dominant_value,
            "input_count": len(tx.inputs),
            "confidence": "high" if max_count >= 8 or equal_share >= 0.5 else "medium",
        }
    return {"detected": False}


def heuristic_consolidation(tx: "Transaction", **kwargs) -> Dict[str, Any]:
    """
    Consolidation: many inputs, very few outputs.
    """
    if tx.is_coinbase:
        return {"detected": False}

    input_count = len(tx.inputs)
    output_count = len(tx.outputs)
    if input_count < 5 or output_count > 2:
        return {"detected": False}

    in_types = [t for t in _input_script_types(tx) if t in STANDARD_OUTPUT_TYPES]
    out_types = [t for t in _output_script_types(tx) if t in STANDARD_OUTPUT_TYPES]
    same_type = bool(in_types and out_types and Counter(in_types).most_common(1)[0][0] == out_types[0])

    confidence = "high" if input_count >= 12 and same_type else "medium" if same_type else "low"
    return {
        "detected": True,
        "input_count": input_count,
        "output_count": output_count,
        "same_script_type": same_type,
        "confidence": confidence,
    }


def heuristic_address_reuse(
    tx: "Transaction",
    block_output_scripts: Optional[set] = None,
    **kwargs,
) -> Dict[str, Any]:
    """
    Detect output-script reuse in:
    - the same transaction (output script appears in spent prevouts), and/or
    - earlier transactions in the same block.
    """
    if tx.is_coinbase:
        return {"detected": False}

    out_scripts = {
        bytes(o.script)
        for o in tx.outputs
        if o.script and o.script_type in STANDARD_OUTPUT_TYPES
    }
    if not out_scripts:
        return {"detected": False}

    in_scripts = {
        bytes(inp.prevout_script)
        for inp in tx.inputs
        if inp.prevout_script and inp.prevout_script_type in STANDARD_OUTPUT_TYPES
    }

    internal_reuse = bool(out_scripts & in_scripts)
    reused_count = 0
    if block_output_scripts:
        reused_count = len(out_scripts & block_output_scripts)
    cross_tx_reuse = reused_count > 0

    detected = internal_reuse or cross_tx_reuse
    result: Dict[str, Any] = {"detected": detected}
    if detected:
        result["internal_reuse"] = internal_reuse
        result["cross_tx_reuse"] = cross_tx_reuse
        # Keep legacy field for compatibility with existing UI/outputs.
        result["cross_block_reuse"] = cross_tx_reuse
        result["reused_output_count"] = reused_count
        result["confidence"] = "high" if internal_reuse or reused_count >= 2 else "medium"
    return result


def heuristic_self_transfer(tx: "Transaction", **kwargs) -> Dict[str, Any]:
    """
    Self-transfer: outputs remain within the same script-type family as inputs.
    Kept intentionally strict to avoid classifying regular payments as internal moves.
    """
    if tx.is_coinbase or len(tx.outputs) == 0:
        return {"detected": False}

    if heuristic_coinjoin(tx).get("detected"):
        return {"detected": False}

    in_types = {t for t in _input_script_types(tx) if t in STANDARD_OUTPUT_TYPES}
    out_types = [t for t in _output_script_types(tx) if t in STANDARD_OUTPUT_TYPES]
    if not in_types or not out_types:
        return {"detected": False}

    out_type_set = set(out_types)
    if not out_type_set.issubset(in_types):
        return {"detected": False}

    out_values = [o.value for o in tx.outputs if o.script_type in STANDARD_OUTPUT_TYPES]
    if not out_values:
        return {"detected": False}

    if len(out_values) == 1:
        return {
            "detected": True,
            "input_types": sorted(in_types),
            "output_types": sorted(out_type_set),
            "confidence": "high",
            "reason": "single_output_same_type_family",
        }

    if len(out_values) == 2:
        total = sum(out_values)
        dominant = max(out_values)
        minor = min(out_values)
        dominance = (dominant / total) if total else 0.0
        if dominance >= 0.92 and minor <= max(DUST_THRESHOLD * 20, 10_000):
            return {
                "detected": True,
                "input_types": sorted(in_types),
                "output_types": sorted(out_type_set),
                "dominant_output_share": round(dominance, 3),
                "confidence": "medium",
                "reason": "dominant_output_with_tiny_side_output",
            }

    return {"detected": False}


def heuristic_peeling_chain(tx: "Transaction", **kwargs) -> Dict[str, Any]:
    """
    Peeling-chain-like split in one transaction:
    exactly 2 outputs with strong large/small asymmetry.
    """
    if tx.is_coinbase or len(tx.outputs) != 2:
        return {"detected": False}

    values = sorted([o.value for o in tx.outputs])
    if values[0] <= DUST_THRESHOLD:
        return {"detected": False}

    ratio = values[1] / values[0]
    large_share = values[1] / (values[0] + values[1])
    if ratio >= 12 and large_share >= 0.88:
        return {
            "detected": True,
            "small_output_sat": values[0],
            "large_output_sat": values[1],
            "value_ratio": round(ratio, 2),
            "large_output_share": round(large_share, 3),
            "confidence": "high" if ratio >= 30 else "medium",
        }
    return {"detected": False}


_OP_RETURN_PROTOCOLS = {
    b"Om": "Omni",
    b"OT": "OpenTimestamps",
    b"STKS": "Stacks",
    b"id": "OnChain ID",
    b"\x13\x11": "Colored Coins",
}


def heuristic_op_return(tx: "Transaction", **kwargs) -> Dict[str, Any]:
    """
    Detect OP_RETURN outputs and classify embedded data protocol prefixes.
    """
    op_returns = [o for o in tx.outputs if o.script_type == "op_return"]
    if not op_returns:
        return {"detected": False}

    details = []
    for output in op_returns:
        data = output.script[2:] if len(output.script) > 2 else b""
        protocol = "unknown"
        for prefix, name in _OP_RETURN_PROTOCOLS.items():
            if data.startswith(prefix):
                protocol = name
                break
        details.append(
            {
                "data_hex": data[:40].hex(),
                "protocol": protocol,
                "data_length": len(data),
            }
        )

    return {
        "detected": True,
        "count": len(op_returns),
        "protocols": details,
        "confidence": "high",
    }


def heuristic_round_number_payment(tx: "Transaction", **kwargs) -> Dict[str, Any]:
    """
    Identify likely payment outputs with rounded amounts.
    """
    if tx.is_coinbase or not tx.outputs:
        return {"detected": False}

    round_outs = [
        (i, o.value)
        for i, o in enumerate(tx.outputs)
        if _is_round(o.value) and o.value >= 100_000
    ]
    if not round_outs:
        return {"detected": False}

    if len(round_outs) == len(tx.outputs) and len(tx.outputs) > 1:
        return {"detected": False, "reason": "all_outputs_round"}

    has_exact_round = any(_is_exact_round_btc(v) for _, v in round_outs)
    return {
        "detected": True,
        "round_output_count": len(round_outs),
        "round_output_indices": [i for i, _ in round_outs],
        "round_output_values_sat": [v for _, v in round_outs],
        "confidence": "high" if has_exact_round else "medium",
    }


HEURISTICS = {
    "cioh": heuristic_cioh,
    "change_detection": heuristic_change_detection,
    "coinjoin": heuristic_coinjoin,
    "consolidation": heuristic_consolidation,
    "address_reuse": heuristic_address_reuse,
    "self_transfer": heuristic_self_transfer,
    "peeling_chain": heuristic_peeling_chain,
    "op_return": heuristic_op_return,
    "round_number_payment": heuristic_round_number_payment,
}

HEURISTIC_IDS = list(HEURISTICS.keys())


def apply_all(tx: "Transaction", block_output_scripts: Optional[set] = None) -> Dict[str, Any]:
    results = {}
    for heuristic_id, heuristic_fn in HEURISTICS.items():
        try:
            results[heuristic_id] = heuristic_fn(tx, block_output_scripts=block_output_scripts)
        except Exception as exc:
            results[heuristic_id] = {"detected": False, "error": str(exc)}
    return results
