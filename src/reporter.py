"""
Generate human-readable Markdown reports from analysis JSON output.
"""

import datetime
from typing import Any, Dict, List, Tuple


def _script_table(dist: Dict[str, int]) -> str:
    total = sum(dist.values()) or 1
    lines = ["| Script Type | Count | Share |", "|---|---:|---:|"]
    for script_type, count in sorted(dist.items(), key=lambda item: (-item[1], item[0])):
        pct = (count / total) * 100
        lines.append(f"| `{script_type}` | {count:,} | {pct:.1f}% |")
    return "\n".join(lines)


def _fee_table(stats: Dict[str, float]) -> str:
    return "\n".join(
        [
            "| Metric | Value (sat/vB) |",
            "|---|---:|",
            f"| Min | {stats.get('min_sat_vb', 0.0):.2f} |",
            f"| Median | {stats.get('median_sat_vb', 0.0):.2f} |",
            f"| Mean | {stats.get('mean_sat_vb', 0.0):.2f} |",
            f"| Max | {stats.get('max_sat_vb', 0.0):.2f} |",
        ]
    )


def _classification_summary(txs: list) -> Dict[str, int]:
    counts: Dict[str, int] = {}
    for tx in txs:
        classification = tx.get("classification", "unknown")
        counts[classification] = counts.get(classification, 0) + 1
    return counts


def _notable_txs(txs: list, limit: int = 8) -> list:
    priority = {"coinjoin": 0, "consolidation": 1, "self_transfer": 2, "batch_payment": 3}
    filtered = [t for t in txs if t.get("classification") in priority]
    filtered.sort(key=lambda tx: priority.get(tx.get("classification", "unknown"), 9))
    return filtered[:limit]


def _heuristic_table(counts: Dict[str, int]) -> str:
    lines = ["| Heuristic | Transactions Triggered |", "|---|---:|"]
    for heuristic_id, count in sorted(counts.items(), key=lambda item: (-item[1], item[0])):
        lines.append(f"| `{heuristic_id}` | {count:,} |")
    return "\n".join(lines)


def _top_n(counts: Dict[str, int], limit: int = 3) -> List[Tuple[str, int]]:
    return sorted(counts.items(), key=lambda item: (-item[1], item[0]))[:limit]


def _pct(part: int, total: int) -> float:
    if total <= 0:
        return 0.0
    return round((part * 100.0) / total, 2)


def _summary_findings(summary: Dict[str, Any]) -> List[str]:
    total = int(summary.get("total_transactions_analyzed", 0))
    flagged = int(summary.get("flagged_transactions", 0))
    heuristics = summary.get("heuristic_trigger_counts", {}) or {}
    script_dist = summary.get("script_type_distribution", {}) or {}
    fee_stats = summary.get("fee_rate_stats", {}) or {}
    top_heuristics = _top_n(heuristics, limit=2)
    top_scripts = _top_n(script_dist, limit=1)

    findings: List[str] = [
        f"- Flagged ratio: **{_pct(flagged, total):.2f}%** ({flagged:,}/{total:,}).",
        f"- Average detected heuristics per tx: **{summary.get('avg_detected_heuristics', 0.0):.2f}**.",
    ]
    if top_heuristics:
        findings.append(
            f"- Most frequent signal: `{top_heuristics[0][0]}` on **{top_heuristics[0][1]:,}** transactions."
        )
    if len(top_heuristics) > 1:
        findings.append(
            f"- Second signal: `{top_heuristics[1][0]}` on **{top_heuristics[1][1]:,}** transactions."
        )
    if top_scripts:
        findings.append(
            f"- Dominant output script: `{top_scripts[0][0]}` with **{top_scripts[0][1]:,}** outputs."
        )
    findings.append(
        f"- Fee environment: median **{fee_stats.get('median_sat_vb', 0.0):.2f} sat/vB**, "
        f"max **{fee_stats.get('max_sat_vb', 0.0):.2f} sat/vB**."
    )
    return findings


def generate_report(analysis: Dict[str, Any]) -> str:
    filename = analysis.get("file", "unknown")
    block_count = analysis.get("block_count", 0)
    summary = analysis.get("analysis_summary", {})
    blocks = analysis.get("blocks", [])
    generated_at = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")

    lines = [
        f"# Chain Analysis Report: `{filename}`",
        "",
        f"> Generated: {generated_at}",
        "",
        "## File Overview",
        "",
        "| Field | Value |",
        "|---|---|",
        f"| Source file | `{filename}` |",
        f"| Blocks in file | {block_count} |",
        f"| Total transactions analyzed | {summary.get('total_transactions_analyzed', 0):,} |",
        f"| Flagged transactions | {summary.get('flagged_transactions', 0):,} |",
        f"| Heuristics applied | {len(summary.get('heuristics_applied', []))} |",
        "",
        "### Key Findings",
        "",
    ]
    lines.extend(_summary_findings(summary))
    lines.extend(
        [
            "",
            "## Summary Statistics",
            "",
            "### Fee Rate Distribution",
            "",
            _fee_table(summary.get("fee_rate_stats", {})),
            "",
            "### Script Type Breakdown",
            "",
            _script_table(summary.get("script_type_distribution", {})),
            "",
        ]
    )

    file_heuristic_counts = summary.get("heuristic_trigger_counts", {})
    if file_heuristic_counts:
        lines.extend(
            [
                "### Heuristic Findings (File-level)",
                "",
                _heuristic_table(file_heuristic_counts),
                "",
            ]
        )

    for idx, block in enumerate(blocks):
        block_hash = block.get("block_hash", "?")
        height = block.get("block_height", "?")
        ts = block.get("timestamp", 0)
        timestamp = datetime.datetime.utcfromtimestamp(ts).strftime("%Y-%m-%d %H:%M UTC") if ts else "?"
        tx_count = block.get("tx_count", 0)
        block_summary = block.get("analysis_summary", {})
        txs = block.get("transactions", [])
        heuristic_counts = block_summary.get("heuristic_trigger_counts", {})

        lines.extend(
            [
                "---",
                "",
                f"## Block {idx + 1}: Height {height}",
                "",
                "| Field | Value |",
                "|---|---|",
                f"| Hash | `{block_hash}` |",
                f"| Height | {height} |",
                f"| Timestamp | {timestamp} |",
                f"| Transactions | {tx_count:,} |",
                f"| Flagged | {block_summary.get('flagged_transactions', 0):,} |",
                "",
                "### Block Findings",
                "",
            ]
        )
        lines.extend(_summary_findings(block_summary))
        lines.extend(
            [
                "",
                "### Per-heuristic Findings",
                "",
                _heuristic_table(heuristic_counts),
                "",
            ]
        )

        if txs:
            classification_counts = _classification_summary(txs)
            lines.extend(
                [
                    "### Transaction Classifications",
                    "",
                    "| Classification | Count |",
                    "|---|---:|",
                ]
            )
            for classification, count in sorted(classification_counts.items(), key=lambda item: (-item[1], item[0])):
                lines.append(f"| `{classification}` | {count:,} |")
            lines.append("")

            notable = _notable_txs(txs)
            if notable:
                lines.extend(
                    [
                        "### Notable Transactions",
                        "",
                        "| TXID | Classification | Highlights |",
                        "|---|---|---|",
                    ]
                )
                for tx in notable:
                    txid = tx.get("txid", "?")
                    heuristics = tx.get("heuristics", {})
                    highlights = []
                    coinjoin = heuristics.get("coinjoin", {})
                    if coinjoin.get("detected"):
                        highlights.append(
                            f"CoinJoin with {coinjoin.get('equal_output_count', '?')} equal outputs"
                        )
                    consolidation = heuristics.get("consolidation", {})
                    if consolidation.get("detected"):
                        highlights.append(
                            f"Consolidation {consolidation.get('input_count', '?')} in -> "
                            f"{consolidation.get('output_count', '?')} out"
                        )
                    change = heuristics.get("change_detection", {})
                    if change.get("detected"):
                        highlights.append(
                            f"Change idx={change.get('likely_change_index', '?')} "
                            f"({change.get('method', '?')}, {change.get('confidence', '?')})"
                        )
                    lines.append(
                        f"| `{txid[:16]}...` | `{tx.get('classification', 'unknown')}` | "
                        f"{'; '.join(highlights) if highlights else '-'} |"
                    )
                lines.append("")
        else:
            lines.extend(
                [
                    "### Notable Transactions",
                    "",
                    "Transaction-level details are intentionally omitted for this block to keep JSON small.",
                    "",
                ]
            )

        lines.extend(
            [
                "### Block Fee Rate Distribution",
                "",
                _fee_table(block_summary.get("fee_rate_stats", {})),
                "",
                "### Block Script Type Distribution",
                "",
                _script_table(block_summary.get("script_type_distribution", {})),
                "",
            ]
        )

    lines.extend(["---", "", "*Report generated by Bitcoin Chain Analysis Engine (Challenge 3)*"])
    return "\n".join(lines)
