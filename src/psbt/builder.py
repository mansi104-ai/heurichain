"""
Minimal PSBT (BIP174 v0) builder for parsed chain transactions.
This is intended for analysis/debug views, not signing workflows.
"""

import base64
import struct
from typing import Optional

from src.parser.block_parser import Transaction


def _enc_varint(n: int) -> bytes:
    if n < 0xFD:
        return bytes([n])
    if n <= 0xFFFF:
        return b"\xfd" + struct.pack("<H", n)
    if n <= 0xFFFFFFFF:
        return b"\xfe" + struct.pack("<I", n)
    return b"\xff" + struct.pack("<Q", n)


def _serialize_unsigned_tx(tx: Transaction) -> bytes:
    """
    Serialize as legacy unsigned tx for PSBT global unsigned tx:
    no marker/flag, empty scriptSig for all inputs, no witness.
    """
    parts = [struct.pack("<i", tx.version)]
    parts.append(_enc_varint(len(tx.inputs)))
    for inp in tx.inputs:
        parts.append(bytes.fromhex(inp.prev_txid)[::-1])
        parts.append(struct.pack("<I", inp.prev_vout))
        parts.append(b"\x00")  # empty scriptSig
        parts.append(struct.pack("<I", inp.sequence))
    parts.append(_enc_varint(len(tx.outputs)))
    for out in tx.outputs:
        parts.append(struct.pack("<Q", out.value))
        parts.append(_enc_varint(len(out.script)))
        parts.append(out.script)
    parts.append(struct.pack("<I", tx.locktime))
    return b"".join(parts)


def _kv(key: bytes, value: bytes) -> bytes:
    return _enc_varint(len(key)) + key + _enc_varint(len(value)) + value


def _witness_utxo(value_sat: int, script_pubkey: bytes) -> bytes:
    return struct.pack("<q", value_sat) + _enc_varint(len(script_pubkey)) + script_pubkey


def build_psbt_bytes(tx: Transaction) -> bytes:
    """
    Build a minimal PSBT:
    - Global unsigned tx
    - Per-input witness_utxo when prevout is available
    - Empty per-output maps
    """
    out = bytearray()
    out += b"psbt\xff"
    out += _kv(b"\x00", _serialize_unsigned_tx(tx))  # PSBT_GLOBAL_UNSIGNED_TX
    out += b"\x00"  # end global map

    for inp in tx.inputs:
        if inp.prevout_value is not None and inp.prevout_script is not None:
            # PSBT_IN_WITNESS_UTXO = 0x01
            out += _kv(b"\x01", _witness_utxo(int(inp.prevout_value), bytes(inp.prevout_script)))
        out += b"\x00"  # end input map

    for _ in tx.outputs:
        out += b"\x00"  # empty output map

    return bytes(out)


def build_psbt_base64(tx: Transaction) -> str:
    return base64.b64encode(build_psbt_bytes(tx)).decode("ascii")


def tx_summary(tx: Transaction) -> dict:
    total_in: Optional[int] = None
    if tx.is_coinbase:
        total_in = None
    elif all(i.prevout_value is not None for i in tx.inputs):
        total_in = sum(int(i.prevout_value or 0) for i in tx.inputs)

    total_out = sum(o.value for o in tx.outputs)
    fee = None
    fee_rate = None
    if total_in is not None:
        fee = total_in - total_out
        if tx.vsize > 0 and fee is not None:
            fee_rate = round(fee / tx.vsize, 2)

    return {
        "txid": tx.txid,
        "is_coinbase": tx.is_coinbase,
        "input_count": len(tx.inputs),
        "output_count": len(tx.outputs),
        "total_input_sats": total_in,
        "total_output_sats": total_out,
        "fee_sats": fee,
        "vsize": round(tx.vsize, 2),
        "fee_rate_sat_vb": fee_rate,
    }
