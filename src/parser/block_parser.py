"""
Bitcoin block file parser.
Handles XOR-obfuscated blk*.dat files (Bitcoin Core >= 28.x uses xor.dat).
Parses raw blocks, transactions, inputs, outputs, and script types.
"""

import struct
from dataclasses import dataclass, field
from typing import List, Optional, Tuple, Dict

from src.common.primitives import double_sha256, read_varint as common_read_varint

MAINNET_MAGIC = b'\xf9\xbe\xb4\xd9'
MAX_MONEY = 2_100_000_000_000_000

# Script type constants
P2PKH   = "p2pkh"
P2SH    = "p2sh"
P2WPKH  = "p2wpkh"
P2WSH   = "p2wsh"
P2TR    = "p2tr"
OP_RETURN_TYPE = "op_return"
UNKNOWN = "unknown"


@dataclass
class TxInput:
    prev_txid: str       # hex, display order (reversed)
    prev_vout: int
    script: bytes
    sequence: int
    witness: List[bytes] = field(default_factory=list)
    prevout_value: Optional[int] = None
    prevout_script: Optional[bytes] = None
    prevout_script_type: Optional[str] = None


@dataclass
class TxOutput:
    value: int           # satoshis
    script: bytes
    script_type: str = UNKNOWN
    address: Optional[str] = None


@dataclass
class Transaction:
    txid: str            # hex, display order
    version: int
    inputs: List[TxInput]
    outputs: List[TxOutput]
    locktime: int
    is_segwit: bool
    is_coinbase: bool
    weight: int = 0
    vsize: float = 0.0


@dataclass
class BlockHeader:
    version: int
    prev_hash: str
    merkle_root: str
    timestamp: int
    bits: int
    nonce: int
    block_hash: str


@dataclass
class Block:
    header: BlockHeader
    height: int          # from coinbase BIP34
    transactions: List[Transaction]


@dataclass
class UndoCoin:
    value: int
    script: bytes
    script_type: str


def load_xor_key(xor_path: str) -> bytes:
    """Load XOR obfuscation key from xor.dat (up to 8 bytes)."""
    try:
        with open(xor_path, 'rb') as f:
            data = f.read(8)
        return data if data else b''
    except (FileNotFoundError, IOError):
        return b''


def xor_decrypt(data: bytes, key: bytes) -> bytes:
    """Apply cyclic XOR with key to data."""
    if not key:
        return data
    key_len = len(key)
    return bytes(b ^ key[i % key_len] for i, b in enumerate(data))


def dsha256(data: bytes) -> bytes:
    return double_sha256(data)


def read_varint(buf: bytes, offset: int) -> Tuple[int, int]:
    """Read a Bitcoin variable-length integer. Returns (value, new_offset)."""
    value, consumed = common_read_varint(buf, offset)
    return value, offset + consumed


def read_msb_varint(buf: bytes, offset: int) -> Tuple[int, int]:
    """
    Read Bitcoin's MSB base-128 VARINT encoding used by undo Coin serialization.
    """
    value = 0
    while True:
        ch = buf[offset]
        offset += 1
        value = (value << 7) | (ch & 0x7F)
        if ch & 0x80:
            value += 1
            continue
        return value, offset


def classify_script(script: bytes) -> str:
    """Classify output script into known types."""
    n = len(script)
    if n == 0:
        return UNKNOWN
    # OP_RETURN
    if script[0] == 0x6a:
        return OP_RETURN_TYPE
    # P2PKH: OP_DUP OP_HASH160 <20> OP_EQUALVERIFY OP_CHECKSIG
    if n == 25 and script[0:3] == b'\x76\xa9\x14' and script[23:25] == b'\x88\xac':
        return P2PKH
    # P2SH: OP_HASH160 <20> OP_EQUAL
    if n == 23 and script[0:2] == b'\xa9\x14' and script[22] == 0x87:
        return P2SH
    # P2WPKH: OP_0 <20>
    if n == 22 and script[0:2] == b'\x00\x14':
        return P2WPKH
    # P2WSH: OP_0 <32>
    if n == 34 and script[0:2] == b'\x00\x20':
        return P2WSH
    # P2TR: OP_1 <32>
    if n == 34 and script[0:2] == b'\x51\x20':
        return P2TR
    return UNKNOWN


def classify_input_script(script: bytes, witness: List[bytes]) -> str:
    """Infer script type from input script + witness."""
    # SegWit native inputs have empty scriptSig
    if len(script) == 0 and witness:
        if len(witness) == 2:
            # P2WPKH: witness = [sig, pubkey(33)]
            if len(witness[1]) == 33:
                return P2WPKH
            return P2WSH
        if len(witness) == 1 and len(witness[0]) == 65:
            return P2TR
        return P2WSH
    # P2PKH: scriptSig ends with 33/65-byte pubkey
    if len(script) > 0:
        if script[0] == 0x16 and script[1:3] == b'\x00\x14':
            return P2WPKH  # P2SH-P2WPKH
        if script[0] == 0x22 and script[1:3] == b'\x00\x20':
            return P2WSH   # P2SH-P2WSH
        return P2PKH
    return UNKNOWN


def parse_transaction(buf: bytes, offset: int) -> Tuple[Transaction, int, bytes]:
    """Parse a single transaction. Returns (tx, new_offset, raw_no_witness)."""
    start = offset
    version = struct.unpack_from('<i', buf, offset)[0]
    offset += 4

    # Check for SegWit marker
    is_segwit = False
    if buf[offset] == 0x00 and buf[offset + 1] == 0x01:
        is_segwit = True
        offset += 2  # skip marker + flag

    # Inputs
    in_count, offset = read_varint(buf, offset)
    inputs = []
    for _ in range(in_count):
        prev_txid_raw = buf[offset:offset+32]
        prev_txid = prev_txid_raw[::-1].hex()
        offset += 32
        prev_vout = struct.unpack_from('<I', buf, offset)[0]
        offset += 4
        script_len, offset = read_varint(buf, offset)
        script = buf[offset:offset+script_len]
        offset += script_len
        seq = struct.unpack_from('<I', buf, offset)[0]
        offset += 4
        inputs.append(TxInput(prev_txid, prev_vout, script, seq))

    # Outputs
    out_count, offset = read_varint(buf, offset)
    outputs = []
    for _ in range(out_count):
        value = struct.unpack_from('<Q', buf, offset)[0]
        offset += 8
        script_len, offset = read_varint(buf, offset)
        script = buf[offset:offset+script_len]
        offset += script_len
        stype = classify_script(script)
        outputs.append(TxOutput(value, script, stype))

    # Witness
    if is_segwit:
        for inp in inputs:
            wit_count, offset = read_varint(buf, offset)
            for _ in range(wit_count):
                item_len, offset = read_varint(buf, offset)
                inp.witness.append(buf[offset:offset+item_len])
                offset += item_len

    locktime = struct.unpack_from('<I', buf, offset)[0]
    offset += 4

    # Build raw (non-witness) for txid computation
    raw_nw = _serialize_no_witness(version, inputs, outputs, locktime)
    txid = dsha256(raw_nw)[::-1].hex()

    # Detect coinbase
    is_coinbase = (len(inputs) == 1 and
                   inputs[0].prev_txid == '0' * 64 and
                   inputs[0].prev_vout == 0xFFFFFFFF)

    # Compute weight / vsize
    raw_full = buf[start:offset]
    base_size = len(raw_nw)
    total_size = len(raw_full)
    weight = base_size * 3 + total_size  # base*4 - witness_discount => base*3+total
    vsize = weight / 4.0

    tx = Transaction(
        txid=txid,
        version=version,
        inputs=inputs,
        outputs=outputs,
        locktime=locktime,
        is_segwit=is_segwit,
        is_coinbase=is_coinbase,
        weight=weight,
        vsize=vsize,
    )
    return tx, offset, raw_nw


def _serialize_no_witness(version: int, inputs: List[TxInput],
                           outputs: List[TxOutput], locktime: int) -> bytes:
    """Serialize transaction without witness data (for txid)."""
    parts = [struct.pack('<i', version)]
    parts.append(_encode_varint(len(inputs)))
    for inp in inputs:
        parts.append(bytes.fromhex(inp.prev_txid)[::-1])
        parts.append(struct.pack('<I', inp.prev_vout))
        parts.append(_encode_varint(len(inp.script)))
        parts.append(inp.script)
        parts.append(struct.pack('<I', inp.sequence))
    parts.append(_encode_varint(len(outputs)))
    for out in outputs:
        parts.append(struct.pack('<Q', out.value))
        parts.append(_encode_varint(len(out.script)))
        parts.append(out.script)
    parts.append(struct.pack('<I', locktime))
    return b''.join(parts)


def _encode_varint(n: int) -> bytes:
    if n < 0xfd:
        return bytes([n])
    elif n <= 0xffff:
        return b'\xfd' + struct.pack('<H', n)
    elif n <= 0xffffffff:
        return b'\xfe' + struct.pack('<I', n)
    else:
        return b'\xff' + struct.pack('<Q', n)


def _decompress_amount(x: int) -> int:
    if x == 0:
        return 0
    x -= 1
    exponent = x % 10
    x //= 10
    if exponent < 9:
        digit = (x % 9) + 1
        x //= 9
        n = x * 10 + digit
    else:
        n = x + 1
    while exponent > 0:
        n *= 10
        exponent -= 1
    return n


def _decompress_script(buf: bytes, offset: int) -> Tuple[bytes, int]:
    """
    Decompress CScriptCompressor representation.
    """
    code, offset = read_msb_varint(buf, offset)
    if code == 0:
        h160 = buf[offset:offset + 20]
        offset += 20
        return b"\x76\xa9\x14" + h160 + b"\x88\xac", offset
    if code == 1:
        h160 = buf[offset:offset + 20]
        offset += 20
        return b"\xa9\x14" + h160 + b"\x87", offset
    if code in (2, 3):
        xcoord = buf[offset:offset + 32]
        offset += 32
        pubkey = bytes([code]) + xcoord
        return b"\x21" + pubkey + b"\xac", offset
    if code in (4, 5):
        xcoord = buf[offset:offset + 32]
        offset += 32
        # Approximation without full secp recovery. Keep deterministic script bytes.
        pubkey = b"\x04" + xcoord + (b"\x00" * 32)
        return b"\x41" + pubkey + b"\xac", offset
    size = code - 6
    script = buf[offset:offset + size]
    offset += size
    return script, offset


def _parse_txin_undo(buf: bytes, offset: int) -> Tuple[UndoCoin, int]:
    code, offset = read_msb_varint(buf, offset)
    height = code >> 1
    if height > 0:
        _, offset = read_msb_varint(buf, offset)  # tx version, unused
    compressed_amount, offset = read_msb_varint(buf, offset)
    value = _decompress_amount(compressed_amount)
    if value < 0 or value > MAX_MONEY:
        raise ValueError("undo coin value out of range")
    script, offset = _decompress_script(buf, offset)
    return UndoCoin(value=value, script=script, script_type=classify_script(script)), offset


def _parse_block_undo(payload: bytes) -> List[List[UndoCoin]]:
    """
    Parse CBlockUndo payload (excluding trailing 32-byte checksum).
    Returns one entry per non-coinbase transaction, where each entry is
    a list of UndoCoin matching that tx's inputs.
    """
    offset = 0
    tx_undo_count, offset = read_varint(payload, offset)
    tx_undos: List[List[UndoCoin]] = []
    for _ in range(tx_undo_count):
        in_count, offset = read_varint(payload, offset)
        coins: List[UndoCoin] = []
        for _ in range(in_count):
            coin, offset = _parse_txin_undo(payload, offset)
            coins.append(coin)
        tx_undos.append(coins)
    return tx_undos


def parse_undo_from_file(
    rev_path: str,
    xor_path: str,
    max_blocks: Optional[int] = None,
    blocks: Optional[List[Block]] = None,
) -> List[List[List[UndoCoin]]]:
    """
    Parse all CBlockUndo records from rev*.dat.
    Returns:
      [
        [ [UndoCoin,...], [UndoCoin,...], ... ],  # block0 tx undos (non-coinbase txs)
        [ ... ],                                   # block1
        ...
      ]
    """
    xor_key = load_xor_key(xor_path)
    with open(rev_path, "rb") as f:
        raw = f.read()
    if xor_key:
        raw = xor_decrypt(raw, xor_key)

    results: List[List[List[UndoCoin]]] = []
    records: List[bytes] = []
    checksums: List[bytes] = []
    offset = 0
    file_size = len(raw)
    while offset < file_size - 8:
        if raw[offset:offset + 4] != MAINNET_MAGIC:
            offset += 1
            continue
        offset += 4
        rec_size = struct.unpack_from("<I", raw, offset)[0]
        offset += 4
        if rec_size == 0 or offset + rec_size > file_size:
            break
        record = raw[offset:offset + rec_size]
        offset += rec_size

        # rev*.dat stores a 32-byte checksum after each sized CBlockUndo payload.
        # The checksum bytes are not part of CBlockUndo and must be skipped to
        # preserve record alignment for subsequent entries.
        checksum = b""
        if offset + 32 <= file_size:
            checksum = raw[offset:offset + 32]
            offset += 32
        try:
            tx_undos = _parse_block_undo(record)
            results.append(tx_undos)
            records.append(record)
            checksums.append(checksum)
        except Exception:
            # Keep positional alignment with blk*.dat by storing empty undo data.
            results.append([])
            records.append(record)
            checksums.append(checksum)
        if max_blocks is not None and len(results) >= max_blocks:
            break

    # When block list is available, align undo records by checksum identity instead
    # of file position. Some datasets store blk and rev records in different orders.
    if blocks is not None:
        aligned: List[List[List[UndoCoin]]] = [[] for _ in blocks]
        remaining: Dict[int, None] = {i: None for i in range(len(results))}

        for b_idx, block in enumerate(blocks):
            block_hash_le = bytes.fromhex(block.header.block_hash)[::-1]
            matched_idx = None
            for r_idx in remaining.keys():
                ck = checksums[r_idx]
                if not ck:
                    continue
                if dsha256(block_hash_le + records[r_idx]) == ck:
                    matched_idx = r_idx
                    break
            if matched_idx is not None:
                aligned[b_idx] = results[matched_idx]
                remaining.pop(matched_idx, None)
            elif b_idx < len(results):
                # Positional fallback for checksum-unavailable records.
                aligned[b_idx] = results[b_idx]
        return aligned

    return results


def attach_undo_data(blocks: List[Block], undo_blocks: List[List[List[UndoCoin]]]) -> None:
    """
    Attach prevout information from rev undo data to parsed block transactions.
    """
    block_count = min(len(blocks), len(undo_blocks))
    for b_idx in range(block_count):
        block = blocks[b_idx]
        tx_undos = undo_blocks[b_idx]
        tx_count = min(max(len(block.transactions) - 1, 0), len(tx_undos))
        for t_idx in range(tx_count):
            tx = block.transactions[t_idx + 1]  # skip coinbase
            input_undos = tx_undos[t_idx]
            in_count = min(len(tx.inputs), len(input_undos))
            for i in range(in_count):
                coin = input_undos[i]
                tx_in = tx.inputs[i]
                tx_in.prevout_value = coin.value
                tx_in.prevout_script = coin.script
                tx_in.prevout_script_type = coin.script_type


def extract_bip34_height(coinbase_script: bytes) -> int:
    """Extract block height from coinbase input script (BIP34)."""
    try:
        push_len = coinbase_script[0]
        if push_len == 0 or push_len > 5:
            return -1
        raw = coinbase_script[1:1 + push_len]
        height = int.from_bytes(raw, 'little')
        return height
    except Exception:
        return -1


def parse_blocks_from_file(blk_path: str, xor_path: str, max_blocks: Optional[int] = None) -> List[Block]:
    """Parse all blocks from a blk*.dat file, applying XOR if needed."""
    xor_key = load_xor_key(xor_path)

    with open(blk_path, 'rb') as f:
        raw = f.read()

    if xor_key:
        raw = xor_decrypt(raw, xor_key)

    blocks = []
    offset = 0
    file_size = len(raw)

    while offset < file_size - 8:
        # Scan for magic bytes
        if raw[offset:offset+4] != MAINNET_MAGIC:
            offset += 1
            continue

        offset += 4
        block_size = struct.unpack_from('<I', raw, offset)[0]
        offset += 4

        if block_size == 0 or offset + block_size > file_size:
            break

        block_data = raw[offset:offset + block_size]
        offset += block_size

        try:
            block = _parse_block(block_data)
            if block:
                blocks.append(block)
                if max_blocks is not None and len(blocks) >= max_blocks:
                    break
        except Exception:
            continue

    return blocks


def _parse_block(data: bytes) -> Optional[Block]:
    """Parse a single block from its raw bytes (after magic + size stripped)."""
    if len(data) < 80:
        return None

    header_raw = data[:80]
    block_hash = dsha256(header_raw)[::-1].hex()

    version  = struct.unpack_from('<i', header_raw, 0)[0]
    prev_hash = header_raw[4:36][::-1].hex()
    merkle   = header_raw[36:68][::-1].hex()
    timestamp = struct.unpack_from('<I', header_raw, 68)[0]
    bits     = struct.unpack_from('<I', header_raw, 72)[0]
    nonce    = struct.unpack_from('<I', header_raw, 76)[0]

    header = BlockHeader(version, prev_hash, merkle, timestamp, bits, nonce, block_hash)

    offset = 80
    tx_count, offset = read_varint(data, offset)
    transactions = []

    for i in range(tx_count):
        tx, offset, _ = parse_transaction(data, offset)
        transactions.append(tx)

    # Extract height from coinbase
    height = -1
    if transactions and transactions[0].is_coinbase:
        cb_script = transactions[0].inputs[0].script
        height = extract_bip34_height(cb_script)

    return Block(header=header, height=height, transactions=transactions)
