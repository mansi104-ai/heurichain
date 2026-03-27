"""
Web visualizer server for Bitcoin chain analysis results.
Serves the interactive UI and provides /api/* endpoints.
"""

import os
import sys
import json
import glob
import uuid
import time
import threading
import gzip
from pathlib import Path

# Allow imports from project root
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from flask import Flask, jsonify, send_from_directory, request, abort
from werkzeug.utils import secure_filename
from werkzeug.exceptions import HTTPException

from src.parser.block_parser import parse_blocks_from_file, parse_undo_from_file, attach_undo_data
from src.analyzer import analyze_file
from src.reporter import generate_report
from src.psbt.builder import build_psbt_base64, tx_summary

app = Flask(__name__, static_folder="static")
ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUT_DIR = os.path.join(ROOT_DIR, "out")
FIXTURES_DIR = os.path.join(ROOT_DIR, "fixtures")
UPLOAD_DIR = os.path.join(ROOT_DIR, "uploads")
os.makedirs(OUT_DIR, exist_ok=True)
os.makedirs(UPLOAD_DIR, exist_ok=True)

# stem -> source file paths; populated from fixtures and upload API.
SOURCE_REGISTRY = {}
# stem -> parsed block cache
PARSED_CACHE = {}
UPLOAD_JOBS = {}
SOURCE_INDEX_PATH = os.path.join(UPLOAD_DIR, "sources.json")


def _save_uploaded_file(file_storage, target_dir: str, default_name: str) -> str:
    """
    Save uploaded file to disk.
    Supports raw .dat and .dat.gz uploads (auto-decompresses to .dat).
    Returns path to saved file.
    """
    name = secure_filename(file_storage.filename or default_name)
    path = os.path.join(target_dir, name)
    file_storage.save(path)
    if name.lower().endswith(".gz"):
        out_path = path[:-3]
        with gzip.open(path, "rb") as src, open(out_path, "wb") as dst:
            dst.write(src.read())
        return out_path
    return path


def load_analysis(stem: str) -> dict:
    path = os.path.join(OUT_DIR, f"{stem}.json")
    if not os.path.isfile(path):
        return None
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def _register_source(stem: str, blk_path: str, rev_path: str, xor_path: str) -> None:
    SOURCE_REGISTRY[stem] = {
        "blk_path": blk_path,
        "rev_path": rev_path,
        "xor_path": xor_path,
    }
    _persist_sources()


def _persist_sources() -> None:
    try:
        with open(SOURCE_INDEX_PATH, "w", encoding="utf-8") as f:
            json.dump(SOURCE_REGISTRY, f, indent=2)
    except Exception:
        pass


def _load_persisted_sources() -> None:
    if not os.path.isfile(SOURCE_INDEX_PATH):
        return
    try:
        with open(SOURCE_INDEX_PATH, encoding="utf-8") as f:
            data = json.load(f)
        for stem, src in data.items():
            blk = src.get("blk_path", "")
            rev = src.get("rev_path", "")
            xor = src.get("xor_path", "")
            if os.path.isfile(blk) and os.path.isfile(rev):
                SOURCE_REGISTRY[stem] = {"blk_path": blk, "rev_path": rev, "xor_path": xor}
    except Exception:
        return


def _bootstrap_fixture_sources() -> None:
    xor = os.path.join(FIXTURES_DIR, "xor.dat")
    for blk_path in glob.glob(os.path.join(FIXTURES_DIR, "blk*.dat")):
        stem = Path(blk_path).stem
        rev_path = os.path.join(FIXTURES_DIR, f"{Path(blk_path).name.replace('blk', 'rev', 1)}")
        if os.path.isfile(rev_path):
            _register_source(stem, blk_path, rev_path, xor if os.path.isfile(xor) else "")


_bootstrap_fixture_sources()
_load_persisted_sources()


def _resolve_source(stem: str, analysis: dict) -> dict | None:
    src = SOURCE_REGISTRY.get(stem)
    if src:
        return src

    # Fallback for fixtures by analysis file name.
    filename = analysis.get("file", "")
    if filename:
        # Direct fixture lookup (top-level fixtures)
        blk_path = os.path.join(FIXTURES_DIR, filename)
        rev_path = os.path.join(FIXTURES_DIR, filename.replace("blk", "rev", 1))
        xor_path = os.path.join(FIXTURES_DIR, "xor.dat")
        if os.path.isfile(blk_path) and os.path.isfile(rev_path):
            _register_source(stem, blk_path, rev_path, xor_path if os.path.isfile(xor_path) else "")
            return SOURCE_REGISTRY[stem]

        # Recursive lookup for nested fixture/upload runs (e.g. fixtures/generated/*).
        # We resolve by matching basename, then finding sibling rev and optional xor.
        search_roots = [FIXTURES_DIR, UPLOAD_DIR]
        rev_name = filename.replace("blk", "rev", 1)
        for root in search_roots:
            pattern = os.path.join(root, "**", filename)
            for candidate_blk in glob.glob(pattern, recursive=True):
                candidate_rev = os.path.join(os.path.dirname(candidate_blk), rev_name)
                if not os.path.isfile(candidate_rev):
                    continue
                candidate_xor = os.path.join(os.path.dirname(candidate_blk), "xor.dat")
                if not os.path.isfile(candidate_xor):
                    candidate_xor = xor_path if os.path.isfile(xor_path) else ""
                _register_source(stem, candidate_blk, candidate_rev, candidate_xor)
                return SOURCE_REGISTRY[stem]
    return None


def _load_parsed_blocks(stem: str, source: dict, max_blocks: int | None = None):
    stem_cache = PARSED_CACHE.setdefault(stem, {})
    cache_key = max_blocks if max_blocks is not None else -1
    cached = stem_cache.get(cache_key)
    if cached:
        return cached
    blocks = parse_blocks_from_file(source["blk_path"], source.get("xor_path", "") or "", max_blocks=max_blocks)
    undo_blocks = parse_undo_from_file(
        source["rev_path"],
        source.get("xor_path", "") or "",
        max_blocks=max_blocks,
        blocks=blocks,
    )
    attach_undo_data(blocks, undo_blocks)
    stem_cache[cache_key] = blocks
    return blocks


def _run_upload_job(job_id: str, blk_path: str, rev_path: str, xor_path: str, stem: str, make_report: bool) -> None:
    UPLOAD_JOBS[job_id]["status"] = "processing"
    try:
        blocks = parse_blocks_from_file(blk_path, xor_path)
        undo_blocks = parse_undo_from_file(rev_path, xor_path, blocks=blocks)
        attach_undo_data(blocks, undo_blocks)
        analysis = analyze_file(blk_path, blocks)

        json_path = os.path.join(OUT_DIR, f"{stem}.json")
        with open(json_path, "w", encoding="utf-8") as jf:
            json.dump(analysis, jf, indent=2)

        report_path = ""
        if make_report:
            report_path = os.path.join(OUT_DIR, f"{stem}.md")
            with open(report_path, "w", encoding="utf-8") as mf:
                mf.write(generate_report(analysis))

        _register_source(stem, blk_path, rev_path, xor_path)
        PARSED_CACHE.pop(stem, None)
        UPLOAD_JOBS[job_id] = {
            "status": "done",
            "ok": True,
            "stem": stem,
            "json": f"out/{stem}.json",
            "report": f"out/{stem}.md" if report_path else None,
            "block_count": analysis.get("block_count", 0),
            "transactions": analysis.get("analysis_summary", {}).get("total_transactions_analyzed", 0),
        }
    except Exception as e:
        UPLOAD_JOBS[job_id] = {"status": "error", "ok": False, "error": str(e)}


# ─── Health ────────────────────────────────────────────────────────────────────

@app.route("/api/health")
def health():
    return jsonify({"ok": True})


# ─── List available block files ────────────────────────────────────────────────

@app.route("/api/blocks")
def list_blocks():
    files = sorted(glob.glob(os.path.join(OUT_DIR, "*.json")))
    items = []
    for f in files:
        stem = Path(f).stem
        try:
            with open(f, encoding="utf-8") as fh:
                data = json.load(fh)
            items.append({
                "stem": stem,
                "file": data.get("file"),
                "block_count": data.get("block_count"),
                "total_transactions": data.get("analysis_summary", {}).get("total_transactions_analyzed"),
                "flagged": data.get("analysis_summary", {}).get("flagged_transactions"),
            })
        except Exception:
            items.append({"stem": stem})
    return jsonify({"ok": True, "files": items})


# ─── Full analysis for a block file ───────────────────────────────────────────

@app.route("/api/blocks/<stem>")
def get_block_analysis(stem: str):
    data = load_analysis(stem)
    if data is None:
        return jsonify({"ok": False, "error": "Not found"}), 404
    return jsonify(data)


# ─── Per-block detail ─────────────────────────────────────────────────────────

@app.route("/api/blocks/<stem>/<int:block_index>")
def get_block(stem: str, block_index: int):
    data = load_analysis(stem)
    if data is None:
        return jsonify({"ok": False, "error": "Not found"}), 404
    blocks = data.get("blocks", [])
    if block_index >= len(blocks):
        return jsonify({"ok": False, "error": "Block index out of range"}), 404
    return jsonify({"ok": True, "block": blocks[block_index]})


# ─── Transaction search ────────────────────────────────────────────────────────

@app.route("/api/search")
def search_tx():
    txid = request.args.get("txid", "").strip().lower()
    classification = request.args.get("classification", "").strip()
    heuristic = request.args.get("heuristic", "").strip()
    stem = request.args.get("stem", "").strip()

    if not stem:
        return jsonify({"ok": False, "error": "stem required"}), 400

    data = load_analysis(stem)
    if data is None:
        return jsonify({"ok": False, "error": "Not found"}), 404

    results = []
    for block in data.get("blocks", []):
        for tx in block.get("transactions", []):
            if txid and not tx.get("txid", "").startswith(txid):
                continue
            if classification and tx.get("classification") != classification:
                continue
            if heuristic and not tx.get("heuristics", {}).get(heuristic, {}).get("detected"):
                continue
            results.append({
                "txid": tx.get("txid"),
                "classification": tx.get("classification"),
                "block_hash": block.get("block_hash"),
                "block_height": block.get("block_height"),
                "heuristics": {k: v for k, v in tx.get("heuristics", {}).items()
                               if v.get("detected")},
            })
            if len(results) >= 100:
                break
        if len(results) >= 100:
            break

    return jsonify({"ok": True, "count": len(results), "results": results})


@app.route("/api/upload-analyze", methods=["POST"])
def upload_analyze():
    try:
        blk = request.files.get("blk")
        rev = request.files.get("rev")
        xor = request.files.get("xor")
        if blk is None or rev is None:
            return jsonify({"ok": False, "error": "blk and rev files are required"}), 400

        run_dir = os.path.join(UPLOAD_DIR, f"run_{int(time.time())}_{uuid.uuid4().hex[:8]}")
        os.makedirs(run_dir, exist_ok=True)

        blk_name = secure_filename(blk.filename or "blk_upload.dat")
        rev_name = secure_filename(rev.filename or "rev_upload.dat")
        make_report = request.form.get("make_report", "0").strip() in {"1", "true", "True"}

        blk_path = _save_uploaded_file(blk, run_dir, "blk_upload.dat")
        rev_path = _save_uploaded_file(rev, run_dir, "rev_upload.dat")
        xor_path = ""
        if xor and xor.filename:
            xor_path = _save_uploaded_file(xor, run_dir, "xor.dat")
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500

    stem = Path(blk_path).stem
    job_id = uuid.uuid4().hex
    UPLOAD_JOBS[job_id] = {"status": "queued", "ok": True, "stem": stem}
    thread = threading.Thread(
        target=_run_upload_job,
        args=(job_id, blk_path, rev_path, xor_path, stem, make_report),
        daemon=True,
    )
    thread.start()
    return jsonify({"ok": True, "job_id": job_id, "status": "queued", "stem": stem}), 202


@app.route("/api/upload-status/<job_id>")
def upload_status(job_id: str):
    job = UPLOAD_JOBS.get(job_id)
    if not job:
        return jsonify({"ok": False, "error": "job not found"}), 404
    return jsonify(job)


@app.route("/api/tx-summary")
def transaction_summary():
    stem = request.args.get("stem", "").strip()
    txid = request.args.get("txid", "").strip().lower()
    block_index = int(request.args.get("block_index", "0"))
    include_psbt = request.args.get("include_psbt", "1").strip() not in {"0", "false", "False"}

    if not stem or not txid:
        return jsonify({"ok": False, "error": "stem and txid are required"}), 400

    analysis = load_analysis(stem)
    if analysis is None:
        return jsonify({"ok": False, "error": "analysis not found"}), 404

    source = _resolve_source(stem, analysis)
    if source is None:
        return jsonify({"ok": False, "error": "source files unavailable for this stem"}), 404

    try:
        blocks = _load_parsed_blocks(stem, source, max_blocks=block_index + 1)
    except Exception as e:
        return jsonify({"ok": False, "error": f"failed to parse source files: {e}"}), 500

    if block_index < 0 or block_index >= len(blocks):
        return jsonify({"ok": False, "error": "block_index out of range"}), 400

    tx = next((t for t in blocks[block_index].transactions if t.txid == txid), None)
    if tx is None:
        return jsonify({"ok": False, "error": "txid not found in block"}), 404

    summary = tx_summary(tx)
    result = {"ok": True, "summary": summary}
    if include_psbt:
        try:
            result["psbt_base64"] = build_psbt_base64(tx)
        except Exception as e:
            result["psbt_error"] = str(e)
    return jsonify(result)


@app.errorhandler(HTTPException)
def handle_http_exception(e: HTTPException):
    if request.path.startswith("/api/"):
        return jsonify({"ok": False, "error": e.description or e.name}), e.code
    return e


@app.errorhandler(Exception)
def handle_unexpected_exception(e: Exception):
    if request.path.startswith("/api/"):
        return jsonify({"ok": False, "error": str(e)}), 500
    raise e


# ─── Static frontend ───────────────────────────────────────────────────────────

@app.route("/")
def index():
    return send_from_directory(app.static_folder, "index.html")


@app.route("/<path:path>")
def static_files(path):
    return send_from_directory(app.static_folder, path)


# ─── Main ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    _bootstrap_fixture_sources()
    port = int(os.environ.get("PORT", 3000))
    app.run(host="0.0.0.0", port=port, debug=False)
