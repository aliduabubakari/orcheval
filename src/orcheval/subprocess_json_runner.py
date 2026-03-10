#!/usr/bin/env python3
"""
subprocess_json_runner.py
========================

Run an evaluation CLI inside a different Python (e.g., venv/bin/python),
capture stdout/stderr, and guarantee a JSON payload exists on disk even if:

- the subprocess crashes
- the subprocess times out
- the subprocess produces invalid JSON
- the subprocess produces no JSON at all

The returned payload includes a `_subprocess` field with command + tails.
"""

from __future__ import annotations

import json
import os
import subprocess
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional


def run_cli_json(
    *,
    python_exe: Path,
    script_path: Path,
    args: List[str],
    out_json: Path,
    cwd: Path,
    env: Optional[Dict[str, str]] = None,
    timeout_s: int = 120,
    stub_type: str = "unknown_cli",
    orchestrator: str = "unknown",
    file_path: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Runs: <python_exe> <script_path> <args...> and expects the CLI to write out_json.

    Always returns a dict and always writes out_json (fallback stub if needed).
    """
    out_json = Path(out_json)
    out_json.parent.mkdir(parents=True, exist_ok=True)

    cmd = [str(python_exe), str(script_path)] + list(args)

    merged_env = os.environ.copy()
    if env:
        merged_env.update({k: str(v) for k, v in env.items()})

    timed_out = False
    stdout = ""
    stderr = ""
    returncode: int = 0

    try:
        res = subprocess.run(
            cmd,
            cwd=str(cwd),
            env=merged_env,
            capture_output=True,
            text=True,
            timeout=timeout_s,
            check=False,
        )
        returncode = int(res.returncode)
        stdout = res.stdout or ""
        stderr = res.stderr or ""

    except subprocess.TimeoutExpired as e:
        timed_out = True
        returncode = -9
        stdout = (e.stdout or "") if isinstance(e.stdout, str) else ""
        stderr = (e.stderr or "") if isinstance(e.stderr, str) else ""

    payload: Optional[Dict[str, Any]] = None

    if out_json.exists():
        try:
            payload = json.loads(out_json.read_text(encoding="utf-8"))
        except Exception as parse_e:
            payload = None
            stderr = (stderr + "\n" + f"[controller] failed to parse JSON: {type(parse_e).__name__}: {parse_e}")[-8000:]

    if payload is None:
        payload = {
            "evaluation_type": stub_type,
            "file_path": file_path,
            "orchestrator": orchestrator,
            "timestamp": datetime.now().isoformat(),
            "ok": False,
            "metadata": {
                "error": "no_json_emitted",
                "timed_out": timed_out,
            },
        }

    payload["_subprocess"] = {
        "cmd": cmd,
        "cwd": str(cwd),
        "returncode": returncode,
        "timed_out": bool(timed_out),
        "stdout_tail": stdout[-4000:],
        "stderr_tail": stderr[-4000:],
    }

    # Guarantee: write something parseable
    out_json.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
    return payload