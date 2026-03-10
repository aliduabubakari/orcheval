#!/usr/bin/env python3
"""
Import Smoke Test CLI
=====================

Stages:
  1) compile      : py_compile.compile(file, doraise=True)
  2) import_exec  : importlib loads module from file path (exec_module)

Output:
  - JSON always (stdout by default; write to --out if provided)
  - Includes stage, error_type, missing_module (if ModuleNotFoundError), traceback

This is designed to run in the orchestrator-specific venv to isolate dependency failures.
"""

from __future__ import annotations

import argparse
import json
import os
import platform
import sys
import traceback
import py_compile
import importlib.util
from datetime import datetime
from pathlib import Path

# bootstrap (so we can reuse orchestrator detection)
SCRIPTS_DIR = Path(__file__).resolve().parents[1]  # .../scripts
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

from ..base_evaluator import BaseEvaluator, Orchestrator  # noqa: E402


def _emit(payload: dict, out: str | None) -> None:
    txt = json.dumps(payload, indent=2, default=str)
    if out:
        p = Path(out)
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(txt, encoding="utf-8")
    else:
        print(txt)


def main() -> None:
    ap = argparse.ArgumentParser(description="Compile + import smoke test for generated workflow code.")
    ap.add_argument("file", help="Path to generated workflow Python file")
    ap.add_argument(
        "--orchestrator",
        default="auto",
        choices=["auto", "airflow", "prefect", "dagster"],
        help="Orchestrator label (for reporting only). Default auto-detect from code.",
    )
    ap.add_argument("--out", default=None, help="Write JSON to this file; otherwise print to stdout")
    args = ap.parse_args()

    file_path = Path(args.file)

    code = ""
    try:
        code = file_path.read_text(encoding="utf-8")
    except Exception:
        pass

    if args.orchestrator == "auto":
        orch = BaseEvaluator().detect_orchestrator(code).value
    else:
        orch = Orchestrator(args.orchestrator).value

    payload = {
        "evaluation_type": "import_smoke_test",
        "file_path": str(file_path),
        "orchestrator": orch,
        "timestamp": datetime.now().isoformat(),
        "ok": False,
        "stages": {"compile_ok": False, "import_exec_ok": False},
        "metadata": {
            "python": sys.executable,
            "python_version": platform.python_version(),
            "smoke_env_flag": "PIPELINE_SMOKE_TEST=1",
        },
        "error": None,
    }

    # Stage 1: compile
    try:
        py_compile.compile(str(file_path), doraise=True)
        payload["stages"]["compile_ok"] = True
    except Exception as e:
        payload["error"] = {
            "stage": "compile",
            "error_type": type(e).__name__,
            "message": str(e),
            "traceback": traceback.format_exc(),
        }
        _emit(payload, args.out)
        return

    # Stage 2: import/exec
    try:
        mod_name = f"smoke_{file_path.stem}_{os.getpid()}"
        spec = importlib.util.spec_from_file_location(mod_name, str(file_path))
        if spec is None or spec.loader is None:
            raise RuntimeError("spec_from_file_location returned no loader/spec")

        module = importlib.util.module_from_spec(spec)

        # Let generated code optionally guard import-time side effects
        os.environ["PIPELINE_SMOKE_TEST"] = "1"

        spec.loader.exec_module(module)

        payload["stages"]["import_exec_ok"] = True
        payload["ok"] = True

    except ModuleNotFoundError as e:
        payload["error"] = {
            "stage": "import_exec",
            "error_type": type(e).__name__,
            "missing_module": getattr(e, "name", None),
            "message": str(e),
            "traceback": traceback.format_exc(),
        }
    except Exception as e:
        payload["error"] = {
            "stage": "import_exec",
            "error_type": type(e).__name__,
            "message": str(e),
            "traceback": traceback.format_exc(),
        }

    _emit(payload, args.out)


if __name__ == "__main__":
    main()