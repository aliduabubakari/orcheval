#!/usr/bin/env python3
"""
Standalone CLI for penalty-free Platform Compliance Testing (PCT).

Guarantee:
- Always emits a JSON payload (even if tester.evaluate throws).
"""

# -------------------- bootstrap --------------------
import sys
from pathlib import Path

SCRIPTS_DIR = Path(__file__).resolve().parents[2]  # .../scripts
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))
# --------------------------------------------------

import json
import logging
import traceback
import platform
from datetime import datetime

from ..base_evaluator import Orchestrator, BaseEvaluator
from .airflow_compliance import AirflowComplianceTester
from .prefect_compliance import PrefectComplianceTester
from .dagster_compliance import DagsterComplianceTester


TESTERS = {
    Orchestrator.AIRFLOW: AirflowComplianceTester,
    Orchestrator.PREFECT: PrefectComplianceTester,
    Orchestrator.DAGSTER: DagsterComplianceTester,
}


def _print_pct_summary(payload: dict) -> None:
    summary_pct = payload.get("metadata", {}).get("PCT", 0.0)
    gates_passed = payload.get("gates_passed", False)
    dims = payload.get("metadata", {}).get("PCT_dimensions", {})

    issues = payload.get("issues", [])
    crit = sum(1 for i in issues if i.get("severity") == "critical")
    maj = sum(1 for i in issues if i.get("severity") == "major")
    minor = sum(1 for i in issues if i.get("severity") == "minor")

    print("\n" + "=" * 80)
    print("PCT — Platform Compliance Summary (penalty-free, gate-based)")
    print("=" * 80)
    print(f"File:         {payload.get('file_path')}")
    print(f"Orchestrator: {payload.get('orchestrator')}")
    print(f"Gates passed: {gates_passed}")
    print(f"PCT:          {float(summary_pct):.2f}/10")
    if dims:
        print("Dimensions:")
        for k, v in dims.items():
            print(f"  - {k}: {float(v):.2f}")
    print(f"Issues: total={len(issues)} critical={crit} major={maj} minor={minor}")
    print("=" * 80)


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Run penalty-free PCT platform compliance evaluation.")
    parser.add_argument("file", help="Path to generated workflow Python file")
    parser.add_argument(
        "--orchestrator",
        default="auto",
        choices=["auto", "airflow", "prefect", "dagster"],
        help="Orchestrator to test; default auto-detect",
    )
    parser.add_argument("--out", default=None, help="Write full JSON result to this path")
    parser.add_argument("--out-dir", default=None, help="Write JSON result into this directory (auto filename)")
    parser.add_argument("--knowledge-pack", default=None, help="Path to local knowledge-pack JSON")
    parser.add_argument("--knowledge-pack-version", default=None, help="Expected knowledge-pack version")
    parser.add_argument("--knowledge-pack-mode", default="legacy", choices=["legacy", "pack", "auto"])
    parser.add_argument("--print-summary", action="store_true")
    parser.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    args = parser.parse_args()

    logging.basicConfig(level=getattr(logging, args.log_level), format="%(levelname)s - %(message)s")

    file_path = Path(args.file)
    code = ""
    try:
        code = file_path.read_text(encoding="utf-8")
    except Exception:
        pass

    if args.orchestrator == "auto":
        detector = BaseEvaluator()
        orch = detector.detect_orchestrator(code)
    else:
        orch = Orchestrator(args.orchestrator)

    # --- GUARANTEED JSON PAYLOAD ---
    try:
        if orch not in TESTERS:
            payload = {
                "evaluation_type": "platform_compliance",
                "file_path": str(file_path),
                "orchestrator": orch.value,
                "timestamp": datetime.now().isoformat(),
                "gates_passed": False,
                "gate_checks": [],
                "metadata": {"PCT": 0.0, "error": f"No tester for orchestrator={orch.value}"},
                "scores": {},
                "issues": [],
            }
        else:
            tester_cfg = {
                "knowledge_pack_mode": args.knowledge_pack_mode,
                "knowledge_pack": args.knowledge_pack,
                "knowledge_pack_version": args.knowledge_pack_version,
            }
            tester = TESTERS[orch](config=tester_cfg)
            result = tester.evaluate(file_path)
            payload = result.to_dict()
            payload["issues"] = [i.to_dict() for i in result.all_issues]

    except Exception as e:
        payload = {
            "evaluation_type": "platform_compliance",
            "file_path": str(file_path),
            "orchestrator": orch.value if orch else "unknown",
            "timestamp": datetime.now().isoformat(),
            "gates_passed": False,
            "gate_checks": [],
            "metadata": {
                "PCT": 0.0,
                "error": str(e),
                "error_type": type(e).__name__,
                "traceback": traceback.format_exc(),
                "stage": "pct_cli_exception",
                "python": sys.executable,
                "python_version": platform.python_version(),
            },
            "scores": {},
            "issues": [
                {
                    "severity": "critical",
                    "category": "pct_runtime",
                    "message": f"pct_cli crashed: {type(e).__name__}: {e}",
                    "line": None,
                    "tool": "pct_cli",
                    "details": {},
                }
            ],
        }

    if args.print_summary:
        _print_pct_summary(payload)

    if args.out:
        out_path = Path(args.out)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
        print(f"Wrote: {out_path}")
        return

    if args.out_dir:
        out_dir = Path(args.out_dir)
        out_dir.mkdir(parents=True, exist_ok=True)
        stem = file_path.stem
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        out_path = out_dir / f"pct_{orch.value}_{stem}_{ts}.json"
        out_path.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
        print(f"Wrote: {out_path}")
        return

    # default: stdout JSON
    print(json.dumps(payload, indent=2, default=str))


if __name__ == "__main__":
    main()
