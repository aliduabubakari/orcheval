#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

from .carbon_tracker import try_start_tracker, try_stop_tracker


def _emit(payload: Dict[str, Any], out: Path) -> None:
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")


def _numeric(v: Any) -> float:
    if isinstance(v, bool):
        return 1.0 if v else 0.0
    if isinstance(v, (int, float)):
        return float(v)
    if isinstance(v, str):
        try:
            return float(v)
        except Exception:
            return float(len(v))
    return 0.0


def _run_workload(rows: List[Dict[str, Any]], *, orchestrator: str, max_tasks: int, seed: int) -> Dict[str, Any]:
    t0 = time.time()
    row_count = len(rows)
    if row_count == 0:
        return {
            "rows_processed": 0,
            "tasks_exercised": 0,
            "numeric_accumulator": 0.0,
            "hash_accumulator": 0,
            "duration_s": 0.0,
        }

    task_factor_map = {
        "airflow": 1.3,
        "prefect": 1.0,
        "dagster": 1.15,
    }
    factor = task_factor_map.get(orchestrator, 1.0)
    task_budget = max(1, min(int(max_tasks), int(max_tasks * factor)))

    numeric_acc = 0.0
    hash_acc = 0

    for t in range(task_budget):
        for r in rows:
            for k, v in r.items():
                numeric_acc += _numeric(v) * (1.0 + (t * 0.01))
                blob = f"{seed}:{t}:{k}:{v}".encode("utf-8", errors="ignore")
                h = hashlib.sha256(blob).hexdigest()
                hash_acc ^= int(h[:8], 16)

    duration_s = max(0.0, time.time() - t0)
    return {
        "rows_processed": row_count,
        "tasks_exercised": task_budget,
        "numeric_accumulator": round(numeric_acc, 6),
        "hash_accumulator": int(hash_acc),
        "duration_s": round(duration_s, 6),
    }


def _load_module_from_file(workflow_file: Path):
    spec = importlib.util.spec_from_file_location("orcheval_runtime_workflow", str(workflow_file))
    if spec is None or spec.loader is None:
        raise RuntimeError("unable_to_create_module_spec")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _run_native_airflow(workflow_file: Path, max_tasks: int) -> Dict[str, Any]:
    from airflow.models import DAG  # type: ignore

    module = _load_module_from_file(workflow_file)
    dags = [v for v in vars(module).values() if isinstance(v, DAG)]
    if not dags:
        raise RuntimeError("no_dag_object_found")
    dag = dags[0]
    tasks = list(getattr(dag, "tasks", []) or [])
    task_budget = max(1, min(int(max_tasks), len(tasks) or 1))
    executed = 0
    skipped = 0
    for t in tasks[:task_budget]:
        fn = getattr(t, "python_callable", None)
        if callable(fn):
            try:
                fn()
                executed += 1
            except Exception:
                skipped += 1
        else:
            skipped += 1
    return {"tasks_exercised": task_budget, "native_tasks_executed": executed, "native_tasks_skipped": skipped}


def _run_native_prefect(workflow_file: Path, max_tasks: int) -> Dict[str, Any]:
    try:
        from prefect.flows import Flow  # type: ignore
    except Exception:
        Flow = None  # type: ignore

    module = _load_module_from_file(workflow_file)
    flow_obj = None
    if Flow is not None:
        for v in vars(module).values():
            if isinstance(v, Flow):
                flow_obj = v
                break
    if flow_obj is None:
        for v in vars(module).values():
            if hasattr(v, "fn") and callable(getattr(v, "fn")):
                flow_obj = v
                break
    if flow_obj is None:
        raise RuntimeError("no_prefect_flow_found")
    fn = getattr(flow_obj, "fn", None)
    if not callable(fn):
        raise RuntimeError("prefect_flow_not_callable")

    ran = False
    try:
        fn()
        ran = True
    except Exception:
        ran = False
    task_budget = max(1, int(max_tasks))
    return {"tasks_exercised": task_budget, "native_tasks_executed": 1 if ran else 0, "native_tasks_skipped": 0 if ran else 1}


def _run_native_dagster(workflow_file: Path, max_tasks: int) -> Dict[str, Any]:
    try:
        from dagster import JobDefinition  # type: ignore
    except Exception:
        JobDefinition = None  # type: ignore

    module = _load_module_from_file(workflow_file)
    job = None
    if JobDefinition is not None:
        for v in vars(module).values():
            if isinstance(v, JobDefinition):
                job = v
                break
    if job is None:
        for v in vars(module).values():
            if hasattr(v, "execute_in_process") and callable(getattr(v, "execute_in_process")):
                job = v
                break
    if job is None:
        raise RuntimeError("no_dagster_job_found")

    executed = False
    try:
        # We intentionally avoid full asset/backfill behavior and run an in-process subset call.
        result = job.execute_in_process()
        executed = bool(getattr(result, "success", True))
    except Exception:
        executed = False
    task_budget = max(1, int(max_tasks))
    return {"tasks_exercised": task_budget, "native_tasks_executed": 1 if executed else 0, "native_tasks_skipped": 0 if executed else 1}


def _run_native_workload(workflow_file: Path, *, orchestrator: str, max_tasks: int) -> Dict[str, Any]:
    t0 = time.time()
    if orchestrator == "airflow":
        base = _run_native_airflow(workflow_file, max_tasks)
    elif orchestrator == "prefect":
        base = _run_native_prefect(workflow_file, max_tasks)
    elif orchestrator == "dagster":
        base = _run_native_dagster(workflow_file, max_tasks)
    else:
        raise RuntimeError(f"native_adapter_unsupported_orchestrator:{orchestrator}")
    base["duration_s"] = round(max(0.0, time.time() - t0), 6)
    return base


def main() -> None:
    parser = argparse.ArgumentParser(description="Runtime representative energy workload runner")
    parser.add_argument("--in", dest="in_path", required=True)
    parser.add_argument("--out", dest="out_path", required=True)
    args = parser.parse_args()

    in_path = Path(args.in_path)
    out_path = Path(args.out_path)

    try:
        req = json.loads(in_path.read_text(encoding="utf-8"))
        if not isinstance(req, dict):
            raise ValueError("input payload must be JSON object")
    except Exception as e:
        payload = {
            "evaluation_type": "runtime_energy",
            "timestamp": datetime.now().isoformat(),
            "ok": False,
            "error": {"error_type": type(e).__name__, "message": str(e)},
        }
        _emit(payload, out_path)
        return

    orchestrator = str(req.get("orchestrator") or "unknown")
    mode = str(req.get("mode") or "unknown")
    data_source = str(req.get("data_source") or "none")
    workflow_file = Path(str(req.get("workflow_file") or ""))
    execution_adapter = str(req.get("execution_adapter") or "representative").strip().lower()
    max_rows = int(req.get("max_rows") or 500)
    max_tasks = int(req.get("max_tasks") or 25)
    seed = int(req.get("seed") or 1729)

    rows_raw = req.get("rows")
    rows: List[Dict[str, Any]] = []
    if isinstance(rows_raw, list):
        for r in rows_raw:
            if isinstance(r, dict):
                rows.append(r)

    tracker = try_start_tracker(
        project_name=f"orcheval_runtime_{orchestrator}_{mode}",
        extra={
            "measurement_scope": "runtime_representative_workload",
            "disclaimer": (
                "This measurement covers a bounded representative workload used for comparative "
                "evaluation; it is not full production workflow runtime."
            ),
        },
    )
    adapter_attempts: List[Dict[str, Any]] = []
    selected_adapter = execution_adapter if execution_adapter in {"native", "representative", "auto"} else "representative"
    run_stats: Dict[str, Any]
    adapter_used = "representative"
    try:
        if selected_adapter == "representative":
            run_stats = _run_workload(rows, orchestrator=orchestrator, max_tasks=max_tasks, seed=seed)
            adapter_attempts.append({"adapter": "representative", "status": "success"})
        elif selected_adapter == "native":
            run_stats = _run_native_workload(workflow_file, orchestrator=orchestrator, max_tasks=max_tasks)
            adapter_used = "native"
            adapter_attempts.append({"adapter": "native", "status": "success"})
        else:
            try:
                run_stats = _run_native_workload(workflow_file, orchestrator=orchestrator, max_tasks=max_tasks)
                adapter_used = "native"
                adapter_attempts.append({"adapter": "native", "status": "success"})
            except Exception as e:
                adapter_attempts.append({"adapter": "native", "status": "failed", "reason": f"{type(e).__name__}:{e}"})
                run_stats = _run_workload(rows, orchestrator=orchestrator, max_tasks=max_tasks, seed=seed)
                adapter_used = "representative"
                adapter_attempts.append({"adapter": "representative", "status": "success"})
    except Exception as e:
        payload = {
            "evaluation_type": "runtime_energy",
            "timestamp": datetime.now().isoformat(),
            "ok": False,
            "mode": mode,
            "data_source": data_source,
            "orchestrator": orchestrator,
            "error": {"error_type": type(e).__name__, "message": str(e)},
            "runtime_measurement": {
                "status": "failed",
                "execution_adapter_requested": selected_adapter,
                "execution_adapter_used": adapter_used,
                "adapter_attempts": adapter_attempts,
            },
        }
        _emit(payload, out_path)
        return
    energy = try_stop_tracker(tracker)
    if tracker is None:
        energy["measurement_scope"] = "runtime_representative_workload"
        energy["disclaimer"] = (
            "This measurement covers a bounded representative workload used for comparative "
            "evaluation; it is not full production workflow runtime."
        )

    measured_kwh = energy.get("energy_consumed_kwh")
    measured_kg = energy.get("emissions_kgco2eq")
    rows_processed = int(run_stats.get("rows_processed") or 0)

    normalized = {
        "energy_kwh_per_1k_rows": None,
        "emissions_kgco2eq_per_1k_rows": None,
    }
    if isinstance(measured_kwh, (int, float)) and rows_processed > 0:
        normalized["energy_kwh_per_1k_rows"] = round((float(measured_kwh) / rows_processed) * 1000.0, 10)
    if isinstance(measured_kg, (int, float)) and rows_processed > 0:
        normalized["emissions_kgco2eq_per_1k_rows"] = round((float(measured_kg) / rows_processed) * 1000.0, 10)

    payload = {
        "evaluation_type": "runtime_energy",
        "timestamp": datetime.now().isoformat(),
        "ok": True,
        "mode": mode,
        "data_source": data_source,
        "orchestrator": orchestrator,
        "runtime_measurement": {
            "status": "ok",
            "execution_adapter_requested": selected_adapter,
            "execution_adapter_used": adapter_used,
            "adapter_attempts": adapter_attempts,
            "rows_processed": rows_processed,
            "tasks_exercised": int(run_stats.get("tasks_exercised") or 0),
            "duration_s": float(run_stats.get("duration_s") or 0.0),
            "numeric_accumulator": run_stats.get("numeric_accumulator"),
            "hash_accumulator": run_stats.get("hash_accumulator"),
            "energy_profile": energy,
            "normalized": normalized,
            "bounds": {
                "max_rows": max_rows,
                "max_tasks": max_tasks,
                "seed": seed,
            },
        },
    }
    _emit(payload, out_path)


if __name__ == "__main__":
    main()
