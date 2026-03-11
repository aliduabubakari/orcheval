#!/usr/bin/env python3
"""Workflow DAG comparator for two or more generated workflows."""

from __future__ import annotations

import argparse
import ast
import itertools
import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple

from .base_evaluator import BaseEvaluator
from .energy.structural_energy_analyzer import StructuralEnergyAnalyzer

WORKFLOW_EXTENSIONS = {".py", ".yaml", ".yml"}


def _now_iso() -> str:
    return datetime.now().isoformat()


def _norm_id(value: Any) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    return text


def _jaccard(a: Set[Any], b: Set[Any]) -> float:
    if not a and not b:
        return 1.0
    if not a or not b:
        return 0.0
    return round(float(len(a & b)) / float(len(a | b)), 4)


def _detect_orchestrator(path: Path, code: str) -> str:
    if path.suffix.lower() in {".yaml", ".yml"}:
        if "kestra" in code.lower() or "tasks:" in code:
            return "kestra"
        return "yaml"
    return BaseEvaluator().detect_orchestrator(code).value


def _extract_yaml_signature(code: str) -> Tuple[Set[str], Set[Tuple[str, str]], List[str]]:
    errors: List[str] = []
    tasks: Set[str] = set()
    edges: Set[Tuple[str, str]] = set()

    try:
        import yaml  # type: ignore
    except Exception:
        return tasks, edges, ["PyYAML not installed; YAML graph extraction skipped"]

    try:
        root = yaml.safe_load(code)
    except Exception as e:
        return tasks, edges, [f"YAML parse failed: {type(e).__name__}: {e}"]

    def walk(node: Any) -> None:
        if isinstance(node, dict):
            task_id = _norm_id(node.get("id"))
            if task_id:
                tasks.add(task_id)

            deps = node.get("dependsOn")
            if task_id and isinstance(deps, list):
                for dep in deps:
                    dep_id = _norm_id(dep)
                    if dep_id:
                        edges.add((dep_id, task_id))
            elif task_id and isinstance(deps, str):
                dep_id = _norm_id(deps)
                if dep_id:
                    edges.add((dep_id, task_id))

            for v in node.values():
                walk(v)
            return

        if isinstance(node, list):
            for item in node:
                walk(item)

    walk(root)
    return tasks, edges, errors


def _get_name(node: ast.AST) -> Optional[str]:
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        parent = _get_name(node.value)
        return f"{parent}.{node.attr}" if parent else node.attr
    return None


def _extract_python_fallback(code: str) -> Tuple[Set[str], Set[Tuple[str, str]], List[str]]:
    tasks: Set[str] = set()
    edges: Set[Tuple[str, str]] = set()
    errors: List[str] = []

    try:
        tree = ast.parse(code)
    except Exception as e:
        return tasks, edges, [f"Python AST parse failed: {type(e).__name__}: {e}"]

    var_to_task: Dict[str, str] = {}

    for node in ast.walk(tree):
        if isinstance(node, ast.Assign) and isinstance(node.value, ast.Call):
            task_id: Optional[str] = None
            for kw in node.value.keywords or []:
                if kw.arg in {"task_id", "name"} and isinstance(kw.value, ast.Constant) and isinstance(kw.value.value, str):
                    task_id = kw.value.value
                    break
            if task_id is None:
                task_id = _get_name(node.value.func)
            if not task_id:
                continue
            for tgt in node.targets:
                if isinstance(tgt, ast.Name):
                    var_to_task[tgt.id] = task_id
            tasks.add(task_id)

        if isinstance(node, ast.BinOp) and isinstance(node.op, (ast.RShift, ast.LShift)):
            left = _get_name(node.left)
            right = _get_name(node.right)
            if left and right:
                src = var_to_task.get(left, left)
                dst = var_to_task.get(right, right)
                if isinstance(node.op, ast.RShift):
                    edges.add((src, dst))
                else:
                    edges.add((dst, src))

        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
            owner = _get_name(node.func.value)
            method = node.func.attr
            if method in {"set_downstream", "set_upstream"} and owner and node.args:
                target = _get_name(node.args[0])
                if target:
                    src = var_to_task.get(owner, owner)
                    dst = var_to_task.get(target, target)
                    if method == "set_downstream":
                        edges.add((src, dst))
                    else:
                        edges.add((dst, src))

    return tasks, edges, errors


def extract_workflow_signature(path: Path) -> Dict[str, Any]:
    code = path.read_text(encoding="utf-8", errors="ignore")
    orchestrator = _detect_orchestrator(path, code)
    errors: List[str] = []

    tasks: Set[str] = set()
    edges: Set[Tuple[str, str]] = set()

    if path.suffix.lower() in {".yaml", ".yml"}:
        tasks, edges, errors = _extract_yaml_signature(code)
    else:
        try:
            analyzer = StructuralEnergyAnalyzer()
            profile = analyzer.analyze(path, orchestrator=orchestrator)

            for t in profile.get("tasks", {}).get("canonical_tasks", []):
                if isinstance(t, dict):
                    tid = _norm_id(t.get("id") or t.get("name"))
                    if tid:
                        tasks.add(tid)

            for e in profile.get("edges", {}).get("effective_edges_simple", []):
                if isinstance(e, dict):
                    src = _norm_id(e.get("from"))
                    dst = _norm_id(e.get("to"))
                    if src and dst:
                        edges.add((src, dst))

            for err in profile.get("errors", []):
                if isinstance(err, dict):
                    msg = _norm_id(err.get("message") or err.get("error_type"))
                    if msg:
                        errors.append(msg)

        except Exception as e:
            errors.append(f"Structural analyzer failed: {type(e).__name__}: {e}")

        if not tasks:
            fb_tasks, fb_edges, fb_errors = _extract_python_fallback(code)
            tasks |= fb_tasks
            edges |= fb_edges
            errors.extend(fb_errors)

    return {
        "file_path": str(path),
        "orchestrator": orchestrator,
        "task_count": len(tasks),
        "edge_count": len(edges),
        "tasks": sorted(tasks),
        "edges": [{"from": s, "to": d} for (s, d) in sorted(edges)],
        "errors": errors,
    }


def resolve_workflow_inputs(inputs: Iterable[Path], *, recursive: bool = True) -> List[Path]:
    files: List[Path] = []
    for raw in inputs:
        path = Path(raw)
        if path.is_file():
            if path.suffix.lower() in WORKFLOW_EXTENSIONS:
                files.append(path)
            continue
        if path.is_dir():
            pattern = "**/*" if recursive else "*"
            for candidate in sorted(path.glob(pattern)):
                if candidate.is_file() and candidate.suffix.lower() in WORKFLOW_EXTENSIONS:
                    files.append(candidate)
            continue

    deduped: List[Path] = []
    seen: Set[str] = set()
    for f in files:
        key = str(f.resolve())
        if key not in seen:
            deduped.append(f)
            seen.add(key)
    return deduped


def compare_workflows(paths: Iterable[Path]) -> Dict[str, Any]:
    files = [Path(p) for p in paths]
    signatures = [extract_workflow_signature(p) for p in files]

    pairwise: List[Dict[str, Any]] = []
    all_task_sets: List[Set[str]] = []
    all_edge_sets: List[Set[Tuple[str, str]]] = []

    for sig in signatures:
        all_task_sets.append(set(sig.get("tasks", [])))
        edge_set: Set[Tuple[str, str]] = set()
        for e in sig.get("edges", []):
            if isinstance(e, dict):
                src = _norm_id(e.get("from"))
                dst = _norm_id(e.get("to"))
                if src and dst:
                    edge_set.add((src, dst))
        all_edge_sets.append(edge_set)

    for i, j in itertools.combinations(range(len(signatures)), 2):
        left = signatures[i]
        right = signatures[j]

        left_tasks = all_task_sets[i]
        right_tasks = all_task_sets[j]
        left_edges = all_edge_sets[i]
        right_edges = all_edge_sets[j]

        task_overlap = sorted(left_tasks & right_tasks)
        edge_overlap = sorted(left_edges & right_edges)

        pairwise.append({
            "left_index": i,
            "right_index": j,
            "left_file": left["file_path"],
            "right_file": right["file_path"],
            "left_orchestrator": left["orchestrator"],
            "right_orchestrator": right["orchestrator"],
            "task_overlap_count": len(task_overlap),
            "edge_overlap_count": len(edge_overlap),
            "task_jaccard": _jaccard(left_tasks, right_tasks),
            "edge_jaccard": _jaccard(left_edges, right_edges),
            "similarity_score": round((0.6 * _jaccard(left_tasks, right_tasks)) + (0.4 * _jaccard(left_edges, right_edges)), 4),
            "task_overlap": task_overlap,
            "edge_overlap": [{"from": s, "to": d} for (s, d) in edge_overlap],
            "task_only_left": sorted(left_tasks - right_tasks),
            "task_only_right": sorted(right_tasks - left_tasks),
        })

    common_tasks_all = sorted(set.intersection(*all_task_sets) if all_task_sets else set())
    common_edges_all = sorted(set.intersection(*all_edge_sets) if all_edge_sets else set())
    union_tasks_all = sorted(set.union(*all_task_sets) if all_task_sets else set())
    union_edges_all = sorted(set.union(*all_edge_sets) if all_edge_sets else set())

    return {
        "analysis_type": "dag_comparison",
        "schema_version": "1.0.0",
        "timestamp": _now_iso(),
        "input_count": len(signatures),
        "inputs": signatures,
        "pairwise": pairwise,
        "aggregate": {
            "common_tasks_all": common_tasks_all,
            "common_edges_all": [{"from": s, "to": d} for (s, d) in common_edges_all],
            "common_task_count_all": len(common_tasks_all),
            "common_edge_count_all": len(common_edges_all),
            "union_task_count_all": len(union_tasks_all),
            "union_edge_count_all": len(union_edges_all),
        },
    }


def _print_summary(payload: Dict[str, Any]) -> None:
    print("=" * 80)
    print("DAG Comparison Summary")
    print("=" * 80)
    print(f"Inputs: {payload.get('input_count', 0)}")

    for i, item in enumerate(payload.get("inputs", [])):
        print(
            f"[{i}] {item.get('file_path')} | orchestrator={item.get('orchestrator')} "
            f"tasks={item.get('task_count')} edges={item.get('edge_count')}"
        )

    print("-" * 80)
    for row in payload.get("pairwise", []):
        print(
            f"{Path(row.get('left_file', '')).name} vs {Path(row.get('right_file', '')).name}: "
            f"score={row.get('similarity_score')} task_j={row.get('task_jaccard')} edge_j={row.get('edge_jaccard')}"
        )
    print("=" * 80)


def main() -> None:
    parser = argparse.ArgumentParser(description="Compare 2+ workflow DAGs across orchestrators.")
    parser.add_argument("inputs", nargs="+", help="Workflow files and/or folders to compare (minimum 2 workflows after expansion)")
    parser.add_argument("--no-recursive", action="store_true", help="For folder inputs, only scan top-level files")
    parser.add_argument("--out", default=None, help="Write JSON report to exact file path")
    parser.add_argument("--out-dir", default=None, help="Write JSON report to this directory")
    parser.add_argument("--stdout", action="store_true", help="Print full JSON to stdout")
    parser.add_argument("--print-summary", action="store_true", help="Print human-readable summary")
    args = parser.parse_args()

    paths = resolve_workflow_inputs([Path(p) for p in args.inputs], recursive=not bool(args.no_recursive))
    if len(paths) < 2:
        raise SystemExit("Need at least two workflow files for comparison after expanding inputs.")

    payload = compare_workflows(paths)

    if args.print_summary:
        _print_summary(payload)

    txt = json.dumps(payload, indent=2, default=str)

    if args.out:
        out_path = Path(args.out)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(txt, encoding="utf-8")
        print(f"Wrote: {out_path}")
        return

    if args.stdout:
        print(txt)
        return

    if args.out_dir:
        out_root = Path(args.out_dir)
    else:
        out_root = paths[0].parent / "orcheval_reports" / "comparisons"
    out_root.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_path = out_root / f"dag_compare_{len(paths)}files_{ts}.json"
    out_path.write_text(txt, encoding="utf-8")
    print(f"Wrote: {out_path}")


if __name__ == "__main__":
    main()
