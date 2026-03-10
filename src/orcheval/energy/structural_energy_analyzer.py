#!/usr/bin/env python3
"""
Structural Energy Analyzer (v1.3)
=================================

Adds:
- Optional PipeSpec sidecar ingestion (JSON/YAML) alongside OPOS
- Canonical precedence:
    tasks: OPOS > PipeSpec > code
    edges: OPOS > PipeSpec > code
- Generic mapping improvements:
    dotted names + last-segment fallback
    token overlap + difflib fuzzy match (with ambiguity control)
- Explicit + implicit "dataflow" edge extraction from code (TaskFlow/Prefect/Dagster-ish)
- Mismatch noise reduction:
    mismatch compares only high-confidence code edges to effective spec edges

No hard new deps:
- JSON supported
- YAML supported only if PyYAML is installed
- Prefer JSON when both exist
"""

from __future__ import annotations

import ast
import difflib
import hashlib
import json
import logging
import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple


try:
    from ..base_evaluator import BaseEvaluator
except Exception:  # pragma: no cover
    BaseEvaluator = None  # type: ignore


SCHEMA_VERSION = "1.3.0"


# ---------------------------------------------------------------------
# Tier taxonomy (heuristic)
# ---------------------------------------------------------------------

OPERATOR_TIER_RULES: List[Tuple[str, str]] = [
    (r"\b(EmptyOperator|DummyOperator|BranchPythonOperator|ShortCircuitOperator|TriggerDagRunOperator)\b", "control"),
    (r"\b(\w*Sensor)\b", "polling_heavy"),
    (r"\b(SparkSubmitOperator|Dataproc\w*Operator|KubernetesPodOperator|DockerOperator|SageMaker\w*Operator)\b", "heavy_compute"),
    (r"\b(S3\w*Operator|GCS\w*Operator|BigQuery\w*Operator|Snowflake\w*Operator|Http\w*Operator)\b", "io_bound"),
    (r"\b(PythonOperator|BashOperator)\b", "light_compute"),
]

DEFAULT_TIER = "unknown"

OPOS_EXECUTOR_TIER: Dict[str, str] = {
    "python_script": "light_compute",
    "python": "light_compute",
    "http_request": "io_bound",
    "http": "io_bound",
    "sql": "io_bound",
    "database_query": "io_bound",
    "spark": "heavy_compute",
    "spark_submit": "heavy_compute",
    "docker": "heavy_compute",
    "kubernetes_pod": "heavy_compute",
    "sensor": "polling_heavy",
}

PIPESPEC_EXECUTOR_TIER: Dict[str, str] = {
    "python": "light_compute",
    "http": "io_bound",
    "sql": "io_bound",
    "database": "io_bound",
    "bash": "light_compute",
    "spark": "heavy_compute",
    "docker": "heavy_compute",
    "kubernetes": "heavy_compute",
}


# ---------------------------------------------------------------------
# Utils
# ---------------------------------------------------------------------

def _now_iso() -> str:
    return datetime.now().isoformat()


def _sha256_text(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8", errors="ignore")).hexdigest()


def _clamp01(x: float) -> float:
    return max(0.0, min(1.0, float(x)))


def _safe_dict(x: Any) -> Dict[str, Any]:
    return x if isinstance(x, dict) else {}


def _safe_list(x: Any) -> List[Any]:
    return x if isinstance(x, list) else []


def _norm_id(s: str) -> str:
    s = (s or "").strip().lower()
    s = re.sub(r"[^a-z0-9]+", "_", s)
    return s.strip("_")


def _tokenize(s: str) -> Set[str]:
    s = _norm_id(s)
    if not s:
        return set()
    toks = {t for t in s.split("_") if t and len(t) >= 2}
    return toks


def _jaccard(a: Set[str], b: Set[str]) -> float:
    if not a or not b:
        return 0.0
    return len(a & b) / max(1, len(a | b))


# ---------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------

@dataclass
class TaskRecord:
    id: str
    name: Optional[str]
    kind: str
    operator_type: Optional[str]
    tier: str
    aliases: Dict[str, Any]
    details: Dict[str, Any]
    sources: List[str]


@dataclass(frozen=True)
class EdgeRecord:
    src: str
    dst: str
    source: str
    confidence: float


# ---------------------------------------------------------------------
# Sidecar loading (OPOS + PipeSpec)
# ---------------------------------------------------------------------

def _try_load_json(path: Path) -> Optional[Dict[str, Any]]:
    try:
        return json.loads(path.read_text(encoding="utf-8", errors="ignore"))
    except Exception:
        return None


def _try_load_yaml(path: Path) -> Optional[Dict[str, Any]]:
    try:
        import yaml  # type: ignore
    except Exception:
        return None
    try:
        obj = yaml.safe_load(path.read_text(encoding="utf-8", errors="ignore"))
        return obj if isinstance(obj, dict) else None
    except Exception:
        return None


def _find_sidecar(code_file: Path, *, base_names: List[str]) -> Optional[Path]:
    """
    Prefer JSON to avoid requiring PyYAML.
    """
    candidates: List[Path] = []
    for bn in base_names:
        candidates.extend([
            code_file.parent / f"{bn}.json",
            code_file.parent / f"{bn}.yaml",
            code_file.parent / f"{bn}.yml",
        ])
    for c in candidates:
        if c.exists():
            # Prefer JSON if both exist: we built candidates in json->yaml->yml order.
            return c
    return None


def _load_sidecar(code_file: Path, *, kind: str, explicit_path: Optional[Path], base_names: List[str]) -> Tuple[Optional[Dict[str, Any]], Dict[str, Any]]:
    meta: Dict[str, Any] = {
        f"{kind}_found": False,
        f"{kind}_loaded": False,
        f"{kind}_path": None,
        f"{kind}_format": None,
        "yaml_supported": None,
        "error": None,
    }

    path = explicit_path or _find_sidecar(code_file, base_names=base_names)
    if path is None:
        return None, meta

    meta[f"{kind}_found"] = True
    meta[f"{kind}_path"] = str(path)

    suffix = path.suffix.lower()
    if suffix == ".json":
        meta[f"{kind}_format"] = "json"
        payload = _try_load_json(path)
        meta[f"{kind}_loaded"] = bool(isinstance(payload, dict))
        if not meta[f"{kind}_loaded"]:
            meta["error"] = "failed_to_parse_json"
        return payload, meta

    if suffix in (".yaml", ".yml"):
        try:
            import yaml  # type: ignore
            meta["yaml_supported"] = True
        except Exception:
            meta["yaml_supported"] = False
            meta["error"] = "pyyaml_not_installed"
            return None, meta

        meta[f"{kind}_format"] = "yaml"
        payload = _try_load_yaml(path)
        meta[f"{kind}_loaded"] = bool(isinstance(payload, dict))
        if not meta[f"{kind}_loaded"]:
            meta["error"] = "failed_to_parse_yaml"
        return payload, meta

    meta["error"] = f"unsupported_extension:{suffix}"
    return None, meta


# ---------------------------------------------------------------------
# AST helpers
# ---------------------------------------------------------------------

def _get_name(node: ast.AST) -> Optional[str]:
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        base = _get_name(node.value)
        return f"{base}.{node.attr}" if base else node.attr
    return None


def _last_segment(name: str) -> str:
    return (name or "").split(".")[-1]


def _flatten_task_ref(node: ast.AST) -> List[str]:
    if isinstance(node, (ast.List, ast.Tuple, ast.Set)):
        out: List[str] = []
        for elt in node.elts:
            out.extend(_flatten_task_ref(elt))
        return [x for x in out if x]
    n = _get_name(node)
    return [n] if n else []


def _call_func_name(call: ast.Call) -> str:
    fn = call.func
    if isinstance(fn, ast.Name):
        return fn.id
    if isinstance(fn, ast.Attribute):
        return fn.attr
    return ""


def _decorator_name(dec: ast.AST) -> str:
    if isinstance(dec, ast.Name):
        return dec.id
    if isinstance(dec, ast.Attribute):
        return dec.attr
    if isinstance(dec, ast.Call):
        if isinstance(dec.func, ast.Name):
            return dec.func.id
        if isinstance(dec.func, ast.Attribute):
            return dec.func.attr
    return ""


def _extract_kw_string(call: ast.Call, kw: str) -> Optional[str]:
    for k in call.keywords or []:
        if k.arg == kw and isinstance(k.value, ast.Constant) and isinstance(k.value.value, str):
            return k.value.value
    return None


# ---------------------------------------------------------------------
# Analyzer
# ---------------------------------------------------------------------

class StructuralEnergyAnalyzer:
    def __init__(self, *, logger: Optional[logging.Logger] = None) -> None:
        self.logger = logger or logging.getLogger(self.__class__.__name__)

    def analyze(
        self,
        file_path: Path,
        *,
        orchestrator: Optional[str] = None,
        opos_path: Optional[Path] = None,
        pipespec_path: Optional[Path] = None,
        min_edge_confidence_for_metrics: float = 0.70,
        min_edge_confidence_for_mismatch: float = 0.85,
    ) -> Dict[str, Any]:
        code_file = Path(file_path)
        code = code_file.read_text(encoding="utf-8", errors="ignore")

        orch = orchestrator or self._detect_orchestrator(code)

        opos_payload, opos_meta = _load_sidecar(
            code_file, kind="opos", explicit_path=opos_path,
            base_names=["opos", "intermediate_opos"]
        )
        pipespec_payload, pipespec_meta = _load_sidecar(
            code_file, kind="pipespec", explicit_path=pipespec_path,
            base_names=["pipespec", "intermediate_pipespec"]
        )

        opos_tasks, opos_edges = self._extract_opos_graph(opos_payload)
        pipespec_tasks, pipespec_edges, pipespec_signals = self._extract_pipespec_graph(pipespec_payload)

        payload: Dict[str, Any] = {
            "schema_version": SCHEMA_VERSION,
            "analysis_type": "structural_energy_profile",
            "timestamp": _now_iso(),
            "file_path": str(code_file),
            "orchestrator": orch,
            "code_sha256": _sha256_text(code),

            "spec_context": {
                "opos": {"load_meta": opos_meta, "summary": self._opos_summary(opos_payload)},
                "pipespec": {"load_meta": pipespec_meta, "summary": self._pipespec_summary(pipespec_payload)},
            },

            "task_resolution": {},
            "tasks": {
                "canonical_source": None,
                "canonical_tasks": [],
                "unmapped_code_tasks": [],
            },
            "edges": {
                "effective_source": None,
                "effective_edges": [],
                "effective_edges_simple": [],
                "opos_edges": [],
                "pipespec_edges": [],
                "code_edges": [],
                "mismatch": {
                    "code_vs_effective": None,
                    "opos_vs_pipespec": None,
                },
            },
            "metrics": {},
            "signals": {},
            "schedule": {},

            "disclaimer": (
                "This profile is computed from static structure (code + optional OPOS/PipeSpec). "
                "It is intended for design guidance, not measurement. "
                "Actual energy/carbon depends on runtime duration, data volume, hardware, and environment."
            ),
            "config": {
                "min_edge_confidence_for_metrics": float(min_edge_confidence_for_metrics),
                "min_edge_confidence_for_mismatch": float(min_edge_confidence_for_mismatch),
            },
            "errors": [],
        }

        # Parse AST (optional)
        tree: Optional[ast.AST]
        try:
            tree = ast.parse(code)
        except SyntaxError as e:
            payload["errors"].append({
                "stage": "ast_parse",
                "error_type": "SyntaxError",
                "message": f"Syntax error at line {e.lineno}: {e.msg}",
            })
            tree = None
        except Exception as e:
            payload["errors"].append({
                "stage": "ast_parse",
                "error_type": type(e).__name__,
                "message": str(e),
            })
            tree = None

        # Code extraction
        code_tasks: List[TaskRecord] = []
        var_to_taskid: Dict[str, str] = {}
        decorated_task_fns: Set[str] = set()
        code_edge_records: List[EdgeRecord] = []

        if tree is not None:
            code_tasks, var_to_taskid, decorated_task_fns = self._extract_code_tasks(code, tree)
            code_edge_records.extend(self._extract_code_edges_explicit(tree))
            code_edge_records.extend(self._extract_code_edges_dataflow(tree, decorated_task_fns))

        # Choose canonical spec task base
        if opos_tasks:
            canonical_source = "opos"
            spec_base_tasks = opos_tasks
            spec_secondary_tasks = pipespec_tasks
        elif pipespec_tasks:
            canonical_source = "pipespec"
            spec_base_tasks = pipespec_tasks
            spec_secondary_tasks = []
        else:
            canonical_source = "code"
            spec_base_tasks = []
            spec_secondary_tasks = []

        payload["tasks"]["canonical_source"] = canonical_source

        # Resolve canonical tasks: spec base (OPOS or PipeSpec) enriched by secondary spec + code
        canonical_tasks, unmapped_code_tasks, alias_map, mapping_debug = self._resolve_canonical_tasks_multi(
            spec_base_tasks=spec_base_tasks,
            spec_secondary_tasks=spec_secondary_tasks,
            code_tasks=code_tasks,
            var_to_taskid=var_to_taskid,
        )

        payload["task_resolution"] = mapping_debug
        payload["tasks"]["canonical_tasks"] = [t.__dict__ for t in canonical_tasks]
        payload["tasks"]["unmapped_code_tasks"] = [t.__dict__ for t in unmapped_code_tasks]

        # Build edge sets (canonicalized)
        opos_edge_records = self._canonicalize_edge_pairs(
            [EdgeRecord(a, b, "opos.flow.edges", 1.0) for (a, b) in opos_edges],
            alias_map,
        )
        pipespec_edge_records = self._canonicalize_edge_pairs(
            [EdgeRecord(a, b, "pipespec.flow_structure.edges", 0.95) for (a, b) in pipespec_edges],
            alias_map,
        )
        code_edge_records = self._canonicalize_edge_pairs(code_edge_records, alias_map)

        payload["edges"]["opos_edges"] = [{"from": e.src, "to": e.dst} for e in sorted({(e.src, e.dst) for e in opos_edge_records})]
        payload["edges"]["pipespec_edges"] = [{"from": e.src, "to": e.dst} for e in sorted({(e.src, e.dst) for e in pipespec_edge_records})]
        payload["edges"]["code_edges"] = [
            {"from": e.src, "to": e.dst, "source": e.source, "confidence": round(float(e.confidence), 3)}
            for e in sorted(set(code_edge_records), key=lambda x: (x.src, x.dst, x.source))
        ]

        # Effective edge source precedence: OPOS > PipeSpec > code
        if opos_edge_records:
            effective_source = "opos"
            effective = opos_edge_records
            node_whitelist = {t.id for t in opos_tasks}  # stabilize metrics to canonical spec node set
        elif pipespec_edge_records:
            effective_source = "pipespec"
            effective = pipespec_edge_records
            node_whitelist = {t.id for t in pipespec_tasks}
        else:
            effective_source = "code"
            effective = code_edge_records
            node_whitelist = None

        payload["edges"]["effective_source"] = effective_source

        effective_for_metrics = [e for e in effective if float(e.confidence) >= float(min_edge_confidence_for_metrics)]
        payload["edges"]["effective_edges"] = [
            {"from": e.src, "to": e.dst, "source": e.source, "confidence": round(float(e.confidence), 3)}
            for e in sorted(set(effective), key=lambda x: (x.src, x.dst, x.source))
        ]
        payload["edges"]["effective_edges_simple"] = [{"from": a, "to": b} for (a, b) in sorted({(e.src, e.dst) for e in effective})]

        # Mismatch diagnostics (low-noise):
        #  - code vs effective (only high-confidence code edges)
        #  - opos vs pipespec if both present
        code_hi = {(e.src, e.dst) for e in code_edge_records if float(e.confidence) >= float(min_edge_confidence_for_mismatch)}
        eff_set = {(e.src, e.dst) for e in effective}
        if effective_source in ("opos", "pipespec") and code_hi:
            payload["edges"]["mismatch"]["code_vs_effective"] = self._edge_mismatch_summary(code_hi, eff_set, label="code_vs_effective")

        if opos_edge_records and pipespec_edge_records:
            oset = {(e.src, e.dst) for e in opos_edge_records}
            pset = {(e.src, e.dst) for e in pipespec_edge_records}
            payload["edges"]["mismatch"]["opos_vs_pipespec"] = self._edge_mismatch_summary(oset, pset, label="opos_vs_pipespec")

        # Metrics computed on effective graph
        payload["metrics"] = self._graph_metrics(
            tasks=canonical_tasks if node_whitelist else (canonical_tasks + unmapped_code_tasks),
            edges=[(e.src, e.dst) for e in effective_for_metrics],
            node_whitelist=node_whitelist,
        )

        # Signals (code + spec)
        payload["signals"] = self._signals(
            code=code,
            canonical_tasks=canonical_tasks,
            unmapped_code_tasks=unmapped_code_tasks,
            effective_edges=effective,
            opos_payload=opos_payload,
            pipespec_payload=pipespec_payload,
            pipespec_signals=pipespec_signals,
        )

        # Schedule: OPOS schedule.enabled > PipeSpec schedule hints > code
        payload["schedule"] = self._schedule_profile(code, opos_payload=opos_payload, pipespec_payload=pipespec_payload)

        return payload

    # -----------------------------------------------------------------
    # Orchestrator detection
    # -----------------------------------------------------------------

    def _detect_orchestrator(self, code: str) -> str:
        if BaseEvaluator is None:
            return "unknown"
        try:
            o = BaseEvaluator().detect_orchestrator(code)
            return getattr(o, "value", str(o))
        except Exception:
            return "unknown"

    # -----------------------------------------------------------------
    # OPOS extraction
    # -----------------------------------------------------------------

    def _opos_summary(self, opos: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        if not isinstance(opos, dict):
            return {"present": False}
        comps = _safe_list(opos.get("components"))
        edges = _safe_list(_safe_dict(opos.get("flow")).get("edges"))
        return {
            "present": True,
            "opos_version": opos.get("opos_version"),
            "pipeline_id": opos.get("pipeline_id"),
            "name": _safe_dict(opos.get("metadata")).get("name"),
            "component_count": len(comps),
            "edge_count": len(edges),
            "pattern": _safe_dict(opos.get("flow")).get("pattern"),
            "schedule_enabled": _safe_dict(opos.get("schedule")).get("enabled"),
        }

    def _extract_opos_graph(self, opos: Optional[Dict[str, Any]]) -> Tuple[List[TaskRecord], List[Tuple[str, str]]]:
        if not isinstance(opos, dict):
            return [], []

        tasks: List[TaskRecord] = []
        edges: List[Tuple[str, str]] = []

        for c in _safe_list(opos.get("components")):
            if not isinstance(c, dict):
                continue
            cid = (c.get("id") or "").strip()
            if not cid:
                continue

            executor = _safe_dict(c.get("executor"))
            ex_type = executor.get("type")
            ex_type_str = str(ex_type) if ex_type is not None else None
            tier = OPOS_EXECUTOR_TIER.get(str(ex_type_str).lower(), "unknown")

            tasks.append(TaskRecord(
                id=cid,
                name=c.get("name"),
                kind="opos_component",
                operator_type=ex_type_str,
                tier=tier,
                aliases={
                    "opos_component_id": cid,
                    "opos_component_name": c.get("name"),
                    "norm_id": _norm_id(cid),
                    "tokens": sorted(_tokenize(cid) | _tokenize(str(c.get("name") or ""))),
                },
                details={
                    "category": c.get("category"),
                    "integrations_used": _safe_list(c.get("integrations_used")),
                    "retry": c.get("retry"),
                    "upstream_policy": c.get("upstream_policy"),
                },
                sources=["opos"],
            ))

        flow = _safe_dict(opos.get("flow"))
        for e in _safe_list(flow.get("edges")):
            if not isinstance(e, dict):
                continue
            a = (e.get("from") or "").strip()
            b = (e.get("to") or "").strip()
            if a and b:
                edges.append((a, b))

        return tasks, edges

    # -----------------------------------------------------------------
    # PipeSpec extraction
    # -----------------------------------------------------------------

    def _pipespec_summary(self, ps: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        if not isinstance(ps, dict):
            return {"present": False}
        comps = _safe_list(ps.get("components"))
        fs = _safe_dict(ps.get("flow_structure"))
        edges = _safe_list(fs.get("edges"))
        meta = _safe_dict(ps.get("metadata"))
        ar = _safe_dict(meta.get("analysis_results"))
        return {
            "present": True,
            "pipespec_version": ps.get("pipespec_version"),
            "name": _safe_dict(ps.get("pipeline_summary")).get("name"),
            "component_count": len(comps),
            "edge_count": len(edges),
            "pattern": _safe_dict(fs).get("pattern"),
            "analysis_detected_patterns": ar.get("detected_patterns"),
            "analysis_has_parallelism": ar.get("has_parallelism"),
            "analysis_has_sensors": ar.get("has_sensors"),
            "analysis_total_components": ar.get("total_components"),
            "analysis_complexity_score": ar.get("complexity_score"),
        }

    def _extract_pipespec_graph(
        self, ps: Optional[Dict[str, Any]]
    ) -> Tuple[List[TaskRecord], List[Tuple[str, str]], Dict[str, Any]]:
        if not isinstance(ps, dict):
            return [], [], {"present": False}

        tasks: List[TaskRecord] = []
        edges: List[Tuple[str, str]] = []

        # Signals to expose (data movement, executor types, etc.)
        io_total = 0
        io_by_kind: Dict[str, int] = {}
        io_by_direction: Dict[str, int] = {}
        executor_types: Dict[str, int] = {}
        integrations_by_type: Dict[str, int] = {}
        retry_attempts: List[int] = []

        for c in _safe_list(ps.get("components")):
            if not isinstance(c, dict):
                continue
            cid = (c.get("id") or "").strip()
            if not cid:
                continue

            ex = (c.get("executor_type") or "").strip().lower() if c.get("executor_type") else None
            ex_tier = PIPESPEC_EXECUTOR_TIER.get(ex or "", "unknown")
            if ex:
                executor_types[ex] = executor_types.get(ex, 0) + 1

            # io_spec is rich in your pipespec examples
            ios = _safe_list(c.get("io_spec"))
            io_total += len(ios)
            for io in ios:
                if not isinstance(io, dict):
                    continue
                k = (io.get("kind") or "unknown").strip().lower()
                d = (io.get("direction") or "unknown").strip().lower()
                io_by_kind[k] = io_by_kind.get(k, 0) + 1
                io_by_direction[d] = io_by_direction.get(d, 0) + 1

            # retry policy
            rp = _safe_dict(c.get("retry_policy"))
            ma = rp.get("max_attempts")
            if isinstance(ma, int):
                retry_attempts.append(ma)

            # integrations used
            for conn in _safe_list(c.get("connections")):
                if not isinstance(conn, dict):
                    continue
                t = (conn.get("type") or "unknown").strip().lower()
                integrations_by_type[t] = integrations_by_type.get(t, 0) + 1

            tasks.append(TaskRecord(
                id=cid,
                name=c.get("name"),
                kind="pipespec_component",
                operator_type=ex,
                tier=ex_tier,
                aliases={
                    "pipespec_component_id": cid,
                    "pipespec_component_name": c.get("name"),
                    "norm_id": _norm_id(cid),
                    "tokens": sorted(_tokenize(cid) | _tokenize(str(c.get("name") or ""))),
                },
                details={
                    "category": c.get("category"),
                    "executor_type": c.get("executor_type"),
                    "retry_policy": c.get("retry_policy"),
                    "datasets": c.get("datasets"),
                    "io_spec_count": len(ios),
                },
                sources=["pipespec"],
            ))

        fs = _safe_dict(ps.get("flow_structure"))
        for e in _safe_list(fs.get("edges")):
            if not isinstance(e, dict):
                continue
            a = (e.get("from") or "").strip()
            b = (e.get("to") or "").strip()
            if a and b:
                edges.append((a, b))

        meta = _safe_dict(ps.get("metadata"))
        ar = _safe_dict(meta.get("analysis_results"))
        ps_signals = {
            "present": True,
            "analysis_results": ar,
            "pipeline_patterns": _safe_dict(ps.get("pipeline_summary")).get("flow_patterns"),
            "executor_types": executor_types,
            "io": {
                "io_total": io_total,
                "io_by_kind": io_by_kind,
                "io_by_direction": io_by_direction,
            },
            "integrations_by_type": integrations_by_type,
            "retry": {
                "max_attempts_values": retry_attempts,
                "max_attempts_max": max(retry_attempts) if retry_attempts else None,
                "max_attempts_mean": (sum(retry_attempts) / len(retry_attempts)) if retry_attempts else None,
            },
        }

        return tasks, edges, ps_signals

    # -----------------------------------------------------------------
    # Code extraction
    # -----------------------------------------------------------------

    def _tier_for_operator(self, operator_type: Optional[str]) -> str:
        if not operator_type:
            return DEFAULT_TIER
        for pattern, tier in OPERATOR_TIER_RULES:
            if re.search(pattern, operator_type):
                return tier
        if operator_type.endswith("Sensor"):
            return "polling_heavy"
        if operator_type.endswith("Operator"):
            return "light_compute"
        return DEFAULT_TIER

    def _extract_code_tasks(self, code: str, tree: ast.AST) -> Tuple[List[TaskRecord], Dict[str, str], Set[str]]:
        tasks: List[TaskRecord] = []
        var_to_taskid: Dict[str, str] = {}
        decorated_task_fns: Set[str] = set()

        # Airflow operators: var = SomeOperator(task_id="...")
        for node in ast.walk(tree):
            if isinstance(node, ast.Assign) and isinstance(node.value, ast.Call):
                call = node.value
                fn_name = _call_func_name(call)
                if not (fn_name.endswith("Operator") or fn_name.endswith("Sensor")):
                    continue
                for tgt in node.targets:
                    var = _get_name(tgt)
                    if not var:
                        continue
                    task_id = _extract_kw_string(call, "task_id") or _last_segment(var)
                    var_to_taskid[var] = task_id

                    tasks.append(TaskRecord(
                        id=task_id,
                        name=task_id,
                        kind="airflow_operator",
                        operator_type=fn_name,
                        tier=self._tier_for_operator(fn_name),
                        aliases={
                            "python_var": var,
                            "python_var_last": _last_segment(var),
                            "airflow_task_id": task_id,
                            "norm_id": _norm_id(task_id),
                            "tokens": sorted(_tokenize(task_id) | _tokenize(var)),
                        },
                        details={"detected_via": "assign_call"},
                        sources=["code"],
                    ))

        # Decorated tasks: @task / @op / @asset
        for node in ast.walk(tree):
            if isinstance(node, ast.FunctionDef):
                decs = {_decorator_name(d) for d in (node.decorator_list or [])}
                if {"task", "op", "asset"} & decs:
                    decorated_task_fns.add(node.name)
                    kind = "decorated_task"
                    operator_type = "task_decorator"
                    tier = "light_compute"
                    if "op" in decs:
                        kind = "dagster_op"
                        operator_type = "dagster.op"
                    if "asset" in decs:
                        kind = "dagster_asset"
                        operator_type = "dagster.asset"
                        tier = "io_bound"

                    tasks.append(TaskRecord(
                        id=node.name,
                        name=node.name,
                        kind=kind,
                        operator_type=operator_type,
                        tier=tier,
                        aliases={
                            "python_fn": node.name,
                            "norm_id": _norm_id(node.name),
                            "tokens": sorted(_tokenize(node.name)),
                        },
                        details={"detected_via": "decorator"},
                        sources=["code"],
                    ))

        return tasks, var_to_taskid, decorated_task_fns

    def _extract_code_edges_explicit(self, tree: ast.AST) -> List[EdgeRecord]:
        edges: List[EdgeRecord] = []

        # >> and <<
        for node in ast.walk(tree):
            if isinstance(node, ast.BinOp) and isinstance(node.op, (ast.RShift, ast.LShift)):
                left = _flatten_task_ref(node.left)
                right = _flatten_task_ref(node.right)
                if not left or not right:
                    continue
                if isinstance(node.op, ast.RShift):
                    for a in left:
                        for b in right:
                            edges.append(EdgeRecord(a, b, "code.binop", 0.90))
                else:
                    for a in left:
                        for b in right:
                            edges.append(EdgeRecord(b, a, "code.binop", 0.90))

        # chain(a,b,c)
        for node in ast.walk(tree):
            if isinstance(node, ast.Call) and _call_func_name(node) == "chain":
                args: List[str] = []
                for a in node.args:
                    args.extend(_flatten_task_ref(a))
                for i in range(len(args) - 1):
                    edges.append(EdgeRecord(args[i], args[i + 1], "code.chain", 0.85))

        # set_downstream / set_upstream
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call) or not isinstance(node.func, ast.Attribute):
                continue
            method = node.func.attr
            obj = _get_name(node.func.value)
            if not obj:
                continue

            if method == "set_downstream":
                targets: List[str] = []
                for a in node.args or []:
                    targets.extend(_flatten_task_ref(a))
                for t in targets:
                    edges.append(EdgeRecord(obj, t, "code.set_downstream", 0.80))

            if method == "set_upstream":
                targets = []
                for a in node.args or []:
                    targets.extend(_flatten_task_ref(a))
                for t in targets:
                    edges.append(EdgeRecord(t, obj, "code.set_upstream", 0.80))

        return edges

    def _invoked_task_name(self, call: ast.Call, decorated_task_fns: Set[str]) -> Optional[str]:
        fn = call.func
        if isinstance(fn, ast.Name):
            return fn.id if fn.id in decorated_task_fns else None
        if isinstance(fn, ast.Attribute):
            # extract.submit(...)
            base = _get_name(fn.value)
            base_last = _last_segment(base or "")
            if fn.attr in ("submit", "map") and base_last in decorated_task_fns:
                return base_last
        return None

    def _extract_code_edges_dataflow(self, tree: ast.AST, decorated_task_fns: Set[str]) -> List[EdgeRecord]:
        if not decorated_task_fns:
            return []

        edges: List[EdgeRecord] = []
        var_origin: Dict[str, str] = {}

        # y = extract()
        for node in ast.walk(tree):
            if isinstance(node, ast.Assign) and isinstance(node.value, ast.Call):
                prod = self._invoked_task_name(node.value, decorated_task_fns)
                if not prod:
                    continue
                for tgt in node.targets:
                    v = _get_name(tgt)
                    if v:
                        var_origin[v] = prod
                        var_origin[_last_segment(v)] = prod

        # load(y) or load(extract())
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            cons = self._invoked_task_name(node, decorated_task_fns)
            if not cons:
                continue

            for arg in node.args or []:
                if isinstance(arg, ast.Call):
                    prod2 = self._invoked_task_name(arg, decorated_task_fns)
                    if prod2 and prod2 != cons:
                        edges.append(EdgeRecord(prod2, cons, "code.dataflow_nested_call", 0.65))

                if isinstance(arg, ast.Name):
                    prod = var_origin.get(arg.id)
                    if prod and prod != cons:
                        edges.append(EdgeRecord(prod, cons, "code.dataflow_var", 0.60))

        return edges

    # -----------------------------------------------------------------
    # Canonicalization / mapping
    # -----------------------------------------------------------------

    def _best_match_spec(
        self,
        query: str,
        spec_ids: List[str],
        *,
        threshold: float = 0.84,
        min_gap: float = 0.06,
    ) -> Tuple[Optional[str], float, bool]:
        qn = _norm_id(query)
        if not qn or not spec_ids:
            return None, 0.0, False

        scored: List[Tuple[str, float]] = []
        for cid in spec_ids:
            score = difflib.SequenceMatcher(a=qn, b=_norm_id(cid)).ratio()
            scored.append((cid, score))
        scored.sort(key=lambda x: x[1], reverse=True)

        best, best_s = scored[0]
        second_s = scored[1][1] if len(scored) > 1 else 0.0
        ambiguous = (best_s - second_s) < min_gap

        if best_s >= threshold and not ambiguous:
            return best, best_s, False
        return None, best_s, ambiguous

    def _aliases_for_task(self, t: TaskRecord) -> Set[str]:
        out: Set[str] = set()
        if t.id:
            out.add(t.id)
            out.add(_norm_id(t.id))
            out |= _tokenize(t.id)

        for v in (t.aliases or {}).values():
            if isinstance(v, str) and v.strip():
                s = v.strip()
                out.add(s)
                out.add(_last_segment(s))
                out.add(_norm_id(s))
                out.add(_norm_id(_last_segment(s)))
                out |= _tokenize(s)

        # allow tokens list stored as list
        toks = t.aliases.get("tokens")
        if isinstance(toks, list):
            for tok in toks:
                if isinstance(tok, str) and tok:
                    out.add(tok)

        return {x for x in out if x}

    def _resolve_canonical_tasks_multi(
        self,
        *,
        spec_base_tasks: List[TaskRecord],
        spec_secondary_tasks: List[TaskRecord],
        code_tasks: List[TaskRecord],
        var_to_taskid: Dict[str, str],
    ) -> Tuple[List[TaskRecord], List[TaskRecord], Dict[str, str], Dict[str, Any]]:
        """
        Base tasks are canonical (OPOS or PipeSpec).
        We enrich them using:
          - secondary spec tasks (pipespec enriches opos OR vice-versa)
          - code tasks
        """
        debug: Dict[str, Any] = {
            "spec_base_count": len(spec_base_tasks),
            "spec_secondary_count": len(spec_secondary_tasks),
            "code_task_count": len(code_tasks),
            "mapped_secondary_tasks": 0,
            "mapped_code_tasks": 0,
            "unmapped_code_tasks": 0,
            "mapping_strategies_used": [],
            "mapping_confidence": "low",
        }

        # If no spec base, canonical is code
        if not spec_base_tasks:
            alias_map: Dict[str, str] = {}
            for t in code_tasks:
                for a in self._aliases_for_task(t):
                    alias_map[a] = t.id
            debug["mapped_code_tasks"] = len(code_tasks)
            debug["mapping_confidence"] = "medium" if code_tasks else "low"
            return code_tasks, [], alias_map, debug

        canonical: Dict[str, TaskRecord] = {t.id: t for t in spec_base_tasks}
        spec_ids = [t.id for t in spec_base_tasks]

        alias_map: Dict[str, str] = {}
        for t in spec_base_tasks:
            for a in self._aliases_for_task(t):
                alias_map[a] = t.id

        # help edges: var -> task_id
        for var, tid in (var_to_taskid or {}).items():
            alias_map[var] = tid
            alias_map[_last_segment(var)] = tid
            alias_map[_norm_id(var)] = tid
            alias_map[_norm_id(_last_segment(var))] = tid
            alias_map[tid] = tid
            alias_map[_norm_id(tid)] = tid

        strategies: Set[str] = set()

        def _map_to_canonical_id(candidates: List[str]) -> Optional[str]:
            # exact/normalized
            for s in candidates:
                if not s:
                    continue
                if s in canonical:
                    strategies.add("exact")
                    return s
                ns = _norm_id(s)
                if ns in alias_map:
                    strategies.add("normalized")
                    return alias_map[ns]

            # token overlap
            ctoks: Set[str] = set()
            for s in candidates:
                ctoks |= _tokenize(s)
            if ctoks:
                best = None
                best_score = 0.0
                for oid in spec_ids:
                    score = _jaccard(ctoks, _tokenize(oid))
                    if score > best_score:
                        best_score = score
                        best = oid
                if best is not None and best_score >= 0.50:
                    strategies.add("token_overlap")
                    return best

            # fuzzy
            for s in candidates:
                bid, _, amb = self._best_match_spec(s, spec_ids)
                if bid is not None:
                    strategies.add("fuzzy")
                    return bid

            return None

        # Enrich canonical with secondary spec (if any)
        for st in spec_secondary_tasks:
            cands = [st.id, str(st.name or "")]
            cid = _map_to_canonical_id(cands)
            if cid and cid in canonical:
                tgt = canonical[cid]
                tgt.sources = sorted(set(tgt.sources + st.sources))
                tgt.details.setdefault("spec_secondary", [])
                tgt.details["spec_secondary"].append({"kind": st.kind, "id": st.id})
                tgt.aliases.update({k: v for k, v in st.aliases.items() if v is not None})
                if tgt.operator_type is None and st.operator_type is not None:
                    tgt.operator_type = st.operator_type
                if tgt.tier == "unknown" and st.tier != "unknown":
                    tgt.tier = st.tier
                debug["mapped_secondary_tasks"] += 1

        # Enrich canonical with code
        unmapped_code: List[TaskRecord] = []
        for ct in code_tasks:
            cands: List[str] = []
            for key in ("airflow_task_id", "python_var", "python_fn", "python_var_last"):
                v = ct.aliases.get(key)
                if isinstance(v, str) and v.strip():
                    cands.append(v.strip())
            cands.append(ct.id)

            cid = _map_to_canonical_id(cands)
            if cid and cid in canonical:
                tgt = canonical[cid]
                tgt.sources = sorted(set(tgt.sources + ["code"]))
                tgt.details.setdefault("code", {})
                tgt.details["code"].setdefault("kinds", [])
                tgt.details["code"]["kinds"] = sorted(set(tgt.details["code"]["kinds"] + [ct.kind]))
                if tgt.operator_type is None and ct.operator_type is not None:
                    tgt.operator_type = ct.operator_type
                if tgt.tier == "unknown" and ct.tier != "unknown":
                    tgt.tier = ct.tier
                tgt.aliases.update({k: v for k, v in ct.aliases.items() if v is not None})
                debug["mapped_code_tasks"] += 1
            else:
                unmapped_code.append(ct)
                debug["unmapped_code_tasks"] += 1

        debug["mapping_strategies_used"] = sorted(strategies)
        # confidence: based on mapping coverage
        if debug["code_task_count"] == 0:
            debug["mapping_confidence"] = "high"
        else:
            cov = debug["mapped_code_tasks"] / max(1, debug["code_task_count"])
            if cov >= 0.70:
                debug["mapping_confidence"] = "high"
            elif cov >= 0.40:
                debug["mapping_confidence"] = "medium"
            else:
                debug["mapping_confidence"] = "low"

        # Expand alias map with canonical aliases
        for t in canonical.values():
            for a in self._aliases_for_task(t):
                alias_map[a] = t.id

        # Let unmapped code tasks still contribute alias entries to avoid losing endpoints
        for t in unmapped_code:
            for a in self._aliases_for_task(t):
                alias_map.setdefault(a, t.id)

        return list(canonical.values()), unmapped_code, alias_map, debug

    def _canonicalize_edge_pairs(self, edges: List[EdgeRecord], alias_map: Dict[str, str]) -> List[EdgeRecord]:
        out: List[EdgeRecord] = []
        for e in edges:
            a = (e.src or "").strip()
            b = (e.dst or "").strip()
            if not a or not b:
                continue

            a_candidates = [a, _last_segment(a), _norm_id(a), _norm_id(_last_segment(a))]
            b_candidates = [b, _last_segment(b), _norm_id(b), _norm_id(_last_segment(b))]

            ca = next((alias_map[c] for c in a_candidates if c in alias_map), None) or a
            cb = next((alias_map[c] for c in b_candidates if c in alias_map), None) or b
            out.append(EdgeRecord(ca, cb, e.source, float(e.confidence)))
        return out

    def _edge_mismatch_summary(self, left: Set[Tuple[str, str]], right: Set[Tuple[str, str]], *, label: str) -> Dict[str, Any]:
        only_left = sorted(left - right)
        only_right = sorted(right - left)
        denom = max(1, len(right))
        ratio = (len(only_left) + len(only_right)) / denom
        return {
            "label": label,
            "left_count": len(left),
            "right_count": len(right),
            "symmetric_diff_count": len(only_left) + len(only_right),
            "symmetric_diff_ratio_vs_right": round(float(ratio), 4),
            "only_in_left": [{"from": a, "to": b} for (a, b) in only_left[:50]],
            "only_in_right": [{"from": a, "to": b} for (a, b) in only_right[:50]],
            "note": "Diagnostic only. Differences are expected with dynamic graphs or incomplete code parsing.",
        }

    # -----------------------------------------------------------------
    # Metrics
    # -----------------------------------------------------------------

    def _graph_metrics(
        self,
        *,
        tasks: List[TaskRecord],
        edges: List[Tuple[str, str]],
        node_whitelist: Optional[Set[str]] = None,
    ) -> Dict[str, Any]:
        nodes: Set[str] = set(node_whitelist) if node_whitelist else {t.id for t in tasks if t.id}
        for a, b in edges:
            if node_whitelist is None:
                nodes.add(a)
                nodes.add(b)
            else:
                if a in node_whitelist:
                    nodes.add(a)
                if b in node_whitelist:
                    nodes.add(b)

        n = len(nodes)
        m = len(set(edges))
        if n == 0:
            return {
                "total_tasks": 0, "total_edges": 0, "has_cycle": False, "connected_components": 0,
                "critical_path_length": None, "max_parallel_width": None, "parallelism_index": 0.0,
                "sequential_bottleneck_ratio": None, "max_fan_out": 0, "avg_out_degree": 0.0,
            }

        adj: Dict[str, Set[str]] = {u: set() for u in nodes}
        rev: Dict[str, Set[str]] = {u: set() for u in nodes}

        for a, b in set(edges):
            if a == b:
                continue
            if node_whitelist is not None and (a not in node_whitelist or b not in node_whitelist):
                continue
            adj.setdefault(a, set()).add(b)
            rev.setdefault(b, set()).add(a)

        out_degrees = [len(adj[u]) for u in nodes]
        max_fan_out = max(out_degrees) if out_degrees else 0
        avg_out_degree = (sum(out_degrees) / n) if n else 0.0

        cc = self._connected_components(nodes, adj, rev)
        topo, has_cycle = self._toposort(nodes, adj, rev)

        critical_path = None
        max_width = None
        sequential_ratio = None
        parallelism_index = 0.0

        if not has_cycle and topo:
            dist: Dict[str, int] = {u: 1 for u in nodes}
            level: Dict[str, int] = {}

            for u in topo:
                preds = rev.get(u, set())
                if preds:
                    best_pred = max(preds, key=lambda p: dist.get(p, 1))
                    dist[u] = dist.get(best_pred, 1) + 1
                    level[u] = max(level.get(p, 1) for p in preds) + 1
                else:
                    dist[u] = 1
                    level[u] = 1

            critical_path = max(dist.values()) if dist else 1

            counts: Dict[int, int] = {}
            for u in nodes:
                lv = level.get(u, 1)
                counts[lv] = counts.get(lv, 0) + 1
            max_width = max(counts.values()) if counts else 1

            parallelism_index = (max_width / n) if n else 0.0
            sequential_ratio = (critical_path / n) if n else None

        return {
            "total_tasks": n,
            "total_edges": m,
            "has_cycle": bool(has_cycle),
            "connected_components": int(cc),
            "critical_path_length": int(critical_path) if isinstance(critical_path, int) else None,
            "max_parallel_width": int(max_width) if isinstance(max_width, int) else None,
            "parallelism_index": float(_clamp01(parallelism_index)),
            "sequential_bottleneck_ratio": float(sequential_ratio) if sequential_ratio is not None else None,
            "max_fan_out": int(max_fan_out),
            "avg_out_degree": float(avg_out_degree),
        }

    def _connected_components(self, nodes: Set[str], adj: Dict[str, Set[str]], rev: Dict[str, Set[str]]) -> int:
        seen: Set[str] = set()
        comps = 0
        for start in nodes:
            if start in seen:
                continue
            comps += 1
            stack = [start]
            seen.add(start)
            while stack:
                u = stack.pop()
                nbrs = set(adj.get(u, set())) | set(rev.get(u, set()))
                for v in nbrs:
                    if v not in seen:
                        seen.add(v)
                        stack.append(v)
        return comps

    def _toposort(self, nodes: Set[str], adj: Dict[str, Set[str]], rev: Dict[str, Set[str]]) -> Tuple[List[str], bool]:
        indeg: Dict[str, int] = {u: len(rev.get(u, set())) for u in nodes}
        q = [u for u in nodes if indeg[u] == 0]
        out: List[str] = []

        while q:
            u = q.pop()
            out.append(u)
            for v in adj.get(u, set()):
                indeg[v] = indeg.get(v, 0) - 1
                if indeg[v] == 0:
                    q.append(v)

        has_cycle = (len(out) != len(nodes))
        return out, has_cycle

    # -----------------------------------------------------------------
    # Signals and schedule
    # -----------------------------------------------------------------

    def _signals(
        self,
        *,
        code: str,
        canonical_tasks: List[TaskRecord],
        unmapped_code_tasks: List[TaskRecord],
        effective_edges: List[EdgeRecord],
        opos_payload: Optional[Dict[str, Any]],
        pipespec_payload: Optional[Dict[str, Any]],
        pipespec_signals: Dict[str, Any],
    ) -> Dict[str, Any]:
        cl = code.lower()

        xcom_push = len(re.findall(r"\bxcom_push\b", cl))
        xcom_pull = len(re.findall(r"\bxcom_pull\b", cl))
        xcomarg = len(re.findall(r"\bxcomarg\b", cl))

        has_sensor = bool(re.search(r"\b\w*Sensor\b", code))
        poke_mode = bool(re.search(r"mode\s*=\s*['\"]poke['\"]", cl))
        reschedule_mode = bool(re.search(r"mode\s*=\s*['\"]reschedule['\"]", cl))

        prefect_submit = "submit(" in cl
        mapping_hint = ".map(" in cl
        futures_hint = "concurrent.futures" in cl or "asyncio" in cl

        tier_counts: Dict[str, int] = {}
        heavy = 0
        polling = 0
        for t in canonical_tasks:
            tier_counts[t.tier] = tier_counts.get(t.tier, 0) + 1
            if t.tier == "heavy_compute":
                heavy += 1
            if t.tier == "polling_heavy":
                polling += 1

        edge_sources: Dict[str, int] = {}
        for e in effective_edges:
            edge_sources[e.source] = edge_sources.get(e.source, 0) + 1

        return {
            "xcom": {
                "xcom_push_count": xcom_push,
                "xcom_pull_count": xcom_pull,
                "xcomarg_count": xcomarg,
                "xcom_total_signals": xcom_push + xcom_pull + xcomarg,
            },
            "sensors": {
                "has_sensor": has_sensor,
                "poke_mode_detected": poke_mode,
                "reschedule_mode_detected": reschedule_mode,
            },
            "parallelism_hints": {
                "prefect_submit_detected": prefect_submit,
                "map_detected": mapping_hint,
                "async_futures_hint_detected": futures_hint,
            },
            "operator_tiers": {
                "distribution": tier_counts,
                "heavy_compute_count": heavy,
                "polling_heavy_count": polling,
            },
            "edges": {
                "effective_edge_count": len({(e.src, e.dst) for e in effective_edges}),
                "effective_edge_sources": edge_sources,
            },
            "spec_signals": {
                "opos_present": isinstance(opos_payload, dict),
                "pipespec_present": isinstance(pipespec_payload, dict),
                "pipespec": pipespec_signals,
                "unmapped_code_task_count": len(unmapped_code_tasks),
            },
        }

    def _schedule_profile(self, code: str, *, opos_payload: Optional[Dict[str, Any]], pipespec_payload: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        # OPOS schedule.enabled
        if isinstance(opos_payload, dict):
            sch = _safe_dict(opos_payload.get("schedule"))
            if "enabled" in sch:
                return {"source": "opos", "enabled": bool(sch.get("enabled")), "schedule_hint": "opos.schedule.enabled"}

        # PipeSpec: may have parameters.schedule.enabled.default or similar
        if isinstance(pipespec_payload, dict):
            params = _safe_dict(pipespec_payload.get("parameters"))
            sch = _safe_dict(params.get("schedule"))
            enabled = _safe_dict(sch.get("enabled")).get("default")
            if isinstance(enabled, bool):
                return {"source": "pipespec", "enabled": enabled, "schedule_hint": "pipespec.parameters.schedule.enabled.default"}

        # Code fallback
        if "@daily" in code:
            return {"source": "code", "enabled": True, "schedule_hint": "@daily", "runs_per_year_estimate": 365}
        if "@hourly" in code:
            return {"source": "code", "enabled": True, "schedule_hint": "@hourly", "runs_per_year_estimate": 8760}

        return {"source": "none", "enabled": None, "schedule_hint": None}