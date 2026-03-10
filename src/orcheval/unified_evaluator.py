#!/usr/bin/env python3
"""
Unified evaluator (single source of truth)
==========================================

Produces a unified JSON record that should be sufficient for:
- quantitative scoring (SAT, PCT, combined)
- executability gating (import smoke test)
- qualitative error analysis (error_events + embedded exception metadata)

Components:
- SAT: EnhancedStaticAnalyzer (controller env)
- import_smoke: compile + import/exec (subprocess in target orchestrator venv)
- PCT: platform compliance (subprocess in target orchestrator venv)

Combined score:
  S_code = alpha*SAT + (1-alpha)*PCT
Gated by:
  - yaml_valid gate (if provided)
  - execution gate: (PCT gates_passed) AND (import_smoke.ok)

IMPORTANT:
- Unified JSON embeds generation metadata so it is truly self-contained.
"""

from __future__ import annotations

import sys
import os
import json
import logging
import platform
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional, List, Tuple

from .base_evaluator import EvaluationResult, Orchestrator
from .enhanced_static_analyzer import EnhancedStaticAnalyzer
from .subprocess_json_runner import run_cli_json


UNIFIED_SCHEMA_VERSION = "2.0.0"

# Maps run_type strings (written by generators) to canonical strategy names
# (written by the runner post-patch into generation_metadata.json).
# Used by _build_run_context to infer strategy when the explicit field is absent.
_RUN_TYPE_TO_STRATEGY: Dict[str, str] = {
    "non_reasoning":             "non_reasoning",
    "chain_of_thought":          "cot",
    "plan_then_code":            "plan_code",
    "self_refine_agent":         "self_refine",
    "react_agent_non_reasoning": "react",
    "prompt_repetition":         "repetition",
}


def _mean(values: List[float]) -> float:
    vals = [float(v) for v in values if v is not None]
    return sum(vals) / len(vals) if vals else 0.0


def _clamp10(x: float) -> float:
    return max(0.0, min(10.0, float(x)))


def _issue_summary(issues: List[Dict[str, Any]]) -> Dict[str, int]:
    return {
        "total": len(issues),
        "critical": sum(1 for i in issues if i.get("severity") == "critical"),
        "major": sum(1 for i in issues if i.get("severity") == "major"),
        "minor": sum(1 for i in issues if i.get("severity") == "minor"),
        "info": sum(1 for i in issues if i.get("severity") == "info"),
    }


def _safe_dict(x: Any) -> Dict[str, Any]:
    return x if isinstance(x, dict) else {}


def _safe_list(x: Any) -> List[Any]:
    return x if isinstance(x, list) else []


def _load_generation_metadata(code_file: Path) -> Optional[Dict[str, Any]]:
    meta_path = code_file.parent / "generation_metadata.json"
    if meta_path.exists():
        try:
            return json.loads(meta_path.read_text(encoding="utf-8"))
        except Exception:
            return None
    return None


def _parse_orchestrator(value: Any) -> Optional[Orchestrator]:
    if isinstance(value, str):
        v = value.strip().lower()
        try:
            return Orchestrator(v)
        except Exception:
            return None
    return None


def _venv_python_from_dir(venv_dir: Path) -> Path:
    return Path(venv_dir) / "bin" / "python"  # macOS/Linux layout


def _resolve_python_for_orchestrator(
    orch: Orchestrator,
    *,
    airflow_venv: Optional[Path] = None,
    prefect_venv: Optional[Path] = None,
    dagster_venv: Optional[Path] = None,
    airflow_python: Optional[Path] = None,
    prefect_python: Optional[Path] = None,
    dagster_python: Optional[Path] = None,
) -> Optional[Path]:
    env_map = {
        Orchestrator.AIRFLOW: ("AIRFLOW_PYTHON", "AIRFLOW_VENV"),
        Orchestrator.PREFECT: ("PREFECT_PYTHON", "PREFECT_VENV"),
        Orchestrator.DAGSTER: ("DAGSTER_PYTHON", "DAGSTER_VENV"),
    }

    if orch == Orchestrator.AIRFLOW:
        if airflow_python:
            return airflow_python
        if airflow_venv:
            return _venv_python_from_dir(airflow_venv)

    if orch == Orchestrator.PREFECT:
        if prefect_python:
            return prefect_python
        if prefect_venv:
            return _venv_python_from_dir(prefect_venv)

    if orch == Orchestrator.DAGSTER:
        if dagster_python:
            return dagster_python
        if dagster_venv:
            return _venv_python_from_dir(dagster_venv)

    py_env, venv_env = env_map.get(orch, (None, None))
    if py_env and os.environ.get(py_env):
        return Path(os.environ[py_env])
    if venv_env and os.environ.get(venv_env):
        return _venv_python_from_dir(Path(os.environ[venv_env]))

    return None


def _summarize_token_usage(gen_meta: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Normalize generation token usage into stable fields.
    Supports all 6 generator shapes:
      non_reasoning / cot / repetition:
        token_usage: {input_tokens, output_tokens}
      plan_code:
        token_usage: {total_input, total_output, plan_stage:{...}, code_stage:{...}}
      self_refine:
        token_usage: {input_tokens_total, generation_tokens_total, critique_tokens_total, ...}
      react:
        token_usage: {input_tokens_total, output_tokens_total, reasoning_tokens_total, calls:[...]}
    """
    out = {
        "gen_input_tokens": None,
        "gen_output_tokens": None,
        "gen_total_tokens": None,
        "gen_reasoning_tokens": None,
        "gen_calls": None,
    }
    if not isinstance(gen_meta, dict):
        return out

    tu = gen_meta.get("token_usage")
    if not isinstance(tu, dict):
        return out

    # --- react: input_tokens_total + output_tokens_total + reasoning_tokens_total ---
    if "input_tokens_total" in tu and "output_tokens_total" in tu:
        inp  = tu.get("input_tokens_total")
        outp = tu.get("output_tokens_total")
        rsn  = tu.get("reasoning_tokens_total")
        try: inp_i = int(inp)
        except Exception: inp_i = None
        try: outp_i = int(outp)
        except Exception: outp_i = None
        try: rsn_i = int(rsn)
        except Exception: rsn_i = None
        out["gen_input_tokens"]    = inp_i
        out["gen_output_tokens"]   = outp_i
        out["gen_reasoning_tokens"] = rsn_i
        total = sum(v for v in (inp_i, outp_i, rsn_i) if v is not None) or None
        out["gen_total_tokens"] = total
        out["gen_calls"] = len(tu["calls"]) if isinstance(tu.get("calls"), list) else None
        return out

    # --- self_refine: input_tokens_total alone (no output_tokens_total) ---
    if "input_tokens_total" in tu:
        inp = tu.get("input_tokens_total")
        try: inp_i = int(inp)
        except Exception: inp_i = None
        out_gen  = tu.get("generation_tokens_total") or 0
        out_crit = tu.get("critique_tokens_total") or 0
        out_ref  = tu.get("refine_tokens_total") or 0
        try: outp_i = int(out_gen) + int(out_crit) + int(out_ref)
        except Exception: outp_i = None
        out["gen_input_tokens"]  = inp_i
        out["gen_output_tokens"] = outp_i
        out["gen_total_tokens"]  = (inp_i + outp_i) if (inp_i is not None and outp_i is not None) else None
        return out

    # --- plan_code: total_input / total_output ---
    if "total_input" in tu and "total_output" in tu:
        try: inp_i = int(tu["total_input"])
        except Exception: inp_i = None
        try: outp_i = int(tu["total_output"])
        except Exception: outp_i = None
        out["gen_input_tokens"]  = inp_i
        out["gen_output_tokens"] = outp_i
        out["gen_total_tokens"]  = (inp_i + outp_i) if (inp_i is not None and outp_i is not None) else None
        return out

    # --- simple flat form (non_reasoning, cot, repetition) ---
    inp  = tu.get("input_tokens")
    outp = tu.get("output_tokens")
    try: inp_i = int(inp) if inp is not None else None
    except Exception: inp_i = None
    try: outp_i = int(outp) if outp is not None else None
    except Exception: outp_i = None

    out["gen_input_tokens"]  = inp_i
    out["gen_output_tokens"] = outp_i
    out["gen_total_tokens"]  = (inp_i + outp_i) if (inp_i is not None and outp_i is not None) else None
    return out


def _build_run_context(code_file: Path, gen_meta: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Stable run identifiers to make unified JSON self-contained.

    Reads from generation_metadata.json which is post-patched by the runner
    to include: strategy, extra_repetition_mode, repetition (rep index).
    """
    ctx: Dict[str, Any] = {
        "pipeline_id":          None,
        "variant_stem":         None,
        "class_id":             None,
        "class_name":           None,
        "orchestrator":         None,
        "provider":             None,
        "model_key":            None,
        "model_name":           None,
        "generation_mode":      None,
        "prompt_sha256":        None,
        "repetition":           None,
        # NEW fields — written by runner post-patch into generation_metadata.json
        "strategy":             None,
        "extra_repetition_mode": None,
    }

    if isinstance(gen_meta, dict):
        ctx["pipeline_id"]    = gen_meta.get("pipeline_id")
        ctx["variant_stem"]   = gen_meta.get("variant_stem")
        ctx["class_id"]       = gen_meta.get("class_id")
        ctx["class_name"]     = gen_meta.get("class_name")
        ctx["orchestrator"]   = gen_meta.get("orchestrator")
        ctx["generation_mode"] = gen_meta.get("run_type") or gen_meta.get("generation_mode")
        ctx["prompt_sha256"]  = gen_meta.get("prompt_sha256")
        ctx["repetition"]     = gen_meta.get("repetition")

        # strategy: written by runner post-patch; infer from run_type as fallback
        ctx["strategy"] = (
            gen_meta.get("strategy")
            or _RUN_TYPE_TO_STRATEGY.get(gen_meta.get("run_type", ""), None)
        )

        # extra_repetition_mode: only set for the 'repetition' strategy
        ctx["extra_repetition_mode"] = (
            gen_meta.get("extra_repetition_mode")
            or gen_meta.get("repetition_mode")
        )

        mi = gen_meta.get("model_info") or {}
        if isinstance(mi, dict):
            ctx["provider"]    = mi.get("provider")
            ctx["model_key"]   = mi.get("model_key")
            ctx["model_name"]  = mi.get("model_name")

    # Fallback parse from path layout:
    # .../<pipeline_id>/<variant_stem>/rep_XX/<file>.py
    if not ctx["variant_stem"]:
        try:
            ctx["variant_stem"] = code_file.parent.parent.name
        except Exception:
            pass
    if not ctx["pipeline_id"]:
        try:
            ctx["pipeline_id"] = code_file.parent.parent.parent.name
        except Exception:
            pass

    vs = ctx.get("variant_stem") or ""
    if not ctx["class_id"] and isinstance(vs, str) and vs.startswith("C"):
        ctx["class_id"] = vs.split("_", 1)[0]
    if not ctx["class_name"] and isinstance(vs, str) and "_" in vs:
        ctx["class_name"] = vs.split("_", 1)[1]

    return ctx


def _ensure_eval_payload_shape(payload: Dict[str, Any], *, kind: str) -> Dict[str, Any]:
    """
    Ensure every evaluator payload includes:
      - issues (list)
      - issue_summary (dict)
      - overall_score (float when available)
    """
    if not isinstance(payload, dict):
        return {
            "evaluation_type": kind,
            "file_path": None,
            "orchestrator": "unknown",
            "timestamp": datetime.now().isoformat(),
            "gates_passed": False,
            "gate_checks": [],
            "scores": {},
            "issues": [],
            "issue_summary": _issue_summary([]),
            "overall_score": 0.0,
            "metadata": {"error": "invalid_payload_shape"},
        }

    issues = payload.get("issues")
    if not isinstance(issues, list):
        issues = []
        payload["issues"] = issues

    payload["issue_summary"] = _issue_summary(issues)

    meta = _safe_dict(payload.get("metadata"))
    if kind.upper() == "PCT":
        overall = float(meta.get("PCT", 0.0) or 0.0) if payload.get("gates_passed") else 0.0
        payload["overall_score"] = _clamp10(overall)
    elif kind.upper() == "SAT":
        overall = meta.get("SAT", None)
        if overall is None:
            overall = payload.get("overall_score", 0.0)
        try:
            payload["overall_score"] = _clamp10(float(overall))
        except Exception:
            payload["overall_score"] = 0.0

    payload["metadata"] = meta
    return payload


def _extract_error_events(smoke: Dict[str, Any], pct: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Compact structured events for qualitative postprocessing.
    Do NOT attempt full taxonomy here; just record the key error signals.
    """
    events: List[Dict[str, Any]] = []

    # Smoke failure event
    if isinstance(smoke, dict) and not bool(smoke.get("ok", False)):
        err = _safe_dict(smoke.get("error"))
        events.append({
            "source":         "import_smoke",
            "stage":          err.get("stage"),
            "error_type":     err.get("error_type"),
            "missing_module": err.get("missing_module"),
            "message":        (err.get("message") or "")[:500],
        })

    # PCT gate failures
    if isinstance(pct, dict) and not bool(pct.get("gates_passed", False)):
        for g in _safe_list(pct.get("gate_checks")):
            if not isinstance(g, dict):
                continue
            if bool(g.get("is_critical", False)) and not bool(g.get("passed", True)):
                events.append({
                    "source":     "pct_gate",
                    "stage":      g.get("name"),
                    "error_type": "GateFailed",
                    "message":    (g.get("message") or "")[:500],
                })

    # PCT platform load exception
    try:
        ex = pct["scores"]["loadability"]["details"]["platform_load"].get("exception")
        if isinstance(ex, dict) and ex.get("error_type"):
            events.append({
                "source":         "pct_platform_load_exception",
                "stage":          ex.get("stage"),
                "error_type":     ex.get("error_type"),
                "missing_module": ex.get("missing_module"),
                "message":        (ex.get("message") or "")[:500],
            })
    except Exception:
        pass

    # Dimension exceptions
    try:
        dim_ex = pct.get("metadata", {}).get("dimension_exceptions")
        if isinstance(dim_ex, list):
            for d in dim_ex:
                events.append({
                    "source":     "pct_dimension_exception",
                    "stage":      str(d),
                    "error_type": "DimensionException",
                    "message":    "dimension threw exception (see scores.<dim>.details.exception)",
                })
    except Exception:
        pass

    return events


class UnifiedEvaluator:
    SAT_DIMS = ["correctness", "code_quality", "best_practices", "maintainability", "robustness"]
    PCT_DIMS = ["loadability", "structure_validity", "configuration_validity", "task_validity", "executability"]

    def __init__(
        self,
        config: Optional[Dict[str, Any]] = None,
        intermediate_yaml: Optional[Dict[str, Any]] = None,
        alpha: float = 0.5,
        yaml_valid: Optional[bool] = None,
        pct_mode: str = "subprocess",
        artifacts_dir: Optional[Path] = None,
        pct_timeout_s: int = 120,
        smoke_timeout_s: int = 60,
        airflow_venv: Optional[Path] = None,
        prefect_venv: Optional[Path] = None,
        dagster_venv: Optional[Path] = None,
        airflow_python: Optional[Path] = None,
        prefect_python: Optional[Path] = None,
        dagster_python: Optional[Path] = None,
    ):
        self.config = config or {}
        self.intermediate_yaml = intermediate_yaml
        self.alpha = float(alpha)
        self.yaml_valid = yaml_valid

        self.pct_mode = (pct_mode or "subprocess").strip().lower()
        self.pct_timeout_s = int(pct_timeout_s)
        self.smoke_timeout_s = int(smoke_timeout_s)

        self.airflow_venv = airflow_venv
        self.prefect_venv = prefect_venv
        self.dagster_venv = dagster_venv

        self.airflow_python = airflow_python
        self.prefect_python = prefect_python
        self.dagster_python = dagster_python

        self.logger = logging.getLogger(self.__class__.__name__)
        self.static_analyzer = EnhancedStaticAnalyzer(self.config)
        if intermediate_yaml:
            self.static_analyzer.set_reference(intermediate_yaml)

        self.artifacts_dir = Path(artifacts_dir) if artifacts_dir else None

        # repo root for subprocess cwd
        self.repo_root = Path(__file__).resolve().parents[2]

    def _artifact_root_for(self, code_file: Path) -> Path:
        return self.artifacts_dir if self.artifacts_dir else code_file.parent

    def _pct_out_path(self, code_file: Path, orch: Orchestrator) -> Path:
        root = self._artifact_root_for(code_file)
        return root / (code_file.name + f".pct_{orch.value}.json")

    def _smoke_out_path(self, code_file: Path, orch: Orchestrator) -> Path:
        root = self._artifact_root_for(code_file)
        return root / (code_file.name + f".smoke_{orch.value}.json")

    def _run_pct_subprocess(self, code_file: Path, orch: Orchestrator) -> Dict[str, Any]:
        py = _resolve_python_for_orchestrator(
            orch,
            airflow_venv=self.airflow_venv,
            prefect_venv=self.prefect_venv,
            dagster_venv=self.dagster_venv,
            airflow_python=self.airflow_python,
            prefect_python=self.prefect_python,
            dagster_python=self.dagster_python,
        )
        if py is None or not Path(py).exists():
            payload = {
                "evaluation_type": "platform_compliance",
                "file_path": str(code_file),
                "orchestrator": orch.value,
                "timestamp": datetime.now().isoformat(),
                "gates_passed": False,
                "gate_checks": [],
                "metadata": {"PCT": 0.0, "error": "missing_orchestrator_venv_python"},
                "scores": {},
                "issues": [{
                    "severity": "critical",
                    "category": "env",
                    "message": f"No venv python configured for orchestrator={orch.value}",
                    "line": None,
                    "tool": "unified_evaluator",
                    "details": {},
                }],
            }
            return _ensure_eval_payload_shape(payload, kind="PCT")

        out_json = self._pct_out_path(code_file, orch)
        script = self.repo_root / "scripts" / "evaluators" / "platform_compliance" / "pct_cli.py"

        env = {
            "PYTHONPATH": str(self.repo_root / "scripts"),
            "AIRFLOW_HOME": str(self.repo_root / ".airflow_home_eval"),
            "DAGSTER_HOME": str(self.repo_root / ".dagster_home_eval"),
            "PREFECT_HOME": str(self.repo_root / ".prefect_home_eval"),
        }

        payload = run_cli_json(
            python_exe=Path(py),
            script_path=script,
            args=[str(code_file), "--orchestrator", orch.value, "--out", str(out_json)],
            out_json=out_json,
            cwd=self.repo_root,
            env=env,
            timeout_s=self.pct_timeout_s,
            stub_type="platform_compliance",
            orchestrator=orch.value,
            file_path=str(code_file),
        )
        return _ensure_eval_payload_shape(payload, kind="PCT")

    def _run_smoke_subprocess(self, code_file: Path, orch: Orchestrator) -> Dict[str, Any]:
        py = _resolve_python_for_orchestrator(
            orch,
            airflow_venv=self.airflow_venv,
            prefect_venv=self.prefect_venv,
            dagster_venv=self.dagster_venv,
            airflow_python=self.airflow_python,
            prefect_python=self.prefect_python,
            dagster_python=self.dagster_python,
        )
        if py is None or not Path(py).exists():
            return {
                "evaluation_type": "import_smoke_test",
                "file_path": str(code_file),
                "orchestrator": orch.value,
                "timestamp": datetime.now().isoformat(),
                "ok": False,
                "stages": {"compile_ok": False, "import_exec_ok": False},
                "metadata": {"error": "missing_orchestrator_venv_python"},
                "error": {
                    "stage": "controller",
                    "error_type": "MissingVenv",
                    "message": f"No venv python configured for orchestrator={orch.value}",
                    "traceback": None,
                },
            }

        out_json = self._smoke_out_path(code_file, orch)
        script = self.repo_root / "scripts" / "evaluators" / "import_smoke_test_cli.py"

        env = {
            "PYTHONPATH": str(self.repo_root / "scripts"),
            "PIPELINE_SMOKE_TEST": "1",
            "AIRFLOW_HOME": str(self.repo_root / ".airflow_home_eval"),
            "DAGSTER_HOME": str(self.repo_root / ".dagster_home_eval"),
            "PREFECT_HOME": str(self.repo_root / ".prefect_home_eval"),
        }

        payload = run_cli_json(
            python_exe=Path(py),
            script_path=script,
            args=[str(code_file), "--orchestrator", orch.value, "--out", str(out_json)],
            out_json=out_json,
            cwd=self.repo_root,
            env=env,
            timeout_s=self.smoke_timeout_s,
            stub_type="import_smoke_test",
            orchestrator=orch.value,
            file_path=str(code_file),
        )
        return payload

    def _get_sat(self, static_result: EvaluationResult) -> float:
        sat = static_result.metadata.get("SAT")
        if sat is not None:
            return _clamp10(float(sat))
        vals = [static_result.scores[d].raw_score for d in self.SAT_DIMS if d in static_result.scores]
        return _clamp10(_mean(vals))

    def _get_pct_from_payload(self, pct_payload: Dict[str, Any]) -> float:
        if not isinstance(pct_payload, dict) or not bool(pct_payload.get("gates_passed", False)):
            return 0.0
        meta = _safe_dict(pct_payload.get("metadata"))
        pct = meta.get("PCT", None)
        if pct is not None:
            return _clamp10(float(pct))

        scores = _safe_dict(pct_payload.get("scores"))
        vals: List[float] = []
        for d in self.PCT_DIMS:
            sd = _safe_dict(scores.get(d))
            if "raw_score" in sd:
                try:
                    vals.append(float(sd["raw_score"]))
                except Exception:
                    pass
        return _clamp10(_mean(vals))

    def evaluate(self, file_path: Path, orchestrator: Optional[Orchestrator] = None) -> Dict[str, Any]:
        """
        Always returns a dict. Writes nothing by itself; caller writes unified.json.
        """
        code_file = Path(file_path)
        self.logger.info(f"Running unified evaluation on: {code_file}")

        try:
            # Load generation metadata (post-patched by runner to include strategy/repetition)
            gen_meta = _load_generation_metadata(code_file)
            token_summary = _summarize_token_usage(gen_meta)
            run_context = _build_run_context(code_file, gen_meta)

            # SAT (controller env)
            static_result = self.static_analyzer.evaluate(code_file)
            sat_value = self._get_sat(static_result)

            detected_orchestrator = static_result.orchestrator

            # Resolve target orchestrator
            target_orchestrator = orchestrator
            orch_source = "explicit_argument"

            if target_orchestrator is None or target_orchestrator == Orchestrator.UNKNOWN:
                from_meta = _parse_orchestrator((gen_meta or {}).get("orchestrator"))
                if from_meta is not None:
                    target_orchestrator = from_meta
                    orch_source = "generation_metadata"
                else:
                    target_orchestrator = detected_orchestrator
                    orch_source = "static_detection"

            if target_orchestrator is None:
                target_orchestrator = Orchestrator.UNKNOWN

            # Run venv subprocess evaluators
            smoke_payload = self._run_smoke_subprocess(code_file, target_orchestrator)
            pct_payload   = self._run_pct_subprocess(code_file, target_orchestrator)

            # Ensure SAT payload has issues + issue_summary + overall_score
            sat_payload = static_result.to_dict()
            sat_issues  = [i.to_dict() for i in static_result.all_issues]
            sat_payload["issues"]       = sat_issues
            sat_payload["issue_summary"] = _issue_summary(sat_issues)
            sat_payload["overall_score"] = float(sat_value)

            pct_payload = _ensure_eval_payload_shape(pct_payload, kind="PCT")

            # Compute combined score and gate signals
            pct_value    = self._get_pct_from_payload(pct_payload)
            platform_gate = bool(pct_payload.get("gates_passed", False))
            smoke_ok      = bool(smoke_payload.get("ok", False)) if isinstance(smoke_payload, dict) else False
            yaml_gate_ok  = True if self.yaml_valid is None else bool(self.yaml_valid)

            execution_gate = bool(platform_gate and smoke_ok)

            if (not yaml_gate_ok) or (not execution_gate):
                combined_score = 0.0
            else:
                combined_score = self.alpha * sat_value + (1.0 - self.alpha) * pct_value
            combined_score = _clamp10(combined_score)

            pct_issues   = pct_payload.get("issues", []) if isinstance(pct_payload.get("issues"), list) else []
            paper_issues = sat_issues + pct_issues
            error_events = _extract_error_events(smoke_payload, pct_payload)

            unified = {
                "schema_version": UNIFIED_SCHEMA_VERSION,

                "file_path":             str(code_file),
                "orchestrator":          target_orchestrator.value,
                "evaluation_timestamp":  datetime.now().isoformat(),
                "alpha":                 float(self.alpha),
                "yaml_valid":            yaml_gate_ok,

                # Self-contained run context (now includes strategy + extra_repetition_mode)
                "run_context": run_context,
                "generation": {
                    "token_usage_summary":  token_summary,
                    "generation_metadata":  gen_meta,
                },

                "static_analysis":    _ensure_eval_payload_shape(sat_payload, kind="SAT"),
                "import_smoke":       smoke_payload,
                "platform_compliance": pct_payload,
                "semantic_analysis":  None,

                "error_events": error_events,

                "summary": {
                    "static_score":     round(float(sat_value), 4),
                    "compliance_score": round(float(pct_value), 4),
                    "combined_score":   round(float(combined_score), 4),

                    "platform_gate_passed":  platform_gate,
                    "import_smoke_ok":       smoke_ok,
                    "execution_gate_passed": execution_gate,
                    "passed":                execution_gate,
                    "passed_executable":     execution_gate,

                    "issues": _issue_summary(paper_issues),

                    "import_smoke_error_type":     _safe_dict(smoke_payload.get("error")).get("error_type") if isinstance(smoke_payload, dict) else None,
                    "import_smoke_missing_module": _safe_dict(smoke_payload.get("error")).get("missing_module") if isinstance(smoke_payload, dict) else None,

                    "semantic_fidelity_oracle":  None,
                    "semantic_fidelity_variant": None,
                    "semantic_issues": {"total": 0, "critical": 0, "major": 0, "minor": 0, "info": 0},
                },

                "metadata": {
                    "evaluation_context": {
                        "target_orchestrator":    target_orchestrator.value,
                        "detected_orchestrator":  detected_orchestrator.value if detected_orchestrator else "unknown",
                        "orchestrator_source":    orch_source,
                        "controller_python":      sys.executable,
                        "controller_python_version": platform.python_version(),
                        "repo_root":              str(self.repo_root),
                        "artifacts_dir":          str(self._artifact_root_for(code_file)),
                        "pct_mode":               self.pct_mode,
                    }
                }
            }
            return unified

        except Exception as e:
            self.logger.exception(f"UnifiedEvaluator crashed: {e}")
            return {
                "schema_version":        UNIFIED_SCHEMA_VERSION,
                "file_path":             str(file_path),
                "orchestrator":          "unknown",
                "evaluation_timestamp":  datetime.now().isoformat(),
                "alpha":                 float(self.alpha),
                "yaml_valid":            True if self.yaml_valid is None else bool(self.yaml_valid),
                "run_context":           {},
                "generation":            {},
                "static_analysis":       None,
                "import_smoke":          None,
                "platform_compliance":   None,
                "semantic_analysis":     None,
                "error_events": [{
                    "source":     "unified_evaluator",
                    "stage":      "crash",
                    "error_type": type(e).__name__,
                    "message":    str(e)[:500],
                }],
                "summary": {
                    "static_score":           None,
                    "compliance_score":       None,
                    "combined_score":         None,
                    "platform_gate_passed":   False,
                    "import_smoke_ok":        False,
                    "execution_gate_passed":  False,
                    "passed":                 False,
                    "passed_executable":      False,
                    "issues": {"total": 0, "critical": 0, "major": 0, "minor": 0, "info": 0},
                    "import_smoke_error_type":     type(e).__name__,
                    "import_smoke_missing_module": None,
                    "semantic_fidelity_oracle":    None,
                    "semantic_fidelity_variant":   None,
                    "semantic_issues": {"total": 0, "critical": 0, "major": 0, "minor": 0, "info": 0},
                },
                "error": {"stage": "unified_evaluator", "message": str(e), "error_type": type(e).__name__},
            }


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Run unified evaluation (SAT + smoke + PCT).")
    parser.add_argument("file", help="Path to generated workflow Python file")

    parser.add_argument("--orchestrator", default="auto", choices=["auto", "airflow", "prefect", "dagster"])
    parser.add_argument("--alpha", type=float, default=0.5)
    parser.add_argument("--yaml-valid", default="none", choices=["true", "false", "none"])

    parser.add_argument("--airflow-venv",  default=None)
    parser.add_argument("--prefect-venv",  default=None)
    parser.add_argument("--dagster-venv",  default=None)

    parser.add_argument("--airflow-python", default=None)
    parser.add_argument("--prefect-python", default=None)
    parser.add_argument("--dagster-python", default=None)

    parser.add_argument("--artifacts-dir",  default=None)
    parser.add_argument("--pct-timeout-s",  type=int, default=120)
    parser.add_argument("--smoke-timeout-s", type=int, default=60)

    parser.add_argument("--out",       default=None)
    parser.add_argument("--stdout",    action="store_true")
    parser.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    args = parser.parse_args()

    logging.basicConfig(level=getattr(logging, args.log_level), format="%(levelname)s - %(message)s")

    yaml_valid = None if args.yaml_valid == "none" else (args.yaml_valid == "true")

    ue = UnifiedEvaluator(
        alpha=args.alpha,
        yaml_valid=yaml_valid,
        artifacts_dir=Path(args.artifacts_dir) if args.artifacts_dir else None,
        pct_timeout_s=args.pct_timeout_s,
        smoke_timeout_s=args.smoke_timeout_s,
        airflow_venv=Path(args.airflow_venv) if args.airflow_venv else None,
        prefect_venv=Path(args.prefect_venv) if args.prefect_venv else None,
        dagster_venv=Path(args.dagster_venv) if args.dagster_venv else None,
        airflow_python=Path(args.airflow_python) if args.airflow_python else None,
        prefect_python=Path(args.prefect_python) if args.prefect_python else None,
        dagster_python=Path(args.dagster_python) if args.dagster_python else None,
    )

    orch = None if args.orchestrator == "auto" else Orchestrator(args.orchestrator)
    payload = ue.evaluate(Path(args.file), orchestrator=orch)

    txt = json.dumps(payload, indent=2, default=str)

    if args.out:
        out_path = Path(args.out)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(txt, encoding="utf-8")
        print(f"Wrote: {out_path}")
        return

    if args.stdout or not args.out:
        print(txt)


if __name__ == "__main__":
    main()