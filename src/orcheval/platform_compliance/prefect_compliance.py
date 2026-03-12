"""
Prefect-specific platform compliance tester gate-based, penalty-free scoring (issues logged).

PATCHED for qualitative error analysis:
- Capture exception_type + full traceback in platform load checks
- Include python context in details
"""

import ast
import importlib.util
import os
import re
import subprocess
import sys
import tempfile
import traceback
import platform
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Tuple

from ..base_evaluator import (
    EvaluationScore,
    Issue,
    Severity,
    Orchestrator,
)
from .pct_base import BasePlatformComplianceTester

# Check if Prefect is available
try:
    import prefect
    PREFECT_AVAILABLE = True
    PREFECT_VERSION = prefect.__version__
except ImportError:
    PREFECT_AVAILABLE = False
    PREFECT_VERSION = None


def _tb_full() -> str:
    return traceback.format_exc()


def _tb_tail(max_chars: int = 6000) -> str:
    tb = traceback.format_exc()
    if len(tb) > max_chars:
        return tb[-max_chars:]
    return tb


class PrefectComplianceTester(BasePlatformComplianceTester):
    """Prefect-specific compliance testing with weighted penalties."""

    ORCHESTRATOR = Orchestrator.PREFECT

    def _get_orchestrator_runtime_version(self) -> str | None:
        override = super()._get_orchestrator_runtime_version()
        return override if override else PREFECT_VERSION

    def _evaluate_minimum_structure(self, code: str) -> Tuple[bool, Dict[str, Any]]:
        flow_tokens, flow_prov = self._pack_rule(
            check_id="pct.prefect.minimum_structure.flow_tokens",
            default=["@flow"],
            capability_refs=["supports_flow_task_decorators"],
        )
        task_tokens, task_prov = self._pack_rule(
            check_id="pct.prefect.minimum_structure.task_tokens",
            default=["@task", "def "],
            capability_refs=["supports_flow_task_decorators"],
        )

        c = code or ""
        cl = c.lower()
        flow_list = [str(t).lower() for t in (flow_tokens if isinstance(flow_tokens, list) else []) if str(t).strip()]
        task_list = [str(t).lower() for t in (task_tokens if isinstance(task_tokens, list) else []) if str(t).strip()]

        has_flow = any(tok in cl for tok in flow_list)
        has_tasks = any(tok in cl for tok in task_list)

        details: Dict[str, Any] = {
            "has_flow": has_flow,
            "has_tasks": has_tasks,
            "flow_tokens": flow_list,
            "task_tokens": task_list,
            "provenance": {
                "flow_tokens": flow_prov,
                "task_tokens": task_prov,
            },
        }
        return bool(has_flow and has_tasks), details

    def _check_minimum_structure(self, code: str) -> bool:
        """Check if code has minimum Prefect structure."""
        ok, _details = self._evaluate_minimum_structure(code)
        return ok

    def _check_minimum_structure_details(self, code: str) -> Dict[str, Any]:
        _ok, details = self._evaluate_minimum_structure(code)
        return details

    # ═══════════════════════════════════════════════════════════════════════
    # LOADABILITY
    # ═══════════════════════════════════════════════════════════════════════
    def _check_platform_load(
        self,
        code: str,
        file_path: Path
    ) -> Tuple[float, List[Issue], Dict]:
        """Check if Prefect can load the flow."""
        issues: List[Issue] = []
        details: Dict[str, Any] = {
            "prefect_available": PREFECT_AVAILABLE,
            "prefect_version": PREFECT_VERSION,
            "module_loadable": False,
            "flows_found": [],
            "tasks_found": [],
            # Qualitative diagnostics:
            "python_executable": sys.executable,
            "python_version": platform.python_version(),
            "sys_path_head": list(sys.path[:10]),
            "temp_module_path": None,
            "exception": None,
        }

        if not PREFECT_AVAILABLE:
            return 2.0, [Issue(
                severity=Severity.INFO,
                category="platform",
                message="Prefect not installed - cannot verify load",
                details={"prefect_available": False},
            )], details

        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".py", delete=False, encoding="utf-8"
        ) as f:
            f.write(code)
            temp_path = f.name

        details["temp_module_path"] = temp_path

        try:
            spec = importlib.util.spec_from_file_location("prefect_test_module", temp_path)
            if spec and spec.loader:
                module = importlib.util.module_from_spec(spec)
                sys.modules["prefect_test_module"] = module

                try:
                    spec.loader.exec_module(module)
                    details["module_loadable"] = True

                    # Find flows and tasks (heuristic; Prefect's decorator wrapping can be subtle)
                    try:
                        import prefect  # noqa: F401
                    except Exception as e:
                        details["exception"] = {
                            "stage": "prefect_internal_imports",
                            "error_type": type(e).__name__,
                            "message": str(e),
                            "traceback": _tb_full(),
                            "traceback_tail": _tb_tail(),
                        }
                        issues.append(Issue(
                            severity=Severity.MAJOR,
                            category="platform",
                            message=f"Prefect import failed during discovery: {type(e).__name__}: {e}",
                            details=details["exception"],
                        ))
                        return 1.0, issues, details

                    for attr_name in dir(module):
                        if attr_name.startswith("_"):
                            continue
                        attr = getattr(module, attr_name, None)
                        if attr is None:
                            continue

                        # Extremely permissive heuristics (keep your original approach but don't crash)
                        try:
                            if hasattr(attr, "fn") or "flow" in str(type(attr)).lower():
                                details["flows_found"].append(attr_name)
                            elif hasattr(attr, "is_task") or "task" in str(type(attr)).lower():
                                details["tasks_found"].append(attr_name)
                        except Exception:
                            continue

                    if details["flows_found"]:
                        return 4.0, issues, details

                    issues.append(Issue(
                        severity=Severity.MAJOR,
                        category="platform",
                        message="Module loaded but no flows detected",
                        details={
                            "flows_found": details["flows_found"],
                            "tasks_found": details["tasks_found"],
                        },
                    ))
                    return 2.0, issues, details

                except ModuleNotFoundError as e:
                    details["exception"] = {
                        "stage": "exec_module",
                        "error_type": type(e).__name__,
                        "missing_module": getattr(e, "name", None),
                        "message": str(e),
                        "traceback": _tb_full(),
                        "traceback_tail": _tb_tail(),
                    }
                    issues.append(Issue(
                        severity=Severity.MAJOR,
                        category="platform",
                        message=f"Missing module during load: {getattr(e, 'name', None)}",
                        details=details["exception"],
                    ))
                    return 1.0, issues, details

                except Exception as e:
                    details["exception"] = {
                        "stage": "exec_module",
                        "error_type": type(e).__name__,
                        "message": str(e),
                        "traceback": _tb_full(),
                        "traceback_tail": _tb_tail(),
                    }
                    issues.append(Issue(
                        severity=Severity.MAJOR,
                        category="platform",
                        message=f"Load error: {type(e).__name__}: {e}",
                        details=details["exception"],
                    ))
                    return 1.0, issues, details

                finally:
                    if "prefect_test_module" in sys.modules:
                        del sys.modules["prefect_test_module"]

            issues.append(Issue(
                severity=Severity.CRITICAL,
                category="platform",
                message="Could not create module spec",
                details={"temp_module_path": temp_path},
            ))
            return 0.0, issues, details

        except Exception as e:
            details["exception"] = {
                "stage": "outer_platform_load",
                "error_type": type(e).__name__,
                "message": str(e),
                "traceback": _tb_full(),
                "traceback_tail": _tb_tail(),
            }
            issues.append(Issue(
                severity=Severity.CRITICAL,
                category="platform",
                message=f"Exception in platform load: {type(e).__name__}: {e}",
                details=details["exception"],
            ))
            return 0.0, issues, details

        finally:
            if os.path.exists(temp_path):
                os.unlink(temp_path)

    # ═══════════════════════════════════════════════════════════════════════
    # STRUCTURE VALIDITY
    # ═══════════════════════════════════════════════════════════════════════
    def _check_required_constructs(
        self,
        code: str,
        tree: ast.AST
    ) -> Tuple[float, List[Issue], Dict]:
        """Check for required Prefect constructs."""
        issues = []
        details = {
            "has_flow": False,
            "has_tasks": False,
            "flow_count": 0,
            "task_count": 0,
        }

        score = 0.0

        flow_count = code.count("@flow")
        if flow_count > 0:
            details["has_flow"] = True
            details["flow_count"] = flow_count
            score += 2.0
        else:
            issues.append(Issue(
                severity=Severity.CRITICAL,
                category="structure",
                message="No @flow decorator found",
            ))

        task_count = code.count("@task")
        if task_count > 0:
            details["has_tasks"] = True
            details["task_count"] = task_count
            score += 1.5
        else:
            issues.append(Issue(
                severity=Severity.MINOR,
                category="structure",
                message="No @task decorators found (OK if using inline logic)",
            ))
            score += 0.5

        if "from prefect import" in code or "import prefect" in code:
            score += 0.5

        return min(4.0, score), issues, details

    # ═══════════════════════════════════════════════════════════════════════
    # CONFIGURATION VALIDITY
    # ═══════════════════════════════════════════════════════════════════════
    def _check_schedule_config(
        self,
        code: str,
        tree: ast.AST
    ) -> Tuple[float, List[Issue], Dict]:
        """Check Prefect schedule configuration."""
        issues = []
        details = {
            "has_schedule": False,
            "has_deployment": False,
            "schedule_type": None,
        }

        score = 0.5

        schedule_patterns = [
            "CronSchedule", "IntervalSchedule", "RRuleSchedule",
            "schedule=", "cron=", "interval=",
        ]

        for pattern in schedule_patterns:
            if pattern in code:
                details["has_schedule"] = True
                details["schedule_type"] = pattern.replace("=", "")
                score += 1.0
                break

        if "Deployment" in code or "deployment" in code.lower():
            details["has_deployment"] = True
            score += 0.5

        if "work_pool" in code or "work_queue" in code:
            score += 0.5

        return min(2.5, score), issues, details

    def _check_default_args(
        self,
        code: str,
        tree: ast.AST
    ) -> Tuple[float, List[Issue], Dict]:
        """Check Prefect default configuration."""
        issues = []
        details = {
            "has_task_runner": False,
            "has_log_prints": False,
            "has_result_storage": False,
        }

        score = 0.5

        if "task_runner=" in code or "TaskRunner" in code:
            details["has_task_runner"] = True
            score += 0.75

        if "log_prints=" in code:
            details["has_log_prints"] = True
            score += 0.5

        if "persist_result" in code or "result_storage" in code:
            details["has_result_storage"] = True
            score += 0.75

        return min(2.5, score), issues, details

    # ═══════════════════════════════════════════════════════════════════════
    # TASK VALIDITY
    # ═══════════════════════════════════════════════════════════════════════
    def _check_task_definitions(
        self,
        code: str,
        tree: ast.AST
    ) -> Tuple[float, List[Issue], Dict]:
        """Check Prefect task definitions."""
        issues = []
        details = {
            "task_count": 0,
            "flow_count": 0,
            "has_docstrings": False,
            "has_names": False,
        }

        score = 0.0

        task_count = code.count("@task")
        flow_count = code.count("@flow")

        details["task_count"] = task_count
        details["flow_count"] = flow_count

        if flow_count > 0:
            score += 2.0
        else:
            issues.append(Issue(
                severity=Severity.CRITICAL,
                category="task",
                message="No @flow decorators found",
            ))
            return 0.0, issues, details

        if task_count > 0:
            score += 1.0

        if 'name="' in code or "name='" in code:
            details["has_names"] = True
            score += 0.5

        if '"""' in code or "'''" in code:
            details["has_docstrings"] = True
            score += 0.5

        return min(4.0, score), issues, details

    def _check_operator_usage(
        self,
        code: str,
        tree: ast.AST
    ) -> Tuple[float, List[Issue], Dict]:
        """Check Prefect decorator usage."""
        issues = []
        details = {
            "uses_flow_decorator": False,
            "uses_task_decorator": False,
            "has_proper_imports": False,
        }

        score = 0.0

        if "from prefect import" in code:
            details["has_proper_imports"] = True
            score += 1.0

        if "@flow" in code:
            details["uses_flow_decorator"] = True
            score += 1.0

        if "@task" in code:
            details["uses_task_decorator"] = True
            score += 0.5

        if "@flow(" in code or "@task(" in code:
            score += 0.5

        return min(3.0, score), issues, details

    # ═══════════════════════════════════════════════════════════════════════
    # EXECUTABILITY
    # ═══════════════════════════════════════════════════════════════════════
    def _check_dryrun_capability(
        self,
        code: str,
        file_path: Path
    ) -> Tuple[float, List[Issue], Dict]:
        """Check Prefect dry-run/validation capability."""
        issues = []
        test_patterns, prov = self._pack_rule(
            check_id="pct.prefect.dryrun.test_patterns",
            default=["@flow", "def "],
            capability_refs=["supports_flow_runtime_invocation"],
        )
        pattern_list = [str(p) for p in (test_patterns if isinstance(test_patterns, list) else []) if str(p).strip()]
        details = {
            "prefect_available": PREFECT_AVAILABLE,
            "can_validate": False,
            "test_patterns": pattern_list,
            "provenance": prov,
        }

        if not PREFECT_AVAILABLE:
            return 2.0, [Issue(
                severity=Severity.INFO,
                category="executability",
                message="Prefect not installed - cannot validate",
                details={"prefect_available": False},
            )], details

        if all(p in code for p in pattern_list[:2]) if len(pattern_list) >= 2 else any(p in code for p in pattern_list):
            details["can_validate"] = True
            return 4.0, issues, details

        return 2.0, issues, details

    def _extract_task_ids(self, code: str) -> set:
        """Extract task names from Prefect code."""
        task_ids = set()

        pattern = r"@task[^)]*\)?\s*\ndef\s+(\w+)"
        task_ids.update(re.findall(pattern, code, re.MULTILINE))

        pattern2 = r"@task\s*\([^)]*name\s*=\s*['\"]([^'\"]+)['\"]"
        task_ids.update(re.findall(pattern2, code))

        pattern3 = r"@flow[^)]*\)?\s*\ndef\s+(\w+)"
        task_ids.update(re.findall(pattern3, code, re.MULTILINE))

        return task_ids
