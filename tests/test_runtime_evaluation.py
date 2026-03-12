from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

from orcheval.energy.runtime_evaluation import (
    EnergyEvaluationRequest,
    LLMProviderError,
    _recipe_to_rows,
    build_provider,
    evaluate_energy,
)
from orcheval.unified_evaluator import UnifiedEvaluator


def _write_workflow(tmp_path: Path) -> Path:
    f = tmp_path / "flow.py"
    f.write_text(
        """
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

with DAG(dag_id='x', start_date=datetime(2024,1,1), schedule=None) as dag:
    t1 = PythonOperator(task_id='a', python_callable=lambda: 1)
""".strip()
        + "\n",
        encoding="utf-8",
    )
    return f


def test_mode_precedence_auto_falls_to_heuristic_without_data_or_llm(tmp_path: Path) -> None:
    wf = _write_workflow(tmp_path)
    req = EnergyEvaluationRequest(
        file_path=wf,
        code_text=wf.read_text(encoding="utf-8"),
        orchestrator="airflow",
        repo_root=Path.cwd(),
        runtime_python=Path(sys.executable),
        mode_selected="auto",
        sample_paths=[],
        llm_provider=None,
        llm_model=None,
    )

    res = evaluate_energy(req).to_dict()
    attempted = res["attempted_modes"]
    assert attempted[0]["mode"] == "sample"
    assert attempted[0]["status"] == "skipped"
    assert attempted[1]["mode"] == "synthetic"
    assert attempted[1]["status"] == "skipped"
    assert res["mode_used"] == "heuristic"
    assert isinstance(res.get("metadata", {}).get("warnings"), list)


def test_synthetic_rows_are_deterministic() -> None:
    recipe = {
        "row_count": 4,
        "columns": [
            {"name": "id", "type": "int"},
            {"name": "score", "type": "float"},
            {"name": "name", "type": "str"},
        ],
    }
    sig = "abcd" * 16
    rows_a = _recipe_to_rows(recipe, signature_hash=sig, seed=1729, max_rows=10)
    rows_b = _recipe_to_rows(recipe, signature_hash=sig, seed=1729, max_rows=10)
    assert rows_a == rows_b


def test_provider_error_normalization() -> None:
    with pytest.raises(LLMProviderError) as e:
        build_provider("unknown_provider", model="x", api_key_env=None, base_url=None, timeout_s=5)
    assert e.value.code == "unsupported_provider"


def test_privacy_no_raw_rows_persisted_for_sample_mode(tmp_path: Path) -> None:
    wf = _write_workflow(tmp_path)
    sample = tmp_path / "sample.csv"
    sample.write_text("id,value\n1,sensitive123\n2,ok\n", encoding="utf-8")

    req = EnergyEvaluationRequest(
        file_path=wf,
        code_text=wf.read_text(encoding="utf-8"),
        orchestrator="airflow",
        repo_root=Path.cwd(),
        runtime_python=Path(sys.executable),
        mode_selected="sample",
        sample_paths=[sample],
        max_rows=100,
        max_tasks=5,
        timeout_s=30,
    )

    res = evaluate_energy(req).to_dict()
    blob = json.dumps(res)
    assert "sensitive123" not in blob
    assert res["privacy"]["raw_rows_persisted"] is False


class _FakeProvider:
    provider_name = "openai"

    def health_check(self):
        return True, None

    def generate_synthetic_spec(self, minimized_spec, *, seed: int, max_rows: int):
        return {
            "row_count": min(max_rows, 5),
            "columns": [
                {"name": "id", "type": "int", "example": 1},
                {"name": "amount", "type": "float", "example": 2.2},
                {"name": "category", "type": "str", "example": "A"},
            ],
        }


@pytest.mark.parametrize("orch", ["airflow", "prefect", "dagster"])
def test_synthetic_mode_runtime_for_supported_orchestrators(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, orch: str) -> None:
    wf = _write_workflow(tmp_path)
    import orcheval.energy.runtime_evaluation as reval

    monkeypatch.setattr(reval, "build_provider", lambda *args, **kwargs: _FakeProvider())

    req = EnergyEvaluationRequest(
        file_path=wf,
        code_text=wf.read_text(encoding="utf-8"),
        orchestrator=orch,
        repo_root=Path.cwd(),
        runtime_python=Path(sys.executable),
        mode_selected="synthetic",
        llm_provider="openai",
        llm_model="gpt-test",
        max_rows=100,
        max_tasks=5,
        timeout_s=30,
    )

    res = evaluate_energy(req).to_dict()
    assert res["mode_used"] == "synthetic"
    assert isinstance(res["runtime_measurement"], dict)
    assert res["runtime_measurement"].get("status") == "ok"
    assert res["runtime_measurement"].get("execution_adapter_requested") == "representative"
    assert res["runtime_measurement"].get("execution_adapter_used") in {"representative", "native"}
    assert res["runtime_measurement"].get("energy_profile", {}).get("measurement_scope") == "runtime_representative_workload"
    assert res["synthetic_info"]["signature_hash"]


def test_unified_regression_without_energy_enabled(tmp_path: Path) -> None:
    wf = _write_workflow(tmp_path)
    ue = UnifiedEvaluator()
    payload = ue.evaluate(wf)
    assert "summary" in payload
    assert payload.get("energy_evaluation") is None
