from __future__ import annotations

import json
from pathlib import Path

from orcheval.energy.design_rules import EcoDesignRuleEngine
from orcheval.energy.runtime_evaluation import EnergyEvaluationRequest, evaluate_energy
from orcheval.knowledge_pack import resolve_knowledge_pack, resolve_rule_value
from orcheval.platform_compliance.airflow_compliance import AirflowComplianceTester


SIMPLE_AIRFLOW = """
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

with DAG(dag_id='x', start_date=datetime(2024,1,1), schedule=None) as dag:
    t1 = PythonOperator(task_id='a', python_callable=lambda: 1)
""".strip() + "\n"


def test_pack_resolution_and_rule_value_from_bundled_pack() -> None:
    res = resolve_knowledge_pack(
        orchestrator="airflow",
        orchestrator_version="2.8.1",
        mode="pack",
        pack_path=None,
        pack_version=None,
    )
    assert res.status == "resolved"
    assert res.applied is True

    value, provenance = resolve_rule_value(
        res,
        check_id="pct.airflow.dryrun.test_patterns",
        default=["fallback"],
        capability_refs=["supports_dag_test"],
    )
    assert isinstance(value, list)
    assert provenance["rule_source"] == "pack"
    assert provenance["check_id"] == "pct.airflow.dryrun.test_patterns"


def test_legacy_mode_bypasses_pack_resolution() -> None:
    res = resolve_knowledge_pack(
        orchestrator="airflow",
        orchestrator_version="2.8.1",
        mode="legacy",
        pack_path=None,
        pack_version=None,
    )
    assert res.status == "legacy_bypass"
    assert res.applied is False


def test_unknown_version_conservative_degrade() -> None:
    res = resolve_knowledge_pack(
        orchestrator="airflow",
        orchestrator_version="99.99.99",
        mode="pack",
        pack_path=None,
        pack_version=None,
    )
    assert res.status == "unknown_version_conservative"
    assert res.applied is False
    assert res.uncertainty


def test_approximate_version_match_same_major() -> None:
    res = resolve_knowledge_pack(
        orchestrator="airflow",
        orchestrator_version="2.9.4",
        mode="pack",
        pack_path=None,
        pack_version=None,
    )
    assert res.applied is True
    assert res.status in {"resolved", "resolved_approximate"}
    assert res.resolved_version in {"2", "2.9", "2.8"}


def test_pct_gate_and_dryrun_include_provenance(tmp_path: Path) -> None:
    wf = tmp_path / "wf.py"
    wf.write_text(SIMPLE_AIRFLOW, encoding="utf-8")

    tester = AirflowComplianceTester(
        config={
            "knowledge_pack_mode": "pack",
        }
    )
    result = tester.evaluate(wf).to_dict()

    kp = result.get("metadata", {}).get("knowledge_pack", {})
    assert kp.get("mode_effective") == "pack"
    assert isinstance(result.get("metadata", {}).get("warnings"), list)

    gate = next(g for g in result.get("gate_checks", []) if g.get("name") == "minimum_structure")
    gate_prov = gate.get("details", {}).get("provenance", {}).get("dag_tokens", {})
    assert gate_prov.get("check_id") == "pct.airflow.minimum_structure.dag_tokens"
    assert gate_prov.get("rule_source") in {"core", "pack"}

    dryrun = (
        result.get("scores", {})
        .get("executability", {})
        .get("details", {})
        .get("dryrun", {})
    )
    dryrun_prov = dryrun.get("provenance", {})
    assert dryrun_prov.get("check_id") == "pct.airflow.dryrun.test_patterns"
    assert dryrun_prov.get("rule_source") in {"core", "pack"}


def test_energy_rules_can_use_pack_parameters(tmp_path: Path) -> None:
    custom_pack = {
        "pack_id": "custom",
        "pack_version": "1",
        "schema_version": "1.0.0",
        "orchestrators": {
            "airflow": {
                "generic": {
                    "capabilities": {},
                    "rule_params": {
                        "energy.rule.high_xcom_intensity.threshold": 1,
                    },
                },
                "versions": {
                    "2": {
                        "capabilities": {},
                        "rule_params": {},
                    }
                },
            }
        },
    }
    pack_path = tmp_path / "custom_pack.json"
    pack_path.write_text(json.dumps(custom_pack), encoding="utf-8")

    res = resolve_knowledge_pack(
        orchestrator="airflow",
        orchestrator_version="2.8.0",
        mode="pack",
        pack_path=pack_path,
        pack_version=None,
    )
    assert res.applied is True

    profile = {
        "metrics": {"total_tasks": 2, "total_edges": 1, "parallelism_index": 0.1, "sequential_bottleneck_ratio": 0.2, "max_fan_out": 1},
        "signals": {
            "xcom": {"xcom_total_signals": 2},
            "sensors": {"has_sensor": False, "poke_mode_detected": False},
            "operator_tiers": {"heavy_compute_count": 0, "polling_heavy_count": 0},
            "dependency_info": {"dependency_syntax_detected": True},
        },
        "schedule": {"runs_per_year_estimate": 1},
        "edges": {},
    }
    out = EcoDesignRuleEngine(knowledge_pack_resolution=res).evaluate(profile)
    rules = out.get("rules", [])
    xcom = next(r for r in rules if r.get("rule_id") == "HIGH_XCOM_INTENSITY")
    prov = xcom.get("evidence", {}).get("provenance", {})
    assert prov.get("rule_source") == "pack"
    assert out.get("knowledge_pack_applied") is True


def test_energy_evaluation_reports_pack_uncertainty(monkeypatch, tmp_path: Path) -> None:
    wf = tmp_path / "wf.py"
    wf.write_text(SIMPLE_AIRFLOW, encoding="utf-8")

    import orcheval.energy.runtime_evaluation as reval

    monkeypatch.setattr(reval, "detect_orchestrator_version", lambda *args, **kwargs: "99.0.0")

    req = EnergyEvaluationRequest(
        file_path=wf,
        code_text=wf.read_text(encoding="utf-8"),
        orchestrator="airflow",
        repo_root=Path.cwd(),
        mode_selected="heuristic",
        knowledge_pack_mode="pack",
    )
    payload = evaluate_energy(req).to_dict()
    kp = payload.get("metadata", {}).get("knowledge_pack", {})
    assert kp.get("status") == "unknown_version_conservative"
    assert payload.get("metadata", {}).get("uncertainty")
