#!/usr/bin/env python3
"""
Design Rules Engine (v1.1)
==========================

Adds:
- Optional rule that warns if OPOS edges disagree with code edges significantly.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional


RULES_SCHEMA_VERSION = "1.1.0"


@dataclass
class FiredRule:
    rule_id: str
    severity: str          # info | minor | major
    title: str
    message: str
    evidence: Dict[str, Any]
    recommendation: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "rule_id": self.rule_id,
            "severity": self.severity,
            "title": self.title,
            "message": self.message,
            "evidence": self.evidence,
            "recommendation": self.recommendation,
        }


class EcoDesignRuleEngine:
    def evaluate(self, profile: Dict[str, Any]) -> Dict[str, Any]:
        fired: List[FiredRule] = []

        metrics = profile.get("metrics") or {}
        signals = profile.get("signals") or {}
        schedule = profile.get("schedule") or {}
        edges_block = profile.get("edges") or {}

        total_tasks = int(metrics.get("total_tasks") or 0)
        total_edges = int(metrics.get("total_edges") or 0)
        parallelism_index = metrics.get("parallelism_index")
        sequential_ratio = metrics.get("sequential_bottleneck_ratio")
        max_fan_out = int(metrics.get("max_fan_out") or 0)

        runs_per_year = schedule.get("runs_per_year_estimate")

        xcom_total = ((signals.get("xcom") or {}).get("xcom_total_signals") or 0)
        sensors = signals.get("sensors") or {}
        poke = bool(sensors.get("poke_mode_detected"))
        has_sensor = bool(sensors.get("has_sensor"))

        tiers = (signals.get("operator_tiers") or {})
        heavy_count = int(tiers.get("heavy_compute_count") or 0)
        polling_count = int(tiers.get("polling_heavy_count") or 0)

        dep_info = signals.get("dependency_info") or {}
        dep_detected = bool(dep_info.get("dependency_syntax_detected"))

        mismatch = edges_block.get("edge_mismatch")

        # ------------------------------------------------------------
        # Rule: OPOS vs code mismatch (new in v1.1)
        # ------------------------------------------------------------
        
        if isinstance(mismatch, dict):
            ratio = float(mismatch.get("symmetric_diff_ratio_vs_opos") or 0.0)
            mapping_conf = mismatch.get("mapping_confidence")

            # Only warn when mapping is at least medium AND mismatch is large.
            if mapping_conf in ("high", "medium") and ratio >= 0.80:
                fired.append(FiredRule(
                    rule_id="SPEC_CODE_EDGE_MISMATCH",
                    severity="info",
                    title="Spec (OPOS) vs code dependency graph differ (diagnostic)",
                    message=(
                        "OPOS edges differ substantially from the high-confidence code-extracted edges. "
                        "This can be normal for dynamic/TaskFlow styles, but may also indicate generation drift."
                    ),
                    evidence={
                        "ratio": ratio,
                        "code_edges_compared_count": mismatch.get("code_edges_compared_count"),
                        "opos_edges_count": mismatch.get("opos_edges_count"),
                        "mapping_confidence": mapping_conf,
                        "effective_edges_source": edges_block.get("effective_source"),
                    },
                    recommendation="If you expect explicit dependencies in code, verify the generator preserved the OPOS flow edges.",
                ))


        # ------------------------------------------------------------
        # Rule: poke-mode sensors
        # ------------------------------------------------------------
        if has_sensor and poke:
            fired.append(FiredRule(
                rule_id="SENSOR_POKE_MODE",
                severity="major",
                title="Poke-mode sensor detected",
                message=(
                    "Poke-mode sensors repeatedly wake up and check conditions, which can waste worker time/energy. "
                    "Consider using reschedule mode or an event-driven trigger where possible."
                ),
                evidence={
                    "has_sensor": has_sensor,
                    "poke_mode_detected": poke,
                    "polling_heavy_count": polling_count,
                },
                recommendation="For Airflow sensors: set mode='reschedule' and tune poke_interval/timeout.",
            ))

        # ------------------------------------------------------------
        # Rule: mostly sequential graph
        # ------------------------------------------------------------
        if total_tasks >= 4 and isinstance(sequential_ratio, (int, float)) and sequential_ratio >= 0.80:
            fired.append(FiredRule(
                rule_id="DEEP_SEQUENTIAL_CHAIN",
                severity="minor",
                title="Pipeline is mostly sequential",
                message=(
                    "The critical path is close to the total task count, suggesting limited parallelism. "
                    "This can increase wall-clock time and keep infrastructure running longer."
                ),
                evidence={
                    "total_tasks": total_tasks,
                    "sequential_bottleneck_ratio": sequential_ratio,
                    "parallelism_index": parallelism_index,
                },
                recommendation="Look for independent steps that can run concurrently (fan-out/fan-in pattern).",
            ))

        # ------------------------------------------------------------
        # Rule: multiple tasks but no dependencies detected (when effective graph is empty)
        # ------------------------------------------------------------
        if total_tasks >= 3 and total_edges == 0 and not dep_detected:
            fired.append(FiredRule(
                rule_id="NO_DEPENDENCIES_DETECTED",
                severity="minor",
                title="Multiple tasks but dependencies not detected",
                message=(
                    "Multiple tasks were detected, but no dependency edges were found. "
                    "This can indicate missing dependencies or a framework style not covered by the parser."
                ),
                evidence={
                    "total_tasks": total_tasks,
                    "total_edges": total_edges,
                    "dependency_syntax_detected": dep_detected,
                    "effective_edges_source": edges_block.get("effective_source"),
                },
                recommendation="Prefer explicit dependencies in code or rely on OPOS edges as the canonical graph.",
            ))

        # ------------------------------------------------------------
        # Rule: High XCom signaling
        # ------------------------------------------------------------
        if xcom_total >= 10:
            fired.append(FiredRule(
                rule_id="HIGH_XCOM_INTENSITY",
                severity="minor",
                title="High XCom usage detected",
                message=(
                    "High XCom push/pull frequency can increase orchestration overhead (metadata DB traffic) "
                    "and may hurt performance/energy efficiency."
                ),
                evidence={"xcom_total_signals": xcom_total},
                recommendation="Prefer passing references/paths to data instead of large payloads via XCom.",
            ))

        # ------------------------------------------------------------
        # Rule: Heavy compute + high schedule frequency
        # ------------------------------------------------------------
        if heavy_count > 0 and isinstance(runs_per_year, int) and runs_per_year >= 8760:
            fired.append(FiredRule(
                rule_id="HIGH_FREQUENCY_HEAVY_COMPUTE",
                severity="major",
                title="High-frequency schedule with heavy compute",
                message=(
                    "Heavy-compute tasks combined with an hourly-or-faster schedule can amplify energy/carbon impact."
                ),
                evidence={
                    "heavy_compute_count": heavy_count,
                    "runs_per_year_estimate": runs_per_year,
                },
                recommendation="Consider event-driven triggering, incremental processing, caching, and idempotency.",
            ))

        # ------------------------------------------------------------
        # Rule: Large fan-out
        # ------------------------------------------------------------
        if total_tasks >= 8 and max_fan_out >= 5:
            fired.append(FiredRule(
                rule_id="LARGE_FAN_OUT",
                severity="info",
                title="Large fan-out detected",
                message=(
                    "A high fan-out can increase scheduling overhead and create bursts of parallel activity."
                ),
                evidence={"max_fan_out": max_fan_out, "total_tasks": total_tasks},
                recommendation="Consider pools/queues/concurrency controls to avoid resource spikes.",
            ))

        return {
            "schema_version": RULES_SCHEMA_VERSION,
            "rule_engine": "EcoDesignRuleEngine",
            "rule_count": len(fired),
            "rules": [r.to_dict() for r in fired],
            "disclaimer": (
                "Rules are heuristic guidance based on static structure (and optional OPOS). "
                "They do not measure actual energy usage."
            ),
        }