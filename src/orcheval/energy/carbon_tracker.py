#!/usr/bin/env python3
"""
Carbon/Energy Tracker Wrapper (v1)
==================================

This wraps CodeCarbon in a "best-effort" way for use inside subprocess smoke tests.

Goal:
- Measure *import/parse overhead* energy (exec_module stage) in the orchestrator venv
- Never break smoke tests if codecarbon isn't installed or fails

Usage pattern (later, inside import_smoke_test_cli.py):

  from evaluators.energy.carbon_tracker import try_start_tracker, try_stop_tracker

  tracker = try_start_tracker(country_iso_code="GBR")
  spec.loader.exec_module(module)
  energy_payload = try_stop_tracker(tracker)

  payload["energy_profile"] = energy_payload
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional
import platform
import time
import sys
import traceback


@dataclass
class TrackerHandle:
    impl: Any
    started_at: float
    config: Dict[str, Any]


def try_start_tracker(
    *,
    country_iso_code: Optional[str] = None,
    measure_power_secs: int = 1,
    project_name: str = "workflow_smoke_import",
    extra: Optional[Dict[str, Any]] = None,
) -> Optional[TrackerHandle]:
    """
    Start a CodeCarbon tracker if available.

    Returns:
      - TrackerHandle if started
      - None if unavailable or failed
    """
    try:
        # Delayed import (optional dependency)
        from codecarbon import OfflineEmissionsTracker  # type: ignore
    except Exception:
        return None

    cfg = {
        "country_iso_code": country_iso_code,
        "measure_power_secs": int(measure_power_secs),
        "project_name": project_name,
    }
    if extra and isinstance(extra, dict):
        cfg.update(extra)

    try:
        # OfflineEmissionsTracker is good for CI/offline environments.
        # We avoid writing to file if possible.
        tracker = OfflineEmissionsTracker(
            country_iso_code=country_iso_code,
            measure_power_secs=int(measure_power_secs),
            project_name=project_name,
            log_level="error",
            save_to_file=False,
        )
        tracker.start()
        return TrackerHandle(impl=tracker, started_at=time.time(), config=cfg)
    except Exception:
        return None


def try_stop_tracker(handle: Optional[TrackerHandle]) -> Dict[str, Any]:
    """
    Stop tracker and return a JSON-serializable payload.

    If handle is None, returns available=False payload.
    """
    base = {
        "tool": "codecarbon",
        "available": bool(handle is not None),
        "python": sys.executable,
        "python_version": platform.python_version(),
        "platform": platform.platform(),
        "measurement_scope": "import_exec_only",
        "disclaimer": (
            "This measurement covers only the energy/carbon during import/exec of the workflow module "
            "(DAG/flow/job parse and registration). It does not include task runtime energy."
        ),
    }

    if handle is None:
        base["status"] = "not_started"
        return base

    try:
        result = handle.impl.stop()
        duration_s = max(0.0, time.time() - float(handle.started_at))

        # CodeCarbon return values differ across versions.
        # Common cases:
        # - stop() returns emissions (float)
        # - tracker has attributes: energy_consumed, emissions, duration, etc.
        energy_kwh = getattr(handle.impl, "energy_consumed", None)
        emissions_kg = getattr(handle.impl, "emissions", None)

        payload = {
            **base,
            "status": "ok",
            "duration_s": duration_s,
            "energy_consumed_kwh": float(energy_kwh) if energy_kwh is not None else None,
            "emissions_kgco2eq": float(emissions_kg) if emissions_kg is not None else None,
            "stop_return_value": result if isinstance(result, (int, float, str, type(None))) else str(result),
            "config": handle.config,
        }
        return payload

    except Exception as e:
        return {
            **base,
            "status": "error",
            "error_type": type(e).__name__,
            "message": str(e),
            "traceback": traceback.format_exc(),
            "config": handle.config,
        }