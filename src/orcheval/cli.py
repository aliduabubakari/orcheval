#!/usr/bin/env python3
"""Optional Typer + Rich interface for orcheval."""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import List, Optional

from .base_evaluator import Orchestrator
from .dag_comparator import compare_workflows, resolve_workflow_inputs
from .unified_evaluator import UnifiedEvaluator


def main() -> None:
    try:
        import typer
        from rich.console import Console
        from rich.table import Table
    except Exception:
        raise SystemExit("Typer/Rich CLI is optional. Install with: pip install 'orcheval[cli]'")

    app = typer.Typer(help="Interactive CLI for workflow evaluation and DAG comparison.")
    console = Console()

    @app.command("evaluate")
    def evaluate_command(
        file: str,
        orchestrator: str = typer.Option("auto", help="auto|airflow|prefect|dagster"),
        alpha: float = typer.Option(0.5),
        yaml_valid: str = typer.Option("none", help="true|false|none"),
        reports_dir: Optional[str] = typer.Option(None, help="Report artifact directory"),
        out: Optional[str] = typer.Option(None, help="Write unified JSON to exact file"),
        stdout: bool = typer.Option(False, help="Print full JSON to stdout"),
        dry_run_ephemeral_venv: bool = typer.Option(False, help="Create temporary venv for dry-run checks"),
        dry_run_extra_package: List[str] = typer.Option([], help="Extra pip package to install in dry-run venv"),
        dry_run_pip_constraint: Optional[str] = typer.Option(None, help="Pip constraints file path for dry-run installs"),
        dry_run_capture_freeze: bool = typer.Option(False, help="Capture pip freeze output in dry-run metadata"),
        dry_run_log_tail_chars: int = typer.Option(1200, help="Per-step stdout/stderr tail length in dry-run metadata"),
        include_generation_context: bool = typer.Option(False, help="Include run_context/generation blocks in unified JSON"),
        track_carbon: bool = typer.Option(False, help="Enable CodeCarbon during smoke stage"),
        carbon_scale_factor: float = typer.Option(1.0, help="Scale measured carbon values"),
    ) -> None:
        yv = None if yaml_valid == "none" else (yaml_valid == "true")
        orch = None if orchestrator == "auto" else Orchestrator(orchestrator)

        evaluator = UnifiedEvaluator(
            alpha=alpha,
            yaml_valid=yv,
            artifacts_dir=Path(reports_dir) if reports_dir else None,
            dry_run_ephemeral_venv=dry_run_ephemeral_venv,
            dry_run_extra_packages=list(dry_run_extra_package or []),
            dry_run_pip_constraint=Path(dry_run_pip_constraint) if dry_run_pip_constraint else None,
            dry_run_capture_freeze=bool(dry_run_capture_freeze),
            dry_run_log_tail_chars=int(dry_run_log_tail_chars),
            include_generation_context=bool(include_generation_context),
            track_carbon=track_carbon,
            carbon_scale_factor=float(carbon_scale_factor),
        )

        payload = evaluator.evaluate(Path(file), orchestrator=orch)
        txt = json.dumps(payload, indent=2, default=str)

        if out:
            out_path = Path(out)
            out_path.parent.mkdir(parents=True, exist_ok=True)
            out_path.write_text(txt, encoding="utf-8")
            console.print(f"Wrote: {out_path}")
        elif stdout:
            print(txt)
        else:
            root = Path(reports_dir) if reports_dir else (Path(file).parent / "orcheval_reports")
            root.mkdir(parents=True, exist_ok=True)
            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            out_path = root / f"{Path(file).stem}.unified_{payload.get('orchestrator', 'unknown')}_{ts}.json"
            out_path.write_text(txt, encoding="utf-8")
            console.print(f"Wrote: {out_path}")

        if not stdout:
            summary = payload.get("summary", {})
            table = Table(title="Unified Evaluation Summary")
            table.add_column("Metric")
            table.add_column("Value", justify="right")
            table.add_row("Orchestrator", str(payload.get("orchestrator")))
            table.add_row("Static Score", str(summary.get("static_score")))
            table.add_row("Compliance Score", str(summary.get("compliance_score")))
            table.add_row("Combined Score", str(summary.get("combined_score")))
            table.add_row("Executable", str(summary.get("passed_executable")))
            table.add_row("Issues (critical)", str((summary.get("issues") or {}).get("critical")))
            console.print(table)

    @app.command("compare")
    def compare_command(
        files: List[str],
        no_recursive: bool = typer.Option(False, help="For folder inputs, only scan top-level files"),
        out: Optional[str] = typer.Option(None, help="Write JSON to exact file"),
        out_dir: Optional[str] = typer.Option(None, help="Write JSON into this directory"),
        stdout: bool = typer.Option(False, help="Print full JSON to stdout"),
    ) -> None:
        paths = resolve_workflow_inputs([Path(p) for p in files], recursive=not bool(no_recursive))
        if len(paths) < 2:
            raise typer.BadParameter("At least two files are required.")

        payload = compare_workflows(paths)
        txt = json.dumps(payload, indent=2, default=str)

        if out:
            out_path = Path(out)
            out_path.parent.mkdir(parents=True, exist_ok=True)
            out_path.write_text(txt, encoding="utf-8")
            console.print(f"Wrote: {out_path}")
        elif stdout:
            print(txt)
        else:
            root = Path(out_dir) if out_dir else (paths[0].parent / "orcheval_reports" / "comparisons")
            root.mkdir(parents=True, exist_ok=True)
            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            out_path = root / f"dag_compare_{len(paths)}files_{ts}.json"
            out_path.write_text(txt, encoding="utf-8")
            console.print(f"Wrote: {out_path}")

        if not stdout:
            pairs = payload.get("pairwise", [])
            table = Table(title="DAG Comparison")
            table.add_column("Pair")
            table.add_column("Score", justify="right")
            table.add_column("Task J", justify="right")
            table.add_column("Edge J", justify="right")
            for row in pairs:
                pair_name = f"{Path(row.get('left_file', '')).name} vs {Path(row.get('right_file', '')).name}"
                table.add_row(pair_name, str(row.get("similarity_score")), str(row.get("task_jaccard")), str(row.get("edge_jaccard")))
            console.print(table)

    app()


if __name__ == "__main__":
    main()
