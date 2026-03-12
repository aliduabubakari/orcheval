#!/usr/bin/env python3
"""Optional Typer + Rich interface for orcheval."""

from __future__ import annotations

import json
import shlex
import subprocess
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
        from rich.panel import Panel
        from rich.table import Table
    except Exception:
        raise SystemExit("Typer/Rich CLI is optional. Install with: pip install 'orcheval[cli]'")

    app = typer.Typer(help="Interactive CLI for workflow evaluation and DAG comparison.")
    console = Console()

    def _choose(prompt: str, options: List[str], default: str) -> str:
        opts = [o.lower() for o in options]
        while True:
            raw = typer.prompt(f"{prompt} ({'/'.join(options)})", default=default).strip().lower()
            if raw in opts:
                return raw
            console.print(f"[red]Invalid choice:[/red] {raw}. Allowed: {', '.join(options)}")

    def _split_paths(raw: str) -> List[str]:
        parts = [p.strip() for p in raw.replace(",", " ").split()]
        return [p for p in parts if p]

    def _shell_join(parts: List[str]) -> str:
        return " ".join(shlex.quote(p) for p in parts)

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
        knowledge_pack_mode: str = typer.Option("legacy", help="legacy|pack|auto"),
        knowledge_pack: Optional[str] = typer.Option(None, help="Path to local knowledge-pack JSON"),
        knowledge_pack_version: Optional[str] = typer.Option(None),
        track_carbon: bool = typer.Option(False, help="Enable CodeCarbon during smoke stage"),
        carbon_scale_factor: float = typer.Option(1.0, help="Scale measured carbon values"),
        enable_energy_eval: bool = typer.Option(False, help="Enable service-ready runtime energy evaluation"),
        energy_mode: str = typer.Option("auto", help="auto|sample|synthetic|heuristic"),
        energy_sample_path: List[str] = typer.Option([], help="Sample data file/folder path (repeatable)"),
        energy_max_rows: int = typer.Option(500),
        energy_max_tasks: int = typer.Option(25),
        energy_timeout_s: int = typer.Option(120),
        energy_seed: int = typer.Option(1729),
        energy_execution_adapter: str = typer.Option("representative", help="representative|native|auto"),
        llm_provider: Optional[str] = typer.Option(None, help="openai|anthropic|openrouter|deepinfra|deepseek"),
        llm_model: Optional[str] = typer.Option(None),
        llm_api_key_env: Optional[str] = typer.Option(None),
        llm_base_url: Optional[str] = typer.Option(None),
        llm_timeout_s: int = typer.Option(30),
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
            knowledge_pack_mode=knowledge_pack_mode,
            knowledge_pack=Path(knowledge_pack) if knowledge_pack else None,
            knowledge_pack_version=knowledge_pack_version,
            track_carbon=track_carbon,
            carbon_scale_factor=float(carbon_scale_factor),
            enable_energy_evaluation=bool(enable_energy_eval),
            energy_mode=energy_mode,
            energy_sample_paths=[Path(p) for p in (energy_sample_path or [])],
            energy_max_rows=int(energy_max_rows),
            energy_max_tasks=int(energy_max_tasks),
            energy_timeout_s=int(energy_timeout_s),
            energy_seed=int(energy_seed),
            energy_execution_adapter=energy_execution_adapter,
            llm_provider=llm_provider,
            llm_model=llm_model,
            llm_api_key_env=llm_api_key_env,
            llm_base_url=llm_base_url,
            llm_timeout_s=int(llm_timeout_s),
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
            table.add_row("Energy Mode Used", str(summary.get("energy_mode_used")))
            table.add_row("Energy Data Source", str(summary.get("energy_data_source")))
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

    @app.command("agent")
    def agent_command(
        run: bool = typer.Option(False, "--run", help="Execute the generated command immediately"),
    ) -> None:
        console.print("[bold]Orcheval Command Builder[/bold]")
        mode = _choose(
            "What do you want to run?",
            ["unified", "sat", "pct", "smoke", "compare"],
            "unified",
        )

        is_compare = mode == "compare"
        if is_compare:
            raw_inputs = typer.prompt("Provide workflow file/folder paths (space or comma separated)")
            inputs = _split_paths(raw_inputs)
            if len(inputs) < 1:
                raise typer.BadParameter("At least one input path is required.")
            recursive = typer.confirm("For folder inputs, scan recursively?", default=True)
            save_mode = _choose("Output mode", ["default", "out-dir", "out", "stdout"], "default")

            cmd_parts = ["orcheval-compare"] + inputs
            if not recursive:
                cmd_parts.append("--no-recursive")

            if save_mode == "out-dir":
                out_dir = typer.prompt("Output directory path")
                cmd_parts.extend(["--out-dir", out_dir])
            elif save_mode == "out":
                out = typer.prompt("Exact output JSON path")
                cmd_parts.extend(["--out", out])
            elif save_mode == "stdout":
                cmd_parts.append("--stdout")

            command_text = _shell_join(cmd_parts)
            console.print(Panel(command_text, title="Generated Command", expand=False))

            if run:
                result = subprocess.run(cmd_parts, check=False)
                raise typer.Exit(result.returncode)
            return

        input_mode = _choose("Input type", ["file", "folder"], "file")
        target = typer.prompt("Path to workflow file or folder")
        recursive = True
        if input_mode == "folder":
            recursive = typer.confirm("Scan folder recursively?", default=True)

        cmd_map = {
            "unified": "orcheval-unified",
            "sat": "orcheval-sat",
            "pct": "orcheval-pct",
            "smoke": "orcheval-smoke",
        }
        base_cmd = cmd_map[mode]
        cmd_parts = shlex.split(base_cmd)

        if mode in {"unified", "pct", "smoke"}:
            orchestrator = _choose("Target orchestrator", ["auto", "airflow", "prefect", "dagster"], "auto")
        else:
            orchestrator = "auto"

        save_mode = _choose("Output mode", ["default", "out-dir", "out", "stdout"], "default")

        extra_parts: List[str] = []
        if mode in {"unified", "pct", "smoke"} and orchestrator != "auto":
            extra_parts.extend(["--orchestrator", orchestrator])

        if mode == "unified":
            dry_run = typer.confirm("Use dry-run ephemeral venv?", default=True)
            if dry_run:
                extra_parts.append("--dry-run-ephemeral-venv")
                use_constraint = typer.confirm("Use pip constraints file?", default=False)
                if use_constraint:
                    cpath = typer.prompt("Constraints file path")
                    extra_parts.extend(["--dry-run-pip-constraint", cpath])
            sat_only = typer.confirm("SAT only (skip PCT/smoke and use SAT analyzer command)?", default=False)
            if sat_only:
                cmd_parts = ["orcheval-sat"]
                extra_parts = []
            else:
                include_ctx = typer.confirm("Include generation context blocks in JSON?", default=False)
                if include_ctx:
                    extra_parts.append("--include-generation-context")
                kp_mode = _choose("Knowledge-pack mode", ["legacy", "pack", "auto"], "legacy")
                extra_parts.extend(["--knowledge-pack-mode", kp_mode])
                if kp_mode in {"pack", "auto"}:
                    kp_path = typer.prompt("Knowledge-pack path (optional)", default="").strip()
                    if kp_path:
                        extra_parts.extend(["--knowledge-pack", kp_path])
                    kp_ver = typer.prompt("Knowledge-pack version (optional)", default="").strip()
                    if kp_ver:
                        extra_parts.extend(["--knowledge-pack-version", kp_ver])

            if not sat_only:
                enable_energy = typer.confirm(
                    "Enable runtime energy evaluation (sample/synthetic/heuristic fallback)?",
                    default=False,
                )
                if enable_energy:
                    extra_parts.append("--enable-energy-eval")
                    emode = _choose("Energy mode", ["auto", "sample", "synthetic", "heuristic"], "auto")
                    extra_parts.extend(["--energy-mode", emode])
                    eadapter = _choose("Runtime adapter", ["representative", "native", "auto"], "representative")
                    extra_parts.extend(["--energy-execution-adapter", eadapter])

                    if emode in {"auto", "sample"}:
                        sample_input = typer.prompt(
                            "Sample data path(s) (optional, space/comma separated)",
                            default="",
                        ).strip()
                        for p in _split_paths(sample_input):
                            extra_parts.extend(["--energy-sample-path", p])

                    if emode in {"auto", "synthetic"}:
                        use_llm = typer.confirm("Configure LLM provider for synthetic mode?", default=False)
                        if use_llm:
                            provider = _choose(
                                "LLM provider",
                                ["openai", "anthropic", "openrouter", "deepinfra", "deepseek"],
                                "openai",
                            )
                            model = typer.prompt("LLM model")
                            extra_parts.extend(["--llm-provider", provider, "--llm-model", model])

                            api_env = typer.prompt("API key env var name (optional)", default="").strip()
                            if api_env:
                                extra_parts.extend(["--llm-api-key-env", api_env])
                            base_url = typer.prompt("Custom base URL (optional)", default="").strip()
                            if base_url:
                                extra_parts.extend(["--llm-base-url", base_url])

                    max_rows = typer.prompt("Energy max rows", default="500").strip()
                    max_tasks = typer.prompt("Energy max tasks", default="25").strip()
                    timeout_s = typer.prompt("Energy timeout seconds", default="120").strip()
                    seed = typer.prompt("Energy deterministic seed", default="1729").strip()
                    extra_parts.extend(
                        [
                            "--energy-max-rows",
                            max_rows,
                            "--energy-max-tasks",
                            max_tasks,
                            "--energy-timeout-s",
                            timeout_s,
                            "--energy-seed",
                            seed,
                        ]
                    )

        if mode == "pct":
            kp_mode = _choose("Knowledge-pack mode", ["legacy", "pack", "auto"], "legacy")
            extra_parts.extend(["--knowledge-pack-mode", kp_mode])
            if kp_mode in {"pack", "auto"}:
                kp_path = typer.prompt("Knowledge-pack path (optional)", default="").strip()
                if kp_path:
                    extra_parts.extend(["--knowledge-pack", kp_path])
                kp_ver = typer.prompt("Knowledge-pack version (optional)", default="").strip()
                if kp_ver:
                    extra_parts.extend(["--knowledge-pack-version", kp_ver])

        if mode == "smoke":
            track_carbon = typer.confirm("Enable carbon tracking?", default=False)
            if track_carbon:
                extra_parts.append("--track-carbon")
                scale = typer.prompt("Carbon scale factor", default="1.0")
                extra_parts.extend(["--carbon-scale-factor", scale])

        if save_mode == "out-dir":
            out_dir = typer.prompt("Output directory path")
            extra_parts.extend(["--out-dir", out_dir])
        elif save_mode == "out":
            out = typer.prompt("Exact output JSON path")
            extra_parts.extend(["--out", out])
        elif save_mode == "stdout":
            extra_parts.append("--stdout")

        if input_mode == "file":
            full_parts = cmd_parts + [target] + extra_parts
            command_text = _shell_join(full_parts)
            run_parts = full_parts
        else:
            depth_expr = "" if recursive else "-maxdepth 1 "
            folder_q = shlex.quote(target)
            prefix = _shell_join(cmd_parts)
            suffix = _shell_join(extra_parts)
            base_line = f'{prefix} "$f" {suffix}'.strip()
            command_text = (
                f"find {folder_q} {depth_expr}-type f -name '*.py' -print0 | "
                f"while IFS= read -r -d '' f; do {base_line}; done"
            )
            run_parts = []

        console.print(Panel(command_text, title="Generated Command", expand=False))

        if run:
            if run_parts:
                result = subprocess.run(run_parts, check=False)
                raise typer.Exit(result.returncode)
            result = subprocess.run(command_text, shell=True, check=False)
            raise typer.Exit(result.returncode)

    app()


if __name__ == "__main__":
    main()
