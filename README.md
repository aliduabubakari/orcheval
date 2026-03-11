# orcheval

Static evaluation toolkit for generated workflow code (Airflow, Prefect, Dagster, plus YAML/Kestra comparison support).

It is designed to work well with `pipeline-codegen` outputs and produce JSON artifacts you can use in benchmarks and CI.

## What it does

- SAT static analysis (code quality, correctness, maintainability, robustness signals)
- Import smoke test in an orchestrator runtime
- PCT platform compliance checks
- Unified JSON report with scores, gate outcomes, and error events
- Optional dry-run ephemeral venv setup (create -> install -> run -> discard)
- DAG comparator for 2+ workflow files (including mixed orchestrators)
- Optional carbon tracking during smoke import stage, including scaling metadata

## Installation

Base:

```bash
pip install "orcheval[yaml] @ git+https://github.com/aliduabubakari/orcheval.git"
```

With carbon support:

```bash
pip install "orcheval[yaml,energy] @ git+https://github.com/aliduabubakari/orcheval.git"
```

With optional interactive Typer+Rich CLI:

```bash
pip install "orcheval[yaml,cli] @ git+https://github.com/aliduabubakari/orcheval.git"
```

With SAT tooling enabled (`flake8`, `pylint`, `radon`, `bandit`):

```bash
pip install "orcheval[yaml,sat] @ git+https://github.com/aliduabubakari/orcheval.git"
```

## Unified Evaluation

### Basic

```bash
orcheval-unified path/to/generated_workflow.py
```

By default, unified JSON + smoke/PCT artifacts are written to:

`<workflow_dir>/orcheval_reports/`

Use `--stdout` to print JSON instead.

By default, `run_context` and `generation` sections are omitted unless populated.
Use `--include-generation-context` to always include them.

### Select orchestrator manually

```bash
orcheval-unified path/to/pipeline.py --orchestrator airflow
```

### Save unified JSON to a specific directory or file

```bash
orcheval-unified path/to/pipeline.py --out-dir ./reports
orcheval-unified path/to/pipeline.py --out ./reports/my_run.json
```

### Use a dedicated reports directory for all artifacts

```bash
orcheval-unified path/to/pipeline.py --reports-dir ./reports/all_artifacts
```

## Dry-Run Ephemeral Venv

Creates a temporary virtualenv, installs packages, runs smoke+PCT, then discards the env.

```bash
orcheval-unified path/to/pipeline.py \
  --orchestrator airflow \
  --dry-run-ephemeral-venv
```

Add extra packages if your generated code needs them:

```bash
orcheval-unified path/to/pipeline.py \
  --orchestrator prefect \
  --dry-run-ephemeral-venv \
  --dry-run-extra-package pandas==2.2.3 \
  --dry-run-extra-package requests
```

Use a pip constraints file (recommended for Airflow):

```bash
orcheval-unified path/to/pipeline.py \
  --orchestrator airflow \
  --dry-run-ephemeral-venv \
  --dry-run-pip-constraint ./constraints-airflow-2.8.txt
```

Notes:

- Dry-run setup metadata (venv path, install commands, return codes, output tails, cleanup status) is recorded under:
  - `metadata.evaluation_context.dry_run_env`
- If setup fails, the unified report records the failure as a blocking environment error.
- To keep reports smaller, `pip freeze` is not captured by default. Enable with `--dry-run-capture-freeze`.

## Carbon Tracking + Scaling

Smoke import stage can capture CodeCarbon measurements and optionally scale them.

```bash
orcheval-unified path/to/pipeline.py \
  --orchestrator dagster \
  --dry-run-ephemeral-venv \
  --track-carbon \
  --carbon-country-iso ITA \
  --carbon-scale-factor 50
```

The smoke payload includes:

- measured values (`measured_energy_consumed_kwh`, `measured_emissions_kgco2eq`)
- scaled values (`scaled_energy_consumed_kwh`, `scaled_emissions_kgco2eq`)
- scaling metadata (`scale_factor`, `scaling_applied`, `scale_note`)

## Comparator (2+ DAGs)

Compare structural similarity across two or more workflows, including mixed orchestrators.
No baseline DAG is required.

```bash
orcheval-compare dag_a.py dag_b.py
orcheval-compare dag_a.py dag_b.py dag_c.yaml --out-dir ./reports/comparisons
orcheval-compare ./generated_workflows_folder
orcheval-compare ./set_a ./set_b --no-recursive
```

Comparator output includes:

- per-input task/edge extraction summary
- pairwise overlap and Jaccard scores
- aggregate common/union task and edge counts

Default output location:

`<first_input_dir>/orcheval_reports/comparisons/`

## Optional Interactive CLI (Typer + Rich)

Only used when you call `orcheval` explicitly.

```bash
orcheval evaluate path/to/pipeline.py --dry-run-ephemeral-venv
orcheval compare dag_a.py dag_b.py dag_c.py
```

## Standalone Smoke / PCT Commands

```bash
orcheval-smoke path/to/generated_workflow.py --orchestrator airflow --out smoke.json
orcheval-pct path/to/generated_workflow.py --orchestrator auto --out pct.json
```

## Working with pipeline-codegen outputs

Typical generated layout:

```text
generated_workflows/<pipeline>/<orchestrator>/<mode>/<entrypoint>
```

Point `orcheval-unified` at the generated entrypoint (for example `pipeline.py`, `flow.py`, `definitions.py`).

If `artifacts.json` includes target version/dependency hints, dry-run env setup will use them when installing orchestrator/runtime packages.

## Roadmap

- dashboard/report visualization layer (planned)
- richer semantic comparators and trend summaries
