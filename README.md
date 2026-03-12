# orcheval

Static evaluation toolkit for generated workflow code (Airflow, Prefect, Dagster, plus YAML/Kestra comparison support).

`orcheval` is designed for `pipeline-codegen` outputs and emits JSON artifacts for CI, benchmarking, and quality gates.

For architecture and implementation details, see [Docs/technical.md](/Users/abubakarialidu/orcheval/Docs/technical.md).

## Features

- SAT static analysis
- Import smoke tests in orchestrator runtime environments
- PCT platform compliance checks
- Unified report (SAT + smoke + PCT + optional energy)
- Dry-run ephemeral venv (create -> install -> run -> discard)
- DAG comparator for 2+ workflows (including mixed orchestrators)
- Optional carbon tracking and runtime energy evaluation
- Optional deterministic knowledge-pack mode (`legacy|pack|auto`)

## Installation

Base:

```bash
pip install "orcheval[yaml] @ git+https://github.com/aliduabubakari/orcheval.git"
```

With carbon support:

```bash
pip install "orcheval[yaml,energy] @ git+https://github.com/aliduabubakari/orcheval.git"
```

With Typer + Rich CLI:

```bash
pip install "orcheval[yaml,cli] @ git+https://github.com/aliduabubakari/orcheval.git"
```

With SAT tooling (`flake8`, `pylint`, `radon`, `bandit`):

```bash
pip install "orcheval[yaml,sat] @ git+https://github.com/aliduabubakari/orcheval.git"
```

## Quick Start

### Unified evaluation

```bash
orcheval-unified path/to/generated_workflow.py
```

Defaults:

- Unified + artifacts written to `<workflow_dir>/orcheval_reports/`
- `run_context` / `generation` sections are omitted unless meaningful

Useful output controls:

```bash
orcheval-unified path/to/pipeline.py --out-dir ./reports
orcheval-unified path/to/pipeline.py --out ./reports/my_run.json
orcheval-unified path/to/pipeline.py --stdout
```

### Manual orchestrator selection

```bash
orcheval-unified path/to/pipeline.py --orchestrator airflow
```

### Dedicated artifact directory

```bash
orcheval-unified path/to/pipeline.py --reports-dir ./reports/all_artifacts
```

## Knowledge-Pack Mode

Default is strict compatibility mode:

```bash
orcheval-unified path/to/pipeline.py --knowledge-pack-mode legacy
```

Pack-driven deterministic mode:

```bash
orcheval-unified path/to/pipeline.py \
  --knowledge-pack-mode pack \
  --orchestrator-version 2.8.3 \
  --knowledge-pack ./packs/default_pack_v1.json \
  --knowledge-pack-version 2026.03.0
```

Modes:

- `legacy`: built-in evaluator behavior (default)
- `pack`: use knowledge-pack parameters
- `auto`: try pack resolution, then conservative fallback

If `--orchestrator-version` is omitted, `orcheval` attempts runtime detection.
If an exact version is unavailable in the pack, it uses the closest version in the same major line and emits warnings in report metadata.

## Dry-Run Ephemeral Venv

```bash
orcheval-unified path/to/pipeline.py \
  --orchestrator airflow \
  --dry-run-ephemeral-venv
```

With extra runtime dependencies:

```bash
orcheval-unified path/to/pipeline.py \
  --orchestrator prefect \
  --dry-run-ephemeral-venv \
  --dry-run-extra-package pandas==2.2.3 \
  --dry-run-extra-package requests
```

With pip constraints (recommended for Airflow):

```bash
orcheval-unified path/to/pipeline.py \
  --orchestrator airflow \
  --dry-run-ephemeral-venv \
  --dry-run-pip-constraint ./constraints-airflow-2.8.txt
```

## Energy + Carbon

### Import-stage carbon tracking

```bash
orcheval-unified path/to/pipeline.py \
  --orchestrator dagster \
  --dry-run-ephemeral-venv \
  --track-carbon \
  --carbon-country-iso ITA \
  --carbon-scale-factor 50
```

### Runtime energy evaluation

```bash
orcheval-unified path/to/pipeline.py \
  --enable-energy-eval \
  --energy-mode auto
```

Sample mode:

```bash
orcheval-unified path/to/pipeline.py \
  --enable-energy-eval \
  --energy-mode sample \
  --energy-sample-path ./samples/
```

Synthetic mode:

```bash
orcheval-unified path/to/pipeline.py \
  --enable-energy-eval \
  --energy-mode synthetic \
  --llm-provider openrouter \
  --llm-model openai/gpt-4.1-mini \
  --llm-api-key-env OPENROUTER_API_KEY
```

Common runtime controls:

- `--energy-max-rows`
- `--energy-max-tasks`
- `--energy-timeout-s`
- `--energy-seed`
- `--energy-execution-adapter representative|native|auto`

## Comparator

```bash
orcheval-compare dag_a.py dag_b.py
orcheval-compare dag_a.py dag_b.py dag_c.yaml --out-dir ./reports/comparisons
orcheval-compare ./generated_workflows_folder
orcheval-compare ./set_a ./set_b --no-recursive
```

## Interactive CLI

```bash
orcheval evaluate path/to/pipeline.py --dry-run-ephemeral-venv
orcheval compare dag_a.py dag_b.py dag_c.py
orcheval agent
```

`orcheval agent` is a guided command builder and can include knowledge-pack and energy options.

## Standalone Commands

```bash
orcheval-sat path/to/generated_workflow.py --out-dir ./reports/sat
orcheval-smoke path/to/generated_workflow.py --orchestrator airflow --out smoke.json
orcheval-pct path/to/generated_workflow.py --orchestrator auto --out pct.json
orcheval-pct path/to/generated_workflow.py --knowledge-pack-mode pack --orchestrator-version 2.8.3 --knowledge-pack ./packs/default_pack_v1.json
```

## Knowledge-Pack Update Utility

Create candidate pack + review report (offline deterministic flow):

```bash
orcheval-knowledge-pack-update \
  --base-pack ./packs/default_pack_v1.json \
  --snapshot ./curated_snapshot.json \
  --out-dir ./orcheval_reports/knowledge_packs
```

## Generated Workflow Layout

Typical `pipeline-codegen` layout:

```text
generated_workflows/<pipeline>/<orchestrator>/<mode>/<entrypoint>
```

Point `orcheval-unified` at the generated entrypoint (`pipeline.py`, `flow.py`, `definitions.py`, etc.).
