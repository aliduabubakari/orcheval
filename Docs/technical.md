# Technical Design: orcheval

## Overview

`orcheval` is a multi-stage workflow evaluator with a CLI-first architecture. It combines static analysis, import/loadability checks, platform compliance checks, optional runtime energy estimation, and comparator utilities into a unified JSON report.

Primary design goals:

- deterministic outputs for repeatable benchmarking
- resilient execution with explicit failure diagnostics
- isolated runtime checks via subprocess and optional ephemeral venv
- conservative fallback behavior when runtime or dependency context is incomplete

## System Architecture

Core modules:

- `src/orcheval/unified_evaluator.py`: orchestration layer combining SAT, smoke, PCT, and energy blocks
- `src/orcheval/enhanced_static_analyzer.py`: SAT static scoring
- `src/orcheval/import_smoke_test_cli.py`: compile/import smoke stage
- `src/orcheval/platform_compliance/*`: orchestrator-specific PCT implementations
- `src/orcheval/energy/*`: structural analysis, design rules, runtime energy evaluation, runtime runner
- `src/orcheval/subprocess_json_runner.py`: guaranteed JSON subprocess wrapper

Execution model:

1. SAT runs in controller environment.
2. Smoke/PCT run in orchestrator runtime Python (configured venv or dry-run ephemeral venv).
3. Energy evaluation runs with mode precedence (`sample -> synthetic -> heuristic` in `auto`) and explicit fallback tracing.
4. Unified payload is assembled with summary, raw blocks, and evaluation context metadata.

## Knowledge-Pack Hybrid Model

### Motivation

Historically, evaluator checks were mostly hard-coded. Version-specific behavior can drift over time and become brittle. Knowledge packs provide deterministic parameterization without moving invariant safety logic out of code.

### Runtime Components

- `src/orcheval/knowledge_pack.py`
- bundled pack: `src/orcheval/knowledge_packs/default_pack_v1.json`

Resolver inputs:

- orchestrator
- detected orchestrator version or explicit override (`--orchestrator-version`)
- mode (`legacy|pack|auto`)
- optional explicit pack path/version

Resolver outputs:

- status (`legacy_bypass`, `resolved`, `unknown_version_conservative`, etc.)
- applied flag
- selected capability profile
- rule parameter map
- uncertainty diagnostics
- provenance metadata (`pack_id`, `pack_version`, `pack_path`)

Behavior:

- `legacy`: bypass pack usage entirely
- `pack`: require pack resolution; unknown versions degrade conservatively
- `auto`: attempt pack; fall back to conservative core behavior when unresolved
- approximate match policy: when exact version is unavailable, select closest version in the same major line and emit warnings

No runtime web lookup is performed.

### Provenance Contract

When a pack-aware check resolves parameters through `resolve_rule_value`, provenance metadata is attached:

- `check_id`
- `rule_source` (`core` or `pack`)
- `capability_refs`
- `knowledge_pack_applied`
- `resolution_status`
- `pack_id`, `pack_version`

This is emitted in check-level details (for PCT gates/dimensions and energy rule evidence).

## PCT Technical Implementation

Entry points:

- `src/orcheval/platform_compliance/pct_base.py`
- orchestrator testers:
  - `airflow_compliance.py`
  - `prefect_compliance.py`
  - `dagster_compliance.py`

Key decisions:

- Gate model remains unchanged: critical gate failure forces `PCT=0`.
- Invariant safety stays in code:
  - parsing/syntax gating
  - safe exception handling per gate/dimension
  - crash-containment behavior
- Version-sensitive pattern lists moved to pack parameters for:
  - minimum structure detection
  - dry-run/executability signals

CLI integration:

- `orcheval-pct` accepts:
  - `--knowledge-pack-mode`
  - `--knowledge-pack`
  - `--knowledge-pack-version`

## Energy Technical Implementation

Entry points:

- `src/orcheval/energy/runtime_evaluation.py`
- `src/orcheval/energy/design_rules.py`
- `src/orcheval/energy/runtime_runner_cli.py`

Key decisions:

- Fallback chain unchanged (`sample -> synthetic -> heuristic` in auto mode).
- Structural extraction and runtime execution safety remain code-owned.
- Rule thresholds and toggles are pack-parameterized in `EcoDesignRuleEngine`.
- Pack context and uncertainty are emitted in `energy_evaluation.metadata.knowledge_pack`.

Runtime adapters:

- `representative` (default)
- `native` (opt-in)
- `auto` (native then representative fallback)

## Unified Evaluator Integration

`UnifiedEvaluator` now propagates knowledge-pack settings to:

- PCT subprocess invocation
- energy evaluation request
- metadata evaluation context

New flags:

- `--knowledge-pack-mode legacy|pack|auto`
- `--knowledge-pack PATH`
- `--knowledge-pack-version VERSION`
- `--orchestrator-version VERSION`

Default remains `legacy` to preserve strict compatibility.

## Offline Knowledge-Pack Update Pipeline

Entry point:

- `src/orcheval/knowledge_pack_updater.py`

Purpose:

- produce deterministic candidate pack artifacts
- validate schema
- emit review report with changed paths and checksums

This workflow is explicitly offline and curated. Runtime evaluators consume approved local pack artifacts only.

## Reporting and Schema Notes

Important metadata blocks:

- unified: `metadata.evaluation_context.*`
- energy: `energy_evaluation.metadata.knowledge_pack`
- uncertainty: emitted when pack/orchestrator version resolution is incomplete

Generation/context payloads remain optional and controlled by `--include-generation-context`.

## Testing Strategy

Primary tests:

- `tests/test_runtime_evaluation.py`
- `tests/test_knowledge_pack_runtime.py`

Coverage includes:

- deterministic pack resolution
- conservative unknown-version degrade
- provenance presence on pack-aware checks
- energy metadata and pack uncertainty output
- regression safety when energy not enabled

## Backward Compatibility

- Default mode (`legacy`) preserves previous behavior.
- Existing SAT flow remains unchanged (SAT migration to pack-driven logic is deferred).
- Existing smoke/PCT/energy command usage remains valid without pack flags.

## Future Work

- SAT hybrid migration to knowledge-pack parameterization
- stronger version range semantics in pack resolver
- pack signing and integrity verification enhancements
- dashboard and longitudinal trend visualization layer
