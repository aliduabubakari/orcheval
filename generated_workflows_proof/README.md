# Generated Workflow Proofs

This directory contains generated artifacts for `samples/opos_e2e_suite`.
Matrix:
- targets: airflow@2.8, prefect@3.x, dagster@1.8, kestra@0.18
- modes: template, llm-assisted (stub provider)

Verification details are captured in `verification_summary.json`.

Regenerate with:
```bash
python scripts/generate_suite_proofs.py
```
