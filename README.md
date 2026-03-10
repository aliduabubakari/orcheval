# orcheval

## Install (git)
pip install "orcheval[yaml] @ git+https://github.com/aliduabubakari/orcheval.git"

## Install with energy measurement support (CodeCarbon)
pip install "orcheval[yaml,energy] @ git+https://github.com/aliduabubakari/orcheval.git"

## Run unified evaluation
orcheval-unified path/to/generated_workflow.py --stdout

## Run smoke test only
orcheval-smoke path/to/generated_workflow.py --orchestrator airflow --out smoke.json

## Run PCT only
orcheval-pct path/to/generated_workflow.py --orchestrator auto --out pct.json