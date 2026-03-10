from dagster import op, job

@op
def ingest_api():
    print("task ingest_api")
    return "ingest_api"

@op
def normalize_raw(upstream_0):
    print("task normalize_raw")
    return "normalize_raw"

@op
def load_warehouse(upstream_0):
    print("task load_warehouse")
    return "load_warehouse"

@job
def airflow_ops_mix():
    ingest_api_result = ingest_api()
    normalize_raw_result = normalize_raw(ingest_api_result)
    load_warehouse_result = load_warehouse(normalize_raw_result)