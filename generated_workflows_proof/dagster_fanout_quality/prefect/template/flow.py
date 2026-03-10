from prefect import flow, task

@task
def extract_source():
    print("task extract_source")

@task
def enrich_region_a():
    print("task enrich_region_a")

@task
def enrich_region_b():
    print("task enrich_region_b")

@task
def quality_gate():
    print("task quality_gate")

@task
def publish_curated():
    print("task publish_curated")

@flow
def dagster_fanout_quality():
    extract_source_future = extract_source.submit()
    enrich_region_a_future = enrich_region_a.submit(wait_for=[extract_source_future])
    enrich_region_b_future = enrich_region_b.submit(wait_for=[extract_source_future])
    quality_gate_future = quality_gate.submit(wait_for=[enrich_region_a_future, enrich_region_b_future])
    publish_curated_future = publish_curated.submit(wait_for=[quality_gate_future])

if __name__ == '__main__':
    dagster_fanout_quality()