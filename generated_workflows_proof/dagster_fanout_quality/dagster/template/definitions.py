from dagster import op, job

@op
def extract_source():
    print("task extract_source")
    return "extract_source"

@op
def enrich_region_a(upstream_0):
    print("task enrich_region_a")
    return "enrich_region_a"

@op
def enrich_region_b(upstream_0):
    print("task enrich_region_b")
    return "enrich_region_b"

@op
def quality_gate(upstream_0, upstream_1):
    print("task quality_gate")
    return "quality_gate"

@op
def publish_curated(upstream_0):
    print("task publish_curated")
    return "publish_curated"

@job
def dagster_fanout_quality():
    extract_source_result = extract_source()
    enrich_region_a_result = enrich_region_a(extract_source_result)
    enrich_region_b_result = enrich_region_b(extract_source_result)
    quality_gate_result = quality_gate(enrich_region_a_result, enrich_region_b_result)
    publish_curated_result = publish_curated(quality_gate_result)