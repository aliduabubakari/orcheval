from dagster import op, job

@op
def poll_events():
    print("task poll_events")
    return "poll_events"

@op
def stage_payload(upstream_0):
    print("task stage_payload")
    return "stage_payload"

@op
def merge_events(upstream_0):
    print("task merge_events")
    return "merge_events"

@op
def notify_ops(upstream_0):
    print("task notify_ops")
    return "notify_ops"

@job
def kestra_ingest_notify():
    poll_events_result = poll_events()
    stage_payload_result = stage_payload(poll_events_result)
    merge_events_result = merge_events(stage_payload_result)
    notify_ops_result = notify_ops(merge_events_result)