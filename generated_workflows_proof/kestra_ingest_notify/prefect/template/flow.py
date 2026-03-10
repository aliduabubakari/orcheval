from prefect import flow, task

@task
def poll_events():
    print("task poll_events")

@task
def stage_payload():
    print("task stage_payload")

@task
def merge_events():
    print("task merge_events")

@task
def notify_ops():
    print("task notify_ops")

@flow
def kestra_ingest_notify():
    poll_events_future = poll_events.submit()
    stage_payload_future = stage_payload.submit(wait_for=[poll_events_future])
    merge_events_future = merge_events.submit(wait_for=[stage_payload_future])
    notify_ops_future = notify_ops.submit(wait_for=[merge_events_future])

if __name__ == '__main__':
    kestra_ingest_notify()