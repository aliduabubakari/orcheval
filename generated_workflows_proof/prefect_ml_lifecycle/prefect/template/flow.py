from prefect import flow, task

@task
def prepare_features():
    print("task prepare_features")

@task
def train_model():
    print("task train_model")

@task
def evaluate_model():
    print("task evaluate_model")

@task
def notify_release():
    print("task notify_release")

@flow
def prefect_ml_lifecycle():
    prepare_features_future = prepare_features.submit()
    train_model_future = train_model.submit(wait_for=[prepare_features_future])
    evaluate_model_future = evaluate_model.submit(wait_for=[train_model_future])
    notify_release_future = notify_release.submit(wait_for=[evaluate_model_future])

if __name__ == '__main__':
    prefect_ml_lifecycle()