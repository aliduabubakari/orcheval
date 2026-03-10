from dagster import op, job

@op
def prepare_features():
    print("task prepare_features")
    return "prepare_features"

@op
def train_model(upstream_0):
    print("task train_model")
    return "train_model"

@op
def evaluate_model(upstream_0):
    print("task evaluate_model")
    return "evaluate_model"

@op
def notify_release(upstream_0):
    print("task notify_release")
    return "notify_release"

@job
def prefect_ml_lifecycle():
    prepare_features_result = prepare_features()
    train_model_result = train_model(prepare_features_result)
    evaluate_model_result = evaluate_model(train_model_result)
    notify_release_result = notify_release(evaluate_model_result)