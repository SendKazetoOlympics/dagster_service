from dagster import asset, define_asset_job
from ultralytics import YOLO

@asset(deps=["training_data", "yaml"])
def trained_yolo_model():
    model = YOLO("./data/models/yolov8l-cls.pt")
    result = model.train(data='data/annotated_frames',batch=16, epochs=100, project='data', name='finetuned_yolo_classifier')
    return result

train_yolo_model_job = define_asset_job(name="train_yolo_model_job", selection="trained_yolo_model")