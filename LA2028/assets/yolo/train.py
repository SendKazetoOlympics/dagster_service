from dagster import asset
from ultralytics import YOLO

@asset(deps=["training_data"])
def train_yolo_model():
    model = YOLO("yolov8n-cls.pt")
    result = model.train(data='data/annotated_frames',batch=16, epochs=100)
    return result