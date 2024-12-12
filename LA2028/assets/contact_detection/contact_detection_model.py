from dagster import asset, define_asset_job, Config
from ultralytics import YOLO

class YOLOConfig(Config):
    data_path: str
    output_path: str

@asset(deps=["training_data", "dataset_description_yaml"])
def trained_yolo_model(config: YOLOConfig):
    model = YOLO(config.output_path + "models/yolov8l.pt")
    result = model.train(data=config.data_path + 'data.yaml',batch=16, epochs=100, project='data', name=config.output_path+'finetuned_yolo_classifier')
    return result

train_yolo_model_job = define_asset_job(name="train_yolo_model_job", selection="trained_yolo_model")