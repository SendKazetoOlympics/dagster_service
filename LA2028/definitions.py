from dagster import Definitions, load_assets_from_package_module, EnvVar

from .assets import core, time_annotation, yolo
from .resources import minio_io, label_studio_io, postgres_io

from .assets.yolo.make_data import upload_frames_to_label_studio_job
from .assets.yolo.train import train_yolo_model_job

core_assets = load_assets_from_package_module(core, group_name="core")
time_annotation_assets = load_assets_from_package_module(time_annotation, group_name="time_annotation")
yolo_assets = load_assets_from_package_module(yolo, group_name="yolo")

all_assets = [*core_assets, *time_annotation_assets,*yolo_assets]

all_job = [upload_frames_to_label_studio_job, train_yolo_model_job]

resources = {
    "minio": minio_io.MinioResource(
        endpoint=EnvVar("MINIO_URL"), access_key=EnvVar("MINIO_API_ACCESSKEY"), secret_key=EnvVar("MINIO_API_SECRETKEY"), port=EnvVar("MINIO_PORT"), bucket_name=EnvVar("MINIO_BUCKET_NAME")
    ),
    "label_studio": label_studio_io.LabelStudioResource(url=EnvVar("LABEL_STUDIO_URL"), access_key=EnvVar("LABEL_STUDIO_API_ACCESSKEY")),
    "postgres": postgres_io.PostgresResource(
        host=EnvVar("POSTGRES_HOST"),
        port=EnvVar("POSTGRES_PORT"),
        dbname=EnvVar("POSTGRES_DB"),
        user=EnvVar("POSTGRES_USER"),
        password=EnvVar("POSTGRES_PASSWORD"),
    ),
}

defs = Definitions(assets=all_assets, resources=resources, jobs=all_job)

