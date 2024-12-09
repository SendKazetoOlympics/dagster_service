from dagster import Definitions, FilesystemIOManager, load_assets_from_package_module, EnvVar

from .assets import monocular_video_data, contact_detection
from .resources import minio_io, label_studio_io, postgres_io


monocular_video_assets = load_assets_from_package_module(monocular_video_data, group_name="monocular_video_data")
contact_detection_assets = load_assets_from_package_module(contact_detection, group_name="contact_detection_model")

all_assets = [*monocular_video_assets, *contact_detection_assets]

all_job = []

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
    "fs_io_manager": FilesystemIOManager(),
}

defs = Definitions(assets=all_assets, resources=resources, jobs=all_job)

