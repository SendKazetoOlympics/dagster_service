from dagster import Definitions, load_assets_from_package_module, EnvVar

from .assets import core, time_annotation
from .resources import minio_io, label_studio_io, postgres_io

video_assets = load_assets_from_package_module(core, group_name="core")
time_annotation_assets = load_assets_from_package_module(time_annotation, group_name="time_annotation")

all_assets = [*video_assets, *time_annotation_assets]

resources = {
    "minio": minio_io.MinioResource(
        endpoint=EnvVar("MINIO_URL"), access_key=EnvVar("MINIO_API_ACCESSKEY"), secret_key=EnvVar("MINIO_API_SECRETKEY"), port=EnvVar("MINIO_PORT")
    ),
    # "label_studio": label_studio_io.LabelStudioResource,
    "postgres": postgres_io.PostgresResource(
        host=EnvVar("POSTGRES_HOST"),
        port=EnvVar("POSTGRES_PORT"),
        dbname=EnvVar("POSTGRES_DB"),
        user=EnvVar("POSTGRES_USER"),
        password=EnvVar("POSTGRES_PASSWORD"),
    ),
}

defs = Definitions(assets=all_assets, resources=resources)
