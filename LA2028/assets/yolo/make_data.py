from dagster import op, job, graph_asset, asset, Config

from ...resources.minio_io import MinioResource
from ...resources.label_studio_io import LabelStudioResource
from ...resources.postgres_io import PostgresResource

@op
def get_frame_ids(minio: MinioResource):
    return [obj.object_name for obj in minio.list_objects("extracted_frames")]

@op
def upload_frames_to_label_studio(minio: MinioResource, label_studio: LabelStudioResource, id_list: list[str]):
    for frame_id in id_list:
        url = minio.get_object_presigned_url(frame_id)
        label_studio.create_task(project_id="8", url=url, file_name=frame_id)

@job
def upload_frames_to_label_studio_job():
    upload_frames_to_label_studio(get_frame_ids())

class ClassifyingDatasetConfig(Config):
    valid_labels: list[str]

@asset
def classified_frames_dataset(minio: MinioResource, postgres: PostgresResource, config: ClassifyingDatasetConfig):
    result = postgres.getClassifiedFrames()
    frame_ids = [row[1] for row in result]
    labels = [row[2] for row in result]
    for frame_id, label in zip(frame_ids, labels):
        if label in config.valid_labels:
            label = label.replace(" ","_")
            new_frame_id = frame_id.split("/")[-1]
            minio.download_object(frame_id, f"data/annotated_frames/{label}/{new_frame_id}")