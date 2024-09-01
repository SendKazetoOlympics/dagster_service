from dagster import op, job

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