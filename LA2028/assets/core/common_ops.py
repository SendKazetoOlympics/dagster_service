from dagster import op, Config
from ...resources.minio_io import MinioResource
from ...resources.postgres_io import PostgresResource

class GetVideoByDateConfig(Config):
    start_date: str
    end_date: str



@op
def GetVideoByDate(postgres: PostgresResource, config: GetVideoByDateConfig) -> list:
    date_list = postgres.selectVideoByDate(config.start_date, config.end_date)
    return date_list

@op
def GetVideosURL(minio: MinioResource, objects: list) -> list[str]:
    return [minio.get_object_presigned_url("highjump", data[1]) for data in objects]

@op
def GetAnnotationsForVideo(postgres: PostgresResource, video_id: str) -> list:
    return postgres.getAnnotationsForVideo(video_id)
