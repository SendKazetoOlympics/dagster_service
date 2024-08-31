from dagster import op, Config
from ...resources.minio_io import MinioResource
from ...resources.postgres_io import PostgresResource


class GetVideoByDateConfig(Config):
    start_date: str
    end_date: str


class TimeAnnotationConfig(Config):
    id: str
    video_id: str
    start_time: float
    end_time: float
    label: str


@op
def get_videos_by_date(
    postgres: PostgresResource, config: GetVideoByDateConfig
) -> list:
    date_list = postgres.selectVideoByDate(config.start_date, config.end_date)
    return date_list


@op
def get_videos_url(minio: MinioResource, videos: list) -> list[str]:
    return [minio.get_object_presigned_url("highjump", data[1]) for data in videos]


@op
def GetTimeAnnotationsForVideo(
    postgres: PostgresResource, video_id: str
) -> list[TimeAnnotationConfig]:
    return [
        TimeAnnotationConfig(
            id=id,
            video_id=video_id,
            start_time=start_time,
            end_time=end_time,
            label=label,
        )
        for (
            id,
            video_id,
            start_time,
            end_time,
            label,
        ) in postgres.getTimeAnnotationsForVideo(video_id)
    ]
