from dagster import asset, op, Config, MaterializeResult, AssetExecutionContext, MetadataValue
from ...resources.minio_io import MinioResource
from ...resources.postgres_io import PostgresResource
from datetime import datetime

class VideoSetConfig(Config):
    names: list[str]
    storage_path: str = 'data/'


@op
def get_videos_by_name(
    postgres: PostgresResource, names: list[str]
) -> list[str]:
    name_list = postgres.selectVideoByNames(names)
    return name_list

@op
def get_videos_url(minio: MinioResource, videos: list[str]) -> list[str]:
    return [minio.get_object_presigned_url(data[1]) for data in videos]

# Get frames best on time annotation

@asset
def video_ids(config: VideoSetConfig) -> MaterializeResult:
    with open(config.storage_path + 'video_ids.txt', 'w') as f:
        for name in config.names:
            f.write(name + '\n')
    
    return MaterializeResult(metadata_entries={
        'date': MetadataValue(str(datetime.now()))
    })