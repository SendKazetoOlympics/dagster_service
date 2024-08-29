from dagster import asset, graph_asset, op, Config
from ...resources.minio_io import MinioResource
from ...resources.postgres_io import PostgresResource

class GetVideoByDateConfig(Config):
    start_date: str
    end_date: str

@op
def GetVideoByDate(postgres: PostgresResource, config: GetVideoByDateConfig) -> list:
    return postgres.selectVideoByDate(config.start_date, config.end_date)

@graph_asset
def ListVideos():
    return GetVideoByDate()

# @graph_asset
# def ListMinioObjects(minio: MinioResource):# -> list:
#     print(1)
#     return []
    # return minio.list_objects("highjump")

# @asset
# def GetMinioObject(bucket: str, object_name: str) -> str:
#     pass