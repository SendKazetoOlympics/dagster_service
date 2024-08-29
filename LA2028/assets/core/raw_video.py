from dagster import asset, graph_asset, op, Config
from ...resources.minio_io import MinioResource
from ...resources.postgres_io import PostgresResource

class GetVideoByDateConfig(Config):
    start_date: str
    end_date: str

@op
def GetVideoByDate(postgres: PostgresResource, config: GetVideoByDateConfig):
    date_list = postgres.selectVideoByDate(config.start_date, config.end_date)
    return date_list

@op
def GetVideosURL(minio: MinioResource, objects: list) -> list[str]:
    return [minio.get_object_presigned_url("highjump", data[1]) for data in objects]

@graph_asset
def ListVideos() -> list:
    return GetVideosURL(objects=GetVideoByDate())
    # return GetVideoByDate()

# @graph_asset
# def ListMinioObjects(minio: MinioResource):# -> list:
#     print(1)
#     return []
    # return minio.list_objects("highjump")

# @asset
# def GetMinioObject(bucket: str, object_name: str) -> str:
#     pass