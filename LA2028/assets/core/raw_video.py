from dagster import asset, graph_asset, op, Config
from ...resources.minio_io import MinioResource
from ...resources.postgres_io import PostgresResource
from .common_ops import GetVideoByDate, GetVideosURL

@graph_asset
def ListVideos() -> list:
    urls = GetVideosURL(objects=GetVideoByDate())
    return urls
    # return GetVideoByDate()

# Get frames best on time annotation


