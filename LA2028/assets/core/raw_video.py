from dagster import asset, graph_asset, op, Config
from ...resources.minio_io import MinioResource
from ...resources.postgres_io import PostgresResource
from .common_ops import get_videos_by_date, get_videos_url


@graph_asset
def ListVideos() -> list:
    urls = get_videos_url(videos=get_videos_by_date())
    return urls
    # return GetVideoByDate()


# Get frames best on time annotation
