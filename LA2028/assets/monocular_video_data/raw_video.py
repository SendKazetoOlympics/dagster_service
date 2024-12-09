from dagster import asset, op, Config, MaterializeResult, AssetExecutionContext, MetadataValue
from ...resources.minio_io import MinioResource
from ...resources.postgres_io import PostgresResource
from datetime import datetime
from pathlib import Path

class VideoSetConfig(Config):
    names: list[str]
    storage_path: str = 'data/'


# Get frames best on time annotation

@asset
def video_ids(context: AssetExecutionContext, config: VideoSetConfig) -> MaterializeResult:
    path = Path(config.storage_path + 'video_ids.txt')
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open('w') as f:
        for name in config.names:
            f.write(name + '\n')

    return MaterializeResult(
        metadata={
        'date': MetadataValue.text(str(datetime.now())),
    })