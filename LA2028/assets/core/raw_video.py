from dagster import asset
from ...resources.minio_io import MinioResource

@asset
def ListMinioObjects(minio: MinioResource) -> list:
    return minio.list_objects("highjump")

# @asset
# def GetMinioObject(bucket: str, object_name: str) -> str:
#     pass