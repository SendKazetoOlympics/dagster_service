from dagster import asset, Definitions, ConfigurableResource
from minio import Minio


class MinioResource(ConfigurableResource):

    client: Minio

    def __init__(self, endpoint: str, access_key: str, secret_key: str):
        self.client = Minio(endpoint, access_key, secret_key)
