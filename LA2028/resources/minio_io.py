from dagster import ConfigurableResource
from minio import Minio


class MinioResource(ConfigurableResource):

    endpoint: str
    access_key: str
    secret_key: str

    def get_object_presigned_url(self, bucket_name: str, object_name: str):
        client = Minio(self.endpoint, self.access_key, self.secret_key)

        return client.presigned_get_object(bucket_name, object_name)