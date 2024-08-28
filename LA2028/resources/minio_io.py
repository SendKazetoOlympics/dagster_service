from dagster import ConfigurableResource
from minio import Minio


class MinioResource(ConfigurableResource):

    endpoint: str
    access_key: str
    secret_key: str
    port: str

    def get_object_presigned_url(self, bucket_name: str, object_name: str):
        client = Minio(self.endpoint + ":" + self.port, self.access_key, self.secret_key)

        return client.presigned_get_object(bucket_name, object_name)

    def list_objects(self, bucket_name: str) -> list:
        print(self.endpoint)
        client = Minio(self.endpoint + ":" + self.port, access_key=self.access_key, secret_key=self.secret_key, secure=False)
        return list(client.list_objects(bucket_name))