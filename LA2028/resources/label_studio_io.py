from dagster import ConfigurableResource
from label_studio_sdk.client import LabelStudio

class LabelStudioResource(ConfigurableResource):

    url: str
    access_key: str

    def get_client(self):
        return LabelStudio(base_url=self.url, api_key=self.access_key)
    
    def create_task(self, project_id: str, url: str, file_name: str):
        client = self.get_client()
        return client.tasks.create(project=project_id, data={"image": url, "file_name": file_name})
    
    def list_tasks(self, project_id: str):
        client = self.get_client()
        return client.tasks.list(project=project_id)