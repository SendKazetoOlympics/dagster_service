from dagster import asset

@asset
def UploadMinioFile() -> None:
    print(1)