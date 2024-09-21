from dagster import op, job, multi_asset, Config, AssetOut
from sklearn.model_selection import train_test_split

from ...resources.minio_io import MinioResource
from ...resources.label_studio_io import LabelStudioResource
from ...resources.postgres_io import PostgresResource

@op
def get_frame_ids(minio: MinioResource):
    return [obj.object_name for obj in minio.list_objects("extracted_frames")]

@op
def upload_frames_to_label_studio(minio: MinioResource, label_studio: LabelStudioResource, id_list: list[str]):
    for frame_id in id_list:
        url = minio.get_object_presigned_url(frame_id)
        label_studio.create_task(project_id="8", url=url, file_name=frame_id)

@job
def upload_frames_to_label_studio_job():
    upload_frames_to_label_studio(get_frame_ids())

class ClassifyingDatasetConfig(Config):
    valid_labels: list[str]

@multi_asset(outs={"training_data": AssetOut(), "test_data": AssetOut(), "yaml": AssetOut()})
def classified_frames_dataset(minio: MinioResource, postgres: PostgresResource, config: ClassifyingDatasetConfig):
    result = postgres.getClassifiedFrames()
    frame_ids = [row[1] for row in result]
    labels = [row[2] for row in result]
    result_train = []
    result_test = []
    frame_ids_train, frame_ids_test, labels_train, labels_test = train_test_split(frame_ids, labels, test_size=0.2)
    for frame_id, label in zip(frame_ids_train, labels_train):
        if label in config.valid_labels:
            label = label.replace(" ","_")
            new_frame_id = frame_id.split("/")[-1]
            result_train.append(minio.download_object(frame_id, f"data/annotated_frames/train/{label}/{new_frame_id}"))
    for frame_id, label in zip(frame_ids_test, labels_test):
        if label in config.valid_labels:
            label = label.replace(" ","_")
            new_frame_id = frame_id.split("/")[-1]
            result_test.append(minio.download_object(frame_id, f"data/annotated_frames/val/{label}/{new_frame_id}"))
    with open("data/annotated_frames/data.yaml", "w") as f:
        f.write("path: data/annotated_frames\n")
        f.write("train: train\n")
        f.write("val: test\n")
        f.write("\n")
        f.write("names:\n")
        for index, label in enumerate(config.valid_labels):
            f.write(f"  {index}: {label.replace(' ','_')}\n")
    return result_train, result_test, "data/annotated_frames/data.yaml"
    