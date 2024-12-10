from dagster import op, job, asset, multi_asset, Config, AssetOut
from ...resources.minio_io import MinioResource
from ...resources.label_studio_io import LabelStudioResource
from ...resources.postgres_io import PostgresResource
from sklearn.model_selection import train_test_split
from pathlib import Path

import os
import numpy as np

class LabelStudioConfig(Config):
    data_path: str # Where the frames are stored
    project_id: str

class DatasetConfig(Config):
    data_path: str # Where the frames are stored
    output_path: str # Where to put the dataset
    project_id: str
    valid_labels: list[str]

@asset(deps=["individual_frames"])
def label_studio_tasks(minio: MinioResource, label_studio: LabelStudioResource, config: LabelStudioConfig):
    id_list = [obj.object_name for obj in minio.list_objects(config.data_path)]

    ls_task_list = label_studio.list_tasks(project_id=config.project_id)
    task_filenames = [task.data["file_name"] for task in ls_task_list]
    for frame_id in id_list:
        if frame_id in task_filenames:
            continue
        url = minio.get_object_presigned_url(frame_id)
        label_studio.create_task(
            project_id=config.project_id, url=url, file_name=frame_id
        )


@multi_asset(
    deps=[label_studio_tasks, "individual_frames"],
    outs={"training_data": AssetOut(), "test_data": AssetOut(),"dataset_description_yaml": AssetOut()},
)
def annotated_dataset(
    label_studio: LabelStudioResource, minio: MinioResource, config: DatasetConfig
):
    
    id_list = [obj.object_name for obj in minio.list_objects(config.data_path)]

    Path(config.output_path+'images/train').mkdir(parents=True, exist_ok=True)
    Path(config.output_path+'images/val').mkdir(parents=True, exist_ok=True)
    Path(config.output_path+'labels/train').mkdir(parents=True, exist_ok=True)
    Path(config.output_path+'labels/val').mkdir(parents=True, exist_ok=True)

    with open(config.output_path + "/data.yaml", "w") as f:
        f.write("path: "+os.getcwd() +'/'+config.output_path+"\n")
        f.write("train: images/train\n")
        f.write("val: images/val\n")
        f.write("\n")
        f.write("names:\n")
        for index, label in enumerate(config.valid_labels):
            f.write(f"  {index}: {label.replace(' ','_')}\n")

    with open(config.output_path + "image_names.txt", "w") as f:
        for frame_id in id_list:
            new_frame_id = frame_id.split("/")[-1]
            f.write(new_frame_id + "\n")
    ls_task_list = label_studio.list_tasks(project_id=config.project_id)
    train_results = []
    test_results = []
    tasks = [task for task in ls_task_list]
    if np.all([task.is_labeled for task in tasks]):
        frame_indices = np.arange(len(tasks))
        frame_indices_train, frame_indices_test = train_test_split(
            frame_indices, test_size=0.2
        )
        for frame_index in frame_indices_train:
            task = tasks[frame_index]
            annotations = []
            for annotation in task.annotations:
                for result in annotation["result"]:
                    if result["type"] == "rectanglelabels":
                        if result["value"]["rectanglelabels"][0] not in config.valid_labels:
                            continue
                        label = config.valid_labels.index(
                            result["value"]["rectanglelabels"][0]
                        )
                        x = result["value"]["x"]/100
                        y = result["value"]["y"]/100
                        width = result["value"]["width"]/100
                        height = result["value"]["height"]/100
                        annotations.append((label, x, y, width, height))
                new_frame_id = task.data["file_name"].split("/")[-1]
            with open(
                config.output_path+"labels/train/" + new_frame_id.split(".")[0] + ".txt",
                "w",
            ) as f:
                for annotation in annotations:
                    f.write(
                        f"{annotation[0]} {annotation[1]} {annotation[2]} {annotation[3]} {annotation[4]}\n"
                    )
            train_results.append(
                minio.download_object(
                    task.data["file_name"],
                    config.output_path+f"images/train/{new_frame_id}",
                )
            )
        for frame_index in frame_indices_test:
            task = tasks[frame_index]
            annotations = []
            for annotation in task.annotations:
                for result in annotation["result"]:
                    if result["type"] == "rectanglelabels":
                        if result["value"]["rectanglelabels"][0] not in config.valid_labels:
                            continue
                        label = config.valid_labels.index(
                            result["value"]["rectanglelabels"][0]
                        )
                        x = result["value"]["x"]/100
                        y = result["value"]["y"]/100
                        width = result["value"]["width"]/100
                        height = result["value"]["height"]/100
                        annotations.append((label, x, y, width, height))
                new_frame_id = task.data["file_name"].split("/")[-1]
            with open(
                config.output_path+"labels/val/" + new_frame_id.split(".")[0] + ".txt",
                "w",
            ) as f:
                for annotation in annotations:
                    f.write(
                        f"{annotation[0]} {annotation[1]} {annotation[2]} {annotation[3]} {annotation[4]}\n"
                    )
            test_results.append(
                minio.download_object(
                    task.data["file_name"],
                    config.output_path+f"images/val/{new_frame_id}",
                )
            )
    else:
        raise ValueError("Not all tasks have been labeled")
    return train_results, test_results, config.output_path + "/data.yaml"
