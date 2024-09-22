from dagster import op, job, asset, multi_asset, Config, AssetOut
from sklearn.model_selection import train_test_split
import numpy as np
import os

from ...resources.minio_io import MinioResource
from ...resources.label_studio_io import LabelStudioResource
from ...resources.postgres_io import PostgresResource


class UploadFrameConfig(Config):
    project_id: str
    n_frames: int
    seed: int
    valid_labels: list[str]


@multi_asset(outs={"image_names": AssetOut(), "yaml": AssetOut()})
def detection_raw_frames(
    minio: MinioResource, label_studio: LabelStudioResource, config: UploadFrameConfig
):
    id_list = [obj.object_name for obj in minio.list_objects("extracted_frames")]
    if len(id_list) > config.n_frames:
        # Shuffle the list
        np.random.seed(config.seed)
        np.random.shuffle(id_list)
        id_list = id_list[: config.n_frames]

    ls_task_list = label_studio.list_tasks(project_id=config.project_id)
    task_filenames = [task.data["file_name"] for task in ls_task_list]
    for frame_id in id_list:
        if frame_id in task_filenames:
            continue
        url = minio.get_object_presigned_url(frame_id)
        label_studio.create_task(
            project_id=config.project_id, url=url, file_name=frame_id
        )

    if not os.path.exists("data/annotated_detected_frames"):
        os.makedirs("data/annotated_detected_frames")

    if not os.path.exists("data/annotated_detected_frames/images"):
        os.makedirs("data/annotated_detected_frames/images")
    if not os.path.exists("data/annotated_detected_frames/images/train"):
        os.makedirs("data/annotated_detected_frames/images/train")
    if not os.path.exists("data/annotated_detected_frames/images/val"):
        os.makedirs("data/annotated_detected_frames/images/val")

    if not os.path.exists("data/annotated_detected_frames/labels"):
        os.makedirs("data/annotated_detected_frames/labels")
    if not os.path.exists("data/annotated_detected_frames/labels/train"):
        os.makedirs("data/annotated_detected_frames/labels/train")
    if not os.path.exists("data/annotated_detected_frames/labels/val"):
        os.makedirs("data/annotated_detected_frames/labels/val")

    with open("data/annotated_detected_frames/data.yaml", "w") as f:
        f.write("path: data/annotated_detected_frames\n")
        f.write("train: train\n")
        f.write("val: test\n")
        f.write("\n")
        f.write("names:\n")
        for index, label in enumerate(config.valid_labels):
            f.write(f"  {index}: {label.replace(' ','_')}\n")

    with open("data/annotated_detected_frames/image_names.txt", "w") as f:
        for frame_id in id_list:
            new_frame_id = frame_id.split("/")[-1]
            f.write(new_frame_id + "\n")
    return (
        "data/annotated_detected_frames/image_names.txt",
        "data/annotated_detected_frames/data.yaml",
    )


@multi_asset(
    outs={"training_data": AssetOut(), "test_data": AssetOut()},
    deps=["image_names", "yaml"],
)
def annotated_dataset(
    label_studio: LabelStudioResource, minio: MinioResource, config: UploadFrameConfig
):
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
                        label = config.valid_labels.index(
                            result["value"]["rectanglelabels"][0]
                        )
                        x = result["value"]["x"]
                        y = result["value"]["y"]
                        width = result["value"]["width"]
                        height = result["value"]["height"]
                        annotations.append((label, x, y, width, height))
                new_frame_id = task.data["file_name"].split("/")[-1].split(".")[0]
            with open(
                "data/annotated_detected_frames/labels/train/" + new_frame_id + ".txt",
                "w",
            ) as f:
                for annotation in annotations:
                    f.write(
                        f"{annotation[0]} {annotation[1]} {annotation[2]} {annotation[3]} {annotation[4]}\n"
                    )
            train_results.append(
                minio.download_object(
                    task.data["file_name"],
                    f"data/annotated_detected_frames/images/train/{new_frame_id}",
                )
            )
        for frame_index in frame_indices_test:
            task = tasks[frame_index]
            annotations = []
            for annotation in task.annotations:
                for result in annotation["result"]:
                    if result["type"] == "rectanglelabels":
                        label = config.valid_labels.index(
                            result["value"]["rectanglelabels"][0]
                        )
                        x = result["value"]["x"]
                        y = result["value"]["y"]
                        width = result["value"]["width"]
                        height = result["value"]["height"]
                        annotations.append((label, x, y, width, height))
                new_frame_id = task.data["file_name"].split("/")[-1].split(".")[0]
            with open(
                "data/annotated_detected_frames/labels/val/" + new_frame_id + ".txt",
                "w",
            ) as f:
                for annotation in annotations:
                    f.write(
                        f"{annotation[0]} {annotation[1]} {annotation[2]} {annotation[3]} {annotation[4]}\n"
                    )
            test_results.append(
                minio.download_object(
                    task.data["file_name"],
                    f"data/annotated_detected_frames/images/val/{new_frame_id}",
                )
            )
    else:
        raise ValueError("Not all tasks have been labeled")
    return train_results, test_results
