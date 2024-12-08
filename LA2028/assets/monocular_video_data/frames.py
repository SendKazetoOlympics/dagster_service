from dagster import graph_asset, op, Config, job, EnvVar, MaterializeResult
import cv2
import numpy.typing as npt
from typing import Optional
from io import BytesIO
from PIL import Image
from .common_ops import (
    GetVideoByDateConfig,
    GetVideoByNameConfig,
)
from ...resources.minio_io import MinioResource
from ...resources.postgres_io import PostgresResource

class CropFrameConfig(Config):
    storage_path: str
    n_frame_gap: int
    get_video_by_date_config: Optional[GetVideoByDateConfig] = None
    get_video_by_name_config: Optional[GetVideoByNameConfig] = None

def get_time_from_frame(frame: int, fps: float) -> float:
    return frame * (1000.0 / fps)


@op
def crop_frames_from_video(minio: MinioResource, postgres: PostgresResource, video_url: str, video_name: str, config: CropFrameConfig) -> MaterializeResult:
    cap = cv2.VideoCapture(video_url)
    upload_path_prefix = video_name.removeprefix("raw_data/").removesuffix(".mp4")
    upload_path_prefix = config.storage_path + upload_path_prefix + '_'
    upload_counter = 0
    gap_counter = 0
    print(f"Extracting frames from {video_name}")
    while True:
        ret, frame = cap.read()
        gap_counter += 1
        if ret:
            if gap_counter == config.n_frame_gap:
                frame = frame[:, :, ::-1]
                bytes = BytesIO()
                Image.fromarray(frame).save(bytes, format='JPEG')
                size = bytes.getbuffer().nbytes
                bytes.seek(0)
                minio.put_object(
                    upload_path_prefix+str(upload_counter)+'.jpeg', bytes, size, content_type="image/jpeg")
                postgres.insertFrame(upload_path_prefix+str(upload_counter)+'.jpeg', video_name)
                upload_counter += 1
                gap_counter = 0
            else:
                continue
        else:
            break
    cap.release()
    # return frames[start_time:end_time]

@graph_asset
def raw_video_frames():
    return crop_frames_from_video()