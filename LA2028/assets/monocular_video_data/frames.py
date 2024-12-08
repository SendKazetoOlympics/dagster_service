from dagster import op, Config, MaterializeResult, asset, AssetKey
import cv2
from io import BytesIO
from PIL import Image
from ...resources.minio_io import MinioResource
from ...resources.postgres_io import PostgresResource

class CropFrameConfig(Config):
    storage_path: str
    n_frame_gap: int

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

@asset(deps=["video_ids"])
def individual_frames(config: CropFrameConfig) -> None:
    crop_frames_from_video()

