from dagster import graph_asset, op, Config, job
import cv2
from .common_ops import GetVideoByDate, GetVideosURL, GetTimeAnnotationsForVideo, TimeAnnotationConfig

def get_time_from_frame(frame: int, fps: float) -> float:
    return frame * (1000.0 / fps)

@op
def CropFrameFromVideo(video_path: str, annotations: list[TimeAnnotationConfig]):
    cap = cv2.VideoCapture(video_path)
    frames = []
    while True:
        ret, frame = cap.read()
        if ret:
            frames.append(frame)
        else:
            break
    cap.release()
    print(len(frames))
    # return frames[start_time:end_time]

@op
def test_name():
    return "5481dc55-387b-4a3f-8605-6f8c623d0dc7"

@job
def ListTimeAnnotations():
    CropFrameFromVideo(test_name(), GetTimeAnnotationsForVideo(test_name()))