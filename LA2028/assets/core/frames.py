from dagster import graph_asset, op, Config
import cv2
from .common_ops import GetVideoByDate, GetVideosURL

def get_time_from_frame(frame: int, fps: float) -> float:
    return frame * (1000.0 / fps)

def CropFrameFromVideo(video_path: str, start_time: int, end_time: int) -> list:
    cap = cv2.VideoCapture(video_path)
    frames = []
    while cap.isOpened():
        ret, frame = cap.read()
        if ret:
            frames.append(frame)
        else:
            break
    cap.release()
    return frames[start_time:end_time]

