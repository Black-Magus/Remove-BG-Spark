import os
import numpy as np
import mediapipe as mp
from mediapipe.tasks import python
from mediapipe.tasks.python import vision

BG_COLOR = (192, 192, 192)  # gray

def init_segmenter():
    model_path = os.environ.get("SEG_MODEL_PATH", "/opt/models/selfie_segmenter.tflite")
    base_options = python.BaseOptions(model_asset_path=model_path)
    options = vision.ImageSegmenterOptions(
        base_options=base_options,
        output_category_mask=True
    )
    return vision.ImageSegmenter.create_from_options(options)

def remove_background_bgr(frame_bgr: np.ndarray, segmenter) -> np.ndarray:
    # OpenCV -> BGR, MediaPipe expects RGB (SRGB)
    frame_rgb = frame_bgr[:, :, ::-1].copy()

    mp_image = mp.Image(image_format=mp.ImageFormat.SRGB, data=frame_rgb)
    segmentation_result = segmenter.segment(mp_image)
    category_mask = segmentation_result.category_mask.numpy_view()

    image_data = mp_image.numpy_view()  # RGB
    bg_image = np.zeros(image_data.shape, dtype=np.uint8)
    bg_image[:] = BG_COLOR

    condition = np.stack((category_mask,) * 3, axis=-1) > 0.2

    # keep foreground (image_data) where mask is True; replace background with bg_image
    output_rgb = np.where(condition, bg_image, image_data)

    # back to BGR
    return output_rgb[:, :, ::-1].copy()
