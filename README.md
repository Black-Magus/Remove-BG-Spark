Small demo of background removal for live video using Docker, Kafka and Spark Structured Streaming.
 *Instead of a webcam it uses a sample video file as the camera source.*
## Overview

- `camera` container reads a video file, encodes frames as JPEG and publishes them to a Kafka topic.
- `spark-job` container runs a PySpark Structured Streaming job that:
  - consumes frames from Kafka (`camera_frames`),
  - runs MediaPipe ImageSegmenter to isolate the person,
  - replaces the background with a solid gray color,
  - writes processed PNG frames to `output/<camera_id>/`.
- `spark-master` and `spark-worker` provide the Spark cluster; `zookeeper` + `kafka` provide messaging.

## Requirements

- Docker and Docker Compose installed and running.
- A sample video file (default: `videos/sample.mp4` inside this repo).
- A few GB of free disk space for output frames and Docker images.

## Quick start

1. Place your input video at `videos/sample.mp4`.  
   - Or change the `VIDEO_SRC` environment variable in `docker-compose.yml` under the `camera` service.
2. Build and start the stack:

   ```bash
   docker compose up -d --build
   ```

3. Watch logs to see frames being streamed and processed:

   ```bash
   docker compose logs -f camera spark-job
   ```

4. Processed frames (with background removed/replaced) will be written to:
   - `output/cam01/` by default (controlled by `CAMERA_ID`).

To stop everything, run:

```bash
docker compose down
```

## Configuration

Key environment variables (see `docker-compose.yml`):

- `CAMERA_ID` (camera): logical id for the video source, used as output folder name (`cam01` by default).
- `VIDEO_SRC` (camera): path or device index for the video; default is `/videos/sample.mp4`.
- `FPS` (camera): frame rate to publish to Kafka.
- `JPEG_QUALITY` (camera): JPEG quality used before sending frames.
- `MAX_WIDTH` (camera): frames wider than this are resized to this width.
- `KAFKA_BOOTSTRAP`, `TOPIC_FRAMES` (camera & spark-job): Kafka connection and topic; default topic is `camera_frames`.
- `OUTPUT_DIR` (spark-job): where processed frames are saved inside containers (`/opt/output`, mapped to `./output`).
- `CHECKPOINT_DIR` (spark-job): Spark Structured Streaming checkpoint directory.

## How it works

- `camera/camera_server.py`
  - Opens the configured video source.
  - Resizes frames if needed, encodes them to JPEG, and publishes JSON payloads (with base64-encoded image data) to Kafka.
- `spark/stream_frames_job.py`
  - Uses Spark Structured Streaming to read the Kafka topic.
  - For each micro-batch, decodes frames and calls `background_remover.remove_background_bgr`.
  - Saves the processed frames as PNG to `OUTPUT_DIR/<camera_id>/frame_XXXXXX.png`.
- `spark/background_remover.py`
  - Loads a MediaPipe ImageSegmenter model (path controlled by `SEG_MODEL_PATH` or default `/opt/models/selfie_segmenter.tflite`).
  - Computes a segmentation mask and replaces the background with a solid gray color.

## Disclaimer

- No real human face is used in this demo.
- All faces are generated (from thispersondoesnotexist and AI-generated Sora).
