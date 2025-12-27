import os, json, base64
import numpy as np
import cv2

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

from background_remover import init_segmenter, remove_background_bgr

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC_FRAMES = os.environ.get("TOPIC_FRAMES", "camera_frames")
OUTPUT_DIR = os.environ.get("OUTPUT_DIR", "/opt/output")
CHECKPOINT_DIR = os.environ.get("CHECKPOINT_DIR", "/opt/checkpoints/camera_frames")

def process_partition(rows):
    """
    Init model ONCE per partition, then process many frames.
    """
    import os
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    segmenter = init_segmenter()

    for r in rows:
        payload = None
        try:
            raw = r.value if hasattr(r, "value") else r["value"]
            payload = json.loads(raw.decode("utf-8"))

            camera_id = payload["camera_id"]
            frame_id = int(payload["frame_id"])
            data_b64 = payload["data_b64"]

            img_bytes = base64.b64decode(data_b64.encode("ascii"))
            arr = np.frombuffer(img_bytes, dtype=np.uint8)
            frame = cv2.imdecode(arr, cv2.IMREAD_COLOR)
            if frame is None:
                continue

            out_bgr = remove_background_bgr(frame, segmenter)

            cam_dir = os.path.join(OUTPUT_DIR, camera_id)
            os.makedirs(cam_dir, exist_ok=True)

            out_path = os.path.join(cam_dir, f"frame_{frame_id:06d}.png")
            cv2.imwrite(out_path, out_bgr)

            yield (camera_id, frame_id, "done", out_path)

        except Exception as e:
            cam = payload.get("camera_id") if isinstance(payload, dict) else "<unknown>"
            fid = payload.get("frame_id") if isinstance(payload, dict) else -1
            yield (cam, fid, "error", str(e))

def foreach_batch(batch_df, batch_id: int):
    # Trigger action so Spark really runs
    _ = batch_df.rdd.mapPartitions(process_partition).count()

if __name__ == "__main__":
    spark = SparkSession.builder.appName("camera-frames-bg-remover").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", TOPIC_FRAMES)
        .option("failOnDataLoss", "false")
        .option("startingOffsets", "latest")
        .load()
        .select(col("value"))
    )

    query = (
        df.writeStream
        .foreachBatch(foreach_batch)
        .option("checkpointLocation", CHECKPOINT_DIR)
        .start()
    )

    query.awaitTermination()
