import os
import json
import boto3
import redis
import numpy as np
import cv2

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from background_remover import init_segmenter, remove_background_bgr

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
MINIO_PUBLIC_BASE_URL = os.environ.get("MINIO_PUBLIC_BASE_URL", "http://localhost:9000")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "minio")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "minio12345")
MINIO_BUCKET = os.environ.get("MINIO_BUCKET", "images")
REDIS_URL = os.environ.get("REDIS_URL", "redis://redis:6379/0")

TOPIC_REQUESTS = os.environ.get("TOPIC_REQUESTS", "bg_requests")
CHECKPOINT_DIR = os.environ.get("CHECKPOINT_DIR", "/tmp/spark_checkpoints/bg_remover")


def process_partition(rows):
    """
    Process kafka rows in a partition.
    Init heavy resources (S3 client, Redis, segmenter) ONCE per partition.
    """
    s3 = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
    )
    rdb = redis.Redis.from_url(REDIS_URL, decode_responses=True)
    segmenter = init_segmenter()

    for r in rows:
        payload = None
        job_id = None

        try:
            # r is a Row with field "value" (binary)
            raw = r.value if hasattr(r, "value") else r["value"]
            payload = json.loads(raw.decode("utf-8"))

            job_id = payload.get("job_id")
            if not job_id:
                raise ValueError("missing_job_id")

            bucket = payload.get("bucket") or MINIO_BUCKET
            in_key = payload.get("in_key")
            out_key = payload.get("out_key")

            if not in_key or not out_key:
                raise ValueError("missing_in_key_or_out_key")

            rdb.hset(f"job:{job_id}", mapping={"status": "processing"})

            obj = s3.get_object(Bucket=bucket, Key=in_key)
            img_bytes = obj["Body"].read()

            arr = np.frombuffer(img_bytes, dtype=np.uint8)
            frame = cv2.imdecode(arr, cv2.IMREAD_COLOR)
            if frame is None:
                raise ValueError("image_decode_failed")

            out_bgr = remove_background_bgr(frame, segmenter)

            ok, enc = cv2.imencode(".png", out_bgr)
            if not ok:
                raise RuntimeError("encode_failed")

            s3.put_object(
                Bucket=bucket,
                Key=out_key,
                Body=enc.tobytes(),
                ContentType="image/png"
            )

            out_url = f"{MINIO_PUBLIC_BASE_URL}/{bucket}/{out_key}"
            rdb.hset(f"job:{job_id}", mapping={"status": "done", "out_url": out_url})

            yield (job_id, "done", out_url, "")

        except Exception as e:
            # If payload parsing failed, payload may be None.
            if isinstance(payload, dict):
                job_id = job_id or payload.get("job_id")

            if job_id:
                rdb.hset(f"job:{job_id}", mapping={"status": "error", "error": str(e)})
                yield (job_id, "error", "", str(e))
            else:
                # optional: surface bad messages for debugging
                yield ("<unknown>", "error", "", f"{type(e).__name__}: {e}")


def foreach_batch(batch_df, batch_id: int):
    """
    Structured Streaming micro-batch handler.
    Use RDD mapPartitions to keep init cost low.
    """
    # Trigger computation (actions)
    _ = batch_df.rdd.mapPartitions(process_partition).count()


if __name__ == "__main__":
    spark = SparkSession.builder.appName("bg-remover-stream").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", TOPIC_REQUESTS)
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
