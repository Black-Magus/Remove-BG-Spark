import os, time, json, base64
import cv2
from confluent_kafka import Producer

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC_FRAMES = os.environ.get("TOPIC_FRAMES", "camera_frames")

CAMERA_ID = os.environ.get("CAMERA_ID", "cam01")
VIDEO_SRC = os.environ.get("VIDEO_SRC", "/videos/sample.mp4")  # khuyến nghị mp4 trên Windows Docker
FPS = float(os.environ.get("FPS", "10"))
JPEG_QUALITY = int(os.environ.get("JPEG_QUALITY", "80"))
MAX_WIDTH = int(os.environ.get("MAX_WIDTH", "640"))

def open_capture(src: str):
    if src.isdigit():
        return cv2.VideoCapture(int(src))
    return cv2.VideoCapture(src)

def maybe_resize(frame):
    h, w = frame.shape[:2]
    if w <= MAX_WIDTH:
        return frame
    scale = MAX_WIDTH / float(w)
    return cv2.resize(frame, (MAX_WIDTH, int(h * scale)), interpolation=cv2.INTER_AREA)

def wait_kafka(max_wait_s=120):
    start = time.time()
    while True:
        try:
            p = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})
            # force metadata request => ensure broker ready
            p.list_topics(timeout=5)
            return p
        except Exception:
            if time.time() - start > max_wait_s:
                raise RuntimeError(f"Kafka not ready after {max_wait_s}s: {KAFKA_BOOTSTRAP}")
            time.sleep(2)

def delivery_report(err, msg):
    if err is not None:
        print(f"[camera] delivery failed: {err}")
    # else: delivered

producer = wait_kafka()

cap = open_capture(VIDEO_SRC)
if not cap.isOpened():
    raise RuntimeError(f"Cannot open VIDEO_SRC={VIDEO_SRC}")

frame_id = 0
interval = 1.0 / FPS

print(f"[camera] Streaming to {KAFKA_BOOTSTRAP} topic={TOPIC_FRAMES} FPS={FPS} SRC={VIDEO_SRC}")

while True:
    ok, frame = cap.read()
    if not ok:
        cap.set(cv2.CAP_PROP_POS_FRAMES, 0)
        continue

    frame = maybe_resize(frame)

    ok2, enc = cv2.imencode(".jpg", frame, [int(cv2.IMWRITE_JPEG_QUALITY), JPEG_QUALITY])
    if not ok2:
        continue

    payload = {
        "camera_id": CAMERA_ID,
        "frame_id": frame_id,
        "ts": time.time(),
        "codec": "jpg",
        "width": int(frame.shape[1]),
        "height": int(frame.shape[0]),
        "data_b64": base64.b64encode(enc.tobytes()).decode("ascii"),
    }

    try:
        producer.produce(TOPIC_FRAMES, json.dumps(payload).encode("utf-8"), callback=delivery_report)
        producer.poll(0)
    except Exception:
        # reconnect if anything weird happens
        producer = wait_kafka()

    if frame_id % 30 == 0:
        print(f"[camera] sent frame_id={frame_id}")

    frame_id += 1
    time.sleep(interval)
