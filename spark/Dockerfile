# Official Spark image (không dùng bitnami để khỏi lỗi tag)
FROM spark:3.5.1-scala2.12-java17-python3-ubuntu

USER root

# System dependencies needed by OpenCV/MediaPipe
RUN apt-get update && apt-get install -y \
    libglib2.0-0 libsm6 libxext6 libxrender1 libgl1 \
 && rm -rf /var/lib/apt/lists/*

# Create a named non-root user so Hadoop/Spark can resolve the UnixPrincipal
RUN useradd -m -u 1000 sparkuser \
 && mkdir -p /opt/spark/logs /opt/spark/work-dir /opt/output /opt/checkpoints \
 && chown -R sparkuser:sparkuser /opt/spark/logs /opt/spark/work-dir /opt/output /opt/checkpoints \
 && chmod -R 775 /opt/spark/logs /opt/spark/work-dir

WORKDIR /opt/app
COPY requirements.txt /opt/app/requirements.txt
RUN pip3 install --no-cache-dir -r /opt/app/requirements.txt

# code + model
COPY background_remover.py /opt/app/background_remover.py
COPY stream_frames_job.py /opt/app/stream_frames_job.py

RUN mkdir -p /opt/models
COPY selfie_segmenter.tflite /opt/models/selfie_segmenter.tflite

ENV SEG_MODEL_PATH=/opt/models/selfie_segmenter.tflite

USER sparkuser
