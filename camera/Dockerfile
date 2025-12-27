FROM python:3.10-slim

USER root

RUN apt-get update && apt-get install -y \
    libglib2.0-0 libsm6 libxext6 libxrender1 libgl1 \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY camera_server.py .

CMD ["python", "camera_server.py"]
