FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y \
    build-essential \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir aioquic redis

COPY client/client.py /app/client.py

ENV PYTHONUNBUFFERED=1

CMD ["python", "client.py"]
