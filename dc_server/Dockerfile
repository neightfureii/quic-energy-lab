FROM python:3.11-slim

WORKDIR /app

# Install build tools required for aioquic compilation
RUN apt-get update && apt-get install -y \
    build-essential \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

# Install python libraries
RUN pip install --no-cache-dir aioquic==0.9.20 redis

# Copy code and certificates
COPY dc_server/server.py /app/server.py
COPY certs /app/certs

EXPOSE 4433/udp

CMD ["python", "server.py"]
