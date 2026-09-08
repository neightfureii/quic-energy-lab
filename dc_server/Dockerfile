FROM python:3.11-slim

WORKDIR /app

# Install build tools required for aioquic compilation and iproute2 for tc netem
RUN apt-get update && apt-get install -y \
    build-essential \
    libssl-dev \
    iproute2 \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install python libraries
COPY dc_server/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy server code, certificates, and entrypoint script
COPY dc_server/server.py /app/server.py
COPY dc_server/entrypoint.sh /app/entrypoint.sh
COPY certs /app/certs

RUN chmod +x /app/entrypoint.sh

EXPOSE 4433/udp

ENTRYPOINT ["/app/entrypoint.sh"]