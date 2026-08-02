import asyncio
import os
import ssl
import redis.asyncio as aioredis
from aioquic.asyncio import serve
from aioquic.quic.configuration import QuicConfiguration
from aioquic.asyncio.protocol import QuicConnectionProtocol

# Environment variables injected via Docker
REGION = os.getenv("REGION", "dc-a")
CARBON_INTENSITY = os.getenv("CARBON_INTENSITY", "400")
REDIS_HOST = os.getenv("REDIS_HOST", "redis")

async def publish_telemetry():
    # Publishes current carbon intensity to Redis every 5 seconds
    r = aioredis.from_url(f"redis://{REDIS_HOST}:6379")
    while True:
        try:
            # Set carbon reading with 15-second TTL
            await r.set(f"carbon:{REGION}", CARBON_INTENSITY, ex=15)
            # Store host/port metadata
            await r.hset(f"dc:{REGION}", mapping={
                "ip": REGION,
                "port": "4433",
                "carbon": CARBON_INTENSITY
            })
            print(f"[{REGION}] Updated Redis -> Carbon Intensity: {CARBON_INTENSITY} gCO2/kWh")
        except Exception as e:
            print(f"[{REGION}] Redis Error: {e}")
        await asyncio.sleep(5)

class CustomQuicProtocol(QuicConnectionProtocol):
    def quic_event_received(self, event):
        # Placeholder where we will inspect incoming flags
        super().quic_event_received(event)

async def main():
    # Configure TLS for QUIC
    configuration = QuicConfiguration(is_client=False)
    configuration.load_cert_chain("certs/tls_cert.pem", "certs/tls_key.pem")

    # Start background telemetry writer
    asyncio.create_task(publish_telemetry())

    # Start QUIC server
    print(f"[{REGION}] Starting Quic Data Center Server on port 4433...")
    await serve(
        "0.0.0.0",
        4433,
        configuration=configuration,
        create_protocol=CustomQuicProtocol
    )
    await asyncio.Event().wait()

if __name__ == "__main__":
    asyncio.run(main())
