import asyncio
import os
import socket
import redis.asyncio as aioredis
from aioquic.asyncio import serve
from aioquic.quic.configuration import QuicConfiguration
from aioquic.asyncio.protocol import QuicConnectionProtocol
from aioquic.quic.parameters import QuicPreferredAddress
from aioquic.quic.events import StreamDataReceived

REGION = os.getenv("REGION", "dc-a")
CARBON_INTENSITY = os.getenv("CARBON_INTENSITY", "400")
REDIS_HOST = os.getenv("REDIS_HOST", "redis")

async def publish_telemetry():
    """Publishes current carbon intensity to Redis every 5 seconds."""
    r = aioredis.from_url(f"redis://{REDIS_HOST}:6379")
    while True:
        try:
            await r.set(f"carbon:{REGION}", CARBON_INTENSITY, ex=15)
            await r.hset(f"dc:{REGION}", mapping={
                "ip": REGION,
                "port": "4433",
                "carbon": CARBON_INTENSITY
            })
            print(f"[{REGION}] Updated Redis -> Carbon Intensity: {CARBON_INTENSITY} gCO2/kWh", flush=True)
        except Exception as e:
            print(f"[{REGION}] Redis Error: {e}", flush=True)
        await asyncio.sleep(5)

async def get_greenest_dc():
    """Queries Redis to locate the DC with the lowest carbon footprint."""
    r = aioredis.from_url(f"redis://{REDIS_HOST}:6379")
    keys = await r.keys("carbon:*")
    best_dc = None
    lowest_carbon = float("inf")

    for key in keys:
        dc_name = key.decode().split(":")[1]
        val = await r.get(key)
        if val:
            carbon_val = int(val.decode())
            if carbon_val < lowest_carbon:
                lowest_carbon = carbon_val
                best_dc = dc_name
    return best_dc, lowest_carbon

class CustomQuicProtocol(QuicConnectionProtocol):
    def quic_event_received(self, event):
        if isinstance(event, StreamDataReceived):
            data = event.data.decode(errors="ignore")
            print(f"[{REGION}] Received Stream Data: {data}", flush=True)
            
            if "DEFERRABLE_WORKLOAD" in data:
                asyncio.create_task(self.handle_redirection(event.stream_id))
                
        super().quic_event_received(event)

    async def handle_redirection(self, stream_id):
        target_dc, carbon_val = await get_greenest_dc()
        print(f"[{REGION}] [CONTROL PLANE] Deferrable workload detected!", flush=True)
        print(f"[{REGION}] [CONTROL PLANE] Current node intensity: {CARBON_INTENSITY} gCO2/kWh", flush=True)
        print(f"[{REGION}] [CONTROL PLANE] Optimal Target Node: {target_dc} ({carbon_val} gCO2/kWh)", flush=True)

        response = f"REDIRECT:{target_dc}:4433".encode()
        self._quic.send_stream_data(stream_id, response, end_stream=True)
        self.transmit()

async def update_preferred_address(config: QuicConfiguration):
    while True:
        target_dc, lowest_carbon = await get_greenest_dc()

        if target_dc and target_dc != REGION:
            try:
                ip_str = socket.gethostbyname(target_dc)
                
                config.server_preferred_address = QuicPreferredAddress(
                    ipv4_address=(ip_str, 4433),
                    ipv6_address=None,
                    connection_id=os.urandom(8),
                    stateless_reset_token=os.urandom(16)
                )
                print(f"[{REGION}] Transport Parameter Updated: Routing new connections to {target_dc} ({ip_str})")
            except socket.gaierror:
                print(f"[{REGION}] DNS resolution for {target_dc} failed. Retrying...")
                
        await asyncio.sleep(5)

async def main():
    configuration = QuicConfiguration(is_client=False)
    configuration.load_cert_chain("certs/tls_cert.pem", "certs/tls_key.pem")

    asyncio.create_task(publish_telemetry())
    asyncio.create_task(update_preferred_address(configuration))

    print(f"[{REGION}] Starting QUIC Data Center Server on port 4433...", flush=True)
    await serve(
        "0.0.0.0",
        4433,
        configuration=configuration,
        create_protocol=CustomQuicProtocol
    )
    await asyncio.Event().wait()

if __name__ == "__main__":
    asyncio.run(main())
