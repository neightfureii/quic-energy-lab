import asyncio
import os
import redis.asyncio as aioredis
from aioquic.asyncio import serve
from aioquic.quic.configuration import QuicConfiguration

REGION = os.getenv("REGION", "dc-a")
CARBON_INTENSITY = int(os.getenv("CARBON_INTENSITY", "400"))
REDIS_HOST = os.getenv("REDIS_HOST", "redis")

async def update_redis_telemetry(r):
    """Helper to update telemetry keys."""
    await r.set(f"carbon:{REGION}", str(CARBON_INTENSITY), ex=15)
    await r.hset(f"dc:{REGION}", mapping={
        "ip": REGION,
        "port": "4433",
        "carbon": str(CARBON_INTENSITY)
    })
    print(f"[{REGION}] Updated Redis -> Carbon Intensity: {CARBON_INTENSITY} gCO2/kWh", flush=True)

async def publish_telemetry():
    r = aioredis.from_url(f"redis://{REDIS_HOST}:6379")
    # Immediate write on startup so data is instantly available
    await update_redis_telemetry(r)
    while True:
        try:
            await update_redis_telemetry(r)
        except Exception:
            pass
        await asyncio.sleep(5)

async def get_greenest_dc():
    r = aioredis.from_url(f"redis://{REDIS_HOST}:6379")
    keys = await r.keys("carbon:*")
    best_dc = REGION
    lowest_carbon = float('inf')

    for key in keys:
        dc_name = key.decode().split(":")[1]
        val = await r.get(key)
        if val:
            carbon_val = int(val.decode())
            if carbon_val < lowest_carbon:
                lowest_carbon = carbon_val
                best_dc = dc_name
    return best_dc, lowest_carbon

def handle_stream(reader, writer):
    asyncio.create_task(handle_stream_async(reader, writer))

async def handle_stream_async(reader, writer):
    try:
        data = (await reader.read(1024)).decode(errors="ignore")
        print(f"[{REGION}] Received raw stream payload: {repr(data)}", flush=True)
        
        if "DEFERRABLE_WORKLOAD" in data:
            target_dc, carbon_val = await get_greenest_dc()
            print(f"[{REGION}] [SMART INGRESS] Workload received. Optimal Green Node: {target_dc} ({carbon_val} gCO2/kWh)", flush=True)
            
            response = f"REDIRECT:{target_dc}:4433".encode()
            writer.write(response)
            writer.write_eof()
            
        elif "EXECUTE_DEFERRABLE_WORKLOAD" in data:
            print(f"[{REGION}] [GREEN NODE] Workload successfully executed locally under ultra-low carbon energy ({CARBON_INTENSITY} gCO2/kWh) over simulated 80ms link!", flush=True)
            ack = f"WORKLOAD_EXECUTED_SUCCESSFULLY_AT_{REGION}".encode()
            writer.write(ack)
            writer.write_eof()
            
    except Exception as e:
        print(f"[{REGION}] Stream error: {e}", flush=True)
    finally:
        try:
            writer.close()
        except Exception:
            pass

async def main():
    configuration = QuicConfiguration(is_client=False)
    configuration.load_cert_chain("certs/tls_cert.pem", "certs/tls_key.pem")

    asyncio.create_task(publish_telemetry())
    
    # Small pause to let telemetry register before opening port
    await asyncio.sleep(1)

    print(f"[{REGION}] Starting Carbon-Aware QUIC Ingress Server on port 4433...", flush=True)
    
    await serve(
        "0.0.0.0",
        4433,
        configuration=configuration,
        stream_handler=handle_stream
    )
    await asyncio.get_running_loop().create_future()

if __name__ == "__main__":
    asyncio.run(main())