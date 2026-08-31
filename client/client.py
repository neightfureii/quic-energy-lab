import asyncio
import os
import ssl
from aioquic.asyncio import connect
from aioquic.quic.configuration import QuicConfiguration

TARGET_HOST = os.getenv("TARGET_HOST", "dc-a")
TARGET_PORT = int(os.getenv("TARGET_PORT", "4433"))

async def run_client():
    await asyncio.sleep(3)

    configuration = QuicConfiguration(is_client=True)
    configuration.verify_mode = ssl.CERT_NONE

    print(f"[Client] Connecting to Ingress Node -> {TARGET_HOST}:{TARGET_PORT}", flush=True)

    async with connect(
        TARGET_HOST,
        TARGET_PORT,
        configuration=configuration,
    ) as protocol:
        print("[Client] QUIC Handshake successful!", flush=True)

        reader, writer = await protocol.create_stream()
        
        payload = b"DEFERRABLE_WORKLOAD"
        print(f"[Client] Sending workload payload: {payload.decode()}", flush=True)
        
        writer.write(payload)
        writer.write_eof()

        response = await reader.read(1024)
        response_str = response.decode(errors="ignore")
        print(f"[Client] Received Control Directive: {response_str}", flush=True)

        if "REDIRECT:" in response_str:
            target = response_str.split(":")[1]
            print(f"[Client] Success! Ingress successfully steered workflow toward green zone: {target}", flush=True)

if __name__ == "__main__":
    asyncio.run(run_client())