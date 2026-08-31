import asyncio
import os
import ssl
from aioquic.asyncio import connect
from aioquic.quic.configuration import QuicConfiguration

TARGET_HOST = os.getenv("TARGET_HOST", "dc-a")
TARGET_PORT = int(os.getenv("TARGET_PORT", "4433"))

async def run_client():
    configuration = QuicConfiguration(is_client=True)
    configuration.verify_mode = ssl.CERT_NONE

    print(f"[Client] Initiating QUIC connection to Ingress Node -> {TARGET_HOST}:{TARGET_PORT}", flush=True)

    async with connect(
        TARGET_HOST,
        TARGET_PORT,
        configuration=configuration,
    ) as protocol:
        print("[Client] Handshake complete! Connection established.", flush=True)

        reader, writer = await protocol.create_stream()

        payload = b"DEFERRABLE_WORKLOAD"
        print(f"[Client] Sending payload: {payload.decode()}", flush=True)
        
        writer.write(payload)
        writer.write_eof()
        await writer.drain()

        # Read redirection response from server
        response = await reader.read(1024)
        print(f"[Client] Received response from server: {response.decode()}", flush=True)

        await asyncio.sleep(1)
        print("[Client] Workload submitted successfully.", flush=True)

if __name__ == "__main__":
    asyncio.run(run_client())
