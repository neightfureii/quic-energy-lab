import asyncio
from aioquic.asyncio import connect
from aioquic.quic.configuration import QuicConfiguration

async def run_client():
    configuration = QuicConfiguration(is_client=True)
    configuration.verify_mode = False  # Skip cert verification for self-signed lab certs

    initial_host = "dc-a"
    port = 4433

    print(f"[Client] Connecting to Ingress Node -> {initial_host}:{port}", flush=True)
    
    # 1. Connect to Ingress Node (dc-a)
    async with connect(initial_host, port, configuration=configuration) as protocol:
        print("[Client] QUIC Handshake successful with Ingress!", flush=True)
        
        reader, writer = await protocol.create_stream()
        
        payload = b"DEFERRABLE_WORKLOAD"
        print(f"[Client] Sending workload payload to ingress for routing check...", flush=True)
        
        writer.write(payload)
        writer.write_eof()

        # Read the routing directive
        response = await reader.read(1024)
        response_str = response.decode(errors="ignore")
        print(f"[Client] Received Control Directive: {response_str}", flush=True)

        if "REDIRECT:" in response_str:
            # Parse directive format: REDIRECT:dc-c:4433
            parts = response_str.split(":")
            target_host = parts[1]
            target_port = int(parts[2])
            
            print(f"[Client] Steering triggered! Migrating session to green zone: {target_host}:{target_port}", flush=True)
            
            # 2. Establish a new QUIC connection directly to the optimal green node (dc-c)
            async with connect(target_host, target_port, configuration=configuration) as green_protocol:
                print(f"[Client] QUIC Handshake successful with Green Node ({target_host})!", flush=True)
                
                g_reader, g_writer = await green_protocol.create_stream()
                
                final_payload = b"EXECUTE_DEFERRABLE_WORKLOAD"
                print(f"[Client] Transmitting actual payload to green data center...", flush=True)
                
                g_writer.write(final_payload)
                g_writer.write_eof()

                # Await final execution confirmation from dc-c
                ack = await g_reader.read(1024)
                print(f"[Client] Success! Received completion ACK from {target_host}: {ack.decode(errors='ignore')}", flush=True)

if __name__ == "__main__":
    asyncio.run(run_client())