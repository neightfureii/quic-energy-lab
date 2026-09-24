#!/bin/bash

# Assigning network latency profiles based on the region
if [ "$REGION" = "dc-a" ]; then
    LATENCY="5ms"
    JITTER="1ms"
elif [ "$REGION" = "dc-b" ]; then
    LATENCY="30ms"
    JITTER="5ms"
elif [ "$REGION" = "dc-c" ]; then
    LATENCY="80ms"
    JITTER="10ms"
else
    LATENCY="10ms"
    JITTER="2ms"
fi

# Applying Linux Traffic Control (tc netem) rules to the default network interface
echo "[$REGION] Applying network emulation: delay $LATENCY ± $JITTER..."
tc qdisc add dev eth0 root netem delay $LATENCY $JITTER 2>/dev/null || \
tc qdisc change dev eth0 root netem delay $LATENCY $JITTER 2>/dev/null || \
echo "[$REGION] Warning: Could not apply tc netem (requires NET_ADMIN capability)."

# Starting the Python carbon-aware QUIC server
exec python server.py