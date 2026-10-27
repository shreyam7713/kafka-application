#!/usr/bin/env bash
set -euo pipefail
if [[ ! -f /var/lib/hackathon-kafka/data/meta.properties ]]; then
  /opt/hackathon-kafka/bin/kafka-storage.sh format \
    -t MkU3OEVBNTcwNTJENDM2Qk -c /etc/hackathon/server.properties
fi
exec /opt/hackathon-kafka/bin/kafka-server-start.sh /etc/hackathon/server.properties
