#!/bin/bash
# Test kafka-console-consumer with a timeout

(sleep 10 && pkill -f "kafka-console-consumer") &
KILLER_PID=$!

kafka-console-consumer --bootstrap-server localhost:9092 --topic test --from-beginning 2>&1

kill $KILLER_PID 2>/dev/null
