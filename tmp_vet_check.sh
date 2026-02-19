#!/bin/bash
cd /Users/jhwang/works/test/datacore
v vet src/domain/replication.v > /tmp/vet_out.txt 2>&1
echo "EXIT: $?" >> /tmp/vet_out.txt
cat /tmp/vet_out.txt
