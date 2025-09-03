#!/bin/bash

# Run SIRT container
sleep 6 && setsid docker run \
    --name sirt \
    --network host \
    -v /tmp/mini-app:/output \
    seenv/ministream-sirt:latest \
    bash -c "date +%Y-%m-%d_%H:%M:%S > /output/sirt.log && \
             /aps-mini-apps/build/bin/sirt_stream \
             --write-freq 4 \
             --dest-host 128.135.24.120 \
             --dest-port 5100 \
             --window-iter 1 \
             --window-step 4 \
             --window-length 4 \
             -t 2 \
             -c 1427 \
             --pub-addr tcp://*:52000 2>&1 >> /output/sirt.log " &
