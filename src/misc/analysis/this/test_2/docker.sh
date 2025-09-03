#!/bin/bash

# Run DAQ container
sleep 5 && setsid docker run \
    --name daq \
    --network host \
    -v /tmp/mini-app:/output \
    seenv/ministream-daq:latest \
    bash -c "date +%Y-%m-%d_%H:%M:%S > /output/daq.log && \
             PYTHONUNBUFFERED=1 python /aps-mini-apps/build/python/streamer-daq/DAQStream.py \
             --mode 1 \
             --simulation_file /aps-mini-apps/data/tomo_00058_all_subsampled1p_s1079s1081.h5 \
             --d_iteration 1 \
             --publisher_addr tcp://*:50000 \
             --iteration_sleep 1 \
             --synch_addr tcp://*:50001 \
             --synch_count 1 2>&1 >> /output/daq.log " &

# Wait 1 second before starting the next container

# Run DIST container
sleep 6 && setsid docker run \
    --name dist \
    --network host \
    -v /tmp/mini-app:/output \
    seenv/ministream-dist:latest \
    bash -c "date +%Y-%m-%d_%H:%M:%S > /output/dist.log && \
             PYTHONUNBUFFERED=1 python /aps-mini-apps/build/python/streamer-dist/ModDistStreamPubDemo.py \
             --data_source_addr tcp://128.135.24.117:50000 \
             --data_source_synch_addr tcp://128.135.24.117:50001 \
             --cast_to_float32 \
             --normalize \
             --my_distributor_addr tcp://*:5074 \
             --beg_sinogram 1000 \
             --num_sinograms 2 \
             --num_columns 2560 2>&1 >> /output/dist.log" &
