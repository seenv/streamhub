import logging

def daq(args, uuid):
    from globus_compute_sdk import Executor, ShellFunction
    import os, socket, time, datetime
    
    futures, cmd = [], []
    for i in range(args.num_mini):
        cmd.append(f"""
                    bash -c '
                    mkdir -p /tmp/mini-app/
                    setsid docker run \
                    --name daq{i} \
                    --network host \
                    -v /tmp/mini-app:/output \
                    seenv/ministream-daq:latest \
                    bash -c "date +%Y-%m-%d_%H:%M:%S > /output/daq{i}.log && PYTHONUNBUFFERED=1 python /aps-mini-apps/build/python/streamer-daq/DAQStream.py --mode 1 --simulation_file /aps-mini-apps/data/tomo_00058_all_subsampled1p_s1079s1081.h5 --d_iteration 1 --publisher_addr tcp://*:{50000+i*2} --iteration_sleep 1 --synch_addr tcp://*:{50001+i*2} --synch_count 1 2>&1 >> /output/daq{i}.log " &
                    '
                    """)
                    #bash -c "python /aps-mini-apps/build/python/streamer-daq/DAQStream.py --mode 1 --simulation_file /aps-mini-apps/data/tomo_00058_all_subsampled1p_s1079s1081.h5 --d_iteration 1 --publisher_addr tcp://*:50000 --iteration_sleep 1 --synch_addr tcp://*:50001 --synch_count 1 > /output/daq.log " &
                    #TODO: Get the ip and ports from the logs

                    # --synch_count 1 2>&1 | while read line; do echo [$(date '+%Y-%m-%d %H:%M:%S')] $line; done > /output/daq{i}.log " &
                    # --synch_count 1 2>&1 | ts '[%Y-%m-%d %H:%M:%S]' > /output/daq{i}.log " &              #moreutils is installed inside the Docker container


    with Executor(endpoint_id=uuid) as gce:
        for i in range(args.num_mini):
            futures.append(gce.submit(ShellFunction(cmd[i], walltime=15)))

        for i, future in enumerate(futures):
            try:
                result = future.result(timeout=60)
                print(f"stdout{i}: \n{result.stdout}", flush=True)
                cln_stderr = "\n".join(line for line in result.stderr.split("\n") if "WARNING" not in line)
                if cln_stderr.strip():
                    print(f"Stderr{i}: \n{cln_stderr}", flush=True)
            except Exception as e:
                print(f"Task {i} failed: {e}")



def dist(args, uuid):
    from globus_compute_sdk import Executor, ShellFunction
    import os, socket, time, datetime
    
    futures, cmd = [], []
    for i in range (args.num_mini):
        cmd.append(f"""
                    bash -c '
                    mkdir -p /tmp/mini-app/
                    sleep 3 && setsid docker run \
                    --name dist{i} \
                    --network host \
                    -v /tmp/mini-app:/output \
                    seenv/ministream-dist:latest \
                    bash -c "date +%Y-%m-%d_%H:%M:%S > /output/dist{i}.log && PYTHONUNBUFFERED=1 python /aps-mini-apps/build/python/streamer-dist/ModDistStreamPubDemo.py --data_source_addr tcp://{args.prod_ip}:{50000+i*2} --data_source_synch_addr tcp://{args.prod_ip}:{50001+i*2} --cast_to_float32 --normalize --my_distributor_addr tcp://*:{5074+i} --beg_sinogram 1000 --num_sinograms 2 --num_columns 2560 2>&1 >> /output/dist{i}.log" &
                    '
                    """)
                    #bash -c "python /aps-mini-apps/build/python/streamer-dist/ModDistStreamPubDemo.py --data_source_addr tcp://128.135.24.117:50000 --data_source_synch_addr tcp://128.135.24.117:50001 --cast_to_float32 --normalize --my_distributor_addr tcp://*:5074 --beg_sinogram 1000 --num_sinograms 2 --num_columns 2560 > /output/dist.log" &
                    #TODO: Get the ip and ports from the logs
    with Executor(endpoint_id=uuid) as gce:
        for i in range(args.num_mini):
            futures.append(gce.submit(ShellFunction(cmd[i], walltime=15)))

        for i, future in enumerate(futures):
            try:
                result = future.result(timeout=60)
                print(f"stdout{i}: \n{result.stdout}", flush=True)
                cln_stderr = "\n".join(line for line in result.stderr.split("\n") if "WARNING" not in line)
                if cln_stderr.strip():
                    print(f"Stderr{i}: \n{cln_stderr}", flush=True)
            except Exception as e:
                print(f"Task {i} failed: {e}")



def sirt(args, uuid):
    from globus_compute_sdk import Executor, ShellFunction
    import os, socket, time, datetime

    futures, cmd = [], []
    for i in range(args.num_mini):
        cmd.append(f"""
                    bash -c '
                    mkdir -p /tmp/mini-app/
                    sleep 10 && setsid docker run \
                    --name sirt{i} \
                    --network host \
                    -v /tmp/mini-app:/output \
                    seenv/ministream-sirt:latest \
                    bash -c "date +%Y-%m-%d_%H:%M:%S > /output/sirt{i}.log && /aps-mini-apps/build/bin/sirt_stream --write-freq 4 --dest-host {args.c2cs_listener} --dest-port {5100+i} --window-iter 1 --window-step 4 --window-length 4 -t 2 -c 1427 --pub-addr tcp://*:{52000+i} 2>&1 >> /output/sirt{i}.log " &
                    '
                    """)
                    #bash -c " /aps-mini-apps/build/bin/sirt_stream --write-freq 4 --dest-host 128.135.24.120 --dest-port 5100 --window-iter 1 --window-step 4 --window-length 4 -t 2 -c 1427 --pub-addr tcp://*:52000 | tee /output/sirt.log " &
                    #TODO: get the ip and ports from the logs
    
    with Executor(endpoint_id=uuid) as gce:
        for i in range(args.num_mini):
            futures.append(gce.submit(ShellFunction(cmd[i], walltime=15)))

        for i, future in enumerate(futures):
            try:
                result = future.result(timeout=60)
                print(f"stdout{i}: \n{result.stdout}", flush=True)
                cln_stderr = "\n".join(line for line in result.stderr.split("\n") if "WARNING" not in line)
                if cln_stderr.strip():
                    print(f"Stderr{i}: \n{cln_stderr}", flush=True)
            except Exception as e:
                print(f"Task {i} failed: {e}")




"""
docker run             --name daq5             --network host             -v /tmp/mini-app:/output             seenv/ministream-daq:latest             bash -c "python /aps-mini-apps/build/python/streamer-daq/DAQStream.py --mode 1 --simulation_file /aps-mini-apps/data/tomo_00058_all_subsampled1p_s1079s1081.h5 --d_iteration 1 --publisher_addr tcp://*:50008 --iteration_sleep 1 --synch_addr tcp://*:50009 --synch_count 1 > /output/daq5.log " &
docker run             --name daq4             --network host             -v /tmp/mini-app:/output             seenv/ministream-daq:latest             bash -c "python /aps-mini-apps/build/python/streamer-daq/DAQStream.py --mode 1 --simulation_file /aps-mini-apps/data/tomo_00058_all_subsampled1p_s1079s1081.h5 --d_iteration 1 --publisher_addr tcp://*:50006 --iteration_sleep 1 --synch_addr tcp://*:50007 --synch_count 1 > /output/daq4.log " &
docker run             --name daq3             --network host             -v /tmp/mini-app:/output             seenv/ministream-daq:latest             bash -c "python /aps-mini-apps/build/python/streamer-daq/DAQStream.py --mode 1 --simulation_file /aps-mini-apps/data/tomo_00058_all_subsampled1p_s1079s1081.h5 --d_iteration 1 --publisher_addr tcp://*:50004 --iteration_sleep 1 --synch_addr tcp://*:50005 --synch_count 1 > /output/daq3.log " &
docker run             --name daq2             --network host             -v /tmp/mini-app:/output             seenv/ministream-daq:latest             bash -c "python /aps-mini-apps/build/python/streamer-daq/DAQStream.py --mode 1 --simulation_file /aps-mini-apps/data/tomo_00058_all_subsampled1p_s1079s1081.h5 --d_iteration 1 --publisher_addr tcp://*:50002 --iteration_sleep 1 --synch_addr tcp://*:50003 --synch_count 1 > /output/daq2.log " &
docker run             --name daq1             --network host             -v /tmp/mini-app:/output             seenv/ministream-daq:latest             bash -c "python /aps-mini-apps/build/python/streamer-daq/DAQStream.py --mode 1 --simulation_file /aps-mini-apps/data/tomo_00058_all_subsampled1p_s1079s1081.h5 --d_iteration 1 --publisher_addr tcp://*:50000 --iteration_sleep 1 --synch_addr tcp://*:50001 --synch_count 1 > /output/daq1.log " &

docker run             --name dist5             --network host             -v /tmp/mini-app:/output             seenv/ministream-dist:latest             bash -c "python /aps-mini-apps/build/python/streamer-dist/ModDistStreamPubDemo.py --data_source_addr tcp://128.135.24.117:50008 --data_source_synch_addr tcp://128.135.24.117:50009 --cast_to_float32 --normalize --my_distributor_addr tcp://*:47000 --beg_sinogram 1000 --num_sinograms 2 --num_columns 2560 > /output/dist5.log" &
docker run             --name dist4             --network host             -v /tmp/mini-app:/output             seenv/ministream-dist:latest             bash -c "python /aps-mini-apps/build/python/streamer-dist/ModDistStreamPubDemo.py --data_source_addr tcp://128.135.24.117:50006 --data_source_synch_addr tcp://128.135.24.117:50007 --cast_to_float32 --normalize --my_distributor_addr tcp://*:37000 --beg_sinogram 1000 --num_sinograms 2 --num_columns 2560 > /output/dist4.log" &
docker run             --name dist3             --network host             -v /tmp/mini-app:/output             seenv/ministream-dist:latest             bash -c "python /aps-mini-apps/build/python/streamer-dist/ModDistStreamPubDemo.py --data_source_addr tcp://128.135.24.117:50004 --data_source_synch_addr tcp://128.135.24.117:50005 --cast_to_float32 --normalize --my_distributor_addr tcp://*:5076 --beg_sinogram 1000 --num_sinograms 2 --num_columns 2560 > /output/dist3.log" &
docker run             --name dist2             --network host             -v /tmp/mini-app:/output             seenv/ministream-dist:latest             bash -c "python /aps-mini-apps/build/python/streamer-dist/ModDistStreamPubDemo.py --data_source_addr tcp://128.135.24.117:50002 --data_source_synch_addr tcp://128.135.24.117:50003 --cast_to_float32 --normalize --my_distributor_addr tcp://*:5075 --beg_sinogram 1000 --num_sinograms 2 --num_columns 2560 > /output/dist2.log" &
docker run             --name dist1             --network host             -v /tmp/mini-app:/output             seenv/ministream-dist:latest             bash -c "python /aps-mini-apps/build/python/streamer-dist/ModDistStreamPubDemo.py --data_source_addr tcp://128.135.24.117:50000 --data_source_synch_addr tcp://128.135.24.117:50001 --cast_to_float32 --normalize --my_distributor_addr tcp://*:5074 --beg_sinogram 1000 --num_sinograms 2 --num_columns 2560 > /output/dist1.log" &

docker run             --name sirt5             --network host             -v /tmp/mini-app:/output             seenv/ministream-sirt:latest             bash -c " /aps-mini-apps/build/bin/sirt_stream --write-freq 4 --dest-host 128.135.164.120 --dest-port 5104 --window-iter 1 --window-step 4 --window-length 4 -t 2 -c 1427 --pub-addr tcp://*:52004 > /output/sirt5.log " &
docker run             --name sirt4             --network host             -v /tmp/mini-app:/output             seenv/ministream-sirt:latest             bash -c " /aps-mini-apps/build/bin/sirt_stream --write-freq 4 --dest-host 128.135.164.120 --dest-port 5103 --window-iter 1 --window-step 4 --window-length 4 -t 2 -c 1427 --pub-addr tcp://*:52003 > /output/sirt5.log " &
docker run             --name sirt3             --network host             -v /tmp/mini-app:/output             seenv/ministream-sirt:latest             bash -c " /aps-mini-apps/build/bin/sirt_stream --write-freq 4 --dest-host 128.135.164.120 --dest-port 5102 --window-iter 1 --window-step 4 --window-length 4 -t 2 -c 1427 --pub-addr tcp://*:52002 > /output/sirt5.log " &
docker run             --name sirt2             --network host             -v /tmp/mini-app:/output             seenv/ministream-sirt:latest             bash -c " /aps-mini-apps/build/bin/sirt_stream --write-freq 4 --dest-host 128.135.164.120 --dest-port 5101 --window-iter 1 --window-step 4 --window-length 4 -t 2 -c 1427 --pub-addr tcp://*:52001 > /output/sirt5.log " &
docker run             --name sirt1             --network host             -v /tmp/mini-app:/output             seenv/ministream-sirt:latest             bash -c " /aps-mini-apps/build/bin/sirt_stream --write-freq 4 --dest-host 128.135.164.120 --dest-port 5100 --window-iter 1 --window-step 4 --window-length 4 -t 2 -c 1427 --pub-addr tcp://*:52000 > /output/sirt5.log " &
"""
