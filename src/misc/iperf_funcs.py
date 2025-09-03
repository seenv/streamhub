def server(args, endpoint_name , uuid):
    """Starting the iPerf3 Server via globus worker"""
    
    futures, cmd = [], []
    for i in range (args.num_iperf):
        cmd.append(f"""
                    bash -c '
                    sleep 3 && setsid iperf3 -s -p {5074 + i} >> /tmp/iperf{i}.log 2>&1  &
                    '
                    """)
    
    with Executor(endpoint_id=uuid) as gce:
        for i in range(args.num_iperf):
            print(f"Starting iperf server: \n"
              f"    endpoint: {endpoint_name.capitalize()} \n"
              f"    Args: {args} \n")
            logging.info(f"START IPERF: Starting iperf server on endpoint ({endpoint_name.capitalize()}) with args: \n{args}")
            futures.append(gce.submit(ShellFunction(cmd[i], walltime=15)))
            
        for i, future in enumerate(futures):
            try:
                result = future.result(timeout=60)
                logging.debug(f"START IPERF server: {result.stdout}")
            except Exception as e:
                print(f"Task {i} failed: {e}")
                
                
                
def client(args, endpoint_name , uuid):
    """Starting the iPerf3 Client via globus worker"""
    
    futures, cmd = [], []
    for i in range (args.num_iperf):
        cmd.append(f"""
                    bash -c '
                    sleep 3 && setsid iperf3 -c 128.135.24.120 -p {5100 + i} -P 1 -t 60 >> /tmp/iperf{i}.log 2>&1  &
                    '
                    """)

    with Executor(endpoint_id=uuid) as gce:
        for i in range(args.num_iperf):
            print(f"Starting iperf server: \n"
              f"    endpoint: {endpoint_name.capitalize()} \n"
              f"    Args: {args} \n")
            logging.info(f"START IPERF: Starting iperf client on endpoint ({endpoint_name.capitalize()}) with args: \n{args}")
            futures.append(gce.submit(ShellFunction(cmd[i], walltime=15)))


        for i, future in enumerate(futures):
            try:
                result = future.result(timeout=60)
                logging.debug(f"START IPERF Client: {result.stdout}")
            except Exception as e:
                print(f"Task {i} failed: {e}")
                
